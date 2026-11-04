# Databricks notebook source
dbutils.widgets.text("catalog", "transit_dev")
dbutils.widgets.text("full_refresh", "false")
CATALOG = dbutils.widgets.get("catalog")
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
SEGMENTS = f"{CATALOG}.silver.vehicle_segments"
EDGES = f"{CATALOG}.silver.street_edges"
SHAPE_STREETS = f"{CATALOG}.silver.shape_streets"
TRIP_SHAPE = f"{CATALOG}.silver.trip_shape"
TABLE = f"{CATALOG}.silver.segment_streets"

HISTORY_DAYS = 90 # Keep this many days of history in the table

MAX_SNAP_M = 25 # Maximum distance to snap a segment to a street edge
GRID_DEG = 0.00025 # Approximate degrees of latitude/longitude per grid cell (about 28 m x 21 m)
CELL_STEP_M = 10 # Step when walking along an edge to find the grid cells it crosses

# COMMAND ----------
# Days to compute

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import Window
from pyspark.sql import functions as F

TODAY = datetime.now(ZoneInfo("Europe/Rome")).date()
EARLIEST = TODAY - timedelta(days=HISTORY_DAYS)
REBUILD = FULL_REFRESH or not spark.catalog.tableExists(TABLE)


def days_to_process(table):
    """Returns the range of new complete days not yet written to the table, or None if there are none."""
    start = EARLIEST
    if not REBUILD:
        latest = spark.table(table).agg(F.max("date_rome")).first()[0]
        if latest:
            start = max(latest + timedelta(days=1), EARLIEST)
    last_version = spark.table(TRIP_SHAPE).agg(F.max("static_date")).first()[0]
    end = min(TODAY - timedelta(days=1), last_version - timedelta(days=1))
    return (start, end) if start <= end else None


DAYS = days_to_process(TABLE)
if not DAYS:
    dbutils.notebook.exit("up to date")
START, END = DAYS
print(f"computing {START} to {END}")

# COMMAND ----------
# Grid and geometry

M_PER_LAT = 111320.0 # Approximate metres of latitude per degree
M_PER_LON = 82800.0 # Approximate metres of longitude per degree at Rome's latitude


def grid_cell(coord):
    return F.floor(coord / GRID_DEG).cast("int")


def edge_cells(edges):
    """Repeats each edge for every grid cell it passes through."""
    steps = F.greatest(F.ceil(F.col("length_m") / CELL_STEP_M), F.lit(1)).cast("int")
    along = F.col("step") / steps
    lat = F.col("from_lat") + along * (F.col("to_lat") - F.col("from_lat"))
    lon = F.col("from_lon") + along * (F.col("to_lon") - F.col("from_lon"))
    return (
        edges.withColumn("step", F.explode(F.sequence(F.lit(0), steps)))
        .withColumn("cell_lat", grid_cell(lat))
        .withColumn("cell_lon", grid_cell(lon))
        .drop("step")
        .dropDuplicates([*edges.columns, "cell_lat", "cell_lon"])
    )


def point_to_edge_m(lat, lon, from_lat, from_lon, to_lat, to_lon):
    """Returns the distance in metres from a point to the closest spot on an edge."""
    x, y = lon * M_PER_LON, lat * M_PER_LAT
    x1, y1 = from_lon * M_PER_LON, from_lat * M_PER_LAT
    x2, y2 = to_lon * M_PER_LON, to_lat * M_PER_LAT
    dx, dy = x2 - x1, y2 - y1
    length_sq = dx * dx + dy * dy
    along = F.when(
        length_sq > 0, ((x - x1) * dx + (y - y1) * dy) / length_sq
    ).otherwise(F.lit(0.0))
    along = F.least(F.greatest(along, F.lit(0.0)), F.lit(1.0))
    closest_x, closest_y = x1 + along * dx, y1 + along * dy
    return F.sqrt((x - closest_x) * (x - closest_x) + (y - closest_y) * (y - closest_y))

# COMMAND ----------
# Snap segments to the streets of their route

segments = spark.table(SEGMENTS).where(F.col("date_rome").between(START, END))

versions = spark.table(TRIP_SHAPE).select("static_date").distinct()
oldest_version = versions.agg(F.min("static_date")).first()[0]
version_for_day = (
    segments.select("date_rome")
    .distinct()
    .join(versions, F.col("static_date") <= F.col("date_rome"), "left")
    .groupBy("date_rome")
    .agg(F.max("static_date").alias("static_date"))
    .withColumn("static_date", F.coalesce("static_date", F.lit(oldest_version)))
)

next_version = (
    segments.select("date_rome")
    .distinct()
    .join(versions, F.col("static_date") > F.col("date_rome"))
    .groupBy("date_rome")
    .agg(F.min("static_date").alias("static_date"))
)

day_versions = version_for_day.withColumn("priority", F.lit(1)).unionByName(
    next_version.withColumn("priority", F.lit(2))
)
first_found = Window.partitionBy("date_rome", "trip_id").orderBy("priority")
trip_hashes = (
    day_versions.join(spark.table(TRIP_SHAPE), "static_date")
    .withColumn("rank", F.row_number().over(first_found))
    .where(F.col("rank") == 1)
    .select("date_rome", "trip_id", "shape_hash")
)

route_edges = edge_cells(
    spark.table(SHAPE_STREETS)
    .select("shape_hash", "way_id")
    .join(trip_hashes.select("shape_hash").distinct(), "shape_hash")
    .join(spark.table(EDGES), "way_id")
)

offsets = F.array(*[F.lit(offset) for offset in (-1, 0, 1)])

segment_cells = (
    segments.join(trip_hashes, ["date_rome", "trip_id"])
    .withColumn("dow", F.expr("weekday(date_rome)"))
    .withColumn("mid_lat", (F.col("from_lat") + F.col("to_lat")) / 2)
    .withColumn("mid_lon", (F.col("from_lon") + F.col("to_lon")) / 2)
    .withColumn("cell_lat", grid_cell(F.col("mid_lat")))
    .withColumn("cell_lon", grid_cell(F.col("mid_lon")))
    .withColumn("dlat", F.explode(offsets))
    .withColumn("dlon", F.explode(offsets))
    .withColumn("cell_lat", F.col("cell_lat") + F.col("dlat"))
    .withColumn("cell_lon", F.col("cell_lon") + F.col("dlon"))
    .drop("dlat", "dlon", "from_lat", "from_lon", "to_lat", "to_lon")
)

candidates = segment_cells.join(
    route_edges, ["shape_hash", "cell_lat", "cell_lon"]
).withColumn(
    "snap_m",
    point_to_edge_m(
        F.col("mid_lat"), F.col("mid_lon"),
        F.col("from_lat"), F.col("from_lon"),
        F.col("to_lat"), F.col("to_lon"),
    ),
)

nearest = Window.partitionBy("vehicle_id", "to_ts").orderBy("snap_m", "way_id")
snapped = candidates.withColumn("rank", F.row_number().over(nearest)).where(
    (F.col("rank") == 1) & (F.col("snap_m") <= MAX_SNAP_M)
)

# COMMAND ----------
# Write table

segment_streets = snapped.select(
    "date_rome",
    "dow",
    "hour_rome",
    "vehicle_id",
    "route_id",
    "way_id",
    "street",
    "highway",
    "speed_kmh",
    "snap_m",
)

writer = segment_streets.write.mode("overwrite").partitionBy("date_rome")
if REBUILD:
    writer.option("overwriteSchema", "true").saveAsTable(TABLE)
else:
    writer.option(
        "replaceWhere", f"date_rome BETWEEN '{START}' AND '{END}'"
    ).saveAsTable(TABLE)

# COMMAND ----------
# Drop days outside the history window

spark.sql(f"DELETE FROM {TABLE} WHERE date_rome < '{EARLIEST}'")
