# Databricks notebook source
CATALOG = "transit"
EDGES = f"{CATALOG}.silver.street_edges"
ROUTE_STREETS = f"{CATALOG}.silver.route_streets"
TRIP_SHAPE = f"{CATALOG}.silver.trip_shape"
STATIC_PATH = "abfss://raw@sttransitlake.dfs.core.windows.net/rome/static_gtfs/"

HISTORY_DAYS = 90 # Keep this many days of static GTFS versions in the tables

MAX_SNAP_M = 25 # Maximum distance to snap a shape point to a street edge
MIN_SPAN_M = 30 # Minimum distance that a route must run along a street to be considered a match
GRID_DEG = 0.00025 # Approximate degrees of latitude/longitude per grid cell (about 28 m x 21 m)
CELL_STEP_M = 10 # Step when walking along an edge to find the grid cells it crosses
SHAPE_STEP_M = 5 # Step between the points a route line is resampled into

# COMMAND ----------
# GTFS parsers and geometry

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

M_PER_LAT = 111320.0 # Approximate metres of latitude per degree
M_PER_LON = 82800.0 # Approximate metres of longitude per degree at Rome's latitude

POINT = StructType(
    [
        StructField("shape_id", StringType()),
        StructField("seq", IntegerType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
    ]
)

TRIP = StructType(
    [StructField("trip_id", StringType()), StructField("shape_id", StringType())]
)


def read_gtfs_table(payload, name):
    """Reads one CSV file from the static GTFS zip, row by row."""
    import csv
    import io
    import zipfile

    with zipfile.ZipFile(io.BytesIO(payload)) as archive, archive.open(name) as file:
        yield from csv.DictReader(io.TextIOWrapper(file, encoding="utf-8-sig"))


@F.udf(returnType=ArrayType(POINT))
def parse_shapes(payload):
    """Parses shapes.txt into the points of every route line on the map."""
    return [
        (
            row["shape_id"],
            int(row["shape_pt_sequence"]),
            float(row["shape_pt_lat"]),
            float(row["shape_pt_lon"]),
        )
        for row in read_gtfs_table(payload, "shapes.txt")
    ]


@F.udf(returnType=ArrayType(TRIP))
def parse_trips(payload):
    """Parses trips.txt into the route line of every trip."""
    return [
        (row["trip_id"], row["shape_id"])
        for row in read_gtfs_table(payload, "trips.txt")
        if row.get("shape_id")
    ]


def grid_cell(coord):
    return F.floor(coord / GRID_DEG).cast("int")


def distance_m(lat1, lon1, lat2, lon2):
    """Returns the distance in metres between two nearby points, on a flat map."""
    dx = (lon2 - lon1) * M_PER_LON
    dy = (lat2 - lat1) * M_PER_LAT
    return F.sqrt(dx * dx + dy * dy)


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
# Parse static GTFS zips

done = []
if spark.catalog.tableExists(ROUTE_STREETS):
    done = [
        row.static_date
        for row in spark.table(ROUTE_STREETS).select("static_date").distinct().collect()
    ]

new_zips = (
    spark.read.format("binaryFile")
    .load(STATIC_PATH)
    .withColumnRenamed("date", "static_date")
    .where(F.col("static_date") >= F.date_sub(F.current_date(), HISTORY_DAYS))
    .where(~F.col("static_date").isin(done))
)

trips = new_zips.select(
    "static_date", F.explode(parse_trips("content")).alias("trip")
).select("static_date", "trip.*")
trips.write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy(
    "static_date"
).saveAsTable(TRIP_SHAPE)

shape = Window.partitionBy("static_date", "shape_id").orderBy("seq")
next_lat, next_lon = F.lead("lat").over(shape), F.lead("lon").over(shape)
gap_m = distance_m(F.col("lat"), F.col("lon"), F.col("next_lat"), F.col("next_lon"))
steps = F.greatest(F.ceil(F.col("gap_m") / SHAPE_STEP_M), F.lit(1)).cast("int")
along = F.col("step") / F.col("steps")

shape_points = (
    new_zips.select("static_date", F.explode(parse_shapes("content")).alias("point"))
    .select("static_date", "point.*")
    .withColumn("next_lat", next_lat)
    .withColumn("next_lon", next_lon)
    .where(F.col("next_lat").isNotNull())
    .withColumn("gap_m", gap_m)
    .withColumn("steps", steps)
    .withColumn("step", F.explode(F.sequence(F.lit(0), F.col("steps") - 1)))
    .withColumn("piece_m", F.col("gap_m") / F.col("steps"))
    .withColumn("lat", F.col("lat") + along * (F.col("next_lat") - F.col("lat")))
    .withColumn("lon", F.col("lon") + along * (F.col("next_lon") - F.col("lon")))
    .drop("next_lat", "next_lon", "gap_m", "steps")
    .withColumn("cell_lat", grid_cell(F.col("lat")))
    .withColumn("cell_lon", grid_cell(F.col("lon")))
)

# COMMAND ----------
# Snap shape points to edges

offsets = F.array(*[F.lit(offset) for offset in (-1, 0, 1)])

point_cells = (
    shape_points.withColumn("dlat", F.explode(offsets))
    .withColumn("dlon", F.explode(offsets))
    .withColumn("cell_lat", F.col("cell_lat") + F.col("dlat"))
    .withColumn("cell_lon", F.col("cell_lon") + F.col("dlon"))
    .drop("dlat", "dlon")
)

edges = edge_cells(
    spark.table(EDGES).select(
        "way_id", "from_lat", "from_lon", "to_lat", "to_lon", "length_m"
    )
)

candidates = point_cells.join(edges, ["cell_lat", "cell_lon"]).withColumn(
    "snap_m",
    point_to_edge_m(
        F.col("lat"), F.col("lon"),
        F.col("from_lat"), F.col("from_lon"),
        F.col("to_lat"), F.col("to_lon"),
    ),
)

nearest = Window.partitionBy("static_date", "shape_id", "seq", "step").orderBy("snap_m")
matched = (
    candidates.withColumn("rank", F.row_number().over(nearest))
    .where((F.col("rank") == 1) & (F.col("snap_m") <= MAX_SNAP_M))
)

# COMMAND ----------
# Match routes to streets

street_length = (
    spark.table(EDGES).groupBy("way_id").agg(F.sum("length_m").alias("street_m"))
)

route_streets = (
    matched.groupBy("static_date", "shape_id", "way_id")
    .agg(
        F.count("*").alias("points"),
        F.round(F.avg("snap_m"), 1).alias("snap_m"),
        F.round(F.sum("piece_m"), 1).alias("span_m"),
    )
    .join(street_length, "way_id")
    .where(F.col("span_m") >= F.least(F.lit(float(MIN_SPAN_M)), F.col("street_m")))
    .select(
        "static_date", "shape_id", "way_id", "points", "snap_m", "street_m", "span_m"
    )
)

route_streets.write.mode("overwrite").option(
    "partitionOverwriteMode", "dynamic"
).partitionBy("static_date").saveAsTable(ROUTE_STREETS)

# COMMAND ----------
# Drop versions outside the history window

cutoff = f"date_sub(current_date(), {HISTORY_DAYS})"
for table in (TRIP_SHAPE, ROUTE_STREETS):
    spark.sql(f"DELETE FROM {table} WHERE static_date < {cutoff}")
