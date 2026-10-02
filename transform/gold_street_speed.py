# Databricks notebook source
CATALOG = "transit"
SEGMENTS = f"{CATALOG}.silver.vehicle_segments"
EDGES = f"{CATALOG}.silver.street_edges"
ROUTE_STREETS = f"{CATALOG}.silver.route_streets"
TRIP_SHAPE = f"{CATALOG}.silver.trip_shape"
TABLE = f"{CATALOG}.gold.street_speed"

MIN_SEGMENTS = 20 # Minimum segments on a street in a weekday hour to publish its speed
MAX_SNAP_M = 25 # Maximum distance to snap a segment to a street edge
GRID_DEG = 0.00025 # Approximate degrees of latitude/longitude per grid cell (about 28 m x 21 m)
CELL_STEP_M = 10 # Step when walking along an edge to find the grid cells it crosses

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# COMMAND ----------
# Grid and geometry

from pyspark.sql import Window
from pyspark.sql import functions as F

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

versions = spark.table(TRIP_SHAPE).select("static_date").distinct()
oldest_version = versions.agg(F.min("static_date")).first()[0]
version_for_day = (
    spark.table(SEGMENTS)
    .select("date_rome")
    .distinct()
    .join(versions, F.col("static_date") <= F.col("date_rome"), "left")
    .groupBy("date_rome")
    .agg(F.max("static_date").alias("static_date"))
    .withColumn("static_date", F.coalesce("static_date", F.lit(oldest_version)))
)

route_edges = edge_cells(
    spark.table(ROUTE_STREETS)
    .select("static_date", "shape_id", "way_id")
    .join(spark.table(EDGES), "way_id")
)

offsets = F.array(*[F.lit(offset) for offset in (-1, 0, 1)])

segment_cells = (
    spark.table(SEGMENTS)
    .join(version_for_day, "date_rome")
    .join(spark.table(TRIP_SHAPE), ["static_date", "trip_id"])
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
    route_edges, ["static_date", "shape_id", "cell_lat", "cell_lon"]
).withColumn(
    "snap_m",
    point_to_edge_m(
        F.col("mid_lat"), F.col("mid_lon"),
        F.col("from_lat"), F.col("from_lon"),
        F.col("to_lat"), F.col("to_lon"),
    ),
)

nearest = Window.partitionBy("vehicle_id", "to_ts").orderBy("snap_m")
snapped = candidates.withColumn("rank", F.row_number().over(nearest)).where(
    (F.col("rank") == 1) & (F.col("snap_m") <= MAX_SNAP_M)
)

# COMMAND ----------
# Speed per street, weekday and hour

street_speed = (
    snapped.groupBy("way_id", "street", "highway", "dow", "hour_rome")
    .agg(
        F.count("*").alias("segments"),
        F.countDistinct("vehicle_id").alias("vehicles"),
        F.countDistinct("route_id").alias("routes"),
        F.countDistinct("date_rome").alias("days"),
        F.round(F.avg("speed_kmh"), 1).alias("avg_speed_kmh"),
        F.round(F.percentile_approx("speed_kmh", 0.5), 1).alias("median_speed_kmh"),
        F.round(F.avg(F.when(F.col("speed_kmh") > 1, 1.0).otherwise(0.0)), 2).alias(
            "moving_share"
        ),
        F.round(F.avg("snap_m"), 1).alias("snap_m"),
    )
    .where(F.col("segments") >= MIN_SEGMENTS)
)

street_speed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    TABLE
)

# COMMAND ----------
# Quality checks

CHECKS = {
    "hour_out_of_range": ~F.col("hour_rome").between(0, 23),
    "dow_out_of_range": ~F.col("dow").between(0, 6),
    "negative_speed": F.col("median_speed_kmh") < 0,
    "share_out_of_range": ~F.col("moving_share").between(0, 1),
    "snapped_too_far": F.col("snap_m") > MAX_SNAP_M,
}
KEY = ["way_id", "dow", "hour_rome"]

table = spark.table(TABLE)
counts = table.select(
    *[
        F.sum(F.when(condition, 1).otherwise(0)).alias(name)
        for name, condition in CHECKS.items()
    ]
).first()
duplicate_keys = table.groupBy(*KEY).count().where(F.col("count") > 1).count()

failed = {name: count for name, count in counts.asDict().items() if count}
if duplicate_keys:
    failed["duplicate_keys"] = duplicate_keys
if failed:
    raise AssertionError(f"street_speed failed checks: {failed}")
print("all checks passed")

# COMMAND ----------
# Export for the map

SERVING = "abfss://serving@sttransitlake.dfs.core.windows.net/rome"
spark.sql(
    f"CREATE EXTERNAL VOLUME IF NOT EXISTS {CATALOG}.gold.exports LOCATION '{SERVING}'"
)
EXPORTS = f"/Volumes/{CATALOG}/gold/exports"

mapped_ways = table.select("way_id").distinct()
geometry = (
    spark.table(EDGES)
    .join(mapped_ways, "way_id")
    .groupBy("way_id")
    .agg(
        F.array_sort(
            F.collect_list(F.struct("seq", "from_lon", "from_lat", "to_lon", "to_lat"))
        ).alias("ordered")
    )
    .withColumn(
        "path",
        F.concat(
            F.transform("ordered", lambda edge: F.array(edge.from_lon, edge.from_lat)),
            F.array(
                F.array(
                    F.element_at("ordered", -1).to_lon,
                    F.element_at("ordered", -1).to_lat,
                )
            ),
        ),
    )
    .select("way_id", "path")
)

for name, frame in (("street_speed", table), ("street_geometry", geometry)):
    export = frame.toPandas()
    export.attrs = {}
    export.to_parquet(f"{EXPORTS}/{name}.parquet", index=False)
    print(f"{name}: {len(export):,} rows")
