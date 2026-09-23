# Databricks notebook source
CATALOG = "transit"
SEGMENTS = f"{CATALOG}.silver.vehicle_segments"
EDGES = f"{CATALOG}.silver.street_edges"
ROUTE_STREETS = f"{CATALOG}.silver.route_streets"
TRIP_SHAPE = f"{CATALOG}.silver.trip_shape"
TABLE = f"{CATALOG}.gold.street_speed"

MIN_SEGMENTS = 10
MAX_SNAP_M = 50
LOOKUP_DEG = 0.00025

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F

LAT0 = 41.9
M_PER_LAT = 111320.0
M_PER_LON = 82800.0


def cell(coord):
    return F.floor(coord / LOOKUP_DEG).cast("int")


def point_to_edge_m(plat, plon, alat, alon, blat, blon):
    """Distance to the edge itself, not to its ends."""
    px, py = plon * M_PER_LON, plat * M_PER_LAT
    ax, ay = alon * M_PER_LON, alat * M_PER_LAT
    bx, by = blon * M_PER_LON, blat * M_PER_LAT
    dx, dy = bx - ax, by - ay
    span = dx * dx + dy * dy
    raw = F.when(
        span > 0, ((px - ax) * dx + (py - ay) * dy) / span
    ).otherwise(F.lit(0.0))
    t = F.least(F.greatest(raw, F.lit(0.0)), F.lit(1.0))
    cx, cy = ax + t * dx, ay + t * dy
    return F.sqrt((px - cx) * (px - cx) + (py - cy) * (py - cy))

# COMMAND ----------
route_edges = (
    spark.table(ROUTE_STREETS)
    .select("shape_id", "way_id")
    .join(spark.table(EDGES), "way_id")
    .withColumn("cell_lat", cell(F.col("from_lat")))
    .withColumn("cell_lon", cell(F.col("from_lon")))
)

around = F.array(*[F.lit(d) for d in (-1, 0, 1)])

measured = (
    spark.table(SEGMENTS)
    .join(spark.table(TRIP_SHAPE), "trip_id")
    .withColumn("dow", F.expr("weekday(date_rome)"))
    .withColumn("mid_lat", (F.col("from_lat") + F.col("to_lat")) / 2)
    .withColumn("mid_lon", (F.col("from_lon") + F.col("to_lon")) / 2)
    .withColumn("cell_lat", cell(F.col("mid_lat")))
    .withColumn("cell_lon", cell(F.col("mid_lon")))
    .withColumn("dlat", F.explode(around))
    .withColumn("dlon", F.explode(around))
    .withColumn("cell_lat", F.col("cell_lat") + F.col("dlat"))
    .withColumn("cell_lon", F.col("cell_lon") + F.col("dlon"))
    .drop("dlat", "dlon", "from_lat", "from_lon", "to_lat", "to_lon")
)

candidates = measured.join(
    route_edges, ["shape_id", "cell_lat", "cell_lon"]
).withColumn(
    "snap_m",
    point_to_edge_m(
        F.col("mid_lat"), F.col("mid_lon"),
        F.col("from_lat"), F.col("from_lon"),
        F.col("to_lat"), F.col("to_lon"),
    ),
)

nearest = Window.partitionBy("vehicle_id", "ts").orderBy("snap_m")
snapped = candidates.withColumn("rn", F.row_number().over(nearest)).where(
    (F.col("rn") == 1) & (F.col("snap_m") <= MAX_SNAP_M)
)

# COMMAND ----------
street = (
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

street.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)

# COMMAND ----------
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
        F.sum(F.when(cond, 1).otherwise(0)).alias(name)
        for name, cond in CHECKS.items()
    ]
).first()
duplicate_keys = table.groupBy(*KEY).count().where(F.col("count") > 1).count()

failed = {k: v for k, v in counts.asDict().items() if v}
if duplicate_keys:
    failed["duplicate_keys"] = duplicate_keys
if failed:
    raise AssertionError(f"street_speed failed checks: {failed}")
print("all checks passed")

# COMMAND ----------
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.gold.exports")
EXPORTS = f"/Volumes/{CATALOG}/gold/exports"

drawn = table.select("way_id").distinct()
geometry = (
    spark.table(EDGES)
    .join(drawn, "way_id")
    .groupBy("way_id")
    .agg(
        F.array_sort(
            F.collect_list(F.struct("seq", "from_lon", "from_lat", "to_lon", "to_lat"))
        ).alias("ordered")
    )
    .withColumn(
        "path",
        F.concat(
            F.transform("ordered", lambda e: F.array(e.from_lon, e.from_lat)),
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

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT street, hour_rome, median_speed_kmh, segments, vehicles, routes
# MAGIC FROM transit.gold.street_speed
# MAGIC WHERE dow = 0 AND hour_rome = 8 AND segments >= 50
# MAGIC ORDER BY median_speed_kmh LIMIT 20
