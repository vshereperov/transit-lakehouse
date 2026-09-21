# Databricks notebook source
CATALOG = "transit"
SEGMENTS = f"{CATALOG}.silver.vehicle_segments"
TABLE = f"{CATALOG}.gold.speed_grid"

CELL_DEG = 0.005
MIN_SEGMENTS = 10

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# COMMAND ----------
from pyspark.sql import functions as F


def cell_centre(coord):
    return (F.floor(coord / CELL_DEG) + 0.5) * CELL_DEG


grid = (
    spark.table(SEGMENTS)
    .withColumn("mid_lat", (F.col("from_lat") + F.col("to_lat")) / 2)
    .withColumn("mid_lon", (F.col("from_lon") + F.col("to_lon")) / 2)
    .withColumn("cell_lat", cell_centre(F.col("mid_lat")))
    .withColumn("cell_lon", cell_centre(F.col("mid_lon")))
    .withColumn("dow", F.expr("weekday(date_rome)"))
    .groupBy("cell_lat", "cell_lon", "dow", "hour_rome")
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
    )
    .where(F.col("segments") >= MIN_SEGMENTS)
)

grid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)

# COMMAND ----------
CHECKS = {
    "hour_out_of_range": ~F.col("hour_rome").between(0, 23),
    "dow_out_of_range": ~F.col("dow").between(0, 6),
    "negative_speed": F.col("median_speed_kmh") < 0,
    "cell_outside_rome": ~(
        F.col("cell_lat").between(41.6, 42.2) & F.col("cell_lon").between(12.2, 12.9)
    ),
    "share_out_of_range": ~F.col("moving_share").between(0, 1),
}
KEY = ["cell_lat", "cell_lon", "dow", "hour_rome"]

grid_tbl = spark.table(TABLE)
counts = grid_tbl.select(
    *[
        F.sum(F.when(cond, 1).otherwise(0)).alias(name)
        for name, cond in CHECKS.items()
    ]
).first()

duplicate_keys = grid_tbl.groupBy(*KEY).count().where(F.col("count") > 1).count()

failed = {k: v for k, v in counts.asDict().items() if v}
if duplicate_keys:
    failed["duplicate_keys"] = duplicate_keys
if failed:
    raise AssertionError(f"speed_grid failed checks: {failed}")
print("all checks passed")

# COMMAND ----------
# The map reads this file
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.gold.exports")
EXPORT = f"/Volumes/{CATALOG}/gold/exports/speed_grid.parquet"

spark.table(TABLE).toPandas().to_parquet(EXPORT, index=False)
print(f"exported to {EXPORT}")

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT dow, hour_rome, count(*) AS cells, sum(segments) AS segments,
# MAGIC        round(avg(median_speed_kmh), 1) AS avg_of_median
# MAGIC FROM transit.gold.speed_grid
# MAGIC GROUP BY dow, hour_rome ORDER BY dow, hour_rome
