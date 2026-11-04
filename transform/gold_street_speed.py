# Databricks notebook source
dbutils.widgets.text("catalog", "transit_dev")
dbutils.widgets.text("serving", "abfss://serving@sttransitlake.dfs.core.windows.net/dev/rome")
CATALOG = dbutils.widgets.get("catalog")
SERVING = dbutils.widgets.get("serving")
SEGMENT_STREETS = f"{CATALOG}.silver.segment_streets"
EDGES = f"{CATALOG}.silver.street_edges"
TABLE = f"{CATALOG}.gold.street_speed"
STAGING = f"{CATALOG}.gold.street_speed_staging"

MIN_SEGMENTS = 20 # Minimum segments on a street in a weekday hour to publish its speed
MIN_DAY_SHARE = 0.5 # Yesterday needs at least this share of a usual day's segments
BASELINE_DAYS = 7 # Days before yesterday that set a usual day
MIN_STREET_SHARE = 0.9 # The new table needs at least this share of the published streets

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# COMMAND ----------
# Speed per street, weekday and hour

from datetime import datetime, timedelta
from statistics import median
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

street_speed = (
    spark.table(SEGMENT_STREETS)
    .groupBy("way_id", "street", "highway", "dow", "hour_rome")
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
    STAGING
)

# COMMAND ----------
# Quality checks

yesterday = datetime.now(ZoneInfo("Europe/Rome")).date() - timedelta(days=1)
failed = {}

# Yesterday has enough speed measurements compared with a usual day
daily = {
    row.date_rome: row["count"]
    for row in spark.table(SEGMENT_STREETS)
    .where(F.col("date_rome").between(yesterday - timedelta(days=BASELINE_DAYS), yesterday))
    .groupBy("date_rome")
    .count()
    .collect()
}
baseline = [count for day, count in daily.items() if day < yesterday]
if baseline and daily.get(yesterday, 0) < MIN_DAY_SHARE * median(baseline):
    failed["yesterday_missing"] = f"{daily.get(yesterday, 0)} segments, usual {median(baseline)}"

# The new table has not lost too many streets compared with the published one
if spark.catalog.tableExists(TABLE):
    published = spark.table(TABLE).select("way_id").distinct().count()
    streets = spark.table(STAGING).select("way_id").distinct().count()
    if streets < MIN_STREET_SHARE * published:
        failed["streets_dropped"] = f"{streets} streets, published {published}"

if failed:
    raise AssertionError(f"street_speed failed checks: {failed}")
print("all checks passed")

# COMMAND ----------
# Publish

spark.table(STAGING).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABLE)
table = spark.table(TABLE)

# COMMAND ----------
# Export for the map

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
