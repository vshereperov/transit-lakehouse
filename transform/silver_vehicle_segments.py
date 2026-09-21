# Databricks notebook source
CATALOG = "transit"
BRONZE = f"{CATALOG}.bronze.vehicle_positions"
TABLE = f"{CATALOG}.silver.vehicle_segments"

MAX_STALENESS_S = 120   # p99 of the lag is 84 s, only 0.11 % sit above 120 s
MIN_GAP_S = 5           # below this GPS noise dominates the distance
MAX_GAP_S = 180         # the source publishes every 30 s, a longer gap is a dropout
MAX_SPEED_KMH = 100     # surface transit in Rome cannot do this, it is a GPS jump

# Rome bounding box
LAT_MIN, LAT_MAX = 41.6, 42.2
LON_MIN, LON_MAX = 12.2, 12.9

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F

EARTH_R = 6371000


def haversine_m(lat1, lon1, lat2, lon2):
    dlat = F.radians(lat2 - lat1) / 2
    dlon = F.radians(lon2 - lon1) / 2
    a = F.sin(dlat) * F.sin(dlat) + (
        F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon) * F.sin(dlon)
    )
    return 2 * EARTH_R * F.asin(F.sqrt(a))

# COMMAND ----------
dedup = Window.partitionBy("vehicle_id", "vehicle_timestamp").orderBy("feed_timestamp")

points = (
    spark.table(BRONZE)
    .where(F.col("vehicle_timestamp").isNotNull())
    .where(F.col("vehicle_id").isNotNull())
    .where(F.col("latitude").between(LAT_MIN, LAT_MAX))
    .where(F.col("longitude").between(LON_MIN, LON_MAX))
    .where(
        F.col("feed_timestamp").cast("long") - F.col("vehicle_timestamp").cast("long")
        <= MAX_STALENESS_S
    )
    .withColumn("rn", F.row_number().over(dedup))
    .where("rn = 1")
    .drop("rn", "feed_timestamp", "source_file", "ingested_at")
)

# COMMAND ----------
track = Window.partitionBy("vehicle_id").orderBy("vehicle_timestamp")

segments = (
    points.withColumn("prev_lat", F.lag("latitude").over(track))
    .withColumn("prev_lon", F.lag("longitude").over(track))
    .withColumn("prev_ts", F.lag("vehicle_timestamp").over(track))
    .withColumn("prev_trip_id", F.lag("trip_id").over(track))
    .where(F.col("prev_ts").isNotNull())
    .where(F.col("trip_id") == F.col("prev_trip_id"))
    .withColumn(
        "duration_s",
        F.col("vehicle_timestamp").cast("long") - F.col("prev_ts").cast("long"),
    )
    .where(F.col("duration_s").between(MIN_GAP_S, MAX_GAP_S))
    .withColumn(
        "distance_m",
        haversine_m(
            F.col("prev_lat"), F.col("prev_lon"), F.col("latitude"), F.col("longitude")
        ),
    )
    .withColumn("speed_kmh", F.col("distance_m") / F.col("duration_s") * 3.6)
    .where(F.col("speed_kmh") <= MAX_SPEED_KMH)
)

# COMMAND ----------
silver = segments.select(
    "vehicle_id",
    "trip_id",
    "route_id",
    "direction_id",
    "prev_ts",
    F.col("vehicle_timestamp").alias("ts"),
    "duration_s",
    "distance_m",
    "speed_kmh",
    F.col("prev_lat").alias("from_lat"),
    F.col("prev_lon").alias("from_lon"),
    F.col("latitude").alias("to_lat"),
    F.col("longitude").alias("to_lon"),
    F.to_date(F.from_utc_timestamp("vehicle_timestamp", "Europe/Rome")).alias(
        "date_rome"
    ),
    F.hour(F.from_utc_timestamp("vehicle_timestamp", "Europe/Rome")).alias("hour_rome"),
)

silver.write.mode("overwrite").option("overwriteSchema", "true").partitionBy(
    "date_rome"
).saveAsTable(TABLE)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT date_rome, hour_rome, count(*) AS segments,
# MAGIC        round(avg(speed_kmh), 1) AS avg_kmh,
# MAGIC        round(percentile_approx(speed_kmh, 0.5), 1) AS median_kmh
# MAGIC FROM transit.silver.vehicle_segments
# MAGIC GROUP BY date_rome, hour_rome ORDER BY date_rome, hour_rome
