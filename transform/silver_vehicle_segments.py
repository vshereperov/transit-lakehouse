# Databricks notebook source
CATALOG = "transit"
BRONZE = f"{CATALOG}.bronze.vehicle_positions"
TABLE = f"{CATALOG}.silver.vehicle_segments"

HISTORY_DAYS = 90 # Keep this many days of history in the table

MAX_STALENESS_S = 120 # Maximum age of a vehicle position to consider it valid
MIN_GAP_S = 5 # Minimum gap between two vehicle positions to consider it valid
MAX_GAP_S = 90 # Maximum gap between two vehicle positions to consider it valid
MAX_SPEED_KMH = 100 # Maximum speed of a vehicle to consider it valid

# Rome bounding box
LAT_MIN, LAT_MAX = 41.68, 42.10
LON_MIN, LON_MAX = 12.20, 12.75

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

# COMMAND ----------
# Distance between two points

from pyspark.sql import Window
from pyspark.sql import functions as F

EARTH_RADIUS_M = 6371000


def distance_m(lat1, lon1, lat2, lon2):
    """Returns the distance in metres between two points, by the haversine formula."""
    dlat = F.radians(lat2 - lat1) / 2
    dlon = F.radians(lon2 - lon1) / 2
    a = F.sin(dlat) * F.sin(dlat) + (
        F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon) * F.sin(dlon)
    )
    return 2 * EARTH_RADIUS_M * F.asin(F.sqrt(a))

# COMMAND ----------
# Clean up invalid points

same_ping = Window.partitionBy("vehicle_id", "vehicle_timestamp").orderBy(
    "feed_timestamp"
)

points = (
    spark.table(BRONZE)
    .where(F.col("date") >= F.date_sub(F.current_date(), HISTORY_DAYS))
    .where(F.col("vehicle_timestamp").isNotNull())
    .where(F.col("vehicle_id").isNotNull())
    .where(F.col("latitude").between(LAT_MIN, LAT_MAX))
    .where(F.col("longitude").between(LON_MIN, LON_MAX))
    .where(
        F.col("feed_timestamp").cast("long") - F.col("vehicle_timestamp").cast("long")
        <= MAX_STALENESS_S
    )
    .withColumn("copy", F.row_number().over(same_ping))
    .where("copy = 1")
    .drop("copy", "feed_timestamp", "source_file", "ingested_at")
)

# COMMAND ----------
# Compute segments

track = Window.partitionBy("vehicle_id").orderBy("vehicle_timestamp")

segments = (
    points.withColumn("from_lat", F.lag("latitude").over(track))
    .withColumn("from_lon", F.lag("longitude").over(track))
    .withColumn("from_ts", F.lag("vehicle_timestamp").over(track))
    .withColumn("from_trip_id", F.lag("trip_id").over(track))
    .where(F.col("from_ts").isNotNull())
    .where(F.col("trip_id") == F.col("from_trip_id"))
    .withColumn(
        "duration_s",
        F.col("vehicle_timestamp").cast("long") - F.col("from_ts").cast("long"),
    )
    .where(F.col("duration_s").between(MIN_GAP_S, MAX_GAP_S))
    .withColumn(
        "distance_m",
        distance_m(
            F.col("from_lat"), F.col("from_lon"), F.col("latitude"), F.col("longitude")
        ),
    )
    .withColumn("speed_kmh", F.col("distance_m") / F.col("duration_s") * 3.6)
    .where(F.col("speed_kmh") <= MAX_SPEED_KMH)
)

# COMMAND ----------
# Write table

silver = segments.select(
    "vehicle_id",
    "trip_id",
    "route_id",
    "direction_id",
    "from_ts",
    F.col("vehicle_timestamp").alias("to_ts"),
    "duration_s",
    "distance_m",
    "speed_kmh",
    "from_lat",
    "from_lon",
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
