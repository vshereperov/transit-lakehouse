# Databricks notebook source
# MAGIC %pip install --no-deps gtfs-realtime-bindings==2.2.0

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
CATALOG = "transit"
SCHEMA = "bronze"
TABLE = f"{CATALOG}.{SCHEMA}.vehicle_positions"

RAW_PATH = "abfss://raw@sttransitlake.dfs.core.windows.net/rome/vehicle_positions/"
LAKEHOUSE = "abfss://lakehouse@sttransitlake.dfs.core.windows.net"
CATALOG_LOCATION = f"{LAKEHOUSE}/catalog"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/vehicle_positions"

MAX_LAG_MIN = 60 # Maximum age of the newest snapshot to consider data fresh

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} MANAGED LOCATION '{CATALOG_LOCATION}'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------
# Snapshot parser

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

ROW = StructType(
    [
        # snapshot and entity
        StructField("feed_timestamp", LongType()),
        StructField("entity_id", StringType()),
        StructField("is_deleted", BooleanType()),
        # vehicle.trip
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("direction_id", IntegerType()),
        StructField("start_date", StringType()),
        StructField("start_time", StringType()),
        StructField("schedule_relationship", IntegerType()),
        # vehicle.vehicle
        StructField("vehicle_id", StringType()),
        StructField("vehicle_label", StringType()),
        StructField("license_plate", StringType()),
        # vehicle.position
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("bearing", DoubleType()),
        StructField("odometer", DoubleType()),
        StructField("speed", DoubleType()),
        # vehicle
        StructField("stop_id", StringType()),
        StructField("current_stop_sequence", IntegerType()),
        StructField("current_status", IntegerType()),
        StructField("occupancy_status", IntegerType()),
        StructField("vehicle_timestamp", LongType()),
    ]
)


@F.udf(returnType=ArrayType(ROW))
def parse_snapshot(payload):
    """Parses one raw snapshot into a row for each vehicle in it."""
    from google.transit import gtfs_realtime_pb2

    def get(node, field):
        """Returns the field's value, or None if the feed left it unset."""
        return getattr(node, field) if node.HasField(field) else None

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    feed_ts = get(feed.header, "timestamp")

    rows = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        vehicle = entity.vehicle
        trip, descriptor, position = vehicle.trip, vehicle.vehicle, vehicle.position
        rows.append(
            (
                feed_ts,
                entity.id,
                get(entity, "is_deleted"),
                get(trip, "trip_id"),
                get(trip, "route_id"),
                get(trip, "direction_id"),
                get(trip, "start_date"),
                get(trip, "start_time"),
                get(trip, "schedule_relationship"),
                get(descriptor, "id"),
                get(descriptor, "label"),
                get(descriptor, "license_plate"),
                get(position, "latitude"),
                get(position, "longitude"),
                get(position, "bearing"),
                get(position, "odometer"),
                get(position, "speed"),
                get(vehicle, "stop_id"),
                get(vehicle, "current_stop_sequence"),
                get(vehicle, "current_status"),
                get(vehicle, "occupancy_status"),
                get(vehicle, "timestamp"),
            )
        )
    return rows

# COMMAND ----------
# Incremental load from raw

files = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobFilter", "*.pb")
    .load(RAW_PATH)
    .withColumnRenamed("path", "source_file")
)

rows = (
    files.select("source_file", F.explode(parse_snapshot("content")).alias("r"))
    .select("source_file", "r.*")
    .withColumn("feed_timestamp", F.timestamp_seconds("feed_timestamp"))
    .withColumn("vehicle_timestamp", F.timestamp_seconds("vehicle_timestamp"))
    .withColumn("date", F.to_date("feed_timestamp"))
    .withColumn("ingested_at", F.current_timestamp())
)

(
    rows.writeStream.option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .partitionBy("date")
    .toTable(TABLE)
    .awaitTermination()
)

# COMMAND ----------
# Freshness check

lag_s = (
    spark.table(TABLE)
    .where(F.col("date") >= F.date_sub(F.current_date(), 1))
    .agg(
        (
            F.unix_timestamp(F.current_timestamp())
            - F.unix_timestamp(F.max("feed_timestamp"))
        ).alias("lag_s")
    )
    .first()["lag_s"]
)
if lag_s is None:
    raise AssertionError("bronze has no snapshots since yesterday")
if lag_s > MAX_LAG_MIN * 60:
    raise AssertionError(f"bronze is stale: newest snapshot is {lag_s // 60} min old")
print(f"bronze is fresh: newest snapshot is {lag_s // 60} min old")
