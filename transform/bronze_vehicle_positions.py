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

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} MANAGED LOCATION '{CATALOG_LOCATION}'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ROW = StructType(
    [
        StructField("feed_timestamp", TimestampType()),
        StructField("vehicle_id", StringType()),
        StructField("vehicle_label", StringType()),
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("direction_id", IntegerType()),
        StructField("start_date", StringType()),
        StructField("start_time", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("odometer", DoubleType()),
        StructField("stop_id", StringType()),
        StructField("current_stop_sequence", IntegerType()),
        StructField("current_status", IntegerType()),
        StructField("occupancy_status", IntegerType()),
        StructField("vehicle_timestamp", TimestampType()),
    ]
)


@F.udf(returnType=ArrayType(ROW))
def parse_snapshot(payload):
    """One raw .pb blob -> one row per vehicle."""
    import datetime as dt

    from google.transit import gtfs_realtime_pb2

    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(payload)
    feed_ts = dt.datetime.fromtimestamp(msg.header.timestamp, dt.timezone.utc)

    rows = []
    for e in msg.entity:
        if not e.HasField("vehicle"):
            continue
        v = e.vehicle
        rows.append(
            (
                feed_ts,
                v.vehicle.id or None,
                v.vehicle.label or None,
                v.trip.trip_id or None,
                v.trip.route_id or None,
                v.trip.direction_id if v.trip.HasField("direction_id") else None,
                v.trip.start_date or None,
                v.trip.start_time or None,
                v.position.latitude,
                v.position.longitude,
                v.position.odometer if v.position.HasField("odometer") else None,
                v.stop_id or None,
                v.current_stop_sequence,
                v.current_status,
                v.occupancy_status,
                dt.datetime.fromtimestamp(v.timestamp, dt.timezone.utc) if v.timestamp else None,
            )
        )
    return rows

# COMMAND ----------
files = spark.read.format("binaryFile").option("pathGlobFilter", "*.pb").load(RAW_PATH)
files = files.withColumnRenamed("path", "source_file")

if spark.catalog.tableExists(TABLE):
    done = spark.table(TABLE).select("source_file").distinct()
    files = files.join(done, "source_file", "left_anti")

rows = (
    files.select("source_file", F.explode(parse_snapshot("content")).alias("r"))
    .select("source_file", "r.*")
    .withColumn("date", F.to_date("feed_timestamp"))
    .withColumn("ingested_at", F.current_timestamp())
)

rows.write.mode("append").partitionBy("date").saveAsTable(TABLE)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT date, count(*) AS rows, count(DISTINCT vehicle_id) AS vehicles,
# MAGIC        min(feed_timestamp) AS first_snapshot, max(feed_timestamp) AS last_snapshot
# MAGIC FROM transit.bronze.vehicle_positions
# MAGIC GROUP BY date ORDER BY date
