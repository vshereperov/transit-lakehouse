# Databricks notebook source
CATALOG = "transit"
EDGES = f"{CATALOG}.silver.street_edges"
ROUTE_STREETS = f"{CATALOG}.silver.route_streets"
TRIP_SHAPE = f"{CATALOG}.silver.trip_shape"
STATIC_PATH = "abfss://raw@sttransitlake.dfs.core.windows.net/rome/static_gtfs/"

MAX_SNAP_M = 25
POINT_SPACING_M = 13
MIN_SPAN_M = 30
LOOKUP_DEG = 0.00025

# COMMAND ----------
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

LAT0 = 41.9
M_PER_LAT = 111320.0
M_PER_LON = 82800.0

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


@F.udf(returnType=ArrayType(POINT))
def parse_shapes(payload):
    import csv
    import io
    import zipfile

    with zipfile.ZipFile(io.BytesIO(payload)) as z, z.open("shapes.txt") as fh:
        reader = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig"))
        return [
            (
                r["shape_id"],
                int(r["shape_pt_sequence"]),
                float(r["shape_pt_lat"]),
                float(r["shape_pt_lon"]),
            )
            for r in reader
        ]


@F.udf(returnType=ArrayType(TRIP))
def parse_trips(payload):
    import csv
    import io
    import zipfile

    with zipfile.ZipFile(io.BytesIO(payload)) as z, z.open("trips.txt") as fh:
        reader = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig"))
        return [(r["trip_id"], r["shape_id"]) for r in reader if r.get("shape_id")]


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
latest = (
    spark.read.format("binaryFile")
    .load(STATIC_PATH)
    .orderBy(F.col("modificationTime").desc())
    .limit(1)
)

trips = latest.select(F.explode(parse_trips("content")).alias("t")).select("t.*")
trips.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TRIP_SHAPE)

shape_points = (
    latest.select(F.explode(parse_shapes("content")).alias("p"))
    .select("p.*")
    .withColumn("cell_lat", cell(F.col("lat")))
    .withColumn("cell_lon", cell(F.col("lon")))
)

# COMMAND ----------
around = F.array(*[F.lit(d) for d in (-1, 0, 1)])

looking = (
    shape_points.withColumn("dlat", F.explode(around))
    .withColumn("dlon", F.explode(around))
    .withColumn("cell_lat", F.col("cell_lat") + F.col("dlat"))
    .withColumn("cell_lon", F.col("cell_lon") + F.col("dlon"))
    .drop("dlat", "dlon")
)

edges = (
    spark.table(EDGES)
    .select("way_id", "from_lat", "from_lon", "to_lat", "to_lon")
    .withColumn("cell_lat", cell(F.col("from_lat")))
    .withColumn("cell_lon", cell(F.col("from_lon")))
)

candidates = looking.join(edges, ["cell_lat", "cell_lon"]).withColumn(
    "snap_m",
    point_to_edge_m(
        F.col("lat"), F.col("lon"),
        F.col("from_lat"), F.col("from_lon"),
        F.col("to_lat"), F.col("to_lon"),
    ),
)

nearest = Window.partitionBy("shape_id", "seq").orderBy("snap_m")
matched = (
    candidates.withColumn("rn", F.row_number().over(nearest))
    .where((F.col("rn") == 1) & (F.col("snap_m") <= MAX_SNAP_M))
)

# COMMAND ----------
length = (
    spark.table(EDGES).groupBy("way_id").agg(F.sum("length_m").alias("street_m"))
)

route_streets = (
    matched.groupBy("shape_id", "way_id")
    .agg(
        F.count("*").alias("points"),
        F.round(F.avg("snap_m"), 1).alias("snap_m"),
    )
    .join(length, "way_id")
    .withColumn("span_m", F.col("points") * POINT_SPACING_M)
    .where(F.col("span_m") >= F.least(F.lit(float(MIN_SPAN_M)), F.col("street_m")))
    .select("shape_id", "way_id", "points", "snap_m", "street_m", "span_m")
)

route_streets.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    ROUTE_STREETS
)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT count(*) AS links,
# MAGIC        count(DISTINCT shape_id) AS routes,
# MAGIC        count(DISTINCT way_id) AS streets,
# MAGIC        round(avg(snap_m), 1) AS avg_snap_m
# MAGIC FROM transit.silver.route_streets
