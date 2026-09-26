# Databricks notebook source
CATALOG = "transit"
TABLE = f"{CATALOG}.silver.street_edges"

BBOX = (41.68, 12.20, 42.10, 12.75)
HIGHWAYS = (
    "motorway|trunk|primary|secondary|tertiary|unclassified|residential|living_street"
    "|motorway_link|trunk_link|primary_link|secondary_link|tertiary_link"
)
OVERPASS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.osm.ch/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
)
USER_AGENT = "transit-lakehouse/0.1"

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

# COMMAND ----------
import math
import time

import requests

QUERY = f"""
[out:json][timeout:300];
way["highway"~"^({HIGHWAYS})$"]{BBOX};
out geom;
"""


def metres(lat1, lon1, lat2, lon2):
    r = 6371000
    dlat = math.radians(lat2 - lat1) / 2
    dlon = math.radians(lon2 - lon1) / 2
    a = math.sin(dlat) ** 2 + math.cos(math.radians(lat1)) * math.cos(
        math.radians(lat2)
    ) * math.sin(dlon) ** 2
    return 2 * r * math.asin(math.sqrt(a))

# COMMAND ----------
ways = None
for url in OVERPASS:
    try:
        response = requests.post(
            url, data={"data": QUERY}, headers={"User-Agent": USER_AGENT}, timeout=600
        )
        response.raise_for_status()
        ways = [w for w in response.json()["elements"] if w.get("geometry")]
        print(f"{len(ways):,} ways from {url}")
        break
    except Exception as refused:
        print(f"{url}: {refused}")
        time.sleep(5)

if not ways:
    raise RuntimeError("every Overpass mirror refused")

# COMMAND ----------
edges = []
for way in ways:
    tags = way.get("tags", {})
    geometry = way["geometry"]
    for i, (a, b) in enumerate(zip(geometry, geometry[1:], strict=False)):
        length = metres(a["lat"], a["lon"], b["lat"], b["lon"])
        edges.append(
            (
                way["id"],
                tags.get("name"),
                tags.get("highway"),
                i,
                a["lat"],
                a["lon"],
                b["lat"],
                b["lon"],
                length,
            )
        )

print(f"{len(edges):,} edges, {sum(e[-1] for e in edges) / 1000:,.0f} km")

# COMMAND ----------
COLUMNS = [
    "way_id", "street", "highway", "seq",
    "from_lat", "from_lon", "to_lat", "to_lon", "length_m",
]

spark.createDataFrame(edges, COLUMNS).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABLE)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT count(*) AS edges,
# MAGIC        count(DISTINCT way_id) AS roads,
# MAGIC        round(sum(length_m) / 1000) AS km,
# MAGIC        round(100 * count(street) / count(*)) AS pct_named
# MAGIC FROM transit.silver.street_edges
