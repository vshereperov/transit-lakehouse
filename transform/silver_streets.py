# Databricks notebook source
CATALOG = "transit"
TABLE = f"{CATALOG}.silver.street_edges"

BBOX = (41.68, 12.20, 42.10, 12.75) # Rome bounding box
HIGHWAYS = (
    "motorway|trunk|primary|secondary|tertiary|unclassified|residential|living_street"
    "|motorway_link|trunk_link|primary_link|secondary_link|tertiary_link|busway"
)
OVERPASS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
)
USER_AGENT = "transit-lakehouse/0.1"

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

# COMMAND ----------
# Overpass query and distance between two points

import math
import time
from itertools import pairwise

import requests

QUERY = f"""
[out:json][timeout:300];
(
  way["highway"~"^({HIGHWAYS})$"]{BBOX};
  way["highway"="service"]["bus"~"^(yes|designated)$"]{BBOX};
  way["highway"="service"]["psv"~"^(yes|designated)$"]{BBOX};
);
out geom;
"""

EARTH_RADIUS_M = 6371000


def distance_m(lat1, lon1, lat2, lon2):
    """Returns the distance in metres between two points, by the haversine formula."""
    dlat = math.radians(lat2 - lat1) / 2
    dlon = math.radians(lon2 - lon1) / 2
    a = math.sin(dlat) ** 2 + math.cos(math.radians(lat1)) * math.cos(
        math.radians(lat2)
    ) * math.sin(dlon) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))

# COMMAND ----------
# Download streets

ways = None
for url in OVERPASS:
    try:
        response = requests.post(
            url, data={"data": QUERY}, headers={"User-Agent": USER_AGENT}, timeout=600
        )
        response.raise_for_status()
        ways = [way for way in response.json()["elements"] if way.get("geometry")]
        if not ways:
            raise ValueError("empty response")
        print(f"{len(ways):,} ways from {url}")
        break
    except (requests.RequestException, ValueError) as error:
        print(f"{url}: {error}")
        time.sleep(5)

if not ways:
    raise RuntimeError("every Overpass mirror refused")

# COMMAND ----------
# Split streets into edges

edges = []
for way in ways:
    tags = way.get("tags", {})
    geometry = way["geometry"]
    for seq, (start, end) in enumerate(pairwise(geometry)):
        length_m = distance_m(start["lat"], start["lon"], end["lat"], end["lon"])
        edges.append(
            (
                way["id"],
                tags.get("name"),
                tags.get("highway"),
                seq,
                start["lat"],
                start["lon"],
                end["lat"],
                end["lon"],
                length_m,
            )
        )

print(f"{len(edges):,} edges, {sum(edge[-1] for edge in edges) / 1000:,.0f} km")

# COMMAND ----------
# Write table

COLUMNS = [
    "way_id", "street", "highway", "seq",
    "from_lat", "from_lon", "to_lat", "to_lon", "length_m",
]

spark.createDataFrame(edges, COLUMNS).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABLE)
