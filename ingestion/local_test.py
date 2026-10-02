"""Fetches one GTFS-RT snapshot and prints the first five vehicles."""

import datetime as dt

import requests
from google.transit import gtfs_realtime_pb2

URL = "https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb"


def main() -> None:
    resp = requests.get(URL, timeout=20)
    resp.raise_for_status()
    payload = resp.content

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)

    vehicles = [entity for entity in feed.entity if entity.HasField("vehicle")]
    epoch = feed.header.timestamp or max(
        (entity.vehicle.timestamp for entity in vehicles), default=0
    )
    ts = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)

    print(f"snapshot time (UTC): {ts:%Y-%m-%d %H:%M:%S}")
    print(f"payload size:        {len(payload) / 1024:.1f} KB")
    print(f"vehicles in feed:    {len(vehicles)}")

    for entity in vehicles[:5]:
        print(f"\n{entity}")


if __name__ == "__main__":
    main()
