"""Downloads one GTFS-RT snapshot and prints what is inside it."""

import datetime as dt
from pathlib import Path

import requests
from google.transit import gtfs_realtime_pb2

URL = "https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb"


def main() -> None:
    resp = requests.get(URL, timeout=20)
    resp.raise_for_status()
    payload = resp.content

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)

    vehicles = [e for e in feed.entity if e.HasField("vehicle")]
    epoch = feed.header.timestamp or max(
        (e.vehicle.timestamp for e in vehicles), default=0
    )
    ts = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)

    print(f"snapshot time (UTC): {ts:%Y-%m-%d %H:%M:%S}")
    print(f"payload size:        {len(payload) / 1024:.1f} KB")
    print(f"vehicles in feed:    {len(vehicles)}")

    if vehicles:
        v = vehicles[0].vehicle
        print("\nsample record:")
        print(f"  vehicle_id: {v.vehicle.id}")
        print(f"  trip_id:    {v.trip.trip_id}")
        print(f"  route_id:   {v.trip.route_id}")
        print(f"  lat, lon:   {v.position.latitude}, {v.position.longitude}")
        speed = v.position.speed if v.position.HasField("speed") else "n/a"
        print(f"  speed:      {speed}")
        print(f"  timestamp:  {v.timestamp}")

    out = Path("data/sample")
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"vehicle_positions_{ts:%Y%m%dT%H%M%SZ}.pb"
    path.write_bytes(payload)
    print(f"\nsaved {path}")


if __name__ == "__main__":
    main()
