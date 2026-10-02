"""
Azure Functions that save raw GTFS data to ADLS Gen2 as-is.

- collect_realtime: the vehicle positions feed, every 20 s.
- collect_static: the static GTFS zip with route shapes, once a day.
"""

import datetime as dt
import logging
import os

import azure.functions as func
import requests
from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from google.transit import gtfs_realtime_pb2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = func.FunctionApp()
logger = logging.getLogger(__name__)

# Configuration

FEED_URL = os.environ.get(
    "FEED_VEHICLE_POSITIONS_URL",
    "https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb",
)
STATIC_GTFS_URL = os.environ.get(
    "STATIC_GTFS_URL",
    "https://romamobilita.it/sites/default/files/rome_static_gtfs.zip",
)

CITY = os.environ.get("CITY", "rome")
RAW_CONTAINER = os.environ.get("RAW_CONTAINER", "raw")
FEED_NAME = "vehicle_positions"

USER_AGENT = "transit-lakehouse/0.1"
FEED_TIMEOUT_S = 8 # Maximum wait for one snapshot of the vehicle positions feed
STATIC_TIMEOUT_S = 120 # Maximum wait for the static GTFS zip

# Clients

_blob_service = None
_http = requests.Session()
_http.headers["User-Agent"] = USER_AGENT
_http.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=1,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            raise_on_status=False,
        )
    ),
)


def blob_service() -> BlobServiceClient:
    """Authenticates with the function's managed identity."""
    global _blob_service
    if _blob_service is None:
        _blob_service = BlobServiceClient(
            account_url=os.environ["DATALAKE_ACCOUNT_URL"],
            credential=DefaultAzureCredential(),
        )
    return _blob_service


# Helpers

def snapshot_time(payload: bytes) -> dt.datetime:
    """Returns the feed's snapshot time (UTC), rejecting empty feeds."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    if not feed.entity:
        raise ValueError("payload parsed as protobuf but carries no entities")
    ts = feed.header.timestamp or max(
        entity.vehicle.timestamp for entity in feed.entity
    )
    if not ts:
        raise ValueError("feed carries no timestamp")
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)


def realtime_blob_path(city: str, ts: dt.datetime) -> str:
    return (
        f"{city}/{FEED_NAME}/date={ts:%Y-%m-%d}/hour={ts:%H}/"
        f"{FEED_NAME}_{ts:%Y%m%dT%H%M%SZ}.pb"
    )


def static_blob_path(city: str, day: dt.date) -> str:
    return f"{city}/static_gtfs/date={day:%Y-%m-%d}/static_gtfs.zip"


def upload_if_new(path: str, data: bytes) -> bool:
    """Returns True if written, False if the blob already existed."""
    client = blob_service().get_blob_client(container=RAW_CONTAINER, blob=path)
    try:
        client.upload_blob(data, overwrite=False)
        return True
    except ResourceExistsError:
        return False


def fetch(url: str, timeout_s: int) -> bytes:
    resp = _http.get(url, timeout=timeout_s)
    resp.raise_for_status()
    return resp.content


# Timer-triggered functions

@app.timer_trigger(
    schedule="*/20 * * * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def collect_realtime(timer: func.TimerRequest) -> None:
    """Saves the current vehicle positions snapshot every 20 s."""
    payload = fetch(FEED_URL, FEED_TIMEOUT_S)
    path = realtime_blob_path(CITY, snapshot_time(payload))
    if upload_if_new(path, payload):
        logger.info("saved %s (%d bytes)", path, len(payload))
    else:
        logger.info("duplicate snapshot, skipped %s", path)


@app.timer_trigger(
    schedule="0 0 3 * * *",
    arg_name="timer",
    run_on_startup=True,
    use_monitor=True,
)
def collect_static(timer: func.TimerRequest) -> None:
    """Saves today's static GTFS zip once a day at 03:00 UTC."""
    today = dt.datetime.now(dt.timezone.utc).date()
    path = static_blob_path(CITY, today)
    client = blob_service().get_blob_client(container=RAW_CONTAINER, blob=path)
    if client.exists():
        logger.info("static GTFS for %s already stored", today)
        return
    payload = fetch(STATIC_GTFS_URL, STATIC_TIMEOUT_S)
    if upload_if_new(path, payload):
        logger.info("saved %s (%d bytes)", path, len(payload))
