"""
AWS Lambda handler — Translink GTFS-RT TripUpdates → Zerobus → Delta Table.

Fetches the real-time trip updates feed from the Translink SEQ API (protobuf),
flattens each StopTimeUpdate into a row matching the trip_updates Delta table
schema, and ingests via the Databricks Zerobus SDK in protobuf mode.

Each TripUpdate entity may contain multiple StopTimeUpdate entries (one per
stop with a prediction). We emit one table row per StopTimeUpdate, repeating
the trip-level fields for easy downstream querying.
"""

import logging
import os
import time

import requests as http_requests

import gtfs_realtime_pb2 as gtfs_rt
import trip_update_pb2 as tu_pb2
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

# ── Logging ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

TRANSLINK_API_URL_DEFAULT = (
    "https://gtfsrt.api.translink.com.au/api/realtime/SEQ/TripUpdates"
)

# ── Enum helpers ────────────────────────────────────────────────────────
# GTFS-RT uses numeric proto enums; the Delta table stores human-readable
# string names (e.g. "SCHEDULED" instead of 0).

_INCREMENTALITY = gtfs_rt.FeedHeader.DESCRIPTOR.enum_types_by_name["Incrementality"]
_TRIP_SCHEDULE_REL = gtfs_rt.TripDescriptor.DESCRIPTOR.enum_types_by_name["ScheduleRelationship"]
_STOP_SCHEDULE_REL = gtfs_rt.TripUpdate.StopTimeUpdate.DESCRIPTOR.enum_types_by_name["ScheduleRelationship"]


def _enum_name(enum_descriptor, value):
    """Map a protobuf enum int to its string name."""
    val = enum_descriptor.values_by_number.get(value)
    return val.name if val else str(value)


# ── Feed fetching ───────────────────────────────────────────────────────

def fetch_feed(url):
    """GET the Translink GTFS-RT TripUpdates feed and parse the protobuf."""
    resp = http_requests.get(url, timeout=30)
    resp.raise_for_status()
    feed = gtfs_rt.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


# ── Flatten a single entity ─────────────────────────────────────────────

def flatten_stop_time_updates(entity, header):
    """Yield one table-schema protobuf record per StopTimeUpdate in the entity."""
    tu = entity.trip_update
    trip = tu.trip
    vehicle = tu.vehicle

    # Shared fields across all StopTimeUpdate rows for this entity
    shared = dict(
        entity_id=entity.id,
        feed_timestamp=header.timestamp,
        gtfs_realtime_version=header.gtfs_realtime_version,
        incrementality=_enum_name(_INCREMENTALITY, header.incrementality),
        trip_id=trip.trip_id,
        route_id=trip.route_id,
        direction_id=trip.direction_id,
        start_time=trip.start_time,
        start_date=trip.start_date,
        schedule_relationship=_enum_name(_TRIP_SCHEDULE_REL, trip.schedule_relationship),
        vehicle_id=vehicle.id,
        vehicle_label=vehicle.label,
        trip_delay=tu.delay,
        trip_timestamp=tu.timestamp,
        ingested_at=int(time.time() * 1_000_000),
    )

    for stu in tu.stop_time_update:
        # Extract arrival StopTimeEvent (if present)
        arr_delay = 0
        arr_time = 0
        arr_uncertainty = 0
        if stu.HasField("arrival"):
            arr_delay = stu.arrival.delay
            arr_time = stu.arrival.time
            arr_uncertainty = stu.arrival.uncertainty

        # Extract departure StopTimeEvent (if present)
        dep_delay = 0
        dep_time = 0
        dep_uncertainty = 0
        if stu.HasField("departure"):
            dep_delay = stu.departure.delay
            dep_time = stu.departure.time
            dep_uncertainty = stu.departure.uncertainty

        yield tu_pb2.TripUpdate(
            **shared,
            stop_sequence=stu.stop_sequence,
            stop_id=stu.stop_id,
            arrival_delay=arr_delay,
            arrival_time=arr_time,
            arrival_uncertainty=arr_uncertainty,
            departure_delay=dep_delay,
            departure_time=dep_time,
            departure_uncertainty=dep_uncertainty,
            stop_schedule_relationship=_enum_name(_STOP_SCHEDULE_REL, stu.schedule_relationship),
        )


# ── Lambda entry point ──────────────────────────────────────────────────

def lambda_handler(event, context):
    """AWS Lambda handler."""
    translink_url = os.environ.get("TRANSLINK_TRIP_UPDATES_URL", TRANSLINK_API_URL_DEFAULT)
    workspace_url = os.environ["DATABRICKS_WORKSPACE_URL"]
    workspace_id = os.environ["DATABRICKS_WORKSPACE_ID"]
    region = os.environ["DATABRICKS_REGION"]
    client_id = os.environ["DATABRICKS_CLIENT_ID"]
    client_secret = os.environ["DATABRICKS_CLIENT_SECRET"]
    table_name = os.environ["DATABRICKS_TRIP_UPDATES_TABLE_NAME"]

    server_endpoint = f"https://{workspace_id}.zerobus.{region}.cloud.databricks.com"

    logger.info("Fetching Translink GTFS-RT TripUpdates …")
    feed = fetch_feed(translink_url)
    entity_count = len(feed.entity)
    logger.info("Feed contains %d entities (header ts=%d)", entity_count, feed.header.timestamp)

    # ── Zerobus stream setup ────────────────────────────────────────────
    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_props = TableProperties(
        table_name,
        tu_pb2.TripUpdate.DESCRIPTOR,
    )
    options = StreamConfigurationOptions(record_type=RecordType.PROTO)
    stream = sdk.create_stream(
        client_id,
        client_secret,
        table_props,
        options,
    )

    # ── Ingest ──────────────────────────────────────────────────────────
    ingested = 0
    try:
        acks = []
        for entity in feed.entity:
            if entity.HasField("trip_update"):
                for record in flatten_stop_time_updates(entity, feed.header):
                    ack = stream.ingest_record(record)
                    acks.append(ack)

        for ack in acks:
            ack.wait_for_ack()

        ingested = len(acks)
        logger.info("Successfully ingested %d stop-time updates", ingested)
    finally:
        stream.close()

    return {
        "statusCode": 200,
        "body": f"Ingested {ingested} stop-time updates from {entity_count} entities",
    }


# ── Local testing ───────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = lambda_handler({}, None)
    print(result)
