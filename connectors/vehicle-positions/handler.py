"""
AWS Lambda handler — Translink GTFS-RT VehiclePositions → Zerobus → Delta Table.

Fetches the real-time vehicle position feed from the Translink SEQ API (protobuf),
flattens each entity into a row matching the vehicle_positions Delta table schema,
and ingests via the Databricks Zerobus SDK in protobuf mode.
"""

import logging
import os
import time

import requests as http_requests

import gtfs_realtime_pb2 as gtfs_rt
import vehicle_position_pb2 as vp_pb2
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

# ── Logging ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

TRANSLINK_API_URL_DEFAULT = (
    "https://gtfsrt.api.translink.com.au/api/realtime/SEQ/VehiclePositions"
)

# ── Enum helpers ────────────────────────────────────────────────────────
# GTFS-RT uses numeric proto enums; the Delta table stores human-readable
# string names (e.g. "STOPPED_AT" instead of 1).

_INCREMENTALITY = gtfs_rt.FeedHeader.DESCRIPTOR.enum_types_by_name["Incrementality"]
_SCHEDULE_REL = gtfs_rt.TripDescriptor.DESCRIPTOR.enum_types_by_name["ScheduleRelationship"]
_STOP_STATUS = gtfs_rt.VehiclePosition.DESCRIPTOR.enum_types_by_name["VehicleStopStatus"]
_CONGESTION = gtfs_rt.VehiclePosition.DESCRIPTOR.enum_types_by_name["CongestionLevel"]
_OCCUPANCY = gtfs_rt.VehiclePosition.DESCRIPTOR.enum_types_by_name["OccupancyStatus"]
_WHEELCHAIR = gtfs_rt.VehicleDescriptor.DESCRIPTOR.enum_types_by_name["WheelchairAccessible"]


def _enum_name(enum_descriptor, value):
    """Map a protobuf enum int to its string name."""
    val = enum_descriptor.values_by_number.get(value)
    return val.name if val else str(value)


# ── Feed fetching ───────────────────────────────────────────────────────

def fetch_feed(url):
    """GET the Translink GTFS-RT VehiclePositions feed and parse the protobuf."""
    resp = http_requests.get(url, timeout=30)
    resp.raise_for_status()
    feed = gtfs_rt.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


# ── Flatten a single entity ─────────────────────────────────────────────

def flatten_entity(entity, header):
    """Convert a FeedEntity + FeedHeader into a table-schema protobuf record."""
    vp = entity.vehicle
    trip = vp.trip
    vehicle = vp.vehicle
    pos = vp.position

    return vp_pb2.VehiclePosition(
        # FeedEntity
        entity_id=entity.id,
        # FeedHeader
        feed_timestamp=header.timestamp,
        gtfs_realtime_version=header.gtfs_realtime_version,
        incrementality=_enum_name(_INCREMENTALITY, header.incrementality),
        # TripDescriptor
        trip_id=trip.trip_id,
        route_id=trip.route_id,
        direction_id=trip.direction_id,
        start_time=trip.start_time,
        start_date=trip.start_date,
        schedule_relationship=_enum_name(_SCHEDULE_REL, trip.schedule_relationship),
        # VehicleDescriptor
        vehicle_id=vehicle.id,
        vehicle_label=vehicle.label,
        license_plate=vehicle.license_plate,
        wheelchair_accessible=_enum_name(_WHEELCHAIR, vehicle.wheelchair_accessible),
        # Position
        latitude=pos.latitude,
        longitude=pos.longitude,
        bearing=pos.bearing,
        odometer=pos.odometer,
        speed=pos.speed,
        # VehiclePosition scalars
        current_stop_sequence=vp.current_stop_sequence,
        stop_id=vp.stop_id,
        current_status=_enum_name(_STOP_STATUS, vp.current_status),
        vehicle_timestamp=vp.timestamp,
        congestion_level=_enum_name(_CONGESTION, vp.congestion_level),
        occupancy_status=_enum_name(_OCCUPANCY, vp.occupancy_status),
        occupancy_percentage=vp.occupancy_percentage,
        # Ingestion metadata — epoch microseconds for TIMESTAMP column
        ingested_at=int(time.time() * 1_000_000),
    )


# ── Lambda entry point ──────────────────────────────────────────────────

def lambda_handler(event, context):
    """AWS Lambda handler."""
    # ── Read config from env (after dotenv has loaded for local runs) ───
    translink_url = os.environ.get("TRANSLINK_API_URL", TRANSLINK_API_URL_DEFAULT)
    workspace_url = os.environ["DATABRICKS_WORKSPACE_URL"]
    workspace_id = os.environ["DATABRICKS_WORKSPACE_ID"]
    region = os.environ["DATABRICKS_REGION"]
    client_id = os.environ["DATABRICKS_CLIENT_ID"]
    client_secret = os.environ["DATABRICKS_CLIENT_SECRET"]
    table_name = os.environ["DATABRICKS_TABLE_NAME"]

    # Derive Zerobus endpoint: https://<workspace-id>.zerobus.<region>.cloud.databricks.com
    server_endpoint = f"https://{workspace_id}.zerobus.{region}.cloud.databricks.com"

    logger.info("Fetching Translink GTFS-RT VehiclePositions …")
    feed = fetch_feed(translink_url)
    entity_count = len(feed.entity)
    logger.info("Feed contains %d entities (header ts=%d)", entity_count, feed.header.timestamp)

    # ── Zerobus stream setup ────────────────────────────────────────────
    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_props = TableProperties(
        table_name,
        vp_pb2.VehiclePosition.DESCRIPTOR,
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
            if entity.HasField("vehicle"):
                record = flatten_entity(entity, feed.header)
                ack = stream.ingest_record(record)
                acks.append(ack)

        for ack in acks:
            ack.wait_for_ack()

        ingested = len(acks)
        logger.info("Successfully ingested %d vehicle positions", ingested)
    finally:
        stream.close()

    return {
        "statusCode": 200,
        "body": f"Ingested {ingested} of {entity_count} entities",
    }


# ── Local testing ───────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()  # reads .env into os.environ before lambda_handler runs
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = lambda_handler({}, None)
    print(result)
