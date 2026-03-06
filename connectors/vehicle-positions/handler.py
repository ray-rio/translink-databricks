"""
AWS Lambda handler — Translink GTFS-RT VehiclePositions → Zerobus → Delta Table.

Fetches the real-time vehicle position feed from the Translink SEQ API (protobuf),
flattens each entity into a row matching the vehicle_positions Delta table schema,
and ingests via the Databricks Zerobus SDK in protobuf mode.
"""

import logging
import os
import time

import gtfs_realtime_pb2 as gtfs_rt
import vehicle_position_pb2 as vp_pb2
import rt_feed
import rt_ingest

# ── Logging ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# ── Connector-specific enum descriptors ─────────────────────────────────
_STOP_STATUS = gtfs_rt.VehiclePosition.DESCRIPTOR.enum_types_by_name["VehicleStopStatus"]
_CONGESTION = gtfs_rt.VehiclePosition.DESCRIPTOR.enum_types_by_name["CongestionLevel"]
_OCCUPANCY = gtfs_rt.VehiclePosition.DESCRIPTOR.enum_types_by_name["OccupancyStatus"]
_WHEELCHAIR = gtfs_rt.VehicleDescriptor.DESCRIPTOR.enum_types_by_name["WheelchairAccessible"]


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
        incrementality=rt_feed.enum_name(rt_feed.INCREMENTALITY, header.incrementality),
        # TripDescriptor
        trip_id=trip.trip_id,
        route_id=trip.route_id,
        direction_id=trip.direction_id,
        start_time=trip.start_time,
        start_date=trip.start_date,
        schedule_relationship=rt_feed.enum_name(rt_feed.TRIP_SCHEDULE_REL, trip.schedule_relationship),
        # VehicleDescriptor
        vehicle_id=vehicle.id,
        vehicle_label=vehicle.label,
        license_plate=vehicle.license_plate,
        wheelchair_accessible=rt_feed.enum_name(_WHEELCHAIR, vehicle.wheelchair_accessible),
        # Position
        latitude=pos.latitude,
        longitude=pos.longitude,
        bearing=pos.bearing,
        odometer=pos.odometer,
        speed=pos.speed,
        # VehiclePosition scalars
        current_stop_sequence=vp.current_stop_sequence,
        stop_id=vp.stop_id,
        current_status=rt_feed.enum_name(_STOP_STATUS, vp.current_status),
        vehicle_timestamp=vp.timestamp,
        congestion_level=rt_feed.enum_name(_CONGESTION, vp.congestion_level),
        occupancy_status=rt_feed.enum_name(_OCCUPANCY, vp.occupancy_status),
        occupancy_percentage=vp.occupancy_percentage,
        # Ingestion metadata — epoch microseconds for TIMESTAMP column
        ingested_at=int(time.time() * 1_000_000),
    )


# ── Lambda entry point ──────────────────────────────────────────────────

def lambda_handler(event, context):
    """AWS Lambda handler."""
    translink_url = os.environ["TRANSLINK_API_URL"]
    table_name = os.environ["DATABRICKS_TABLE_NAME"]

    logger.info("Fetching Translink GTFS-RT VehiclePositions …")
    feed = rt_feed.fetch_feed(translink_url)
    entity_count = len(feed.entity)
    logger.info("Feed contains %d entities (header ts=%d)", entity_count, feed.header.timestamp)

    records = (
        flatten_entity(entity, feed.header)
        for entity in feed.entity
        if entity.HasField("vehicle")
    )

    ingested = rt_ingest.ingest_records(
        records, vp_pb2.VehiclePosition.DESCRIPTOR, table_name
    )
    logger.info("Successfully ingested %d vehicle positions", ingested)

    return {
        "statusCode": 200,
        "body": f"Ingested {ingested} of {entity_count} entities",
    }


# ── Local testing ───────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = lambda_handler({}, None)
    print(result)
