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
from itertools import chain

import gtfs_realtime_pb2 as gtfs_rt
import trip_update_pb2 as tu_pb2
import rt_feed
import rt_ingest

# ── Logging ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# ── Connector-specific enum descriptor ──────────────────────────────────
_STOP_SCHEDULE_REL = gtfs_rt.TripUpdate.StopTimeUpdate.DESCRIPTOR.enum_types_by_name[
    "ScheduleRelationship"
]


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
        incrementality=rt_feed.enum_name(rt_feed.INCREMENTALITY, header.incrementality),
        trip_id=trip.trip_id,
        route_id=trip.route_id,
        direction_id=trip.direction_id,
        start_time=trip.start_time,
        start_date=trip.start_date,
        schedule_relationship=rt_feed.enum_name(rt_feed.TRIP_SCHEDULE_REL, trip.schedule_relationship),
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
            stop_schedule_relationship=rt_feed.enum_name(
                _STOP_SCHEDULE_REL, stu.schedule_relationship
            ),
        )


# ── Lambda entry point ──────────────────────────────────────────────────

def lambda_handler(event, context):
    """AWS Lambda handler."""
    translink_url = os.environ["TRANSLINK_TRIP_UPDATES_URL"]
    table_name = os.environ["DATABRICKS_TRIP_UPDATES_TABLE_NAME"]

    logger.info("Fetching Translink GTFS-RT TripUpdates …")
    feed = rt_feed.fetch_feed(translink_url)
    entity_count = len(feed.entity)
    logger.info("Feed contains %d entities (header ts=%d)", entity_count, feed.header.timestamp)

    # Flatten: each entity yields N StopTimeUpdate rows
    records = chain.from_iterable(
        flatten_stop_time_updates(entity, feed.header)
        for entity in feed.entity
        if entity.HasField("trip_update")
    )

    ingested = rt_ingest.ingest_records(
        records, tu_pb2.TripUpdate.DESCRIPTOR, table_name
    )
    logger.info("Successfully ingested %d stop-time updates", ingested)

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
