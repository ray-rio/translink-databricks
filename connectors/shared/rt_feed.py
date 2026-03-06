"""
Shared GTFS-RT feed utilities for real-time Lambda connectors.

Provides feed fetching (protobuf parsing) and enum resolution helpers
used by both VehiclePositions and TripUpdates handlers.
"""

import requests as http_requests

import gtfs_realtime_pb2 as gtfs_rt

# ── Pre-cached enum descriptors shared across RT connectors ──────────────

INCREMENTALITY = gtfs_rt.FeedHeader.DESCRIPTOR.enum_types_by_name["Incrementality"]
TRIP_SCHEDULE_REL = gtfs_rt.TripDescriptor.DESCRIPTOR.enum_types_by_name[
    "ScheduleRelationship"
]


def enum_name(enum_descriptor, value):
    """Map a protobuf enum int to its string name."""
    val = enum_descriptor.values_by_number.get(value)
    return val.name if val else str(value)


def fetch_feed(url):
    """GET a Translink GTFS-RT feed URL and parse the protobuf response."""
    resp = http_requests.get(url, timeout=30)
    resp.raise_for_status()
    feed = gtfs_rt.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed
