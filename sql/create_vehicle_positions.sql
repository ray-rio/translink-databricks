-- =============================================================================
-- DDL: Raw layer table for GTFS Realtime VehiclePositions
-- Source: Translink SEQ — https://gtfsrt.api.translink.com.au/api/realtime/SEQ/VehiclePositions
-- Proto ref: https://github.com/google/transit/blob/master/gtfs-realtime/proto/gtfs-realtime.proto
-- =============================================================================

-- Set these before running
DECLARE OR REPLACE catalog_name STRING DEFAULT 'your_catalog';
DECLARE OR REPLACE schema_name  STRING DEFAULT 'your_schema';

CREATE TABLE IF NOT EXISTS IDENTIFIER(catalog_name || '.' || schema_name || '.vehicle_positions') (

  -- FeedEntity
  entity_id                 STRING        COMMENT 'FeedEntity.id — unique identifier within the feed',

  -- FeedHeader metadata
  feed_timestamp            BIGINT        COMMENT 'FeedHeader.timestamp — POSIX time when the feed was created',
  gtfs_realtime_version     STRING        COMMENT 'FeedHeader.gtfs_realtime_version',
  incrementality            STRING        COMMENT 'FeedHeader.incrementality — FULL_DATASET or DIFFERENTIAL',

  -- TripDescriptor
  trip_id                   STRING        COMMENT 'TripDescriptor.trip_id',
  route_id                  STRING        COMMENT 'TripDescriptor.route_id',
  direction_id              INT           COMMENT 'TripDescriptor.direction_id — 0 or 1',
  start_time                STRING        COMMENT 'TripDescriptor.start_time — HH:MM:SS format',
  start_date                STRING        COMMENT 'TripDescriptor.start_date — YYYYMMDD format',
  schedule_relationship     STRING        COMMENT 'TripDescriptor.ScheduleRelationship — SCHEDULED, UNSCHEDULED, CANCELED, etc.',

  -- VehicleDescriptor
  vehicle_id                STRING        COMMENT 'VehicleDescriptor.id — internal system identifier',
  vehicle_label             STRING        COMMENT 'VehicleDescriptor.label — user-visible label',
  license_plate             STRING        COMMENT 'VehicleDescriptor.license_plate',
  wheelchair_accessible     STRING        COMMENT 'VehicleDescriptor.WheelchairAccessible — NO_VALUE, UNKNOWN, WHEELCHAIR_ACCESSIBLE, WHEELCHAIR_INACCESSIBLE',

  -- Position
  latitude                  FLOAT         COMMENT 'Position.latitude — WGS-84 degrees north',
  longitude                 FLOAT         COMMENT 'Position.longitude — WGS-84 degrees east',
  bearing                   FLOAT         COMMENT 'Position.bearing — degrees clockwise from true north',
  odometer                  DOUBLE        COMMENT 'Position.odometer — metres',
  speed                     FLOAT         COMMENT 'Position.speed — metres per second',

  -- VehiclePosition scalar fields
  current_stop_sequence     INT           COMMENT 'Index of current stop in the GTFS stop_times sequence',
  stop_id                   STRING        COMMENT 'Current stop_id from GTFS stops.txt',
  current_status            STRING        COMMENT 'VehicleStopStatus — INCOMING_AT, STOPPED_AT, IN_TRANSIT_TO',
  vehicle_timestamp         BIGINT        COMMENT 'VehiclePosition.timestamp — POSIX time of position reading',
  congestion_level          STRING        COMMENT 'CongestionLevel — UNKNOWN, RUNNING_SMOOTHLY, STOP_AND_GO, CONGESTION, SEVERE_CONGESTION',
  occupancy_status          STRING        COMMENT 'OccupancyStatus — EMPTY, MANY_SEATS_AVAILABLE, FEW_SEATS_AVAILABLE, etc.',
  occupancy_percentage      INT           COMMENT 'Occupancy as a percentage (0–100+)',

  -- Ingestion metadata
  ingested_at               TIMESTAMP     COMMENT 'UTC timestamp when the record was ingested by the Lambda'
)
USING DELTA
COMMENT 'Raw GTFS Realtime VehiclePositions from Translink SEQ, ingested via Zerobus'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
