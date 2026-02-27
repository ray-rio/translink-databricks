-- =============================================================================
-- DDL: Raw layer table for GTFS Realtime TripUpdates
-- Source: Translink SEQ — https://gtfsrt.api.translink.com.au/api/realtime/SEQ/TripUpdates
-- Proto ref: https://github.com/google/transit/blob/master/gtfs-realtime/proto/gtfs-realtime.proto
--
-- Each row represents one StopTimeUpdate within a TripUpdate entity.
-- Trip-level fields are repeated per row for easy querying.
-- =============================================================================

-- Set these before running
DECLARE OR REPLACE catalog_name STRING DEFAULT 'your_catalog';
DECLARE OR REPLACE schema_name  STRING DEFAULT 'your_schema';

CREATE TABLE IF NOT EXISTS IDENTIFIER(catalog_name || '.' || schema_name || '.trip_updates') (

  -- FeedEntity
  entity_id                   STRING        COMMENT 'FeedEntity.id — unique identifier within the feed',

  -- FeedHeader metadata
  feed_timestamp              BIGINT        COMMENT 'FeedHeader.timestamp — POSIX time when the feed was created',
  gtfs_realtime_version       STRING        COMMENT 'FeedHeader.gtfs_realtime_version',
  incrementality              STRING        COMMENT 'FeedHeader.incrementality — FULL_DATASET or DIFFERENTIAL',

  -- TripDescriptor
  trip_id                     STRING        COMMENT 'TripDescriptor.trip_id',
  route_id                    STRING        COMMENT 'TripDescriptor.route_id',
  direction_id                INT           COMMENT 'TripDescriptor.direction_id — 0 or 1',
  start_time                  STRING        COMMENT 'TripDescriptor.start_time — HH:MM:SS format',
  start_date                  STRING        COMMENT 'TripDescriptor.start_date — YYYYMMDD format',
  schedule_relationship       STRING        COMMENT 'TripDescriptor.ScheduleRelationship — SCHEDULED, CANCELED, etc.',

  -- VehicleDescriptor
  vehicle_id                  STRING        COMMENT 'VehicleDescriptor.id — internal system identifier',
  vehicle_label               STRING        COMMENT 'VehicleDescriptor.label — user-visible label',

  -- Trip-level delay
  trip_delay                  INT           COMMENT 'TripUpdate.delay — overall trip delay in seconds (positive=late, negative=early)',
  trip_timestamp              BIGINT        COMMENT 'TripUpdate.timestamp — POSIX time of last progress measurement',

  -- Per-stop prediction (one row per StopTimeUpdate)
  stop_sequence               INT           COMMENT 'StopTimeUpdate.stop_sequence — index from GTFS stop_times.txt',
  stop_id                     STRING        COMMENT 'StopTimeUpdate.stop_id — from GTFS stops.txt',
  arrival_delay               INT           COMMENT 'StopTimeEvent.delay — arrival delay in seconds',
  arrival_time                BIGINT        COMMENT 'StopTimeEvent.time — predicted arrival POSIX time',
  arrival_uncertainty         INT           COMMENT 'StopTimeEvent.uncertainty — arrival prediction uncertainty in seconds',
  departure_delay             INT           COMMENT 'StopTimeEvent.delay — departure delay in seconds',
  departure_time              BIGINT        COMMENT 'StopTimeEvent.time — predicted departure POSIX time',
  departure_uncertainty       INT           COMMENT 'StopTimeEvent.uncertainty — departure prediction uncertainty in seconds',
  stop_schedule_relationship  STRING        COMMENT 'StopTimeUpdate.ScheduleRelationship — SCHEDULED, SKIPPED, NO_DATA, UNSCHEDULED',

  -- Ingestion metadata
  ingested_at                 TIMESTAMP     COMMENT 'UTC timestamp when the record was ingested by the Lambda'
)
USING DELTA
COMMENT 'Raw GTFS Realtime TripUpdates from Translink SEQ, ingested via Zerobus. One row per StopTimeUpdate per entity.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
