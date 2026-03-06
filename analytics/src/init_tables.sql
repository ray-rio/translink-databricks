-- Databricks notebook source
-- Initialise Unity Catalog -- Tables & Volume
--
-- Creates the catalog, schema, bronze tables, and GTFS static volume that must
-- exist before the DLT pipeline or Lambda connectors can run.
-- All objects use IF NOT EXISTS so this notebook is safe to re-run.
--
-- Parameters (passed via base_parameters from the DABs job):
--   catalog -- Unity Catalog name
--   schema  -- Schema name
--
-- Run via DABs:
--   databricks bundle run init_catalog -t dev

-- COMMAND ----------

-- Schema (catalog must already exist -- create it via the Databricks UI if needed)

EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :catalog || '.' || :schema;

-- COMMAND ----------

-- Vehicle Positions Table
-- Raw GTFS Realtime VehiclePositions ingested via Zerobus.

EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS ' || :catalog || '.' || :schema || '.raw_vehicle_positions (
  entity_id                 STRING,
  feed_timestamp            BIGINT,
  gtfs_realtime_version     STRING,
  incrementality            STRING,
  trip_id                   STRING,
  route_id                  STRING,
  direction_id              INT,
  start_time                STRING,
  start_date                STRING,
  schedule_relationship     STRING,
  vehicle_id                STRING,
  vehicle_label             STRING,
  license_plate             STRING,
  wheelchair_accessible     STRING,
  latitude                  FLOAT,
  longitude                 FLOAT,
  bearing                   FLOAT,
  odometer                  DOUBLE,
  speed                     FLOAT,
  current_stop_sequence     INT,
  stop_id                   STRING,
  current_status            STRING,
  vehicle_timestamp         BIGINT,
  congestion_level          STRING,
  occupancy_status          STRING,
  occupancy_percentage      INT,
  ingested_at               TIMESTAMP
)
USING DELTA';

-- COMMAND ----------

-- Trip Updates Table
-- Raw GTFS Realtime TripUpdates ingested via Zerobus.
-- One row per StopTimeUpdate per entity.

EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS ' || :catalog || '.' || :schema || '.raw_trip_updates (
  entity_id                   STRING,
  feed_timestamp              BIGINT,
  gtfs_realtime_version       STRING,
  incrementality              STRING,
  trip_id                     STRING,
  route_id                    STRING,
  direction_id                INT,
  start_time                  STRING,
  start_date                  STRING,
  schedule_relationship       STRING,
  vehicle_id                  STRING,
  vehicle_label               STRING,
  trip_delay                  INT,
  trip_timestamp              BIGINT,
  stop_sequence               INT,
  stop_id                     STRING,
  arrival_delay               INT,
  arrival_time                BIGINT,
  arrival_uncertainty         INT,
  departure_delay             INT,
  departure_time              BIGINT,
  departure_uncertainty       INT,
  stop_schedule_relationship  STRING,
  ingested_at                 TIMESTAMP
)
USING DELTA';

-- COMMAND ----------

-- GTFS Static Volume
-- Managed volume where the gtfs-static Lambda uploads daily CSV files.

EXECUTE IMMEDIATE
  'CREATE VOLUME IF NOT EXISTS ' || :catalog || '.' || :schema || '.gtfs_static';

-- COMMAND ----------

SELECT 'Initialisation complete' AS status, :catalog AS catalog, :schema AS schema;
