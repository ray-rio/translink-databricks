# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Reference — GTFS Static Tables
# MAGIC
# MAGIC Reads CSV files uploaded to the Unity Catalog Volume by the `gtfs-static`
# MAGIC Lambda connector and materialises them as DLT tables for downstream joins.

# COMMAND ----------

import dlt
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

volume_path = spark.conf.get("volume_path")

# ── Schemas ──────────────────────────────────────────────────────────────

AGENCY_SCHEMA = StructType(
    [
        StructField("agency_id", StringType()),
        StructField("agency_name", StringType()),
        StructField("agency_url", StringType()),
        StructField("agency_timezone", StringType()),
        StructField("agency_lang", StringType()),
        StructField("agency_phone", StringType()),
    ]
)

ROUTES_SCHEMA = StructType(
    [
        StructField("route_id", StringType()),
        StructField("agency_id", StringType()),
        StructField("route_short_name", StringType()),
        StructField("route_long_name", StringType()),
        StructField("route_desc", StringType()),
        StructField("route_type", IntegerType()),
        StructField("route_url", StringType()),
        StructField("route_color", StringType()),
        StructField("route_text_color", StringType()),
    ]
)

STOPS_SCHEMA = StructType(
    [
        StructField("stop_id", StringType()),
        StructField("stop_code", StringType()),
        StructField("stop_name", StringType()),
        StructField("stop_desc", StringType()),
        StructField("stop_lat", DoubleType()),
        StructField("stop_lon", DoubleType()),
        StructField("zone_id", StringType()),
        StructField("stop_url", StringType()),
        StructField("location_type", IntegerType()),
        StructField("parent_station", StringType()),
        StructField("platform_code", StringType()),
    ]
)

TRIPS_SCHEMA = StructType(
    [
        StructField("route_id", StringType()),
        StructField("service_id", StringType()),
        StructField("trip_id", StringType()),
        StructField("trip_headsign", StringType()),
        StructField("trip_short_name", StringType()),
        StructField("direction_id", IntegerType()),
        StructField("block_id", StringType()),
        StructField("shape_id", StringType()),
        StructField("wheelchair_accessible", IntegerType()),
    ]
)

CALENDAR_SCHEMA = StructType(
    [
        StructField("service_id", StringType()),
        StructField("monday", IntegerType()),
        StructField("tuesday", IntegerType()),
        StructField("wednesday", IntegerType()),
        StructField("thursday", IntegerType()),
        StructField("friday", IntegerType()),
        StructField("saturday", IntegerType()),
        StructField("sunday", IntegerType()),
        StructField("start_date", StringType()),
        StructField("end_date", StringType()),
    ]
)

CALENDAR_DATES_SCHEMA = StructType(
    [
        StructField("service_id", StringType()),
        StructField("date", StringType()),
        StructField("exception_type", IntegerType()),
    ]
)


# ── DLT Tables ───────────────────────────────────────────────────────────


def _read_csv(schema, filename):
    """Read a GTFS static CSV file from the Volume with an explicit schema."""
    return (
        spark.read.format("csv")
        .option("header", True)
        .schema(schema)
        .load(f"{volume_path}/{filename}")
    )


@dlt.table(comment="GTFS static agency reference data")
def raw_agency():
    return _read_csv(AGENCY_SCHEMA, "agency.txt")


@dlt.table(comment="GTFS static routes reference data")
def raw_routes():
    return _read_csv(ROUTES_SCHEMA, "routes.txt")


@dlt.table(comment="GTFS static stops reference data")
def raw_stops():
    return _read_csv(STOPS_SCHEMA, "stops.txt")


@dlt.table(comment="GTFS static trips reference data")
def raw_trips():
    return _read_csv(TRIPS_SCHEMA, "trips.txt")


@dlt.table(comment="GTFS static calendar reference data")
def raw_calendar():
    return _read_csv(CALENDAR_SCHEMA, "calendar.txt")


@dlt.table(comment="GTFS static calendar date exceptions")
def raw_calendar_dates():
    return _read_csv(CALENDAR_DATES_SCHEMA, "calendar_dates.txt")
