# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Enriched Real-Time Data
# MAGIC
# MAGIC Reads bronze VehiclePositions and TripUpdates (rolling 2-hour window),
# MAGIC deduplicates, and enriches with GTFS static reference data.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")

# ── Delay classification thresholds (seconds) ───────────────────────────
EARLY_THRESHOLD_SECS = -60
ON_TIME_THRESHOLD_SECS = 60
LATE_THRESHOLD_SECS = 300


# ── Helpers ──────────────────────────────────────────────────────────────


def _bronze(table):
    """Read a bronze table by name (full scan)."""
    return spark.read.table(f"{catalog}.{schema}.{table}")


def _dedup(df, partition_cols):
    """Keep the latest ingested row per natural key."""
    w = Window.partitionBy(*partition_cols).orderBy(F.col("ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _ref_routes():
    """Routes reference with standard projection."""
    return dlt.read("raw_routes").select(
        "route_id", "route_short_name", "route_long_name", "route_type"
    )


def _ref_trips():
    """Trips reference with standard projection."""
    return dlt.read("raw_trips").select(
        F.col("trip_id"),
        F.col("trip_headsign"),
        F.col("direction_id").alias("trip_direction_id"),
    )


# ── Silver Vehicle Positions ─────────────────────────────────────────────


@dlt.table(
    comment="Deduplicated and enriched vehicle positions (rolling 2h window)"
)
def slv_vehicle_positions():
    vp = _dedup(_bronze("raw_vehicle_positions"), ["entity_id", "feed_timestamp"])

    routes = _ref_routes()
    stops = dlt.read("raw_stops").select("stop_id", "stop_name", "stop_lat", "stop_lon")
    trips = _ref_trips()

    return (
        vp.join(routes, "route_id", "left")
        .join(stops, "stop_id", "left")
        .join(trips, "trip_id", "left")
        .withColumn(
            "speed_kmh",
            F.when(F.col("speed").isNotNull(), F.round(F.col("speed") * 3.6, 1)),
        )
        .withColumn(
            "feed_time", F.from_unixtime(F.col("feed_timestamp")).cast("timestamp")
        )
        .withColumn(
            "vehicle_time",
            F.from_unixtime(F.col("vehicle_timestamp")).cast("timestamp"),
        )
        .select(
            # Keys
            "entity_id",
            "feed_timestamp",
            "feed_time",
            # Trip
            "trip_id",
            "route_id",
            "route_short_name",
            "route_long_name",
            "route_type",
            "trip_headsign",
            "direction_id",
            "start_time",
            "start_date",
            "schedule_relationship",
            # Vehicle
            "vehicle_id",
            "vehicle_label",
            # Position
            "latitude",
            "longitude",
            "bearing",
            "speed",
            "speed_kmh",
            # Stop
            "current_stop_sequence",
            "stop_id",
            "stop_name",
            F.col("stop_lat").alias("stop_latitude"),
            F.col("stop_lon").alias("stop_longitude"),
            "current_status",
            # Status
            "congestion_level",
            "occupancy_status",
            "vehicle_time",
            "ingested_at",
        )
    )


# ── Silver Trip Updates ──────────────────────────────────────────────────


@dlt.table(
    comment="Deduplicated and enriched trip updates (rolling 2h window)"
)
def slv_trip_updates():
    tu = _dedup(
        _bronze("raw_trip_updates"),
        ["entity_id", "feed_timestamp", "stop_sequence"],
    )

    routes = _ref_routes()
    stops = dlt.read("raw_stops").select("stop_id", "stop_name")
    trips = _ref_trips()

    return (
        tu.join(routes, "route_id", "left")
        .join(stops, "stop_id", "left")
        .join(trips, "trip_id", "left")
        .withColumn(
            "delay_seconds",
            F.coalesce(F.col("arrival_delay"), F.col("departure_delay")),
        )
        .withColumn(
            "delay_category",
            F.when(F.col("delay_seconds").isNull(), "UNKNOWN")
            .when(F.col("delay_seconds") <= EARLY_THRESHOLD_SECS, "EARLY")
            .when(F.col("delay_seconds") <= ON_TIME_THRESHOLD_SECS, "ON_TIME")
            .when(F.col("delay_seconds") <= LATE_THRESHOLD_SECS, "LATE")
            .otherwise("VERY_LATE"),
        )
        .withColumn(
            "feed_time", F.from_unixtime(F.col("feed_timestamp")).cast("timestamp")
        )
        .select(
            # Keys
            "entity_id",
            "feed_timestamp",
            "feed_time",
            # Trip
            "trip_id",
            "route_id",
            "route_short_name",
            "route_long_name",
            "route_type",
            "trip_headsign",
            "direction_id",
            "start_time",
            "start_date",
            "schedule_relationship",
            # Vehicle
            "vehicle_id",
            "vehicle_label",
            # Trip-level delay
            "trip_delay",
            # Stop prediction
            "stop_sequence",
            "stop_id",
            "stop_name",
            "arrival_delay",
            "arrival_time",
            "departure_delay",
            "departure_time",
            "stop_schedule_relationship",
            # Enriched
            "delay_seconds",
            "delay_category",
            "ingested_at",
        )
    )
