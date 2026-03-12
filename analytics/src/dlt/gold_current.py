# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Current State Views
# MAGIC
# MAGIC Business-ready tables derived from silver. Provides the latest known
# MAGIC position for each active vehicle.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dlt.table(
    comment="Latest known position per vehicle, enriched with delay status"
)
def gld_current_vehicle_positions():
    cutoff = F.unix_timestamp() - 15 * 60

    # Latest position per vehicle
    vp = dlt.read("slv_vehicle_positions").filter(F.col("feed_timestamp") >= cutoff)
    w_vp = Window.partitionBy("vehicle_id").orderBy(F.col("feed_timestamp").desc())
    latest_vp = (
        vp.withColumn("_rn", F.row_number().over(w_vp))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Latest trip-level delay per trip_id from trip updates
    tu = dlt.read("slv_trip_updates").filter(F.col("feed_timestamp") >= cutoff)
    w_tu = Window.partitionBy("trip_id").orderBy(F.col("feed_timestamp").desc())
    latest_delay = (
        tu.withColumn("_rn", F.row_number().over(w_tu))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            F.col("trip_id").alias("_tu_trip_id"),
            "trip_delay",
            "delay_seconds",
            "delay_category",
        )
    )

    return latest_vp.join(
        latest_delay, latest_vp.trip_id == latest_delay._tu_trip_id, "left"
    ).drop("_tu_trip_id")
