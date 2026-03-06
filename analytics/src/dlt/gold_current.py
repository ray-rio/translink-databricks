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
    comment="Latest known position per vehicle"
)
def gld_current_vehicle_positions():
    vp = dlt.read("slv_vehicle_positions")
    w = Window.partitionBy("vehicle_id").orderBy(F.col("feed_timestamp").desc())
    return (
        vp.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
