# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# Uses 'when' to add conditions to the counting (like a countIf)
driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(
    sum("points").alias("total_points"),
    count(when(col("position")  == 1, True)).alias("wins")
    )

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------


