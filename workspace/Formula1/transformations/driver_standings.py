# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count, desc, rank
from pyspark.sql.window import Window 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find race years for which the data is to be reprocessed

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") \
.select("race_year") \
.distinct() \
.collect() # <- turns into a list

# COMMAND ----------

race_year_list =[]
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)


# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

# Uses 'when' to add conditions to the counting (like a countIf)
driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality") \
.agg(
    sum("points").alias("total_points"),
    count(when(col("position")  == 1, True)).alias("wins")
    )

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(
    input_df=final_df,
    db_name="f1_presentation",
    table_name="driver_standings",
    folder_path=presentation_folder_path,
    merge_condition=merge_condition,
    partition_column="race_year",
    )
