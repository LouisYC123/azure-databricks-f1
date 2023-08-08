# Databricks notebook source
# MAGIC %md
# MAGIC # Join Ingested data into single table

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %run "/Formula1/includes/configuration"

# COMMAND ----------

presentation_folder_path

# COMMAND ----------

# MAGIC %run "/Formula1/includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# We are renaming columns here so that we dont get conflicts when we merge tables that have columns of the same name
# (Spark doesnt have the _suffixes that pandas does)

drivers = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality")   

# COMMAND ----------

constructors = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team")

# COMMAND ----------

circuits = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races = spark.read.format("delta").load(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results = spark.read.format("delta").load(f"{processed_folder_path}/results") \
.filter(f"data_source = '{v_file_date}'") \
.withColumnRenamed("time","race_time") \
.withColumnRenamed("race_id","result_race_id") \
.withColumnRenamed("data_source", "result_file_date")

# COMMAND ----------

race_circuits = races.join(circuits, races.circuit_id==circuits.circuit_id) \
.select(races.race_id, races.race_year, races.race_name,races.race_date, circuits.circuit_location)

# COMMAND ----------

race_results = results.join(race_circuits, results.result_race_id==race_circuits.race_id) \
.join(drivers, results.driver_id == drivers.driver_id) \
.join(constructors, results.constructor_id == constructors.constructor_id)

# COMMAND ----------

final = race_results.select(
    "race_id",
    "race_year", 
    "race_name", 
    "race_date", 
    "circuit_location", 
    "driver_name", 
    "driver_number", 
    "driver_nationality", 
    "team", 
    "fastest_lap", 
    "race_time", 
    "points",
    "position",
    "result_file_date"
    ) \
.withColumn("created_date", current_timestamp()) \
.withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

final = final.orderBy(final.points.desc())

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(
    input_df=final,
    db_name="f1_presentation",
    table_name="race_results",
    folder_path=presentation_folder_path,
    merge_condition=merge_condition,
    partition_column="race_id",
    )


# COMMAND ----------



# COMMAND ----------

