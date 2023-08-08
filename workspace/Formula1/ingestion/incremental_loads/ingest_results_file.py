# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json

# COMMAND ----------

# MAGIC %md
# MAGIC #### Requirements
# MAGIC  - rename cols into snake case
# MAGIC  - drop status_id
# MAGIC  - create ingestion date col
# MAGIC  - partition the data on write by race_id

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Formula1/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Formula1/includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the data

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", DoubleType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True),
])

# COMMAND ----------

results_df = spark.read.option("header", True).schema(results_schema).json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename cols to snake case and drop status_id

# COMMAND ----------

results_df_renamed = results_df.select(
    col("resultId").alias("result_id"),
    col("raceId").alias("race_id"),
    col("driverId").alias("driver_id"),
    col("constructorId").alias("constructor_id"),
    col("number"),
    col("grid"),
    col("position"),
    col("positionText").alias("position_text"),
    col("positionOrder").alias("position_order"),
    col("points"),
    col("laps"),
    col("time"),
    col("milliseconds"),
    col("fastestLap").alias("fastest_lap"),
    col("rank"),
    col("fastestLapTime").alias("fastest_lap_time"),
    col("fastestLapSpeed").alias("fastest_lap_speed"),
) \
.withColumn("data_source", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - create ingestion date col

# COMMAND ----------

results_df_final = results_df_renamed.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

# de-dup
results_deduped_df = results_df_final.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - partition the data on write by race_id

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"  
merge_delta_data(
    df=results_deduped_df,
    db_name="f1_processed",
    table_name="results",
    folder_path=processed_folder_path,
    merge_condition=merge_condition,
    partition_column="race_id",
    )