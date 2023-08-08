# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest laptimes folder

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %run "/Formula1/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Formula1/includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the set of csv files

# COMMAND ----------

laptimes_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False ),
    StructField("driverId", IntegerType(), True ),
    StructField("lap", IntegerType(), True ),
    StructField("position", IntegerType(), True ),
    StructField("time", StringType(), True ),
    StructField("milliseconds", IntegerType(), True ),
])

laptimes_df = spark.read.schema(laptimes_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times") # specifying a folder here, not files
# You can also use wildcards here - i.e - lap_times_split*.csv

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns

# COMMAND ----------

final_df = laptimes_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processes container in parquet format

# COMMAND ----------

# Composite Key
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"  
merge_delta_data(
    df=final_df,
    db_name="f1_processed",
    table_name="lap_times",
    folder_path=processed_folder_path,
    merge_condition=merge_condition,
    partition_column="race_id",
    )