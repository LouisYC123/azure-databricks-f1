# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying folder

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

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False ),
    StructField("raceId", IntegerType(), True ),
    StructField("driverId", IntegerType(), True ),
    StructField("constructorId", IntegerType(), True ),
    StructField("number", IntegerType(), True ),
    StructField("position", IntegerType(), True ),
    StructField("q1", StringType(), True ),
    StructField("q2", StringType(), True ),
    StructField("q3", StringType(), True ),
])

qualifying_df = spark.read.schema(qualifying_schema).option("multiline", True).json(f"{raw_folder_path}/{v_file_date}/qualifying") # specifying a folder here, not files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processes container in parquet format

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id"  
merge_delta_data(
    df=final_df,
    db_name="f1_processed",
    table_name="qualifying",
    folder_path=processed_folder_path,
    merge_condition=merge_condition,
    partition_column="race_id",
    )