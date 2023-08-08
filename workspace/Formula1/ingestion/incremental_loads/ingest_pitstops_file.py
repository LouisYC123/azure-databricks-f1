# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pitsops.json

# COMMAND ----------

# When you're processing a multi-line JSON, you need to explicitly tell Spark that it is a multi-line JSON. 

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
# MAGIC #### Step 1 - Read the JSON file 

# COMMAND ----------

pitstops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False ),
    StructField("driverId", IntegerType(), True ),
    StructField("stop", StringType(), True ),
    StructField("lap", IntegerType(), True ),
    StructField("time", StringType(), True ),
    StructField("duration", StringType(), True ),
    StructField("milliseconds", IntegerType(), True ),
])

pitstops_df = spark.read.schema(pitstops_schema).option("multiline", True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columnss and add new columns

# COMMAND ----------

final_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processes container in parquet format

# COMMAND ----------

# Composite Key
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"  
merge_delta_data(
    df=final_df,
    db_name="f1_processed",
    table_name="pit_stops",
    folder_path=processed_folder_path,
    merge_condition=merge_condition,
    partition_column="race_id",
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops

# COMMAND ----------



# COMMAND ----------

