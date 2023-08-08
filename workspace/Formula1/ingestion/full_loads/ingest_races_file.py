# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, to_timestamp, concat, lit, current_timestamp

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

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename the columns as required

# COMMAND ----------

races_df_renamed = races_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("year","race_year") \
.withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Add ingestion date to the dataframe

# COMMAND ----------

races_df_renamed = races_df_renamed.withColumn(
    "ingestion_date",
    current_timestamp()
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Create the race_timestamp column

# COMMAND ----------

races_df_renamed = races_df_renamed.withColumn(
    "race_timestamp",
    to_timestamp(concat(col('date'), lit(' '), col('time')),
                 'yyyy-MM-dd HH:mm:ss'
                 )
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5  - Select the required columns

# COMMAND ----------

races_df_final = races_df_renamed.select(
    col("race_id"),
    col("race_year"),
    col("round"),
    col("circuit_id"),
    col("name"),
    col("ingestion_date"),
    col("race_timestamp"),
) \
.withColumn("data_source", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Write to datalake as parquet

# COMMAND ----------

races_df_final.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")