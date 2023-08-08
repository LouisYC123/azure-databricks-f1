# Databricks notebook source
# MAGIC %md
# MAGIC ### Read and write to Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/formula1lgdl/demo"

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1lgdl/raw/2021-03-28/results.json")

# COMMAND ----------

# Save to sql table using 'delta' format
results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# Save to parquet file location
results_df.write.format("delta").mode("overwrite").save("/mnt/formula1lgdl/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create external table on top of '/demo/results_external'
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA 
# MAGIC LOCATION '/mnt/formula1lgdl/demo/results_external'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

# If you didnt want to read 'demo/results_external' using sql:

result_external_df = spark.read.format("delta").load("/mnt/formula1lgdl/demo/results_external")
display(result_external_df)

# COMMAND ----------

# Save to sql table using partitionBy
results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ### Updates and Deletes on Delta Lake

# COMMAND ----------

"""
Delta Lake supports updates, deletes and merges
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position 
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using Python

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1lgdl/demo/results_managed")

deltaTable.update("position <= 10", { "points": "21 - position"})



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deletes

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using Python

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1lgdl/demo/results_managed")

deltaTable.delete("points = 0")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert using Merge

# COMMAND ----------

"""
A merge statement gives you the ability to insert any new records being received. Update any existing records for which new data has been received. And if you had a delete request, then you can apply that delete as well.
"""

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1lgdl/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1lgdl/raw/2021-03-28/drivers.json") \
.filter("driverId between 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname")
)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1lgdl/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname")
)

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate Date
# MAGIC )
# MAGIC USING DELTA
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC  -- Example
# MAGIC
# MAGIC -- MERGE INTO my_table
# MAGIC -- USING my_new_data
# MAGIC -- ON my_table.uniqueId = my_new_data.uniqueId    <--  condition on which you want to 'merge'
# MAGIC -- WHEN MATCHED
# MAGIC     -- <update records>
# MAGIC -- WHEN NOT MATCHED
# MAGIC --   THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, forename, surname, createdDate)
# MAGIC   VALUES (driverId, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, forename, surname, createdDate)
# MAGIC   VALUES (driverId, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using PySpark

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1lgdl/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
    .whenMatchedUpdate(set = {"forename" : "upd.forename", "surname": "upd.surname", "updatedDate": "current_timestamp()"}) \
    .whenNotMatchedInsert(values = 
        {
            "driverId": "upd.driverId",
            "forename": "upd.forename",
            "surname": "upd.surname",
            "createdDate": "current_timestamp()",
        }
        ) \
.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## History & Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- looking at old data
# MAGIC select * from f1_demo.drivers_merge VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- looking at old data
# MAGIC select * from f1_demo.drivers_merge TIMESTAMP AS OF '2023-08-07T08:29:14.000+0000'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using PySpark

# COMMAND ----------

df = spark.read.format('delta').option('timestampAsOf', '2023-08-07T08:29:14.000+0000').load('/mnt/formula1lgdl/demo/drivers_merge')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete data using VACUUM

# COMMAND ----------

"""
vacuum removes both data and history. vacuum removes the history, which is older than 7 days, but you can change that as well.
 """

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- restoring to a previous version using merge
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 src 
# MAGIC   ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Transaction Log

# COMMAND ----------

"""
Hive Meta Store only keeps the kind of information we looked at previously for a spark table that is like the name of the table and the attributes and all of that kind of information. But the transaction logs themselves are not kept within hive meta store, because it will be really inefficient to go and read hive meta store and get all of that information for everything. So Delta Lake uses some clever method to do that. Its not coming from the hive meta store, but  from the Delta Lake itself , in a folder called _delta_log. This contains a json file with all the relevant logging metadata
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC   driverId INT,
# MAGIC   dob DATE, 
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.table_to_convert

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.'/mnt/formula1lgdl/demo/parquet_to_convert'
