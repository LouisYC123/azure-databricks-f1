# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Built-in aggregate functions

# COMMAND ----------

data = spark.read.parquet(f"{presentation_folder_path}/race_results").filter("race_year = 2020")

display(data)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

data.select(count("*")).show()

# COMMAND ----------

data.select(countDistinct("race_name")).show()

# COMMAND ----------

data.select(sum("points")).show()

# COMMAND ----------

data.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
.withColumnRenamed("sum(points)", "total_points") \
.withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
.show() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### GroupBy

# COMMAND ----------

data.groupBy("driver_name").sum('points').show()

# COMMAND ----------

data.groupBy("driver_name") \
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Functions
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

df = spark.read.parquet(f"{presentation_folder_path}/race_results")

demo_df = df.filter("race_year in (2019, 2020)")

# COMMAND ----------

demo_grouped_df = demo_df \
.groupBy("race_year", "driver_name") \
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df = demo_grouped_df.withColumn("rank", rank().over(driverRankSpec))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------


