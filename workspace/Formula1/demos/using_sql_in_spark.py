# Databricks notebook source
# MAGIC %md
# MAGIC ## Access dataframes using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### Objectives
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

"""
Spark gives you two options to use SQL. One is to create a temporary view, and the other one is the global view.
"""

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

# Create a view on top of the dataframe
race_results.createOrReplaceTempView("v_race_results") # 'v_race_results' is the name you give to the view
# This view is only available within a session as its a temp view


# COMMAND ----------

# using the sql magic command to use a sql cell:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# Running sql from a python cell

p_race_year = '2019'
race_results_2019 = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")

display(race_results_2019)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Global Temp Views

# COMMAND ----------

""" Global temp views can be accessed in other notebooks """

# COMMAND ----------

# Create a view on top of the dataframe
race_results.createOrReplaceGlobalTempView("gv_race_results") # 'v_race_results' is the name you give to the view

# COMMAND ----------

""" 
Databricks saves this in a database called global_temp
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------


