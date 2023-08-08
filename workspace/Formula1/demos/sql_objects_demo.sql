-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create a database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE DATABASE demo

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

USE demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a managed table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # switched to python
-- MAGIC
-- MAGIC race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Write the dataframe into a SQL table
-- MAGIC
-- MAGIC # race_results.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT 
  *
FROM 
  demo.race_results_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating a managed table using SQL

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_sql
AS
SELECT
  *
FROM  
  demo.race_results_python
WHERE
  race_year = 2020;

-- COMMAND ----------

SELECT
  *
FROM  
  demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Extrnal managed table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Write the dataframe into a SQL table
-- MAGIC
-- MAGIC race_results.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------


