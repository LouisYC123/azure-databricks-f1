-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT 
  SPLIT(name, ' ')
FROM 
  drivers

-- COMMAND ----------

SELECT 
  SPLIT(name, ' ')[0] AS forename
  , SPLIT(name, ' ')[1] surname
FROM 
  drivers

-- COMMAND ----------

select * from drivers

-- COMMAND ----------

SELECT 
  date_format(dob, 'dd-MM-yyyy') -- format a date into the format you want
  , date_add(dob, 1) -- add one day to a dob
FROM 
  drivers

-- COMMAND ----------

-- MAGIC %md ## Window functions

-- COMMAND ----------

SELECT
  nationality
  , name
  , rank() OVER(PARTITION BY nationality ORDER BY driver_id DESC) as ranked_id
FROM 
 drivers

-- COMMAND ----------


