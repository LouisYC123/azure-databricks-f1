-- Databricks notebook source
SELECT 
  driver_name
  , count(1) AS total_races
  , SUM(calculated_points) AS total_points
  , AVG(calculated_points) AS avg_points
  , RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
FROM 
  f1_presentation.calculated_race_results
WHERE 
  race_year BETWEEN 2001 and 2010
GROUP BY
  driver_name
HAVING 
  count(1) >= 50
ORDER BY
  avg_points DESC

-- COMMAND ----------


