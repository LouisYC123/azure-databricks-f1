-- Databricks notebook source
SELECT 
  team_name
  , count(1) AS total_races
  , SUM(calculated_points) AS total_points
  , AVG(calculated_points) AS avg_points
FROM 
  f1_presentation.calculated_race_results
WHERE 
  race_year BETWEEN 2010 and 2020
GROUP BY
  team_name
HAVING 
  count(1) >= 100
ORDER BY
  avg_points DESC

-- COMMAND ----------


