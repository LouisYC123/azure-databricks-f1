# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, when
from pyspark.sql.functions import sum, when, col, count, desc, rank
from pyspark.sql.window import Window 

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'")


# COMMAND ----------

race_year_list = df_column_to_list(race_results, 'race_year')

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructors = race_results.groupBy("team", "race_year") \
    .agg(
        sum(col("points")).alias("total_points"),
        count(when(col("position") == 1, True)).alias("Wins")
        )


# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructors.withColumn("rank", rank().over(constructor_rank_spec))


# COMMAND ----------

display(final_df)

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(
    input_df=final_df,
    db_name="f1_presentation",
    table_name="constructor_standings",
    folder_path=presentation_folder_path,
    merge_condition=merge_condition,
    partition_column="race_year",
    )

# COMMAND ----------

