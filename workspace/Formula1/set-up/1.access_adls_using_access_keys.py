# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using access keys
# MAGIC
# MAGIC
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(
    scope="formula1-scope", key="formula1dl-account-key"
)

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1lgdl.dfs.core.windows.net", formula1dl_account_key
)
