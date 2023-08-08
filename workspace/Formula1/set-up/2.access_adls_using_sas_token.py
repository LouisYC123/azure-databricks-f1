# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using SAS Token
# MAGIC
# MAGIC
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

sas_token = dbutils.secrets.get(scope="formula1-scope", key="formula1-demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1lgdl.dfs.core.windows.net", "SAS")
spark.conf.set(
    "fs.azure.sas.token.provider.type.formula1lgdl.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
)
spark.conf.set("fs.azure.sas.fixed.token.formula1lgdl.dfs.core.windows.net", sas_token)
