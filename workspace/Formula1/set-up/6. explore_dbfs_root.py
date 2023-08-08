# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Explore DBFS Root
# MAGIC
# MAGIC 1. List all the folders in DBFS root
# MAGIC 2. Interact with DBFS File Browser
# MAGIC 3. Upload file to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC results is where Databricks keeps any temporary outputs during execution. For example, even the output from this display command is kept within the databricks-results folder.

# COMMAND ----------

