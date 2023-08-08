# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using Service Principal
# MAGIC
# MAGIC ### Steps to follow
# MAGIC 1. Register Service Principal  (AKA Azure AD Application)
# MAGIC 2. Generate a secret for the Service Principle / AD Application
# MAGIC 3. Configure Databricks to access the storage account via the Service Principal: Set Spark Config with App / Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor to the Data Lake so that the Service Principal can access the data in the Data Lake.

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-app-client-id")
tenant_id = dbutils.secrets.get(scope="formula1-scope", key="formual1-app-tenant-id")
client_secret = dbutils.secrets.get(
    scope="formula1-scope", key="formula1-app-client-secret"
)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1lgdl.dfs.core.windows.net", "OAuth")
spark.conf.set(
    "fs.azure.account.oauth.provider.type.formula1lgdl.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.formula1lgdl.dfs.core.windows.net", client_id
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.formula1lgdl.dfs.core.windows.net",
    client_secret,
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.formula1lgdl.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
)
