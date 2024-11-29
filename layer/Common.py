# Databricks notebook source
# import

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
import time

# COMMAND ----------

# storage credentials

STORAGE_ACCOUNT = "devrstracersto"
secret = dbutils.secrets.get(scope="azure-key-vault-rstracer", key="sto-secret-key")
app_id = dbutils.secrets.get(scope="azure-key-vault-rstracer", key="sto-app-id")
tenant_id = dbutils.secrets.get(scope="azure-key-vault-rstracer", key="sto-tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", app_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------


# schema

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# COMMAND ----------

# streaming

CHECKPOINT_PATH = f"abfss://rstracer@{STORAGE_ACCOUNT}.dfs.core.windows.net/conf/.checkpoint"
