# Databricks notebook source
# MAGIC %run ../layer/common

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Function

# COMMAND ----------

def get_table_count(schema):
    return spark.sql(f"SHOW TABLES FROM {schema}").filter("isTemporary = false").count()

# COMMAND ----------

def get_raw_table_count():
    return len(dbutils.fs.ls(RAW_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bronze

# COMMAND ----------

while get_raw_table_count() != 17:
    time.sleep(5)

# COMMAND ----------

# MAGIC %run ../layer/bronze

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Silver

# COMMAND ----------

while get_table_count("bronze") != 17:
    time.sleep(5)

# COMMAND ----------

# MAGIC %run ../layer/silver

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Gold

# COMMAND ----------

while get_table_count("silver") != 17:
    time.sleep(5)

# COMMAND ----------

# MAGIC %run ../layer/gold/network_map
