# Databricks notebook source
# MAGIC %run ../layer/Common

# COMMAND ----------

def get_table_count(schema):
    return spark.sql(f"SHOW TABLES FROM {schema}").filter("isTemporary = false").count()

# COMMAND ----------

# MAGIC %run ../layer/Bronze

# COMMAND ----------

while get_table_count("bronze") != 17:
    time.sleep(5)

# COMMAND ----------

# MAGIC %run ../layer/Silver

# COMMAND ----------

while get_table_count("silver") != 17:
    time.sleep(5)

# COMMAND ----------

# MAGIC %run ../layer/Gold
