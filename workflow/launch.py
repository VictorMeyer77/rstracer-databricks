# Databricks notebook source
# MAGIC %run ../layer/common

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Function

# COMMAND ----------

def get_table_count(schema):
    return spark.sql(f"SHOW TABLES FROM {schema}").filter("isTemporary = false").count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bronze

# COMMAND ----------

# MAGIC %run ../layer/bronze

# COMMAND ----------

while get_table_count("bronze") != 17:
    time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Silver

# COMMAND ----------

# MAGIC %run ../layer/silver

# COMMAND ----------

while get_table_count("silver") != 17:
    time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Gold

# COMMAND ----------

# MAGIC %run ../layer/gold/network_intra

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from gold.fact_network_intra_connection

# COMMAND ----------


