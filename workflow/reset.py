# Databricks notebook source
# MAGIC %run ../layer/common

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS bronze CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS silver CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS gold CASCADE;

# COMMAND ----------

dbutils.fs.rm(CHECKPOINT_PATH, True)
