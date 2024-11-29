# Databricks notebook source
# MAGIC %run ../layer/Common

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE bronze CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE silver CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE gold CASCADE;

# COMMAND ----------

dbutils.fs.rm(CHECKPOINT_PATH, True)
