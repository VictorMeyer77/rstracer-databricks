# Databricks notebook source
# MAGIC %run ./common

# COMMAND ----------

spark.conf.set("spark.sql.streaming.schemaInference", "true")

# COMMAND ----------

RAW_TABLES = [
    "dim_file_reg",
    "dim_network_foreign_ip",
    "dim_network_interface",
    "dim_network_open_port",
    "dim_network_socket",
    "dim_network_host",
    "dim_process",
    "fact_file_reg",
    "fact_network_ip",
    "fact_network_packet",
    "fact_process",
    "fact_process_network",
    "file_host",
    "file_service",
    "file_user",
    "tech_chrono",
    "tech_table_count",
]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Function

# COMMAND ----------

def add_meta_columns(df):
    return (
        df.withColumn("file_name", F.col("_metadata.file_name"))
        .withColumn("file_size", F.col("_metadata.file_size"))
        .withColumn(
            "hostname",
            F.substring(
                F.col("_metadata.file_name"),
                0,
                F.length(F.col("_metadata.file_name")) - 23,
            ),
        )
        .withColumn(
            "date_sent",
            F.to_date(
                F.substring(
                    F.col("_metadata.file_name"),
                    F.length(F.col("_metadata.file_name")) - 21,
                    14,
                ),
                "yyyyMMddHHmmss",
            ),
        )
        .withColumn("dtk_inserted_at", F.current_timestamp())
    )

# COMMAND ----------

def raw_to_bronze(table):
    df = spark.readStream.parquet(f"{RAW_PATH}/{table}")
    df = add_meta_columns(df)
    df = df.distinct()
    df.writeStream.outputMode("append").option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/bronze_{table}",
    ).toTable(f"bronze.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run

# COMMAND ----------

for table in RAW_TABLES:
    raw_to_bronze(table)
