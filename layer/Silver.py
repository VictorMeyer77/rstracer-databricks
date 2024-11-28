# Databricks notebook source
from pyspark.sql import functions as F, Window

# COMMAND ----------

# MAGIC %run ../Common

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Functions

# COMMAND ----------

def dedup(df, keys):
    window = Window.partitionBy(keys + ["hostname"]).orderBy(F.col("inserted_at").desc())
    rank_df = df.withColumn("row_number", F.row_number().over(window))
    rank_df = rank_df.filter(F.col("row_number") == 1)
    return rank_df.drop("row_number")

# COMMAND ----------

def add_address_host(df, host_df, address_column, column_name):
    df_with_host = df.alias("source").join(
        host_df.alias("host"),
        F.col(f"source.{address_column}") == F.col("host.address"),
        "left"
    )
    return df_with_host.select("source.*", F.col("host.host").alias(column_name))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Dimension

# COMMAND ----------

# dim_network_host

dim_network_host = spark.read.table("bronze.dim_network_host")
dim_network_host = dim_network_host.select("_id", "address", "host").distinct()
dim_network_host.write.mode("overwrite").saveAsTable("silver.dim_network_host")

# COMMAND ----------

# dim_file_reg

dim_file_reg = spark.read.table("bronze.dim_file_reg")
dim_file_reg = dedup(dim_file_reg, ["pid", "fd", "node"])
dim_file_reg.write.mode("overwrite").saveAsTable("silver.dim_file_reg")

# COMMAND ----------

# dim_network_foreign_ip

dim_network_foreign_ip = spark.read.table("bronze.dim_network_foreign_ip")
dim_network_foreign_ip = add_address_host(dim_network_foreign_ip, dim_network_host, "address", "host")
dim_network_foreign_ip = dedup(dim_network_foreign_ip, ["_id"])
dim_network_foreign_ip.write.mode("overwrite").saveAsTable("silver.dim_network_foreign_ip")

# COMMAND ----------

# dim_network_interface

dim_network_interface = spark.read.table("bronze.dim_network_interface")
dim_network_interface = add_address_host(
    dim_network_interface, dim_network_host, "address", "host"
).withColumn("mask", F.col("address")["mask"])
dim_network_interface = dedup(dim_network_interface, ["_id"])
dim_network_interface.write.mode("overwrite").saveAsTable("silver.dim_network_interface")

# COMMAND ----------

# dim_network_open_port

dim_network_open_port = spark.read.table("bronze.dim_network_open_port")
dim_network_open_port = dedup(dim_network_open_port, ["pid", "port"])
dim_network_open_port.write.mode("overwrite").saveAsTable("silver.dim_network_open_port")

# COMMAND ----------

# dim_network_socket

dim_network_socket = spark.read.table("bronze.dim_network_socket")
dim_network_socket = add_address_host(dim_network_socket, dim_network_host, "source_address", "source_host")
dim_network_socket = dedup(dim_network_socket, ["_id"])
dim_network_socket.write.mode("overwrite").saveAsTable("silver.dim_network_socket")

# COMMAND ----------

# dim_process

dim_process = spark.read.table("bronze.dim_process")
dim_process = dedup(dim_process, ["pid", "started_at"])
dim_process.write.mode("overwrite").saveAsTable("silver.dim_process")

# COMMAND ----------

# file_host -> dim_file_host

file_host = spark.read.table("bronze.file_host")
file_host = dedup(file_host, ["name", "address"])
file_host.write.mode("overwrite").saveAsTable("silver.dim_file_host")

# COMMAND ----------

# file_user -> dim_file_user

file_user = spark.read.table("bronze.file_user")
file_user = dedup(file_user, ["name", "uid"])
file_user.write.mode("overwrite").saveAsTable("silver.dim_file_user")

# COMMAND ----------

# file_service -> dim_file_service

file_service = spark.read.table("bronze.file_service")
file_service = dedup(file_service, ["name", "port", "protocol"])
file_service.write.mode("overwrite").saveAsTable("silver.dim_file_service")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Fact

# COMMAND ----------

# fact_file_reg

fact_file_reg = spark.read.table("bronze.fact_file_reg")
fact_file_reg = dedup(fact_file_reg, ["pid", "fd", "node", "created_at"])
fact_file_reg.write.mode("overwrite").saveAsTable("silver.fact_file_reg")

# COMMAND ----------

# fact_network_ip

fact_network_ip = spark.read.table("bronze.fact_network_ip")
fact_network_ip = add_address_host(fact_network_ip, dim_network_host, "source_address", "source_host")
fact_network_ip = add_address_host(fact_network_ip, dim_network_host, "destination_address", "destination_host")
fact_network_ip = dedup(fact_network_ip, ["_id"])
fact_network_ip.write.mode("overwrite").saveAsTable("silver.fact_network_ip")

# COMMAND ----------

# fact_network_packet

fact_network_packet = spark.read.table("bronze.fact_network_packet")
fact_network_packet = dedup(fact_network_packet, ["_id"])
fact_network_packet.write.mode("overwrite").saveAsTable("silver.fact_network_packet")

# COMMAND ----------

# fact_process

fact_process = spark.read.table("bronze.fact_process")
fact_process = dedup(fact_process, ["pid", "started_at", "created_at"])
fact_process.write.mode("overwrite").saveAsTable("silver.fact_process")

# COMMAND ----------

# fact_process_network

fact_process_network = spark.read.table("bronze.fact_process_network")
fact_process_network = dedup(fact_process_network, ["_id"])
fact_process_network.write.mode("overwrite").saveAsTable("silver.fact_process_network")

# COMMAND ----------

# tech_chrono -> fact_tech_chrono

tech_chrono = spark.read.table("bronze.tech_chrono")
tech_chrono = dedup(tech_chrono, ["name"])
tech_chrono.write.mode("overwrite").saveAsTable("silver.fact_tech_chrono")

# COMMAND ----------

# tech_table_count

tech_table_count = spark.read.table("bronze.tech_table_count")
tech_table_count = dedup(tech_table_count, ["_id"])
tech_table_count.write.mode("overwrite").saveAsTable("silver.tech_table_count")
