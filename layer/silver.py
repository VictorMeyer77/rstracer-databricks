# Databricks notebook source
# MAGIC %run ./common

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Function

# COMMAND ----------

def dedup(df, keys):
    window = Window.partitionBy(keys).orderBy(F.col("inserted_at").desc())
    rank_df = df.withColumn("row_number", F.row_number().over(window))
    rank_df = rank_df.filter(F.col("row_number") == 1)
    return rank_df.drop("row_number")

# COMMAND ----------

def add_address_host(df, address_column, column_name):
    dim_network_host = (
        spark.read.table("bronze.dim_network_host")
        .select("_id", "address", "host")
        .distinct()
    )
    df_with_host = df.alias("source").join(
        dim_network_host.alias("host"),
        F.col(f"source.{address_column}") == F.col("host.address"),
        "left",
    )
    return df_with_host.select("source.*", F.col("host.host").alias(column_name))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Dimension

# COMMAND ----------

# dim_network_host

dim_network_host = spark.readStream.table("bronze.dim_network_host")
dim_network_host_table_keys = ["_id", "address", "host"]


def process_batch_dim_network_host(batch_df, batch_id):
    batch_df = batch_df.select("_id", "address", "host").distinct()
    streaming_merge_table(batch_df, "silver.dim_network_host", dim_network_host_table_keys)


(
    dim_network_host.writeStream.outputMode("update")
    .foreachBatch(process_batch_dim_network_host)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_dim_network_host",
    )
    .start()
)

# COMMAND ----------

# dim_file_reg

dim_file_reg = spark.readStream.table("bronze.dim_file_reg")
dim_file_reg_table_keys = ["pid", "fd", "node", "hostname"]


def process_batch_dim_file_reg(batch_df, batch_id):
    batch_df = dedup(batch_df, dim_file_reg_table_keys)
    streaming_merge_table(batch_df, "silver.dim_file_reg", dim_file_reg_table_keys)


(
    dim_file_reg.writeStream.outputMode("update")
    .foreachBatch(process_batch_dim_file_reg)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_dim_file_reg",
    )
    .start()
)

# COMMAND ----------

# dim_network_foreign_ip

dim_network_foreign_ip = spark.readStream.table("bronze.dim_network_foreign_ip")
dim_network_foreign_ip_table_keys = ["_id", "hostname"]


def process_batch_dim_network_foreign_ip(batch_df, batch_id):
    batch_df = add_address_host(batch_df, "address", "host")
    batch_df = dedup(batch_df, dim_network_foreign_ip_table_keys)
    streaming_merge_table(batch_df, "silver.dim_network_foreign_ip", dim_network_foreign_ip_table_keys)


(
    dim_network_foreign_ip.writeStream.outputMode("update")
    .foreachBatch(process_batch_dim_network_foreign_ip)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_dim_network_foreign_ip",
    )
    .start()
)

# COMMAND ----------

# dim_network_interface

dim_network_interface = spark.readStream.table("bronze.dim_network_interface")
dim_network_interface_table_keys = ["_id", "hostname"]


def process_batch_dim_network_interface(batch_df, batch_id):
    batch_df = add_address_host(batch_df, "address", "host").withColumn(
        "mask", F.col("address")["mask"]
    )
    batch_df = dedup(batch_df, dim_network_interface_table_keys)
    streaming_merge_table(batch_df, "silver.dim_network_interface", dim_network_interface_table_keys)


(
    dim_network_interface.writeStream.outputMode("update")
    .foreachBatch(process_batch_dim_network_interface)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_dim_network_interface",
    )
    .start()
)

# COMMAND ----------

# dim_network_open_port

dim_network_open_port = spark.readStream.table("bronze.dim_network_open_port")
dim_network_open_port_table_keys = ["pid", "port", "hostname"]


def process_batch_dim_network_open_port(batch_df, batch_id):
    batch_df = dedup(batch_df, dim_network_open_port_table_keys)
    streaming_merge_table(batch_df, "silver.dim_network_open_port", dim_network_open_port_table_keys)


(
    dim_network_open_port.writeStream.outputMode("update")
    .foreachBatch(process_batch_dim_network_open_port)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_dim_network_open_port",
    )
    .start()
)

# COMMAND ----------

# dim_network_socket

dim_network_socket = spark.readStream.table("bronze.dim_network_socket")
dim_network_socket_table_keys = ["_id", "hostname"]


def process_batch_dim_network_socket(batch_df, batch_id):
    batch_df = add_address_host(batch_df, "source_address", "source_host")
    batch_df = dedup(batch_df, dim_network_socket_table_keys)
    streaming_merge_table(batch_df, "silver.dim_network_socket", dim_network_socket_table_keys)


(
    dim_network_socket.writeStream.outputMode("update")
    .foreachBatch(process_batch_dim_network_socket)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_dim_network_socket",
    )
    .start()
)

# COMMAND ----------

# dim_process

dim_process = spark.readStream.table("bronze.dim_process")
dim_process_table_keys = ["pid", "started_at", "hostname"]


def process_batch_dim_process(batch_df, batch_id):
    batch_df = dedup(batch_df, dim_process_table_keys)
    streaming_merge_table(batch_df, "silver.dim_process", dim_process_table_keys)


(
    dim_process.writeStream.outputMode("update")
    .foreachBatch(process_batch_dim_process)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_dim_process",
    )
    .start()
)

# COMMAND ----------

# file_host -> dim_file_host

file_host = spark.readStream.table("bronze.file_host")
dim_file_host_table_keys = ["name", "address", "hostname"]


def process_batch_dim_file_host(batch_df, batch_id):
    batch_df = dedup(batch_df, dim_file_host_table_keys)
    streaming_merge_table(batch_df, "silver.dim_file_host", dim_file_host_table_keys)


(
    file_host.writeStream.outputMode("update")
    .foreachBatch(process_batch_dim_file_host)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_dim_file_host",
    )
    .start()
)

# COMMAND ----------

# file_user -> dim_file_user

file_user = spark.readStream.table("bronze.file_user")
dim_file_user_table_keys = ["name", "uid", "hostname"]


def process_batch_dim_file_user(batch_df, batch_id):
    batch_df = dedup(batch_df, dim_file_user_table_keys)
    streaming_merge_table(batch_df, "silver.dim_file_user", dim_file_user_table_keys)


(
    file_user.writeStream.outputMode("update")
    .foreachBatch(process_batch_dim_file_user)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_dim_file_user",
    )
    .start()
)

# COMMAND ----------

# file_service -> dim_file_service

file_service = spark.readStream.table("bronze.file_service")
file_service_table_keys = ["name", "port", "protocol", "hostname"]


def process_batch_dim_file_service(batch_df, batch_id):
    batch_df = dedup(batch_df, file_service_table_keys)
    streaming_merge_table(batch_df, "silver.dim_file_service", file_service_table_keys)


(
    file_service.writeStream.outputMode("update")
    .foreachBatch(process_batch_dim_file_service)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_dim_file_service",
    )
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Fact

# COMMAND ----------

# fact_file_reg

fact_file_reg = spark.readStream.table("bronze.fact_file_reg")
fact_file_reg_table_keys = ["pid", "fd", "node", "created_at", "hostname"]

def process_batch_fact_file_reg(batch_df, batch_id):
    batch_df = dedup(batch_df, fact_file_reg_table_keys)
    streaming_merge_table(batch_df, "silver.fact_file_reg", fact_file_reg_table_keys)

(
    fact_file_reg.writeStream.outputMode("update")
    .foreachBatch(process_batch_fact_file_reg)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_fact_file_reg",
    )
    .start()
)

# COMMAND ----------

# fact_network_ip

fact_network_ip = spark.readStream.table("bronze.fact_network_ip")
fact_network_ip_table_keys = ["_id", "hostname"]


def process_batch_fact_network_ip(batch_df, batch_id):
    batch_df = add_address_host(batch_df, "source_address", "source_host")
    batch_df = add_address_host(batch_df, "destination_address", "destination_host")
    batch_df = dedup(batch_df, fact_network_ip_table_keys)
    streaming_merge_table(batch_df, "silver.fact_network_ip", fact_network_ip_table_keys)


(
    fact_network_ip.writeStream.outputMode("update")
    .foreachBatch(process_batch_fact_network_ip)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_fact_network_ip",
    )
    .start()
)

# COMMAND ----------

# fact_network_packet

fact_network_packet = spark.readStream.table("bronze.fact_network_packet")
fact_network_packet_table_keys = ["_id", "hostname"]


def process_batch_fact_network_packet(batch_df, batch_id):
    batch_df = dedup(batch_df, fact_network_packet_table_keys)
    streaming_merge_table(batch_df, "silver.fact_network_packet", fact_network_packet_table_keys)


(
    fact_network_packet.writeStream.outputMode("update")
    .foreachBatch(process_batch_fact_network_packet)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_fact_network_packet",
    )
    .start()
)

# COMMAND ----------

# fact_process

fact_process = spark.readStream.table("bronze.fact_process")
fact_process_table_keys = ["pid", "started_at", "created_at", "hostname"]


def process_batch_fact_process(batch_df, batch_id):
    batch_df = dedup(batch_df, fact_process_table_keys)
    streaming_merge_table(batch_df, "silver.fact_process", fact_process_table_keys)


(
    fact_process.writeStream.outputMode("update")
    .foreachBatch(process_batch_fact_process)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_fact_process",
    )
    .start()
)

# COMMAND ----------

# fact_process_network

fact_process_network = spark.readStream.table("bronze.fact_process_network")
fact_process_network_table_keys = ["_id", "hostname"]


def process_batch_fact_process_network(batch_df, batch_id):
    batch_df = dedup(batch_df, fact_process_network_table_keys)
    streaming_merge_table(batch_df, "silver.fact_process_network", fact_process_network_table_keys)


(
    fact_process_network.writeStream.outputMode("update")
    .foreachBatch(process_batch_fact_process_network)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/silver_fact_process_network",
    )
    .start()
)

# COMMAND ----------

# tech_chrono -> fact_tech_chrono

tech_chrono = spark.readStream.table("bronze.tech_chrono")
fact_tech_chrono_table_keys = ["name", "hostname"]


def process_batch_fact_tech_chrono(batch_df, batch_id):
    batch_df = dedup(batch_df, fact_tech_chrono_table_keys)
    streaming_merge_table(batch_df, "silver.fact_tech_chrono", fact_tech_chrono_table_keys)


(
    tech_chrono.writeStream.outputMode("update")
    .foreachBatch(process_batch_fact_tech_chrono)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/fact_tech_chrono",
    )
    .start()
)

# COMMAND ----------

# tech_table_count -> fact_tech_table_count

tech_table_count = spark.readStream.table("bronze.tech_table_count")
fact_tech_table_count_table_keys = ["_id", "hostname"]


def process_batch_tech_table_count(batch_df, batch_id):
    batch_df = dedup(batch_df, fact_tech_table_count_table_keys)
    streaming_merge_table(batch_df, "silver.fact_tech_table_count", fact_tech_table_count_table_keys)


(
    tech_table_count.writeStream.outputMode("update")
    .foreachBatch(process_batch_tech_table_count)
    .option(
        "checkpointLocation",
        f"{CHECKPOINT_PATH}/fact_tech_table_count",
    )
    .start()
)
