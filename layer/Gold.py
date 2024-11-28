# Databricks notebook source
# host in same network
# packet exchange in network
# process exchanges

# COMMAND ----------

# MAGIC %run ../Common

# COMMAND ----------

# sources

fact_network_ip = spark.read.table("silver.fact_network_ip")
fact_network_packet = spark.read.table("silver.fact_network_packet")
fact_process_network = spark.read.table("silver.fact_process_network")
dim_process = spark.read.table("silver.dim_process")
dim_network_interface = spark.read.table("silver.dim_network_interface")

# COMMAND ----------

# ip packet with metadata

fact_ip_packet = (
    fact_network_ip.alias("ip")
    .join(
        fact_network_packet.alias("packet"),
        [
            F.col("ip._id") == F.col("packet._id"),
            F.col("ip.hostname") == F.col("packet.hostname"),
        ],
        "inner",
    )
    .select(
        "ip._id",
        "ip.hostname",
        "ip.source_host",
        "ip.source_port",
        "ip.destination_host",
        "ip.destination_port",
        "packet.length",
        "packet.created_at",
        "packet.interface",
    )
)

# COMMAND ----------

# ip packet with process info

network_process = fact_process_network.join(
    dim_process,
    [
        F.col("fact_process_network.pid") == F.col("dim_process.pid"),
        F.col("fact_process_network.hostname") == F.col("dim_process.hostname"),
    ],
    "inner",
).select(
    "fact_process_network.packet_id",
    "fact_process_network.hostname",
    "fact_process_network.pid",
    "dim_process.ppid",
    "dim_process.uid",
    "dim_process.command",
    "dim_process.full_command",
    "dim_process.started_at",
)

# COMMAND ----------

# ip packet with source and destination host

fact_ip_packet_with_host = (
    fact_ip_packet.alias("ip")
    .join(
        dim_network_interface.alias("int"),
        [F.col("ip.source_host") == F.col("int.host")],
        "left",
    )
    .select(
        "ip.*",
        F.col("int.network").alias("source_network"),
        F.col("int.hostname").alias("source_hostname"),
    )
)

fact_ip_packet_with_host = (
    fact_ip_packet_with_host.alias("ip")
    .join(
        dim_network_interface.alias("int"),
        [F.col("ip.destination_host") == F.col("int.host")],
        "left",
    )
    .select(
        "ip.*",
        F.col("int.network").alias("destination_network"),
        F.col("int.hostname").alias("destination_hostname"),
    )
)

# COMMAND ----------

# gold_fact_network_intra_packet

fact_packet_with_process = (
    fact_ip_packet_with_host.filter(
        "source_network == destination_network and source_network is not null and source_host != destination_host"
    )
    .alias("ip")
    .join(
        network_process.alias("net_pro"),
        [
            F.col("ip._id") == F.col("net_pro.packet_id"),
            F.col("ip.hostname") == F.col("net_pro.hostname"),
        ],
        "left",
    )
)

gold_fact_network_intra_packet = fact_packet_with_process.filter(
    F.col("ip.hostname") == F.col("source_hostname")
).select(
    F.col("_id").alias("packet_id"),
    "source_hostname",
    "source_host",
    "source_port",
    F.col("source_network").alias("network"),
    "interface",
    "created_at",
    "length",
    "pid",
    "ppid",
    "uid",
    "command",
    "full_command",
    F.col("started_at").alias("process_started_at"),
    "destination_host",
    "destination_port",
    "destination_hostname",
)

display(gold_fact_network_intra_packet)

# COMMAND ----------

# gold_fact_network_intra_connection

gold_fact_network_intra_connection = (
    gold_fact_network_intra_packet.alias("source")
    .join(
        gold_fact_network_intra_packet.alias("destination"),
        [
            F.col("source.source_host") == F.col("destination.destination_host"),
            F.col("source.source_port") == F.col("destination.destination_port"),
            F.col("source.source_hostname")
            == F.col("destination.destination_hostname"),
            F.col("source.network") == F.col("destination.network"),
            F.abs(
                F.unix_timestamp("source.created_at")
                - F.unix_timestamp("destination.created_at")
            )
            < 1,
        ],
        "inner",
    )
    .select(
        F.col("source.network").alias("network"),
        F.date_trunc("minute", F.col("source.created_at")).alias("created_at"),
        "source.source_hostname",
        "source.source_host",
        "source.source_port",
        F.col("source.interface").alias("source_interface"),
        F.col("source.pid").alias("source_pid"),
        F.col("source.ppid").alias("source_ppid"),
        F.col("source.uid").alias("source_uid"),
        F.col("source.command").alias("source_command"),
        F.col("source.full_command").alias("source_full_command"),
        F.col("source.process_started_at").alias("source_process_started_at"),
        "destination.destination_hostname",
        "destination.destination_host",
        "destination.destination_port",
        F.col("destination.interface").alias("destination_interface"),
        F.col("destination.pid").alias("destination_pid"),
        F.col("destination.ppid").alias("destination_ppid"),
        F.col("destination.uid").alias("destination_uid"),
        F.col("destination.command").alias("destination_command"),
        F.col("destination.full_command").alias("destination_full_command"),
        F.col("destination.process_started_at").alias("destination_process_started_at"),
    )
).distinct()
display(gold_fact_network_intra_connection)
