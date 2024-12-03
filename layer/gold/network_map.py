# Databricks notebook source
import threading

# COMMAND ----------

# MAGIC %run ../common

# COMMAND ----------

WATERMARK_MINUTES = 10
MICRO_BATCH_FREQUENCY = 30

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Function

# COMMAND ----------

def read_table_window(table):
    df = spark.read.table(table)
    return df.filter(
        F.unix_timestamp("dtk_inserted_at")
        > F.unix_timestamp() - WATERMARK_MINUTES * 60
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold.fact_network_map_packet

# COMMAND ----------

# ip packet with metadata


def fact_ip_packet(fact_network_ip):
    fact_network_packet = read_table_window("silver.fact_network_packet")
    return (
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

# ip packet with source and destination host


def fact_packet_with_host(fact_ip_packet):
    dim_network_interface = read_table_window("silver.dim_network_interface")
    fact_ip_packet_with_host = (
        fact_ip_packet.alias("ip")
        .join(
            dim_network_interface.alias("int"),
            [
                F.col("ip.source_host") == F.col("int.host"),
            ],
            "left",
        )
        .filter(F.col("ip.hostname") == F.col("int.hostname"))
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
            [
                F.col("ip.destination_host") == F.col("int.host"),
            ],
            "left",
        )
        .select(
            "ip.*",
            F.col("int.network").alias("destination_network"),
            F.col("int.hostname").alias("destination_hostname"),
        )
    )

    return fact_ip_packet_with_host.filter(
        "source_network is not null and destination_network is not null and source_host != destination_host"
    )

# COMMAND ----------

# ip packet with process info


def fact_packet_with_process(fact_ip_packet):
    fact_process_network = read_table_window("silver.fact_process_network")
    dim_process = read_table_window("silver.dim_process")

    packet_with_process = (
        fact_ip_packet.alias("ip")
        .join(
            fact_process_network.alias("net_pro"),
            [
                F.col("ip._id") == F.col("net_pro.packet_id"),
                F.col("ip.hostname") == F.col("net_pro.hostname"),
            ],
            "left",
        )
        .select("ip.*", "net_pro.pid")
    )

    packet_with_process = (
        packet_with_process.alias("ip")
        .join(
            dim_process.alias("pro"),
            [
                F.col("ip.pid") == F.col("pro.pid"),
                F.col("ip.hostname") == F.col("pro.hostname"),
            ],
            "left",
        )
        .select(
            "ip.*",
            "pro.ppid",
            "pro.uid",
            "pro.command",
            "pro.full_command",
            F.col("started_at").alias("process_started_at"),
        )
    )

    return packet_with_process

# COMMAND ----------

# ensure join time consistency


def dedup_fact_packet_with_process(fact_packet_with_process):
    fact_packet_without_process = fact_packet_with_process.filter(
        "process_started_at is null"
    )
    fact_packet_with_process = fact_packet_with_process.filter(
        "process_started_at is not null and process_started_at < created_at"
    )
    window = Window.partitionBy(["_id", "pid"]).orderBy(
        F.col("process_started_at").asc()
    )
    rank_df = fact_packet_with_process.withColumn(
        "row_number", F.row_number().over(window)
    )
    rank_df = rank_df.filter(F.col("row_number") == 1)
    return rank_df.drop("row_number").unionByName(fact_packet_without_process)

# COMMAND ----------

# gold_fact_network_map_packet -> each row is a source ip packet with a network host destination


def gold_fact_network_map_packet(fact_packet_with_process):
    return fact_packet_with_process.select(
        F.col("_id").alias("packet_id"),
        "source_hostname",
        "source_host",
        "source_port",
        "source_network",
        "interface",
        "created_at",
        "length",
        "pid",
        "ppid",
        "uid",
        "command",
        "full_command",
        "process_started_at",
        "destination_host",
        "destination_port",
        "destination_network",
        "destination_hostname",
        F.current_timestamp().alias("dtk_inserted_at"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## gold.fact_network_map_connection

# COMMAND ----------

# gold_fact_network_map_connection -> each row is a connection between two network hosts, by minutes


def gold_fact_network_map_connection(gold_fact_network_map_packet):
    fact_ip_source_with_dest = (
        gold_fact_network_map_packet.alias("source").join(
            gold_fact_network_map_packet.alias("destination"),
            [
                F.col("source.source_host") == F.col("destination.destination_host"),
                F.col("source.source_port") == F.col("destination.destination_port"),
                F.col("source.source_hostname")
                == F.col("destination.destination_hostname"),
                F.date_trunc("minute", F.col("source.created_at"))
                == F.date_trunc("minute", F.col("source.created_at")),
            ],
            "inner",
        )
    ).withColumn("created_at_trunc", F.date_trunc("minute", F.col("source.created_at")))

    window = Window.partitionBy(
        [
            "created_at_trunc",
            "source.source_network",
            "source.source_host",
            "source.source_port",
            "destination.source_host",
            "destination.source_port",
            "destination.source_network",
        ]
    ).orderBy(F.col("created_at_trunc").asc())
    rank_df = fact_ip_source_with_dest.withColumn(
        "row_number", F.row_number().over(window)
    )
    rank_df = rank_df.filter(F.col("row_number") == 1)

    return rank_df.select(
        F.col("created_at_trunc").alias("created_at"),
        "source.source_network",
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
        F.col("destination.source_network").alias("destination_network"),
        F.col("destination.source_hostname").alias("destination_hostname"),
        F.col("destination.source_host").alias("destination_host"),
        F.col("destination.source_port").alias("destination_port"),
        F.col("destination.interface").alias("destination_interface"),
        F.col("destination.pid").alias("destination_pid"),
        F.col("destination.ppid").alias("destination_ppid"),
        F.col("destination.uid").alias("destination_uid"),
        F.col("destination.command").alias("destination_command"),
        F.col("destination.full_command").alias("destination_full_command"),
        F.col("destination.process_started_at").alias("destination_process_started_at"),
        "source.dtk_inserted_at",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run

# COMMAND ----------

def process_batch_gold_network_map():
    fact_network_ip_df = read_table_window("silver.fact_network_ip")
    fact_ip_packet_df = fact_ip_packet(fact_network_ip_df)
    fact_packet_with_host_df = fact_packet_with_host(fact_ip_packet_df)
    fact_packet_with_process_df = fact_packet_with_process(fact_packet_with_host_df)
    dedup_fact_packet_with_process_df = dedup_fact_packet_with_process(
        fact_packet_with_process_df
    )
    gold_fact_network_map_packet_df = gold_fact_network_map_packet(
        dedup_fact_packet_with_process_df
    )
    gold_fact_network_map_connection_df = gold_fact_network_map_connection(
        gold_fact_network_map_packet_df
    )
    streaming_merge_table(
        gold_fact_network_map_packet_df,
        "gold.fact_network_map_packet",
        ["packet_id", "source_hostname"],
    )
    streaming_merge_table(
        gold_fact_network_map_connection_df,
        "gold.fact_network_map_connection",
        [
            "created_at",
            "source_network",
            "source_host",
            "source_port",
            "destination_host",
            "destination_port",
            "destination_network",
        ],
    )

# COMMAND ----------

def run():
    last_run = 0
    while True:
        if time.time() > last_run + MICRO_BATCH_FREQUENCY:
            start = time.time()
            process_batch_gold_network_map()
            duration = time.time() - start
            if duration > MICRO_BATCH_FREQUENCY:
                print(
                    f"WARNING: batch duration longer than frequency: f{duration} s > {MICRO_BATCH_FREQUENCY} s"
                )
            else:
                time.sleep(MICRO_BATCH_FREQUENCY - duration)
            last_run = time.time()
        else:
            time.sleep(1)

# COMMAND ----------

network_map_thread = threading.Thread(target=run)
network_map_thread.start()
print("Network map thread started")
