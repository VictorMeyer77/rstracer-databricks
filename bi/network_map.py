# Databricks notebook source
dbutils.widgets.text("analysis_history", "15", "Analysis History (minutes)")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Import

# COMMAND ----------

# MAGIC %sh sudo apt-get update && sudo apt-get install -y graphviz

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
import graphviz
import matplotlib.image as mpimg
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Network map

# COMMAND ----------

fact_network_map_connection = (
    spark.read.table("gold.fact_network_map_connection")
    .filter(
        F.unix_timestamp("dtk_inserted_at")
        > F.unix_timestamp() - int(dbutils.widgets.get("analysis_history")) * 60
    )
    .na.fill("unknown")
)

fact_network_map_connection = fact_network_map_connection.groupBy(
    "source_network",
    "source_hostname",
    "source_host",
    "destination_network",
    "destination_hostname",
    "destination_host",
).agg(
    F.collect_set("source_port").alias("source_port"),
    F.collect_set("source_pid").alias("source_pid"),
    F.collect_set("source_command").alias("source_command"),
    F.collect_set("destination_port").alias("destination_port"),
    F.collect_set("destination_pid").alias("destination_pid"),
    F.collect_set("destination_command").alias("destination_command"),
    F.date_format(F.max("created_at"), "yyyy/MM/dd HH:mm:ss").alias("time"),
)

fact_network_map_connection = fact_network_map_connection.withColumns(
    {
        "source_id": F.hash("source_network", "source_hostname", "source_host").cast(
            "string"
        ),
        "destination_id": F.hash(
            "destination_network", "destination_hostname", "destination_host"
        ).cast("string"),
        "edge_id": F.monotonically_increasing_id(),
    }
)

# COMMAND ----------

nodes = (
    fact_network_map_connection.select(
        F.col("source_network").alias("network"),
        F.col("source_id").alias("_id"),
        F.concat("source_host", F.lit("\n"), "source_hostname").alias("label"),
    )
    .unionByName(
        fact_network_map_connection.select(
            F.col("destination_network").alias("network"),
            F.col("destination_id").alias("_id"),
            F.concat("destination_host", F.lit("\n"), "destination_hostname").alias(
                "label"
            ),
        )
    )
    .distinct()
)

edges = fact_network_map_connection.select(
    F.col("edge_id").alias("_id"),
    "source_id",
    "destination_id",
    F.concat(
        F.lit("commands: "),
        F.when(F.size("source_command") == 0, F.lit("unknown")).otherwise(
            F.col("source_command").cast("string")
        ),
        F.lit("->"),
        F.when(F.size("destination_command") == 0, F.lit("unknown")).otherwise(
            F.col("destination_command").cast("string")
        ),
        F.lit("\n"),
        F.col("time"),
    ).alias("label"),
)

networks = nodes.select("network").distinct().toPandas()

# COMMAND ----------

graph = graphviz.Digraph(format="png")
graph.attr(bgcolor="white", ranksep="2", nodesep="1")

for i, row in networks.iterrows():

    network = row["network"]
    network_nodes = nodes.filter(F.col("network") == network).toPandas()
    

    with graph.subgraph(name=f"cluster_{i}") as sub:

        for _, row in network_nodes.iterrows():
            sub.node(row["_id"], row["label"], shape="rectangle", color="#50FA7B", style="filled")

        
        sub.attr(label=network)

network_edges = edges.toPandas()
for _, row in network_edges.iterrows():
    graph.edge(row["source_id"], row["destination_id"], color="#282A36", label=str(row["_id"]))

graph.render("network-mapping", format="png", cleanup=True)

img = mpimg.imread(f"network-mapping.png")
plt.imshow(img)
plt.axis("off")
plt.show()

# COMMAND ----------

display(
    fact_network_map_connection.select(
        "edge_id",
        "time",
        "source_network",
        "source_hostname",
        "source_host",
        "source_port",
        "source_pid",
        "source_command",
        "destination_network",
        "destination_hostname",
        "destination_host",
        "destination_port",
        "destination_pid",
        "destination_command",
    ).orderBy("edge_id")
)

# COMMAND ----------

display(spark.read.table("gold.fact_network_map_packet").filter(
    F.unix_timestamp("dtk_inserted_at")
    > F.unix_timestamp() - int(dbutils.widgets.get("analysis_history")) * 60
))
