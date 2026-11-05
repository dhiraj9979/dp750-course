# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: ingest raw taxi files
# MAGIC Reads every CSV file in the landing volume and stores it as a raw Delta table, adding ingestion metadata.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_dp750_course")
dbutils.widgets.text("schema", "m1_notebook")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

from pyspark.sql import functions as F

raw = (
    spark.read
    .option("header", True)
    .option("recursiveFileLookup", True)
    .csv(f"/Volumes/{catalog}/dp750/landing/taxi/")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

raw.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_trips")

rows = spark.table(f"{catalog}.{schema}.bronze_trips").count()
print(f"Bronze rows: {rows}")

# COMMAND ----------

dbutils.notebook.exit(str(rows))
