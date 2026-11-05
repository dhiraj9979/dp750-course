# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: clean, type, and validate
# MAGIC Casts columns, removes invalid trips, and fails fast if the data looks wrong.
# MAGIC Set the `fail` parameter to `true` to simulate a data-quality failure.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_dp750_course")
dbutils.widgets.text("schema", "m1_notebook")
dbutils.widgets.text("fail", "false")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
simulate_failure = dbutils.widgets.get("fail").lower() == "true"

# COMMAND ----------

from pyspark.sql import functions as F

silver = (
    spark.table(f"{catalog}.{schema}.bronze_trips")
    .select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("pickup_zip").cast("int").alias("pickup_zip"),
    )
    .filter("fare_amount > 0 AND trip_distance > 0")
)

# Fail fast: stop before writing if the data cannot be trusted
if simulate_failure:
    raise ValueError("Simulated data-quality failure in silver: stopping the pipeline")
if silver.limit(1).count() == 0:
    raise ValueError("Silver produced 0 rows: stopping the pipeline")

silver.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_trips")

rows = spark.table(f"{catalog}.{schema}.silver_trips").count()
print(f"Silver rows: {rows}")

# COMMAND ----------

dbutils.notebook.exit(str(rows))
