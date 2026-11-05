# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: business-ready aggregates
# MAGIC Trips and average fare by pickup zip code.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_dp750_course")
dbutils.widgets.text("schema", "m1_notebook")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql import functions as F

gold = (
    spark.table(f"{catalog}.{schema}.silver_trips")
    .groupBy("pickup_zip")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    )
)

gold.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_fares_by_zip")

rows = spark.table(f"{catalog}.{schema}.gold_fares_by_zip").count()
print(f"Gold rows: {rows}")
display(spark.table(f"{catalog}.{schema}.gold_fares_by_zip").orderBy(F.desc("trips")).limit(10))

# COMMAND ----------

dbutils.notebook.exit(str(rows))
