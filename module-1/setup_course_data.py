# Databricks notebook source
# MAGIC %md
# MAGIC # Course setup: data for the Module 1 demos
# MAGIC Attach this notebook to any running compute, then:
# MAGIC - Run **cells 1 and 2 once** before starting the Module 1 demos.
# MAGIC - Run **cell 3** whenever a demo asks you to land a new file.
# MAGIC - Run **cell 4** only if you want to start Module 1 again from scratch.

# COMMAND ----------

# MAGIC %md ## 1. Create the schemas and the landing volume

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog()").first()[0]
print(f"Using catalog: {catalog}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.dp750")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.dp750.landing")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.m1_declarative")
print("Ready: schemas dp750 and m1_declarative, and volume dp750.landing")

# COMMAND ----------

# MAGIC %md ## 2. Land the first batch of taxi files

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog()").first()[0]
(spark.table("samples.nyctaxi.trips").limit(5000)
    .write.mode("overwrite").option("header", True)
    .csv(f"/Volumes/{catalog}/dp750/landing/taxi/batch_001"))
print("First batch landed: batch_001 (5,000 rows)")

# COMMAND ----------

# MAGIC %md ## 3. Land a new file (run when a demo asks you to)

# COMMAND ----------

from datetime import datetime

catalog = spark.sql("SELECT current_catalog()").first()[0]
batch = datetime.now().strftime("batch_%Y%m%d_%H%M%S")
(spark.table("samples.nyctaxi.trips").limit(5000)
    .write.mode("overwrite").option("header", True)
    .csv(f"/Volumes/{catalog}/dp750/landing/taxi/{batch}"))
print(f"New file landed: {batch} (5,000 rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reset Module 1 (optional)
# MAGIC **First delete** any Module 1 job and pipeline you created (Jobs & Pipelines → ⋮ → Delete),
# MAGIC because a pipeline's tables can't be dropped while the pipeline exists.
# MAGIC Then run this cell, followed by cells 1 and 2 again.

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog()").first()[0]
for schema in ["m1_notebook", "m1_declarative", "dp750"]:
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
    print(f"Dropped {catalog}.{schema}")
print("Module 1 reset. Run cells 1 and 2 to set it up again.")
