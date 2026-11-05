# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Orchestrator: run bronze, silver, and gold in order
# MAGIC Each step runs only if the previous one succeeded. If a child notebook fails,
# MAGIC `dbutils.notebook.run` raises an error here and the remaining steps never start.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_dp750_course")
dbutils.widgets.text("schema", "m1_notebook")
dbutils.widgets.text("fail_silver", "false")

params = {
    "catalog": dbutils.widgets.get("catalog"),
    "schema": dbutils.widgets.get("schema"),
}

# COMMAND ----------

bronze_rows = dbutils.notebook.run("./01_bronze", 600, params)
print(f"Step 1 complete: bronze wrote {bronze_rows} rows")

silver_rows = dbutils.notebook.run("./02_silver", 600, {**params, "fail": dbutils.widgets.get("fail_silver")})
print(f"Step 2 complete: silver wrote {silver_rows} rows")

gold_rows = dbutils.notebook.run("./03_gold", 600, params)
print(f"Step 3 complete: gold wrote {gold_rows} rows")
