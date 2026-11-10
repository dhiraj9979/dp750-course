"""Integration test: checks a real silver table in the DEV catalog.

It reads the table written by the Module 2 job (schema m2_job) and checks
its structure and contents. It never touches production data.
"""
import pytest

DEV_SCHEMA = "m2_job"
TABLE = "silver_trips"


def test_dev_silver_table_is_valid(spark):
    catalog = spark.sql("SELECT current_catalog()").first()[0]
    full_name = f"{catalog}.{DEV_SCHEMA}.{TABLE}"

    if not spark.catalog.tableExists(full_name):
        pytest.skip(f"{full_name} not found: run the Module 2 job first")

    df = spark.table(full_name)

    # Structure: the columns downstream steps rely on are present
    assert {"pickup_ts", "trip_distance", "fare_amount", "pickup_zip"} <= set(df.columns)

    # Contents: the table has data, and no invalid fares or distances got through
    assert df.count() > 0
    assert df.filter("fare_amount <= 0 OR trip_distance <= 0").count() == 0
