"""Transformation logic for the silver layer.

Keeping the logic in a plain function (DataFrame in, DataFrame out)
makes it easy to unit test without reading or writing any tables.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_trips(df: DataFrame) -> DataFrame:
    """Type the raw taxi columns and keep only valid trips.

    A trip is valid when:
      - the fare is greater than 2.50
      - the distance is greater than 0
      - the pickup zip code is present
    """
    return (
        df.select(
            F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
            F.col("trip_distance").cast("double").alias("trip_distance"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
            F.col("pickup_zip").cast("int").alias("pickup_zip"),
        )
        .filter("fare_amount > 2.5 AND trip_distance > 0 AND pickup_zip IS NOT NULL")
    )
