"""Unit tests for clean_trips: small, fast, and independent of any table."""
from src.transforms import clean_trips

COLUMNS = ["tpep_pickup_datetime", "trip_distance", "fare_amount", "pickup_zip"]


def make_trips(spark, rows):
    """Build a tiny raw-trips DataFrame, with every value as a string like the CSV files."""
    return spark.createDataFrame(rows, COLUMNS)


def test_keeps_a_valid_trip(spark):
    raw = make_trips(spark, [("2016-02-13 21:47:53", "1.4", "8.0", "10103")])
    assert clean_trips(raw).count() == 1


def test_drops_fares_of_2_50_or_less(spark):
    raw = make_trips(spark, [
        ("2016-02-13 21:47:53", "1.4", "2.5", "10103"),   # exactly 2.50: invalid
        ("2016-02-13 21:47:53", "1.4", "0.0", "10103"),   # zero fare: invalid
        ("2016-02-13 21:47:53", "1.4", "8.0", "10103"),   # valid
    ])
    result = clean_trips(raw)
    assert result.count() == 1
    assert result.first()["fare_amount"] == 8.0


def test_drops_trips_without_distance_or_zip(spark):
    raw = make_trips(spark, [
        ("2016-02-13 21:47:53", "0.0", "8.0", "10103"),   # zero distance: invalid
        ("2016-02-13 21:47:53", "1.4", "8.0", None),      # missing zip: invalid
    ])
    assert clean_trips(raw).count() == 0


def test_casts_columns_to_the_right_types(spark):
    raw = make_trips(spark, [("2016-02-13 21:47:53", "1.4", "8.0", "10103")])
    types = dict(clean_trips(raw).dtypes)
    assert types == {
        "pickup_ts": "timestamp",
        "trip_distance": "double",
        "fare_amount": "double",
        "pickup_zip": "int",
    }
