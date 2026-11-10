"""Shared pytest fixtures.

Placing this file at the repository root also lets the tests import `src`.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Return the Spark session (the cluster's session when run on Databricks)."""
    return SparkSession.builder.getOrCreate()
