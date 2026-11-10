# Databricks notebook source
# MAGIC %md
# MAGIC # Run the test suite
# MAGIC Runs every test under `tests/` with pytest, on the attached cluster.
# MAGIC - Unit tests use small in-memory DataFrames.
# MAGIC - Integration tests read real tables in the dev catalog.

# COMMAND ----------

import sys
import pytest

# Don't write __pycache__ folders into the Git folder
sys.dont_write_bytecode = True

exit_code = pytest.main(["tests", "-v", "-p", "no:cacheprovider"])
assert exit_code == 0, "Some tests failed: see the output above"
