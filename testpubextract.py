import logging

import pytest
from pyspark.sql import SparkSession

def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""

    spark = (SparkSession
             .builder
             .master('local[2]')
             .config('spark.jars.packages', 'com.databricks:spark-avro_2.11:3.0.1')
             .appName('pytest-pyspark-local-testing')
             .enableHiveSupport()
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark

def test_isolated_pub():
	""" TODO write a testcase"""

	##assert results == expected_results

def test_least_num_pubBylocal_auth():
	""" TODO write a testcase"""

	##assert results == expected_results

def test_topCommonwords():
	""" TODO write a testcase"""

	##assert results == expected_results

def test_topstreet_pub():
	""" TODO write a testcase"""

	##assert results == expected_results