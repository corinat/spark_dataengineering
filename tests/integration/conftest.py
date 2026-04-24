import pytest
from sedona.spark import SedonaContext
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    return SedonaContext.builder().appName("IntegrationTests").master("local").getOrCreate()
