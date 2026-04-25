import os

import pyspark
import pytest
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext


def _sedona_jars_bundled() -> bool:
    """Return True if Sedona JARs are already present in PySpark's jars directory (e.g. Docker)."""
    jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
    return any("sedona" in f for f in os.listdir(jars_dir))


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    builder = (
        SedonaContext.builder()
        .appName("IntegrationTests")
        .master("local")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    )

    if not _sedona_jars_bundled():
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-spark-shaded-4.1_2.13:1.9.0,"
            "org.datasyslab:geotools-wrapper:1.9.0-33.5",
        ).config(
            "spark.jars.repositories",
            "https://artifacts.unidata.ucar.edu/repository/unidata-all",
        )

    return builder.getOrCreate()
