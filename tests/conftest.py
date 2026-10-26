import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("movielens-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _string_df(spark, columns, rows):
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def ratings_bronze(spark):
    """Factory: build a Bronze-shaped (all-string) ratings DataFrame from tuples."""

    def build(rows):
        return _string_df(spark, ["userId", "movieId", "rating", "timestamp"], rows)

    return build


@pytest.fixture()
def movies_bronze(spark):
    """Factory: build a Bronze-shaped (all-string) movies DataFrame from tuples."""

    def build(rows):
        return _string_df(spark, ["movieId", "title", "genres"], rows)

    return build
