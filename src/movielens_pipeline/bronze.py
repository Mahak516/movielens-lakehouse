"""Bronze layer: land the raw files exactly as received.

Design rule: Bronze never rejects or reshapes data. Every source column is kept as a
string (so a malformed value can never fail the read) and audit columns record when and
from which file each row arrived. All typing and validation happens in Silver, where bad
rows are quarantined with a reason instead of silently disappearing.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RATINGS_COLUMNS: List[str] = ["userId", "movieId", "rating", "timestamp"]
MOVIES_COLUMNS: List[str] = ["movieId", "title", "genres"]


def _all_string_schema(columns: List[str]) -> StructType:
    return StructType([StructField(name, StringType(), True) for name in columns])


def read_raw_csv(spark: SparkSession, path: str, columns: List[str]) -> DataFrame:
    """Read a MovieLens CSV with every column as a string.

    The explicit schema avoids a costly inference pass over 25M rows and guarantees the
    read cannot fail on a bad value. `quote`/`escape` handle titles that contain commas
    or doubled quotes.
    """
    return (
        spark.read.option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .schema(_all_string_schema(columns))
        .csv(path)
    )


def add_audit_columns(df: DataFrame) -> DataFrame:
    """Add ingestion timestamp and source file path (lineage)."""
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.col("_metadata.file_path")
    )


def build_bronze_ratings(spark: SparkSession, path: str) -> DataFrame:
    return add_audit_columns(read_raw_csv(spark, path, RATINGS_COLUMNS))


def build_bronze_movies(spark: SparkSession, path: str) -> DataFrame:
    return add_audit_columns(read_raw_csv(spark, path, MOVIES_COLUMNS))
