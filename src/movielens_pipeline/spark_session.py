"""SparkSession factory that works both locally and on Databricks."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "movielens-pipeline") -> SparkSession:
    """Return a SparkSession.

    On Databricks the platform already owns the session and its configuration, so we
    just attach to it. Locally we build a local[*] session with settings that make the
    output deterministic and Snowflake-friendly:

    * UTC session time zone, so unix-epoch -> timestamp conversion is reproducible.
    * TIMESTAMP_MICROS parquet timestamps. Spark's default INT96 is legacy and is not
      read reliably by every downstream engine.
    """
    builder = SparkSession.builder.appName(app_name)

    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        builder = (
            builder.master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .config("spark.ui.enabled", "false")
        )

    return builder.getOrCreate()
