"""Tiny read/write helpers so the pipeline can target parquet locally and Delta on Databricks."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

SUPPORTED_FORMATS = ("parquet", "delta")


def _check_format(fmt: str) -> None:
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format '{fmt}'. Use one of {SUPPORTED_FORMATS}.")


def write_dataset(df: DataFrame, path: str, fmt: str = "parquet", mode: str = "overwrite") -> None:
    """Write a DataFrame to `path` in the given format."""
    _check_format(fmt)
    df.write.mode(mode).format(fmt).save(path)


def read_dataset(spark: SparkSession, path: str, fmt: str = "parquet") -> DataFrame:
    """Read a dataset previously written by `write_dataset`."""
    _check_format(fmt)
    return spark.read.format(fmt).load(path)
