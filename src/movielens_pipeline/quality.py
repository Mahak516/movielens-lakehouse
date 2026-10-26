"""Data-quality checks run against the persisted Silver tables.

Each check returns a ``CheckResult`` instead of raising, so a run can report every
problem at once. ``assert_all_passed`` turns failures into a non-zero exit for CI/jobs.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class QualityCheckError(Exception):
    """Raised when one or more data-quality checks fail."""


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str

    def as_dict(self) -> Dict[str, object]:
        return asdict(self)


def check_min_rows(name: str, df: DataFrame, minimum: int = 1) -> CheckResult:
    rows = df.count()
    return CheckResult(name, rows >= minimum, f"rows={rows}, required>={minimum}")


def check_no_nulls(name: str, df: DataFrame, columns: List[str]) -> CheckResult:
    condition = None
    for column in columns:
        part = F.col(column).isNull()
        condition = part if condition is None else (condition | part)
    bad = df.filter(condition).count()
    return CheckResult(name, bad == 0, f"rows_with_nulls={bad} in {columns}")


def check_null_rate(
    name: str, df: DataFrame, column: str, max_rate: float
) -> CheckResult:
    total = df.count()
    if total == 0:
        return CheckResult(name, False, "table is empty")
    nulls = df.filter(F.col(column).isNull()).count()
    rate = nulls / total
    return CheckResult(
        name, rate <= max_rate, f"null_rate={rate:.4f}, allowed<={max_rate}"
    )


def check_unique(name: str, df: DataFrame, key_columns: List[str]) -> CheckResult:
    duplicated_keys = (
        df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
    )
    return CheckResult(
        name, duplicated_keys == 0, f"duplicated_keys={duplicated_keys} on {key_columns}"
    )


def check_range(
    name: str, df: DataFrame, column: str, low: float, high: float
) -> CheckResult:
    bad = df.filter((F.col(column) < low) | (F.col(column) > high)).count()
    return CheckResult(name, bad == 0, f"rows_outside[{low},{high}]={bad}")


def check_referential(
    name: str, child: DataFrame, parent: DataFrame, key: str
) -> CheckResult:
    parent_keys = parent.select(key).distinct()
    orphans = child.join(parent_keys, on=key, how="left_anti").count()
    return CheckResult(name, orphans == 0, f"orphan_rows={orphans} on {key}")


def run_silver_checks(
    ratings: DataFrame, movies: DataFrame, movie_genres: DataFrame
) -> List[CheckResult]:
    """The full Silver contract. Keep in sync with the Snowflake checks in 06_data_quality.sql."""
    return [
        check_min_rows("ratings.min_rows", ratings),
        check_no_nulls(
            "ratings.no_null_columns",
            ratings,
            ["user_id", "movie_id", "rating", "rated_at"],
        ),
        check_unique("ratings.unique_user_movie", ratings, ["user_id", "movie_id"]),
        check_range("ratings.rating_range", ratings, "rating", 0.5, 5.0),
        check_referential("ratings.movie_fk", ratings, movies, "movie_id"),
        check_min_rows("movies.min_rows", movies),
        check_unique("movies.unique_movie_id", movies, ["movie_id"]),
        check_no_nulls("movies.no_null_columns", movies, ["movie_id", "title"]),
        check_null_rate("movies.release_year_null_rate", movies, "release_year", 0.05),
        check_referential("movie_genres.movie_fk", movie_genres, movies, "movie_id"),
        check_unique("movie_genres.unique_pair", movie_genres, ["movie_id", "genre"]),
    ]


def assert_all_passed(results: List[CheckResult]) -> None:
    failed = [r for r in results if not r.passed]
    if failed:
        lines = "\n".join(f"  - {r.name}: {r.detail}" for r in failed)
        raise QualityCheckError(f"{len(failed)} data-quality check(s) failed:\n{lines}")
