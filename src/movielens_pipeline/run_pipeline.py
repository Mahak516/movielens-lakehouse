"""Runner: raw CSV -> Bronze -> Silver (+ quarantine) -> quality checks -> Parquet export.

Usage (from the repo root):

    PYTHONPATH=src python -m movielens_pipeline.run_pipeline \
        --input-dir data/ml-25m --output-dir output --format parquet

Exit code is 0 when every data-quality check passes, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame

from . import bronze, quality, silver
from .spark_session import get_spark
from .storage import read_dataset, write_dataset


def _reject_summary(quarantine: DataFrame) -> List[Dict[str, object]]:
    rows = quarantine.groupBy("source", "reject_reason").count().collect()
    return sorted(
        ({"source": r["source"], "reason": r["reject_reason"], "rows": r["count"]} for r in rows),
        key=lambda item: (str(item["source"]), str(item["reason"])),
    )


def run(input_dir: str, output_dir: str, fmt: str = "parquet", export_files: int = 4) -> Dict[str, object]:
    started = time.time()
    spark = get_spark()
    out = Path(output_dir)

    def p(*parts: str) -> str:
        return str(out.joinpath(*parts))

    # ---- Bronze: land raw data as-is -------------------------------------------------
    write_dataset(bronze.build_bronze_ratings(spark, str(Path(input_dir) / "ratings.csv")), p("bronze", "ratings"), fmt)
    write_dataset(bronze.build_bronze_movies(spark, str(Path(input_dir) / "movies.csv")), p("bronze", "movies"), fmt)
    bronze_ratings = read_dataset(spark, p("bronze", "ratings"), fmt)
    bronze_movies = read_dataset(spark, p("bronze", "movies"), fmt)

    # ---- Silver: clean, validate, quarantine -----------------------------------------
    movies_clean, movies_rejected = silver.clean_movies(bronze_movies)
    ratings_clean, ratings_rejected = silver.clean_ratings(bronze_ratings)
    ratings_final, orphans_rejected = silver.drop_orphan_ratings(ratings_clean, movies_clean)
    movie_genres = silver.explode_genres(movies_clean)
    quarantine = (
        ratings_rejected.unionByName(movies_rejected).unionByName(orphans_rejected)
    )

    write_dataset(movies_clean, p("silver", "movies"), fmt)
    write_dataset(ratings_final, p("silver", "ratings"), fmt)
    write_dataset(movie_genres, p("silver", "movie_genres"), fmt)
    write_dataset(quarantine, p("silver", "quarantine"), fmt)

    # ---- Quality checks run on what was actually persisted ---------------------------
    silver_ratings = read_dataset(spark, p("silver", "ratings"), fmt)
    silver_movies = read_dataset(spark, p("silver", "movies"), fmt)
    silver_genres = read_dataset(spark, p("silver", "movie_genres"), fmt)
    results = quality.run_silver_checks(silver_ratings, silver_movies, silver_genres)

    # ---- Export Silver as plain parquet for Snowflake ---------------------------------
    write_dataset(
        silver_ratings.select("user_id", "movie_id", "rating", "rated_at", "rating_date").repartition(export_files),
        p("export", "ratings"),
        "parquet",
    )
    write_dataset(silver_movies.select("movie_id", "title", "release_year").coalesce(1), p("export", "movies"), "parquet")
    write_dataset(silver_genres.coalesce(1), p("export", "movie_genres"), "parquet")

    report: Dict[str, object] = {
        "bronze_ratings_rows": bronze_ratings.count(),
        "bronze_movies_rows": bronze_movies.count(),
        "silver_ratings_rows": silver_ratings.count(),
        "silver_movies_rows": silver_movies.count(),
        "silver_movie_genres_rows": silver_genres.count(),
        "quarantined": _reject_summary(read_dataset(spark, p("silver", "quarantine"), fmt)),
        "checks": [r.as_dict() for r in results],
        "all_checks_passed": all(r.passed for r in results),
        "elapsed_seconds": round(time.time() - started, 1),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "quality_report.json").write_text(json.dumps(report, indent=2, default=str))
    return report


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input-dir", required=True, help="Folder containing ratings.csv and movies.csv")
    parser.add_argument("--output-dir", default="output", help="Where bronze/silver/export folders are written")
    parser.add_argument("--format", choices=["parquet", "delta"], default="parquet")
    parser.add_argument("--export-files", type=int, default=4, help="Number of parquet files for the ratings export")
    args = parser.parse_args(argv)

    report = run(args.input_dir, args.output_dir, args.format, args.export_files)
    print(json.dumps(report, indent=2, default=str))
    if not report["all_checks_passed"]:
        print("Data-quality checks FAILED", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
