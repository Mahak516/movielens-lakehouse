"""Silver layer: typed, validated, de-duplicated data plus a quarantine of rejected rows.

Every cleaning function returns ``(clean_df, rejected_df)``. Rejected rows are never
dropped silently: they land in a quarantine table with the source, the original record
as JSON, and a machine-readable reason, so data problems stay visible and countable.
"""

from __future__ import annotations

import datetime
from typing import List, Tuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

MIN_RATING = 0.5
MAX_RATING = 5.0
MIN_RELEASE_YEAR = 1874  # first films; anything earlier is a parsing artefact
MAX_EPOCH_SECONDS = 4_102_444_800  # 2100-01-01 UTC; larger values are corrupt
NO_GENRES_LABEL = "(no genres listed)"
UNKNOWN_GENRE = "Unknown"

QUARANTINE_COLUMNS = ["source", "raw_record", "reject_reason", "quarantined_at"]


def _try_cast(column: str, dtype: str) -> Column:
    """Cast that yields NULL instead of raising on bad input (works with ANSI mode on or off)."""
    return F.expr(f"try_cast(`{column}` AS {dtype})")


def _to_quarantine(df: DataFrame, source: str, raw_columns: List[str]) -> DataFrame:
    """Shape rejected rows into the common quarantine schema."""
    return df.select(
        F.lit(source).alias("source"),
        F.to_json(F.struct(*[F.col(c) for c in raw_columns])).alias("raw_record"),
        F.col("reject_reason"),
        F.current_timestamp().alias("quarantined_at"),
    )


def clean_ratings(bronze: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Type, validate and de-duplicate ratings.

    Rules (first matching rule decides the reject reason):
      * user_id / movie_id must be integers
      * rating must be numeric, within 0.5..5.0 and a multiple of 0.5
      * timestamp must be a plausible unix epoch (0..year 2100)
      * one row per (user_id, movie_id): the latest rating wins, older ones are quarantined
    """
    raw_columns = ["raw_user_id", "raw_movie_id", "raw_rating", "raw_timestamp"]

    typed = bronze.select(
        F.col("userId").alias("raw_user_id"),
        F.col("movieId").alias("raw_movie_id"),
        F.col("rating").alias("raw_rating"),
        F.col("timestamp").alias("raw_timestamp"),
        _try_cast("userId", "int").alias("user_id"),
        _try_cast("movieId", "int").alias("movie_id"),
        _try_cast("rating", "double").alias("rating"),
        _try_cast("timestamp", "bigint").alias("ts"),
    )

    half_step_ok = (F.col("rating") * 2) == F.floor(F.col("rating") * 2)
    reason = (
        F.when(
            F.col("user_id").isNull() | F.col("movie_id").isNull(),
            F.lit("null_or_invalid_key"),
        )
        .when(F.col("rating").isNull(), F.lit("null_or_invalid_rating"))
        .when(
            (F.col("rating") < MIN_RATING) | (F.col("rating") > MAX_RATING),
            F.lit("rating_out_of_range"),
        )
        .when(~half_step_ok, F.lit("rating_not_half_step"))
        .when(
            F.col("ts").isNull() | (F.col("ts") < 0) | (F.col("ts") > MAX_EPOCH_SECONDS),
            F.lit("timestamp_out_of_range"),
        )
    )

    flagged = typed.withColumn("reject_reason", reason)
    valid = flagged.filter(F.col("reject_reason").isNull())
    invalid = flagged.filter(F.col("reject_reason").isNotNull())

    # Latest rating per (user, movie) wins; ties broken by higher rating for determinism.
    window = Window.partitionBy("user_id", "movie_id").orderBy(
        F.col("ts").desc(), F.col("rating").desc()
    )
    ranked = valid.withColumn("rn", F.row_number().over(window))

    clean = (
        ranked.filter(F.col("rn") == 1)
        .select(
            "user_id",
            "movie_id",
            "rating",
            F.timestamp_seconds("ts").alias("rated_at"),
        )
        .withColumn("rating_date", F.to_date("rated_at"))
    )

    superseded = ranked.filter(F.col("rn") > 1).withColumn(
        "reject_reason", F.lit("duplicate_superseded")
    )
    rejected = _to_quarantine(
        invalid.unionByName(superseded.drop("rn")), "ratings", raw_columns
    )
    return clean, rejected


def clean_movies(bronze: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Parse title/year, split genres, validate and de-duplicate movies.

    MovieLens stores the year inside the title, e.g. ``Toy Story (1995)``. The year is
    only stripped from the title when it is plausible, so ``Blade Runner 2049 (2017)``
    becomes ``Blade Runner 2049`` / 2017 but a garbage ``(2999)`` is left untouched.
    """
    max_year = datetime.date.today().year + 1
    raw_columns = ["raw_movie_id", "raw_title", "raw_genres"]

    typed = bronze.select(
        F.col("movieId").alias("raw_movie_id"),
        F.col("title").alias("raw_title"),
        F.col("genres").alias("raw_genres"),
        _try_cast("movieId", "int").alias("movie_id"),
        F.trim(F.col("title")).alias("title_full"),
        F.trim(F.col("genres")).alias("genres_trimmed"),
    )

    reason = F.when(F.col("movie_id").isNull(), F.lit("null_or_invalid_key")).when(
        F.col("title_full").isNull() | (F.col("title_full") == ""),
        F.lit("missing_title"),
    )
    flagged = typed.withColumn("reject_reason", reason)
    valid = flagged.filter(F.col("reject_reason").isNull())
    invalid = flagged.filter(F.col("reject_reason").isNotNull())

    window = Window.partitionBy("movie_id").orderBy(F.col("title_full").asc())
    ranked = valid.withColumn("rn", F.row_number().over(window))

    year_text = F.regexp_extract(F.col("title_full"), r"\((\d{4})\)\s*$", 1)
    year_candidate = F.when(year_text != "", year_text.cast("int"))
    release_year = F.when(
        year_candidate.between(MIN_RELEASE_YEAR, max_year), year_candidate
    )
    title = F.when(
        release_year.isNotNull(),
        F.trim(F.regexp_replace(F.col("title_full"), r"\s*\(\d{4}\)\s*$", "")),
    ).otherwise(F.col("title_full"))

    no_genres = (
        F.col("genres_trimmed").isNull()
        | (F.col("genres_trimmed") == "")
        | (F.col("genres_trimmed") == NO_GENRES_LABEL)
    )
    genres = F.when(no_genres, F.array(F.lit(UNKNOWN_GENRE))).otherwise(
        F.array_remove(F.split(F.col("genres_trimmed"), r"\|"), "")
    )

    clean = ranked.filter(F.col("rn") == 1).select(
        "movie_id",
        title.alias("title"),
        release_year.alias("release_year"),
        genres.alias("genres"),
    )

    superseded = ranked.filter(F.col("rn") > 1).withColumn(
        "reject_reason", F.lit("duplicate_movie_id")
    )
    rejected = _to_quarantine(
        invalid.unionByName(superseded.drop("rn")), "movies", raw_columns
    )
    return clean, rejected


def explode_genres(movies: DataFrame) -> DataFrame:
    """One row per (movie_id, genre): the bridge table used by the Snowflake star schema."""
    return (
        movies.select("movie_id", F.explode("genres").alias("genre"))
        .withColumn("genre", F.trim(F.col("genre")))
        .filter(F.col("genre") != "")
        .distinct()
    )


def drop_orphan_ratings(
    ratings: DataFrame, movies: DataFrame
) -> Tuple[DataFrame, DataFrame]:
    """Quarantine ratings that reference a movie missing from the movies table."""
    known = movies.select("movie_id").distinct()
    columns = ["user_id", "movie_id", "rating", "rated_at", "rating_date"]

    kept = ratings.join(known, on="movie_id", how="left_semi").select(*columns)
    orphans = ratings.join(known, on="movie_id", how="left_anti").select(*columns)

    rejected = orphans.select(
        F.lit("ratings").alias("source"),
        F.to_json(F.struct("user_id", "movie_id", "rating", "rated_at")).alias(
            "raw_record"
        ),
        F.lit("unknown_movie").alias("reject_reason"),
        F.current_timestamp().alias("quarantined_at"),
    )
    return kept, rejected
