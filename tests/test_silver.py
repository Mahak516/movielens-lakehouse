from pyspark.sql import functions as F

from movielens_pipeline import silver


# ----------------------------------------------------------------------------- ratings


def test_valid_ratings_are_typed_and_timestamps_converted(ratings_bronze):
    # 964982703 is 2000-07-30 18:45:03 UTC
    clean, rejected = silver.clean_ratings(ratings_bronze([("1", "10", "4.0", "964982703")]))

    row = clean.select(
        "user_id",
        "movie_id",
        "rating",
        F.date_format("rated_at", "yyyy-MM-dd HH:mm:ss").alias("rated_at"),
        F.col("rating_date").cast("string").alias("rating_date"),
    ).collect()

    assert len(row) == 1
    assert row[0]["user_id"] == 1
    assert row[0]["movie_id"] == 10
    assert row[0]["rating"] == 4.0
    assert row[0]["rated_at"] == "2000-07-30 18:45:03"
    assert row[0]["rating_date"] == "2000-07-30"
    assert rejected.count() == 0


def test_invalid_ratings_are_quarantined_with_the_right_reason(ratings_bronze):
    rows = [
        ("1", "1", "abc", "100"),   # not numeric
        ("2", "1", "5.5", "100"),   # above range
        ("3", "1", "0.0", "100"),   # below range
        ("4", "1", "3.3", "100"),   # not a half step
        (None, "1", "3.0", "100"),  # missing user
        ("5", "1", "3.0", "-5"),    # impossible timestamp
        ("6", "1", "3.0", "100"),   # the only valid row
    ]
    clean, rejected = silver.clean_ratings(ratings_bronze(rows))

    assert clean.count() == 1
    reasons = sorted(r["reject_reason"] for r in rejected.collect())
    assert reasons == sorted(
        [
            "null_or_invalid_rating",
            "rating_out_of_range",
            "rating_out_of_range",
            "rating_not_half_step",
            "null_or_invalid_key",
            "timestamp_out_of_range",
        ]
    )
    assert set(rejected.columns) == set(silver.QUARANTINE_COLUMNS)


def test_duplicate_user_movie_keeps_latest_rating(ratings_bronze):
    rows = [
        ("1", "10", "3.0", "1000"),
        ("1", "10", "4.5", "2000"),  # newer: should win
        ("1", "11", "2.0", "1500"),
    ]
    clean, rejected = silver.clean_ratings(ratings_bronze(rows))

    kept = {(r["user_id"], r["movie_id"]): r["rating"] for r in clean.collect()}
    assert kept == {(1, 10): 4.5, (1, 11): 2.0}

    superseded = rejected.collect()
    assert len(superseded) == 1
    assert superseded[0]["reject_reason"] == "duplicate_superseded"


def test_every_input_row_is_either_kept_or_quarantined(ratings_bronze):
    rows = [
        ("1", "1", "3.0", "100"),
        ("1", "1", "4.0", "200"),
        ("2", "1", "bad", "100"),
        ("3", "2", "5.0", "100"),
    ]
    clean, rejected = silver.clean_ratings(ratings_bronze(rows))
    assert clean.count() + rejected.count() == len(rows)


# ------------------------------------------------------------------------------ movies


def test_title_year_and_genres_are_parsed(movies_bronze):
    rows = [
        ("1", "Toy Story (1995)", "Adventure|Animation|Children"),
        ("2", "Babylon 5", "(no genres listed)"),
        ("3", "City of Lost Children, The (Cité des enfants perdus, La) (1995)", "Sci-Fi"),
        ("4", "  Heat (1995)  ", "Action|Crime"),
        ("5", "Blade Runner 2049 (2017)", "Sci-Fi"),
        ("6", "Future Film (2999)", "Drama"),
    ]
    clean, rejected = silver.clean_movies(movies_bronze(rows))
    by_id = {r["movie_id"]: r for r in clean.collect()}

    assert rejected.count() == 0
    assert by_id[1]["title"] == "Toy Story" and by_id[1]["release_year"] == 1995
    assert by_id[1]["genres"] == ["Adventure", "Animation", "Children"]

    # no year in the title -> year is NULL, title untouched
    assert by_id[2]["title"] == "Babylon 5" and by_id[2]["release_year"] is None
    assert by_id[2]["genres"] == ["Unknown"]

    # parentheses inside the title must survive; only the trailing year is stripped
    assert by_id[3]["title"] == "City of Lost Children, The (Cité des enfants perdus, La)"
    assert by_id[3]["release_year"] == 1995

    # surrounding whitespace is trimmed
    assert by_id[4]["title"] == "Heat" and by_id[4]["release_year"] == 1995

    # a number inside the title is not mistaken for the year
    assert by_id[5]["title"] == "Blade Runner 2049" and by_id[5]["release_year"] == 2017

    # an implausible year is not trusted and the title is left intact
    assert by_id[6]["title"] == "Future Film (2999)" and by_id[6]["release_year"] is None


def test_bad_movies_are_quarantined(movies_bronze):
    rows = [
        ("1", "Good Movie (2001)", "Drama"),
        (None, "No Id (2001)", "Drama"),
        ("3", "", "Drama"),
        ("x", "Bad Id (2001)", "Drama"),
    ]
    clean, rejected = silver.clean_movies(movies_bronze(rows))

    assert [r["movie_id"] for r in clean.collect()] == [1]
    reasons = sorted(r["reject_reason"] for r in rejected.collect())
    assert reasons == ["missing_title", "null_or_invalid_key", "null_or_invalid_key"]


def test_duplicate_movie_ids_keep_one_row(movies_bronze):
    rows = [("1", "A Movie (2001)", "Drama"), ("1", "B Movie (2002)", "Comedy")]
    clean, rejected = silver.clean_movies(movies_bronze(rows))

    assert clean.count() == 1
    assert [r["reject_reason"] for r in rejected.collect()] == ["duplicate_movie_id"]


def test_explode_genres_produces_one_row_per_pair(movies_bronze):
    rows = [("1", "A (2001)", "Action|Comedy"), ("2", "B (2002)", "(no genres listed)")]
    clean, _ = silver.clean_movies(movies_bronze(rows))
    pairs = {(r["movie_id"], r["genre"]) for r in silver.explode_genres(clean).collect()}

    assert pairs == {(1, "Action"), (1, "Comedy"), (2, "Unknown")}


# --------------------------------------------------------------------- referential check


def test_ratings_for_unknown_movies_are_quarantined(ratings_bronze, movies_bronze):
    movies, _ = silver.clean_movies(movies_bronze([("1", "Known (2001)", "Drama")]))
    ratings, _ = silver.clean_ratings(
        ratings_bronze([("1", "1", "4.0", "100"), ("1", "999", "3.0", "100")])
    )
    kept, orphans = silver.drop_orphan_ratings(ratings, movies)

    assert [r["movie_id"] for r in kept.collect()] == [1]
    rejected = orphans.collect()
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "unknown_movie"
