import pytest

from movielens_pipeline import quality, silver


def _ratings(spark, rows):
    return spark.createDataFrame(
        rows, "user_id int, movie_id int, rating double, rated_at timestamp, rating_date date"
    )


def test_unique_check_detects_duplicates(spark):
    df = spark.createDataFrame([(1, 1), (1, 1), (2, 1)], "user_id int, movie_id int")
    result = quality.check_unique("dup", df, ["user_id", "movie_id"])
    assert not result.passed
    assert "duplicated_keys=1" in result.detail


def test_unique_check_passes_on_unique_keys(spark):
    df = spark.createDataFrame([(1, 1), (1, 2), (2, 1)], "user_id int, movie_id int")
    assert quality.check_unique("ok", df, ["user_id", "movie_id"]).passed


def test_no_nulls_check(spark):
    df = spark.createDataFrame([(1, "a"), (2, None)], "id int, title string")
    assert not quality.check_no_nulls("nulls", df, ["id", "title"]).passed
    assert quality.check_no_nulls("nulls", df, ["id"]).passed


def test_null_rate_threshold(spark):
    df = spark.createDataFrame([(1, 2000), (2, None), (3, 2001), (4, 2002)], "id int, year int")
    assert quality.check_null_rate("rate", df, "year", max_rate=0.30).passed
    assert not quality.check_null_rate("rate", df, "year", max_rate=0.10).passed


def test_empty_table_fails_null_rate_and_min_rows(spark):
    df = spark.createDataFrame([], "id int, year int")
    assert not quality.check_null_rate("rate", df, "year", max_rate=1.0).passed
    assert not quality.check_min_rows("rows", df, 1).passed


def test_range_check(spark):
    df = spark.createDataFrame([(1, 4.5), (2, 6.0)], "id int, rating double")
    result = quality.check_range("range", df, "rating", 0.5, 5.0)
    assert not result.passed
    assert "rows_outside[0.5,5.0]=1" in result.detail


def test_referential_check(spark):
    parent = spark.createDataFrame([(1,), (2,)], "movie_id int")
    child_ok = spark.createDataFrame([(1,), (2,), (1,)], "movie_id int")
    child_bad = spark.createDataFrame([(1,), (3,)], "movie_id int")

    assert quality.check_referential("fk", child_ok, parent, "movie_id").passed
    bad = quality.check_referential("fk", child_bad, parent, "movie_id")
    assert not bad.passed and "orphan_rows=1" in bad.detail


def test_full_silver_contract_passes_on_clean_data(spark, ratings_bronze, movies_bronze):
    movies, _ = silver.clean_movies(
        movies_bronze([("1", "A (2001)", "Drama|Comedy"), ("2", "B (2002)", "Action")])
    )
    ratings, _ = silver.clean_ratings(
        ratings_bronze([("1", "1", "4.0", "1000"), ("1", "2", "3.5", "1000"), ("2", "1", "5.0", "1000")])
    )
    ratings, _ = silver.drop_orphan_ratings(ratings, movies)
    genres = silver.explode_genres(movies)

    results = quality.run_silver_checks(ratings, movies, genres)
    assert all(r.passed for r in results), [r for r in results if not r.passed]
    quality.assert_all_passed(results)  # must not raise


def test_full_silver_contract_catches_a_broken_table(spark, ratings_bronze, movies_bronze):
    movies, _ = silver.clean_movies(movies_bronze([("1", "A (2001)", "Drama")]))
    genres = silver.explode_genres(movies)
    # Deliberately skip cleaning: a duplicate pair and an orphan movie_id slip through.
    broken = _ratings(
        spark,
        [
            (1, 1, 4.0, None, None),
            (1, 1, 4.0, None, None),
            (2, 99, 3.0, None, None),
        ],
    )

    results = quality.run_silver_checks(broken, movies, genres)
    failed = {r.name for r in results if not r.passed}
    assert {"ratings.unique_user_movie", "ratings.movie_fk", "ratings.no_null_columns"} <= failed
    with pytest.raises(quality.QualityCheckError):
        quality.assert_all_passed(results)
