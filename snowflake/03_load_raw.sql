-- =====================================================================================
-- 03_load_raw.sql  |  Upload the Spark export to the stage, then COPY INTO the RAW tables
--
-- Step A: upload the parquet files produced by the pipeline (output/export/...).
--   Option 1 (UI): Snowsight -> Data -> Databases -> MOVIELENS -> RAW -> Stages ->
--                  STG_SILVER -> "+ Files". Upload the ratings files under the path
--                  "ratings", the movies file under "movies", genres under "movie_genres".
--   Option 2 (SnowSQL CLI; PUT does not work in Snowsight worksheets):
--     PUT file:///ABSOLUTE/PATH/output/export/ratings/part-*.parquet      @MOVIELENS.RAW.STG_SILVER/ratings/      AUTO_COMPRESS=FALSE;
--     PUT file:///ABSOLUTE/PATH/output/export/movies/part-*.parquet       @MOVIELENS.RAW.STG_SILVER/movies/       AUTO_COMPRESS=FALSE;
--     PUT file:///ABSOLUTE/PATH/output/export/movie_genres/part-*.parquet @MOVIELENS.RAW.STG_SILVER/movie_genres/ AUTO_COMPRESS=FALSE;
--
-- Step B: run this script.
-- =====================================================================================

USE WAREHOUSE MOVIELENS_WH;
USE DATABASE MOVIELENS;

LIST @MOVIELENS.RAW.STG_SILVER;   -- sanity check: you should see .parquet files under 3 folders

-- Explicit column lists let the _LOADED_AT default fire. Snowflake keeps load history per
-- file, so re-running COPY INTO skips files that were already loaded (idempotent).
COPY INTO MOVIELENS.RAW.MOVIES (MOVIE_ID, TITLE, RELEASE_YEAR)
FROM (
  SELECT $1:movie_id::NUMBER(10,0),
         $1:title::STRING,
         $1:release_year::NUMBER(4,0)
  FROM @MOVIELENS.RAW.STG_SILVER/movies/
)
FILE_FORMAT = (FORMAT_NAME = 'MOVIELENS.RAW.FF_PARQUET')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO MOVIELENS.RAW.MOVIE_GENRES (MOVIE_ID, GENRE)
FROM (
  SELECT $1:movie_id::NUMBER(10,0),
         $1:genre::STRING
  FROM @MOVIELENS.RAW.STG_SILVER/movie_genres/
)
FILE_FORMAT = (FORMAT_NAME = 'MOVIELENS.RAW.FF_PARQUET')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO MOVIELENS.RAW.RATINGS (USER_ID, MOVIE_ID, RATING, RATED_AT, RATING_DATE)
FROM (
  SELECT $1:user_id::NUMBER(10,0),
         $1:movie_id::NUMBER(10,0),
         $1:rating::NUMBER(2,1),
         $1:rated_at::TIMESTAMP_NTZ,
         $1:rating_date::DATE
  FROM @MOVIELENS.RAW.STG_SILVER/ratings/
)
FILE_FORMAT = (FORMAT_NAME = 'MOVIELENS.RAW.FF_PARQUET')
ON_ERROR = 'ABORT_STATEMENT';

-- Verify. These counts must match silver_*_rows in output/quality_report.json.
SELECT 'RAW.RATINGS'      AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM MOVIELENS.RAW.RATINGS
UNION ALL SELECT 'RAW.MOVIES',       COUNT(*) FROM MOVIELENS.RAW.MOVIES
UNION ALL SELECT 'RAW.MOVIE_GENRES', COUNT(*) FROM MOVIELENS.RAW.MOVIE_GENRES;

-- Troubleshooting: if RATED_AT looks wrong (e.g. 1970 dates), inspect how the file stores it:
--   SELECT $1 FROM @MOVIELENS.RAW.STG_SILVER/ratings/ LIMIT 5;
