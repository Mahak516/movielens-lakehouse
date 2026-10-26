-- =====================================================================================
-- 05_incremental_load.sql  |  Stored procedure + scheduled task for incremental loads
--
-- Flow:  RAW.RATINGS --(append-only stream)--> OPS.RATINGS_BATCH --> DIM_USER, FACT_RATINGS
--
-- Everything runs inside ONE transaction. A stream's offset only advances when the
-- transaction that reads it commits, so if any step fails the transaction rolls back and
-- the batch stays in the stream to be retried. No ratings are ever lost or half-loaded.
-- =====================================================================================

USE WAREHOUSE MOVIELENS_WH;
USE DATABASE MOVIELENS;

CREATE OR REPLACE PROCEDURE MOVIELENS.CORE.SP_LOAD_FACT_RATINGS()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  run_id          STRING DEFAULT UUID_STRING();
  batch_rows      NUMBER DEFAULT 0;
  unmatched_rows  NUMBER DEFAULT 0;
  fact_rows       NUMBER DEFAULT 0;
BEGIN
  BEGIN TRANSACTION;

  -- 1) Capture this batch from the stream (DML on a stream consumes it at commit).
  DELETE FROM MOVIELENS.OPS.RATINGS_BATCH;
  INSERT INTO MOVIELENS.OPS.RATINGS_BATCH (USER_ID, MOVIE_ID, RATING, RATED_AT)
    SELECT USER_ID, MOVIE_ID, RATING, RATED_AT
    FROM MOVIELENS.RAW.RATINGS_STREAM;

  SELECT COUNT(*) INTO :batch_rows FROM MOVIELENS.OPS.RATINGS_BATCH;

  IF (batch_rows = 0) THEN
    COMMIT;
    RETURN 'No new rows in stream; nothing to load.';
  END IF;

  -- 2) Upsert users, widening their first/last activity bounds.
  MERGE INTO MOVIELENS.CORE.DIM_USER t
  USING (
    SELECT USER_ID, MIN(RATED_AT) AS FIRST_RATED_AT, MAX(RATED_AT) AS LAST_RATED_AT
    FROM MOVIELENS.OPS.RATINGS_BATCH
    GROUP BY USER_ID
  ) s
    ON t.USER_ID = s.USER_ID
  WHEN MATCHED THEN UPDATE SET
    FIRST_RATED_AT = LEAST(t.FIRST_RATED_AT, s.FIRST_RATED_AT),
    LAST_RATED_AT  = GREATEST(t.LAST_RATED_AT, s.LAST_RATED_AT)
  WHEN NOT MATCHED THEN
    INSERT (USER_ID, FIRST_RATED_AT, LAST_RATED_AT)
    VALUES (s.USER_ID, s.FIRST_RATED_AT, s.LAST_RATED_AT);

  -- 3) Count ratings whose movie is missing from DIM_MOVIE; they are skipped, but logged.
  SELECT COUNT(*) INTO :unmatched_rows
  FROM MOVIELENS.OPS.RATINGS_BATCH b
  LEFT JOIN MOVIELENS.CORE.DIM_MOVIE m ON m.MOVIE_ID = b.MOVIE_ID
  WHERE m.MOVIE_KEY IS NULL;

  -- 4) Upsert facts. A newer rating for the same (user, movie) replaces the older one;
  --    within the batch the latest rating wins.
  MERGE INTO MOVIELENS.CORE.FACT_RATINGS f
  USING (
    SELECT u.USER_KEY,
           m.MOVIE_KEY,
           TO_NUMBER(TO_CHAR(b.RATED_AT::DATE, 'YYYYMMDD')) AS DATE_KEY,
           b.RATING,
           b.RATED_AT
    FROM MOVIELENS.OPS.RATINGS_BATCH b
    JOIN MOVIELENS.CORE.DIM_USER  u ON u.USER_ID  = b.USER_ID
    JOIN MOVIELENS.CORE.DIM_MOVIE m ON m.MOVIE_ID = b.MOVIE_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY u.USER_KEY, m.MOVIE_KEY ORDER BY b.RATED_AT DESC) = 1
  ) s
    ON f.USER_KEY = s.USER_KEY AND f.MOVIE_KEY = s.MOVIE_KEY
  WHEN MATCHED AND s.RATED_AT > f.RATED_AT THEN UPDATE SET
    DATE_KEY  = s.DATE_KEY,
    RATING    = s.RATING,
    RATED_AT  = s.RATED_AT,
    LOADED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (USER_KEY, MOVIE_KEY, DATE_KEY, RATING, RATED_AT)
    VALUES (s.USER_KEY, s.MOVIE_KEY, s.DATE_KEY, s.RATING, s.RATED_AT);

  fact_rows := SQLROWCOUNT;

  COMMIT;

  INSERT INTO MOVIELENS.OPS.PIPELINE_LOG (RUN_ID, STEP, ROWS_AFFECTED, STATUS, MESSAGE)
    SELECT :run_id, 'load_fact_ratings', :fact_rows, 'SUCCESS',
           'batch_rows=' || :batch_rows || ', skipped_unknown_movie=' || :unmatched_rows;

  RETURN 'Loaded batch of ' || batch_rows || ' rows (' || fact_rows || ' fact rows inserted/updated, '
         || unmatched_rows || ' skipped: unknown movie).';

EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    INSERT INTO MOVIELENS.OPS.PIPELINE_LOG (RUN_ID, STEP, ROWS_AFFECTED, STATUS, MESSAGE)
      SELECT :run_id, 'load_fact_ratings', 0, 'FAILED', :SQLERRM;
    RAISE;
END;
$$;

-- Initial load: the COPY INTO from script 03 already put every rating into the stream.
CALL MOVIELENS.CORE.SP_LOAD_FACT_RATINGS();

SELECT * FROM MOVIELENS.OPS.PIPELINE_LOG ORDER BY LOGGED_AT DESC;

-- ---------- Scheduled task: runs only when the stream has data ------------------------
CREATE OR REPLACE TASK MOVIELENS.OPS.TASK_LOAD_FACT_RATINGS
  WAREHOUSE = MOVIELENS_WH
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('MOVIELENS.RAW.RATINGS_STREAM')
AS
  CALL MOVIELENS.CORE.SP_LOAD_FACT_RATINGS();

ALTER TASK MOVIELENS.OPS.TASK_LOAD_FACT_RATINGS RESUME;

-- ---------- Demo: simulate a new batch arriving ---------------------------------------
-- INSERT INTO MOVIELENS.RAW.RATINGS (USER_ID, MOVIE_ID, RATING, RATED_AT, RATING_DATE)
--   SELECT 9999999, MOVIE_ID, 4.5, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, CURRENT_DATE()
--   FROM MOVIELENS.CORE.DIM_MOVIE LIMIT 5;
-- SELECT SYSTEM$STREAM_HAS_DATA('MOVIELENS.RAW.RATINGS_STREAM');   -- TRUE
-- EXECUTE TASK MOVIELENS.OPS.TASK_LOAD_FACT_RATINGS;               -- or wait up to 5 minutes
-- SELECT * FROM MOVIELENS.OPS.PIPELINE_LOG ORDER BY LOGGED_AT DESC;
-- SELECT COUNT(*) FROM MOVIELENS.CORE.FACT_RATINGS WHERE USER_KEY =
--   (SELECT USER_KEY FROM MOVIELENS.CORE.DIM_USER WHERE USER_ID = 9999999);   -- 5

-- IMPORTANT on a trial account: suspend the task when you are done demoing, otherwise it
-- keeps polling (cheap, but it is still compute you did not intend to spend).
-- ALTER TASK MOVIELENS.OPS.TASK_LOAD_FACT_RATINGS SUSPEND;
