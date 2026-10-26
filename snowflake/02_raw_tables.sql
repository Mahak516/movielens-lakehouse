-- =====================================================================================
-- 02_raw_tables.sql  |  Landing tables, ops tables and the change-capture stream
-- The stream is created BEFORE the first load on purpose: the initial COPY INTO then
-- flows through the same incremental path (stream -> stored procedure -> fact) as every
-- later batch, so there is a single load code path to maintain.
-- =====================================================================================

USE WAREHOUSE MOVIELENS_WH;
USE DATABASE MOVIELENS;

CREATE OR REPLACE TABLE MOVIELENS.RAW.RATINGS (
  USER_ID      NUMBER(10,0),
  MOVIE_ID     NUMBER(10,0),
  RATING       NUMBER(2,1),
  RATED_AT     TIMESTAMP_NTZ,
  RATING_DATE  DATE,
  _LOADED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE MOVIELENS.RAW.MOVIES (
  MOVIE_ID      NUMBER(10,0),
  TITLE         STRING,
  RELEASE_YEAR  NUMBER(4,0),
  _LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE MOVIELENS.RAW.MOVIE_GENRES (
  MOVIE_ID    NUMBER(10,0),
  GENRE       STRING,
  _LOADED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Append-only stream: only new inserts into RAW.RATINGS are tracked, which is all a
-- ratings feed ever does. Append-only streams are cheaper than standard streams.
CREATE OR REPLACE STREAM MOVIELENS.RAW.RATINGS_STREAM
  ON TABLE MOVIELENS.RAW.RATINGS
  APPEND_ONLY = TRUE;

-- Pipeline run log (written by the stored procedure).
CREATE TABLE IF NOT EXISTS MOVIELENS.OPS.PIPELINE_LOG (
  RUN_ID         STRING,
  STEP           STRING,
  ROWS_AFFECTED  NUMBER,
  STATUS         STRING,
  MESSAGE        STRING,
  LOGGED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Work table for one incremental batch. Transient = no fail-safe storage cost.
CREATE OR REPLACE TRANSIENT TABLE MOVIELENS.OPS.RATINGS_BATCH (
  USER_ID   NUMBER(10,0),
  MOVIE_ID  NUMBER(10,0),
  RATING    NUMBER(2,1),
  RATED_AT  TIMESTAMP_NTZ
);
