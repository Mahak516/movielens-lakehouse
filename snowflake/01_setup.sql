-- =====================================================================================
-- 01_setup.sql  |  Warehouse, database, schemas, file format, stage
-- Run once, in a Snowsight worksheet. ACCOUNTADMIN is used for simplicity on a trial
-- account (it already holds the EXECUTE TASK privilege that script 05 needs). In a real
-- account you would use a dedicated role with only the grants it needs.
-- =====================================================================================

USE ROLE ACCOUNTADMIN;

-- XSMALL + 60s auto-suspend keeps trial credits from draining.
CREATE WAREHOUSE IF NOT EXISTS MOVIELENS_WH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS MOVIELENS;

CREATE SCHEMA IF NOT EXISTS MOVIELENS.RAW;        -- landing zone: exactly what was loaded
CREATE SCHEMA IF NOT EXISTS MOVIELENS.CORE;       -- star schema (facts + dimensions)
CREATE SCHEMA IF NOT EXISTS MOVIELENS.ANALYTICS;  -- business-facing views
CREATE SCHEMA IF NOT EXISTS MOVIELENS.OPS;        -- logs, staging for incremental loads, DQ checks

USE WAREHOUSE MOVIELENS_WH;
USE DATABASE MOVIELENS;

-- USE_LOGICAL_TYPE lets Snowflake read Parquet timestamp/date logical types correctly.
CREATE FILE FORMAT IF NOT EXISTS MOVIELENS.RAW.FF_PARQUET
  TYPE = PARQUET
  USE_LOGICAL_TYPE = TRUE;

-- Internal stage that receives the Parquet files exported by the Spark pipeline.
CREATE STAGE IF NOT EXISTS MOVIELENS.RAW.STG_SILVER
  FILE_FORMAT = MOVIELENS.RAW.FF_PARQUET;
