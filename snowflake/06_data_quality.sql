-- =====================================================================================
-- 06_data_quality.sql  |  Warehouse-side data-quality checks as one queryable view
-- Mirrors the Silver contract in src/movielens_pipeline/quality.py, plus reconciliation
-- against the RAW layer. Every check reports the number of FAILING rows (0 = PASS).
-- =====================================================================================

USE WAREHOUSE MOVIELENS_WH;
USE DATABASE MOVIELENS;

CREATE OR REPLACE VIEW MOVIELENS.OPS.V_DQ_CHECKS AS
SELECT 'fact.unique_grain' AS CHECK_NAME, c AS FAILING_ROWS, IFF(c = 0, 'PASS', 'FAIL') AS STATUS
FROM (
  SELECT COUNT(*) AS c FROM (
    SELECT USER_KEY, MOVIE_KEY FROM MOVIELENS.CORE.FACT_RATINGS
    GROUP BY USER_KEY, MOVIE_KEY HAVING COUNT(*) > 1
  )
)
UNION ALL
SELECT 'fact.rating_in_range', c, IFF(c = 0, 'PASS', 'FAIL')
FROM (
  SELECT COUNT(*) AS c FROM MOVIELENS.CORE.FACT_RATINGS
  WHERE RATING < 0.5 OR RATING > 5.0 OR MOD(RATING * 2, 1) <> 0
)
UNION ALL
SELECT 'fact.user_fk', c, IFF(c = 0, 'PASS', 'FAIL')
FROM (
  SELECT COUNT(*) AS c FROM MOVIELENS.CORE.FACT_RATINGS f
  LEFT JOIN MOVIELENS.CORE.DIM_USER u ON u.USER_KEY = f.USER_KEY
  WHERE u.USER_KEY IS NULL
)
UNION ALL
SELECT 'fact.movie_fk', c, IFF(c = 0, 'PASS', 'FAIL')
FROM (
  SELECT COUNT(*) AS c FROM MOVIELENS.CORE.FACT_RATINGS f
  LEFT JOIN MOVIELENS.CORE.DIM_MOVIE m ON m.MOVIE_KEY = f.MOVIE_KEY
  WHERE m.MOVIE_KEY IS NULL
)
UNION ALL
SELECT 'fact.date_fk', c, IFF(c = 0, 'PASS', 'FAIL')
FROM (
  SELECT COUNT(*) AS c FROM MOVIELENS.CORE.FACT_RATINGS f
  LEFT JOIN MOVIELENS.CORE.DIM_DATE d ON d.DATE_KEY = f.DATE_KEY
  WHERE d.DATE_KEY IS NULL
)
UNION ALL
SELECT 'dim_movie.title_present', c, IFF(c = 0, 'PASS', 'FAIL')
FROM (
  SELECT COUNT(*) AS c FROM MOVIELENS.CORE.DIM_MOVIE
  WHERE TITLE IS NULL OR TRIM(TITLE) = ''
)
UNION ALL
SELECT 'bridge.every_movie_has_a_genre', c, IFF(c = 0, 'PASS', 'FAIL')
FROM (
  SELECT COUNT(*) AS c FROM MOVIELENS.CORE.DIM_MOVIE m
  LEFT JOIN MOVIELENS.CORE.BRIDGE_MOVIE_GENRE b ON b.MOVIE_KEY = m.MOVIE_KEY
  WHERE b.MOVIE_KEY IS NULL
)
UNION ALL
-- Reconciliation: every distinct (user, movie) in RAW must be in the fact (difference = 0).
SELECT 'reconcile.raw_vs_fact_rows', c, IFF(c = 0, 'PASS', 'FAIL')
FROM (
  SELECT ABS(
    (SELECT COUNT(*) FROM (SELECT DISTINCT USER_ID, MOVIE_ID FROM MOVIELENS.RAW.RATINGS))
    - (SELECT COUNT(*) FROM MOVIELENS.CORE.FACT_RATINGS)
  ) AS c
);

-- Run the checks:
SELECT * FROM MOVIELENS.OPS.V_DQ_CHECKS ORDER BY STATUS DESC, CHECK_NAME;
