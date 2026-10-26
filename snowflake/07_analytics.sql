-- =====================================================================================
-- 07_analytics.sql  |  Business-facing views using CTEs and window functions
-- Note on the genre bridge: a movie with 3 genres appears in 3 genre groups, so rating
-- counts summed across genres exceed the true total. That is expected for a
-- many-to-many bridge; do not add genre-level counts together.
-- =====================================================================================

USE WAREHOUSE MOVIELENS_WH;
USE DATABASE MOVIELENS;

-- 1) Per-movie statistics with a Bayesian "weighted rating" so a movie rated 5.0 by two
--    people does not outrank one rated 4.6 by thousands. Prior strength = 50 ratings.
CREATE OR REPLACE VIEW MOVIELENS.ANALYTICS.V_MOVIE_STATS AS
WITH g_mean AS (
  SELECT AVG(RATING) AS GLOBAL_MEAN FROM MOVIELENS.CORE.FACT_RATINGS
),
per_movie AS (
  SELECT MOVIE_KEY,
         COUNT(*)       AS RATING_COUNT,
         AVG(RATING)    AS AVG_RATING,
         STDDEV(RATING) AS RATING_STDDEV
  FROM MOVIELENS.CORE.FACT_RATINGS
  GROUP BY MOVIE_KEY
)
SELECT m.MOVIE_KEY,
       m.MOVIE_ID,
       m.TITLE,
       m.RELEASE_YEAR,
       p.RATING_COUNT,
       ROUND(p.AVG_RATING, 3)    AS AVG_RATING,
       ROUND(p.RATING_STDDEV, 3) AS RATING_STDDEV,
       ROUND(  (p.RATING_COUNT / (p.RATING_COUNT + 50)) * p.AVG_RATING
             + (50             / (p.RATING_COUNT + 50)) * g.GLOBAL_MEAN, 3) AS WEIGHTED_RATING
FROM per_movie p
JOIN MOVIELENS.CORE.DIM_MOVIE m ON m.MOVIE_KEY = p.MOVIE_KEY
CROSS JOIN g_mean g;

-- 2) Top 10 movies per genre (window function + QUALIFY).
CREATE OR REPLACE VIEW MOVIELENS.ANALYTICS.V_TOP_MOVIES_BY_GENRE AS
SELECT g.GENRE_NAME,
       s.TITLE,
       s.RELEASE_YEAR,
       s.RATING_COUNT,
       s.AVG_RATING,
       s.WEIGHTED_RATING,
       ROW_NUMBER() OVER (
         PARTITION BY g.GENRE_NAME
         ORDER BY s.WEIGHTED_RATING DESC, s.RATING_COUNT DESC
       ) AS GENRE_RANK
FROM MOVIELENS.ANALYTICS.V_MOVIE_STATS s
JOIN MOVIELENS.CORE.BRIDGE_MOVIE_GENRE b ON b.MOVIE_KEY = s.MOVIE_KEY
JOIN MOVIELENS.CORE.DIM_GENRE g          ON g.GENRE_KEY = b.GENRE_KEY
WHERE s.RATING_COUNT >= 50
QUALIFY GENRE_RANK <= 10;

-- 3) Yearly trend: volume, active users, average rating, year-over-year growth, running total.
CREATE OR REPLACE VIEW MOVIELENS.ANALYTICS.V_RATINGS_TREND_YEARLY AS
WITH yearly AS (
  SELECT d.CAL_YEAR                   AS RATING_YEAR,
         COUNT(*)                     AS RATINGS,
         COUNT(DISTINCT f.USER_KEY)   AS ACTIVE_USERS,
         AVG(f.RATING)                AS AVG_RATING
  FROM MOVIELENS.CORE.FACT_RATINGS f
  JOIN MOVIELENS.CORE.DIM_DATE d ON d.DATE_KEY = f.DATE_KEY
  GROUP BY d.CAL_YEAR
)
SELECT RATING_YEAR,
       RATINGS,
       ACTIVE_USERS,
       ROUND(AVG_RATING, 3) AS AVG_RATING,
       ROUND(100 * (RATINGS - LAG(RATINGS) OVER (ORDER BY RATING_YEAR))
                 / NULLIF(LAG(RATINGS) OVER (ORDER BY RATING_YEAR), 0), 1) AS RATINGS_YOY_PCT,
       SUM(RATINGS) OVER (ORDER BY RATING_YEAR) AS CUMULATIVE_RATINGS
FROM yearly
ORDER BY RATING_YEAR;

-- 4) Cohort retention: of the users who first rated in year X, what share rated again in year Y?
CREATE OR REPLACE VIEW MOVIELENS.ANALYTICS.V_USER_COHORT_RETENTION AS
WITH cohorts AS (
  SELECT USER_KEY, YEAR(FIRST_RATED_AT) AS COHORT_YEAR
  FROM MOVIELENS.CORE.DIM_USER
),
cohort_sizes AS (
  SELECT COHORT_YEAR, COUNT(*) AS COHORT_SIZE
  FROM cohorts
  GROUP BY COHORT_YEAR
),
activity AS (
  SELECT DISTINCT f.USER_KEY, d.CAL_YEAR AS ACTIVITY_YEAR
  FROM MOVIELENS.CORE.FACT_RATINGS f
  JOIN MOVIELENS.CORE.DIM_DATE d ON d.DATE_KEY = f.DATE_KEY
)
SELECT c.COHORT_YEAR,
       a.ACTIVITY_YEAR,
       a.ACTIVITY_YEAR - c.COHORT_YEAR           AS YEARS_SINCE_FIRST_RATING,
       s.COHORT_SIZE,
       COUNT(DISTINCT a.USER_KEY)                AS ACTIVE_USERS,
       ROUND(100 * COUNT(DISTINCT a.USER_KEY) / s.COHORT_SIZE, 1) AS RETENTION_PCT
FROM cohorts c
JOIN activity a      ON a.USER_KEY = c.USER_KEY AND a.ACTIVITY_YEAR >= c.COHORT_YEAR
JOIN cohort_sizes s  ON s.COHORT_YEAR = c.COHORT_YEAR
GROUP BY c.COHORT_YEAR, a.ACTIVITY_YEAR, s.COHORT_SIZE
ORDER BY c.COHORT_YEAR, a.ACTIVITY_YEAR;

-- 5) Genre popularity by release decade (share of that decade's rating volume).
CREATE OR REPLACE VIEW MOVIELENS.ANALYTICS.V_GENRE_POPULARITY_BY_DECADE AS
WITH base AS (
  SELECT FLOOR(m.RELEASE_YEAR / 10) * 10 AS RELEASE_DECADE,
         g.GENRE_NAME,
         f.RATING
  FROM MOVIELENS.CORE.FACT_RATINGS f
  JOIN MOVIELENS.CORE.DIM_MOVIE m          ON m.MOVIE_KEY = f.MOVIE_KEY
  JOIN MOVIELENS.CORE.BRIDGE_MOVIE_GENRE b ON b.MOVIE_KEY = m.MOVIE_KEY
  JOIN MOVIELENS.CORE.DIM_GENRE g          ON g.GENRE_KEY = b.GENRE_KEY
  WHERE m.RELEASE_YEAR IS NOT NULL
),
agg AS (
  SELECT RELEASE_DECADE, GENRE_NAME, COUNT(*) AS RATINGS, AVG(RATING) AS AVG_RATING
  FROM base
  GROUP BY RELEASE_DECADE, GENRE_NAME
)
SELECT RELEASE_DECADE,
       GENRE_NAME,
       RATINGS,
       ROUND(AVG_RATING, 3) AS AVG_RATING,
       ROUND(100 * RATINGS / SUM(RATINGS) OVER (PARTITION BY RELEASE_DECADE), 2) AS PCT_OF_DECADE_RATINGS
FROM agg
ORDER BY RELEASE_DECADE, RATINGS DESC;

-- 6) Most polarizing movies: highest rating spread among well-rated titles.
CREATE OR REPLACE VIEW MOVIELENS.ANALYTICS.V_POLARIZING_MOVIES AS
SELECT TITLE, RELEASE_YEAR, RATING_COUNT, AVG_RATING, RATING_STDDEV
FROM MOVIELENS.ANALYTICS.V_MOVIE_STATS
WHERE RATING_COUNT >= 100
ORDER BY RATING_STDDEV DESC
LIMIT 50;

-- 7) Do heavy raters rate differently? Users split into activity quartiles with NTILE.
CREATE OR REPLACE VIEW MOVIELENS.ANALYTICS.V_USER_ACTIVITY_SEGMENTS AS
WITH per_user AS (
  SELECT USER_KEY, COUNT(*) AS N_RATINGS, AVG(RATING) AS USER_AVG_RATING
  FROM MOVIELENS.CORE.FACT_RATINGS
  GROUP BY USER_KEY
),
segmented AS (
  SELECT USER_KEY, N_RATINGS, USER_AVG_RATING,
         NTILE(4) OVER (ORDER BY N_RATINGS) AS ACTIVITY_QUARTILE
  FROM per_user
)
SELECT ACTIVITY_QUARTILE,
       COUNT(*)                        AS USERS,
       MIN(N_RATINGS)                  AS MIN_RATINGS,
       MAX(N_RATINGS)                  AS MAX_RATINGS,
       ROUND(AVG(N_RATINGS), 1)        AS AVG_RATINGS,
       ROUND(AVG(USER_AVG_RATING), 3)  AS AVG_RATING_GIVEN
FROM segmented
GROUP BY ACTIVITY_QUARTILE
ORDER BY ACTIVITY_QUARTILE;

-- ---------- Try them -------------------------------------------------------------------
-- SELECT * FROM MOVIELENS.ANALYTICS.V_TOP_MOVIES_BY_GENRE WHERE GENRE_NAME = 'Sci-Fi' ORDER BY GENRE_RANK;
-- SELECT * FROM MOVIELENS.ANALYTICS.V_RATINGS_TREND_YEARLY;
-- SELECT * FROM MOVIELENS.ANALYTICS.V_USER_COHORT_RETENTION;
-- SELECT * FROM MOVIELENS.ANALYTICS.V_USER_ACTIVITY_SEGMENTS;

-- ---------- Performance notes (do this and screenshot the results for your README) -------
-- 1. Run a date-filtered query, then open Query Profile and read "Partitions scanned / total":
--      SELECT COUNT(*) FROM MOVIELENS.CORE.FACT_RATINGS WHERE DATE_KEY BETWEEN 20150101 AND 20151231;
-- 2. Cluster the fact table by date and compare:
--      ALTER TABLE MOVIELENS.CORE.FACT_RATINGS CLUSTER BY (DATE_KEY);
--      SELECT SYSTEM$CLUSTERING_INFORMATION('MOVIELENS.CORE.FACT_RATINGS', '(DATE_KEY)');
--    Automatic clustering consumes credits; on a small table the gain may be negligible, which
--    is itself a valid finding to write up. Suspend it afterwards if you want to save credits:
--      ALTER TABLE MOVIELENS.CORE.FACT_RATINGS SUSPEND RECLUSTER;
