# Databricks notebook source
# MAGIC %md
# MAGIC # MovieLens lakehouse: Bronze → Silver on Databricks
# MAGIC
# MAGIC Run this from a Databricks **Git folder** pointing at this repo (so `../src` resolves).
# MAGIC Works on Databricks Free Edition (serverless) and on regular clusters.
# MAGIC
# MAGIC **Before running:** upload `ratings.csv` and `movies.csv` from MovieLens (25M or the small sample)
# MAGIC into the Unity Catalog volume `<catalog>.<schema>.<volume>` created by the first cell.
# MAGIC Adjust the widgets if your catalog is not named `workspace`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from pyspark.sql import functions as F

from movielens_pipeline import bronze, quality, silver

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "movielens")
dbutils.widgets.text("volume", "raw")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

volume_root = f"/Volumes/{catalog}/{schema}/{volume}"
print("Upload ratings.csv and movies.csv to:", volume_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: raw, all-string, with lineage columns

# COMMAND ----------

def table(name: str) -> str:
    return f"{catalog}.{schema}.{name}"


bronze_ratings = bronze.build_bronze_ratings(spark, f"{volume_root}/ratings.csv")
bronze_movies = bronze.build_bronze_movies(spark, f"{volume_root}/movies.csv")

bronze_ratings.write.format("delta").mode("overwrite").saveAsTable(table("bronze_ratings"))
bronze_movies.write.format("delta").mode("overwrite").saveAsTable(table("bronze_movies"))

print("bronze_ratings:", spark.table(table("bronze_ratings")).count())
print("bronze_movies:", spark.table(table("bronze_movies")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: typed, validated, de-duplicated, with a quarantine for rejects

# COMMAND ----------

movies_clean, movies_rejected = silver.clean_movies(spark.table(table("bronze_movies")))
ratings_clean, ratings_rejected = silver.clean_ratings(spark.table(table("bronze_ratings")))
ratings_final, orphans_rejected = silver.drop_orphan_ratings(ratings_clean, movies_clean)
movie_genres = silver.explode_genres(movies_clean)
quarantine = ratings_rejected.unionByName(movies_rejected).unionByName(orphans_rejected)

for name, frame in [
    ("silver_movies", movies_clean),
    ("silver_ratings", ratings_final),
    ("silver_movie_genres", movie_genres),
    ("silver_quarantine", quarantine),
]:
    frame.write.format("delta").mode("overwrite").saveAsTable(table(name))

display(
    spark.table(table("silver_quarantine"))
    .groupBy("source", "reject_reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data-quality checks (fails the notebook if the Silver contract is broken)

# COMMAND ----------

results = quality.run_silver_checks(
    spark.table(table("silver_ratings")),
    spark.table(table("silver_movies")),
    spark.table(table("silver_movie_genres")),
)
display(spark.createDataFrame([r.as_dict() for r in results]))
quality.assert_all_passed(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Silver as parquet for Snowflake
# MAGIC After this cell, download the files from the `export` folder of the volume
# MAGIC (Catalog Explorer → volume → `export`) and upload them to the Snowflake stage.

# COMMAND ----------

export_root = f"{volume_root}/export"

(
    spark.table(table("silver_ratings"))
    .select("user_id", "movie_id", "rating", "rated_at", "rating_date")
    .coalesce(4)
    .write.mode("overwrite")
    .parquet(f"{export_root}/ratings")
)
(
    spark.table(table("silver_movies"))
    .select("movie_id", "title", "release_year")
    .coalesce(1)
    .write.mode("overwrite")
    .parquet(f"{export_root}/movies")
)
spark.table(table("silver_movie_genres")).coalesce(1).write.mode("overwrite").parquet(
    f"{export_root}/movie_genres"
)

print("Exported to", export_root)
