"""Build the silver layer with normalized match-level rows."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .layers import silver_root


def _extract_partition(pattern: str):
    return F.regexp_extract(F.input_file_name(), pattern, 1)


def build_silver_matches(spark: SparkSession, bronze_csv_glob: str) -> DataFrame:
    bronze = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(bronze_csv_glob)
    )

    return (
        bronze.withColumn("league_code", _extract_partition(r"league_code=([^/]+)"))
        .withColumn("season", _extract_partition(r"season=([^/]+)"))
        .withColumn("match_date", F.coalesce(
            F.to_date(F.col("Date"), "dd/MM/yy"),
            F.to_date(F.col("Date"), "dd/MM/yyyy"),
            F.to_date(F.col("Date"), "yyyy-MM-dd"),
        ))
        .withColumn("home_team", F.col("HomeTeam"))
        .withColumn("away_team", F.col("AwayTeam"))
        .withColumn("full_time_home_goals", F.coalesce(F.col("FTHG").cast("int"), F.lit(0)))
        .withColumn("full_time_away_goals", F.coalesce(F.col("FTAG").cast("int"), F.lit(0)))
        .withColumn("full_time_result", F.coalesce(F.col("FTR"), F.lit("U")))
        .withColumn("season_start_year", (F.substring("season", 1, 2).cast("int") + F.lit(2000)))
        .select(
            "league_code",
            "season",
            "season_start_year",
            "match_date",
            "home_team",
            "away_team",
            "full_time_home_goals",
            "full_time_away_goals",
            "full_time_result",
        )
        .where(F.col("home_team").isNotNull() & F.col("away_team").isNotNull())
    )


def write_silver_layer(silver_matches: DataFrame, output_dir: Path) -> Path:
    destination = silver_root(output_dir) / "matches"
    silver_matches.write.mode("overwrite").partitionBy("league_code", "season").parquet(str(destination))
    return destination
