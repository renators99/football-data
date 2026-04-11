"""Build curated gold tables from the silver layer."""

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .layers import gold_root


def build_gold_team_table(silver_matches: DataFrame) -> DataFrame:
    home_rows = (
        silver_matches.select(
            "league_code",
            "season",
            F.col("home_team").alias("team"),
            F.when(F.col("full_time_result") == "H", 1).otherwise(0).alias("wins"),
            F.when(F.col("full_time_result") == "D", 1).otherwise(0).alias("draws"),
            F.when(F.col("full_time_result") == "A", 1).otherwise(0).alias("losses"),
            F.col("full_time_home_goals").alias("goals_for"),
            F.col("full_time_away_goals").alias("goals_against"),
        )
    )

    away_rows = (
        silver_matches.select(
            "league_code",
            "season",
            F.col("away_team").alias("team"),
            F.when(F.col("full_time_result") == "A", 1).otherwise(0).alias("wins"),
            F.when(F.col("full_time_result") == "D", 1).otherwise(0).alias("draws"),
            F.when(F.col("full_time_result") == "H", 1).otherwise(0).alias("losses"),
            F.col("full_time_away_goals").alias("goals_for"),
            F.col("full_time_home_goals").alias("goals_against"),
        )
    )

    return (
        home_rows.unionByName(away_rows)
        .groupBy("league_code", "season", "team")
        .agg(
            F.count(F.lit(1)).alias("matches_played"),
            F.sum("wins").alias("wins"),
            F.sum("draws").alias("draws"),
            F.sum("losses").alias("losses"),
            F.sum("goals_for").alias("goals_for"),
            F.sum("goals_against").alias("goals_against"),
        )
        .withColumn("goal_difference", F.col("goals_for") - F.col("goals_against"))
        .withColumn("points", F.col("wins") * 3 + F.col("draws"))
    )


def build_gold_league_summary(silver_matches: DataFrame) -> DataFrame:
    total_goals = F.col("full_time_home_goals") + F.col("full_time_away_goals")
    return silver_matches.groupBy("league_code", "season").agg(
        F.count(F.lit(1)).alias("matches"),
        F.sum(F.when(F.col("full_time_result") == "H", 1).otherwise(0)).alias("home_wins"),
        F.sum(F.when(F.col("full_time_result") == "D", 1).otherwise(0)).alias("draws"),
        F.sum(F.when(F.col("full_time_result") == "A", 1).otherwise(0)).alias("away_wins"),
        F.avg(total_goals).alias("avg_goals_per_match"),
        F.sum(total_goals).alias("total_goals"),
    )


def write_gold_layer(team_table: DataFrame, league_summary: DataFrame, output_dir: Path) -> tuple[Path, Path]:
    root = gold_root(output_dir)
    team_destination = root / "team_season_table"
    summary_destination = root / "league_season_summary"

    team_table.write.mode("overwrite").partitionBy("league_code", "season").parquet(str(team_destination))
    league_summary.write.mode("overwrite").partitionBy("league_code", "season").parquet(str(summary_destination))
    return team_destination, summary_destination
