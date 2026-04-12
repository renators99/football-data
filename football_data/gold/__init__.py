"""Build curated gold tables from the silver layer."""

import logging
from pathlib import Path
from typing import Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..utils.layers import gold_root, silver_root

LOGGER = logging.getLogger(__name__)


def _safe_ratio(numerator, denominator):
    return F.when(denominator != 0, numerator.cast("double") / denominator.cast("double"))


def _build_team_match_rows(silver_matches: DataFrame) -> DataFrame:
    total_goals = F.col("full_time_home_goals") + F.col("full_time_away_goals")
    both_teams_scored = (
        (F.col("full_time_home_goals") > 0) & (F.col("full_time_away_goals") > 0)
    ).cast("int")
    home_leading_at_half = (F.col("half_time_home_goals") > F.col("half_time_away_goals")).cast("int")
    away_leading_at_half = (F.col("half_time_away_goals") > F.col("half_time_home_goals")).cast("int")
    home_trailing_at_half = (F.col("half_time_home_goals") < F.col("half_time_away_goals")).cast("int")
    away_trailing_at_half = (F.col("half_time_away_goals") < F.col("half_time_home_goals")).cast("int")
    home_avoided_defeat = (F.col("full_time_result") != "A").cast("int")
    away_avoided_defeat = (F.col("full_time_result") != "H").cast("int")
    home_did_not_win = (F.col("full_time_result") != "H").cast("int")
    away_did_not_win = (F.col("full_time_result") != "A").cast("int")

    home_rows = silver_matches.select(
        "league_code",
        "season",
        F.col("home_team").alias("team"),
        F.lit("home").alias("venue"),
        F.when(F.col("full_time_result") == "H", 1).otherwise(0).alias("wins"),
        F.when(F.col("full_time_result") == "D", 1).otherwise(0).alias("draws"),
        F.when(F.col("full_time_result") == "A", 1).otherwise(0).alias("losses"),
        F.col("full_time_home_goals").alias("goals_for"),
        F.col("full_time_away_goals").alias("goals_against"),
        F.col("half_time_home_goals").alias("first_half_goals_for"),
        F.col("half_time_away_goals").alias("first_half_goals_against"),
        (F.col("full_time_home_goals") - F.col("half_time_home_goals")).alias("second_half_goals_for"),
        (F.col("full_time_away_goals") - F.col("half_time_away_goals")).alias("second_half_goals_against"),
        F.col("home_shots").alias("shots_for"),
        F.col("away_shots").alias("shots_against"),
        F.col("home_shots_on_target").alias("shots_on_target_for"),
        F.col("away_shots_on_target").alias("shots_on_target_against"),
        F.col("home_corners").alias("corners_for"),
        F.col("away_corners").alias("corners_against"),
        F.col("home_fouls").alias("fouls_for"),
        F.col("away_fouls").alias("fouls_against"),
        F.col("home_yellow_cards").alias("yellow_cards"),
        F.col("home_red_cards").alias("red_cards"),
        F.when(F.col("full_time_away_goals") == 0, 1).otherwise(0).alias("clean_sheets"),
        F.when(F.col("full_time_home_goals") == 0, 1).otherwise(0).alias("failed_to_score"),
        both_teams_scored.alias("both_teams_scored_matches"),
        F.when(total_goals >= 2, 1).otherwise(0).alias("over_1_5_matches"),
        F.when(total_goals >= 3, 1).otherwise(0).alias("over_2_5_matches"),
        F.when(total_goals >= 4, 1).otherwise(0).alias("over_3_5_matches"),
        home_leading_at_half.alias("leading_at_half"),
        home_trailing_at_half.alias("trailing_at_half"),
        F.when((home_trailing_at_half == 1) & (home_avoided_defeat == 1), 1).otherwise(0).alias("comeback_results"),
        F.when((home_leading_at_half == 1) & (home_did_not_win == 1), 1).otherwise(0).alias("blown_leads"),
    )

    away_rows = silver_matches.select(
        "league_code",
        "season",
        F.col("away_team").alias("team"),
        F.lit("away").alias("venue"),
        F.when(F.col("full_time_result") == "A", 1).otherwise(0).alias("wins"),
        F.when(F.col("full_time_result") == "D", 1).otherwise(0).alias("draws"),
        F.when(F.col("full_time_result") == "H", 1).otherwise(0).alias("losses"),
        F.col("full_time_away_goals").alias("goals_for"),
        F.col("full_time_home_goals").alias("goals_against"),
        F.col("half_time_away_goals").alias("first_half_goals_for"),
        F.col("half_time_home_goals").alias("first_half_goals_against"),
        (F.col("full_time_away_goals") - F.col("half_time_away_goals")).alias("second_half_goals_for"),
        (F.col("full_time_home_goals") - F.col("half_time_home_goals")).alias("second_half_goals_against"),
        F.col("away_shots").alias("shots_for"),
        F.col("home_shots").alias("shots_against"),
        F.col("away_shots_on_target").alias("shots_on_target_for"),
        F.col("home_shots_on_target").alias("shots_on_target_against"),
        F.col("away_corners").alias("corners_for"),
        F.col("home_corners").alias("corners_against"),
        F.col("away_fouls").alias("fouls_for"),
        F.col("home_fouls").alias("fouls_against"),
        F.col("away_yellow_cards").alias("yellow_cards"),
        F.col("away_red_cards").alias("red_cards"),
        F.when(F.col("full_time_home_goals") == 0, 1).otherwise(0).alias("clean_sheets"),
        F.when(F.col("full_time_away_goals") == 0, 1).otherwise(0).alias("failed_to_score"),
        both_teams_scored.alias("both_teams_scored_matches"),
        F.when(total_goals >= 2, 1).otherwise(0).alias("over_1_5_matches"),
        F.when(total_goals >= 3, 1).otherwise(0).alias("over_2_5_matches"),
        F.when(total_goals >= 4, 1).otherwise(0).alias("over_3_5_matches"),
        away_leading_at_half.alias("leading_at_half"),
        away_trailing_at_half.alias("trailing_at_half"),
        F.when((away_trailing_at_half == 1) & (away_avoided_defeat == 1), 1).otherwise(0).alias("comeback_results"),
        F.when((away_leading_at_half == 1) & (away_did_not_win == 1), 1).otherwise(0).alias("blown_leads"),
    )

    return home_rows.unionByName(away_rows)


def build_gold_team_table(silver_matches: DataFrame) -> DataFrame:
    team_matches = _build_team_match_rows(silver_matches)
    return (
        team_matches
        .groupBy("league_code", "season", "team")
        .agg(
            F.count(F.lit(1)).alias("matches_played"),
            F.sum(F.when(F.col("venue") == "home", 1).otherwise(0)).alias("home_matches"),
            F.sum(F.when(F.col("venue") == "away", 1).otherwise(0)).alias("away_matches"),
            F.sum("wins").alias("wins"),
            F.sum("draws").alias("draws"),
            F.sum("losses").alias("losses"),
            F.sum("goals_for").alias("goals_for"),
            F.sum("goals_against").alias("goals_against"),
            F.sum("first_half_goals_for").alias("first_half_goals_for"),
            F.sum("first_half_goals_against").alias("first_half_goals_against"),
            F.sum("second_half_goals_for").alias("second_half_goals_for"),
            F.sum("second_half_goals_against").alias("second_half_goals_against"),
            F.sum("shots_for").alias("shots_for"),
            F.sum("shots_against").alias("shots_against"),
            F.sum("shots_on_target_for").alias("shots_on_target_for"),
            F.sum("shots_on_target_against").alias("shots_on_target_against"),
            F.sum("corners_for").alias("corners_for"),
            F.sum("corners_against").alias("corners_against"),
            F.sum("fouls_for").alias("fouls_for"),
            F.sum("fouls_against").alias("fouls_against"),
            F.sum("yellow_cards").alias("yellow_cards"),
            F.sum("red_cards").alias("red_cards"),
            F.sum("clean_sheets").alias("clean_sheets"),
            F.sum("failed_to_score").alias("failed_to_score"),
            F.sum("both_teams_scored_matches").alias("both_teams_scored_matches"),
            F.sum("over_1_5_matches").alias("over_1_5_matches"),
            F.sum("over_2_5_matches").alias("over_2_5_matches"),
            F.sum("over_3_5_matches").alias("over_3_5_matches"),
            F.sum("leading_at_half").alias("leading_at_half"),
            F.sum("trailing_at_half").alias("trailing_at_half"),
            F.sum("comeback_results").alias("comeback_results"),
            F.sum("blown_leads").alias("blown_leads"),
        )
        .withColumn("goal_difference", F.col("goals_for") - F.col("goals_against"))
        .withColumn("points", F.col("wins") * 3 + F.col("draws"))
        .withColumn("win_rate", _safe_ratio(F.col("wins"), F.col("matches_played")))
        .withColumn("draw_rate", _safe_ratio(F.col("draws"), F.col("matches_played")))
        .withColumn("loss_rate", _safe_ratio(F.col("losses"), F.col("matches_played")))
        .withColumn("points_per_match", _safe_ratio(F.col("points"), F.col("matches_played")))
        .withColumn("goals_for_per_match", _safe_ratio(F.col("goals_for"), F.col("matches_played")))
        .withColumn("goals_against_per_match", _safe_ratio(F.col("goals_against"), F.col("matches_played")))
        .withColumn("goal_difference_per_match", _safe_ratio(F.col("goal_difference"), F.col("matches_played")))
        .withColumn("clean_sheet_rate", _safe_ratio(F.col("clean_sheets"), F.col("matches_played")))
        .withColumn("failed_to_score_rate", _safe_ratio(F.col("failed_to_score"), F.col("matches_played")))
        .withColumn("both_teams_scored_rate", _safe_ratio(F.col("both_teams_scored_matches"), F.col("matches_played")))
        .withColumn("over_1_5_rate", _safe_ratio(F.col("over_1_5_matches"), F.col("matches_played")))
        .withColumn("over_2_5_rate", _safe_ratio(F.col("over_2_5_matches"), F.col("matches_played")))
        .withColumn("over_3_5_rate", _safe_ratio(F.col("over_3_5_matches"), F.col("matches_played")))
        .withColumn("first_half_goals_for_per_match", _safe_ratio(F.col("first_half_goals_for"), F.col("matches_played")))
        .withColumn("first_half_goals_against_per_match", _safe_ratio(F.col("first_half_goals_against"), F.col("matches_played")))
        .withColumn("second_half_goals_for_per_match", _safe_ratio(F.col("second_half_goals_for"), F.col("matches_played")))
        .withColumn("second_half_goals_against_per_match", _safe_ratio(F.col("second_half_goals_against"), F.col("matches_played")))
        .withColumn("avg_shots_for", _safe_ratio(F.col("shots_for"), F.col("matches_played")))
        .withColumn("avg_shots_against", _safe_ratio(F.col("shots_against"), F.col("matches_played")))
        .withColumn("avg_shots_on_target_for", _safe_ratio(F.col("shots_on_target_for"), F.col("matches_played")))
        .withColumn("avg_shots_on_target_against", _safe_ratio(F.col("shots_on_target_against"), F.col("matches_played")))
        .withColumn("shot_dominance_index", _safe_ratio(F.col("shots_for") - F.col("shots_against"), F.col("matches_played")))
        .withColumn(
            "on_target_dominance_index",
            _safe_ratio(F.col("shots_on_target_for") - F.col("shots_on_target_against"), F.col("matches_played")),
        )
        .withColumn(
            "territorial_dominance_proxy",
            _safe_ratio(F.col("corners_for") - F.col("corners_against"), F.col("matches_played")),
        )
        .withColumn("shot_accuracy", _safe_ratio(F.col("shots_on_target_for"), F.col("shots_for")))
        .withColumn("shot_conversion_rate", _safe_ratio(F.col("goals_for"), F.col("shots_for")))
        .withColumn(
            "net_efficiency",
            _safe_ratio(F.col("goals_for"), F.col("shots_on_target_for"))
            - _safe_ratio(F.col("goals_against"), F.col("shots_on_target_against")),
        )
        .withColumn("pressure_without_payoff", F.col("avg_shots_for") - F.col("goals_for_per_match"))
        .withColumn(
            "opponent_suppression_rate",
            F.lit(1.0) - _safe_ratio(F.col("shots_on_target_against"), F.col("shots_against")),
        )
        .withColumn("avg_corners_for", _safe_ratio(F.col("corners_for"), F.col("matches_played")))
        .withColumn("avg_corners_against", _safe_ratio(F.col("corners_against"), F.col("matches_played")))
        .withColumn("avg_fouls_for", _safe_ratio(F.col("fouls_for"), F.col("matches_played")))
        .withColumn("avg_fouls_against", _safe_ratio(F.col("fouls_against"), F.col("matches_played")))
        .withColumn("avg_yellow_cards", _safe_ratio(F.col("yellow_cards"), F.col("matches_played")))
        .withColumn("avg_red_cards", _safe_ratio(F.col("red_cards"), F.col("matches_played")))
        .withColumn("fast_start_rate", _safe_ratio(F.col("leading_at_half"), F.col("matches_played")))
        .withColumn("second_half_surge_index", _safe_ratio(F.col("second_half_goals_for"), F.col("goals_for")))
        .withColumn("second_half_collapse_index", _safe_ratio(F.col("second_half_goals_against"), F.col("goals_against")))
        .withColumn("comeback_rate", _safe_ratio(F.col("comeback_results"), F.col("trailing_at_half")))
        .withColumn("blown_lead_rate", _safe_ratio(F.col("blown_leads"), F.col("leading_at_half")))
        .withColumn(
            "first_half_control_index",
            _safe_ratio(F.col("first_half_goals_for") - F.col("first_half_goals_against"), F.col("matches_played")),
        )
        .withColumn("discipline_points", F.col("yellow_cards") + (F.col("red_cards") * 2))
        .withColumn("discipline_points_per_match", _safe_ratio(F.col("discipline_points"), F.col("matches_played")))
    )


def build_gold_league_summary(silver_matches: DataFrame) -> DataFrame:
    total_goals = F.col("full_time_home_goals") + F.col("full_time_away_goals")
    return (
        silver_matches.groupBy("league_code", "season").agg(
            F.count(F.lit(1)).alias("matches"),
            F.sum(F.when(F.col("full_time_result") == "H", 1).otherwise(0)).alias("home_wins"),
            F.sum(F.when(F.col("full_time_result") == "D", 1).otherwise(0)).alias("draws"),
            F.sum(F.when(F.col("full_time_result") == "A", 1).otherwise(0)).alias("away_wins"),
            F.sum(F.col("full_time_home_goals")).alias("home_goals"),
            F.sum(F.col("full_time_away_goals")).alias("away_goals"),
            F.avg(total_goals).alias("avg_goals_per_match"),
            F.sum(total_goals).alias("total_goals"),
            F.sum(F.when((F.col("full_time_home_goals") > 0) & (F.col("full_time_away_goals") > 0), 1).otherwise(0)).alias("both_teams_scored_matches"),
            F.sum(F.when(total_goals >= 2, 1).otherwise(0)).alias("over_1_5_matches"),
            F.sum(F.when(total_goals >= 3, 1).otherwise(0)).alias("over_2_5_matches"),
            F.sum(F.when(total_goals >= 4, 1).otherwise(0)).alias("over_3_5_matches"),
            F.avg(F.col("home_shots") + F.col("away_shots")).alias("avg_shots_per_match"),
            F.avg(F.col("home_shots_on_target") + F.col("away_shots_on_target")).alias("avg_shots_on_target_per_match"),
            F.avg(F.col("home_corners") + F.col("away_corners")).alias("avg_corners_per_match"),
            F.avg(F.col("home_fouls") + F.col("away_fouls")).alias("avg_fouls_per_match"),
            F.avg(F.col("home_yellow_cards") + F.col("away_yellow_cards")).alias("avg_yellow_cards_per_match"),
            F.avg(F.col("home_red_cards") + F.col("away_red_cards")).alias("avg_red_cards_per_match"),
        )
        .withColumn("home_win_rate", _safe_ratio(F.col("home_wins"), F.col("matches")))
        .withColumn("draw_rate", _safe_ratio(F.col("draws"), F.col("matches")))
        .withColumn("away_win_rate", _safe_ratio(F.col("away_wins"), F.col("matches")))
        .withColumn("avg_home_goals", _safe_ratio(F.col("home_goals"), F.col("matches")))
        .withColumn("avg_away_goals", _safe_ratio(F.col("away_goals"), F.col("matches")))
        .withColumn("both_teams_scored_rate", _safe_ratio(F.col("both_teams_scored_matches"), F.col("matches")))
        .withColumn("over_1_5_rate", _safe_ratio(F.col("over_1_5_matches"), F.col("matches")))
        .withColumn("over_2_5_rate", _safe_ratio(F.col("over_2_5_matches"), F.col("matches")))
        .withColumn("over_3_5_rate", _safe_ratio(F.col("over_3_5_matches"), F.col("matches")))
    )


def write_gold_layer(team_table: DataFrame, league_summary: DataFrame, output_dir: Path) -> tuple[Path, Path]:
    root = gold_root(output_dir)
    team_destination = root / "team_season_table"
    summary_destination = root / "league_season_summary"

    team_table.write.mode("overwrite").partitionBy("league_code", "season").parquet(str(team_destination))
    league_summary.write.mode("overwrite").partitionBy("league_code", "season").parquet(str(summary_destination))
    return team_destination, summary_destination


def run_gold_layer(config: Mapping[str, object]) -> tuple[Path, Path]:
    """Read the silver matches table and materialize curated gold outputs."""

    raw_root = Path(config["output_dir"])
    silver_matches_path = silver_root(raw_root.parent) / "matches"

    spark = (
        SparkSession.builder.appName("football-data-gold")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    try:
        silver_matches = spark.read.parquet(str(silver_matches_path))
        team_table = build_gold_team_table(silver_matches)
        league_summary = build_gold_league_summary(silver_matches)
        destinations = write_gold_layer(team_table, league_summary, raw_root.parent)
        LOGGER.info("Gold layer escrita en %s y %s", destinations[0], destinations[1])
        return destinations
    finally:
        spark.stop()
