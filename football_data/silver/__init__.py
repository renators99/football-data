"""Build the silver layer with normalized match-level rows."""

import logging
from pathlib import Path
from typing import Mapping, cast

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..utils.layers import silver_root

LOGGER = logging.getLogger(__name__)


def _build_league_code_expression(league_name_to_code: Mapping[str, str]):
    mapping_items = []
    for league_name, league_code in league_name_to_code.items():
        mapping_items.extend((F.lit(league_name), F.lit(league_code)))
    return F.create_map(*mapping_items)


def _optional_integer_column(frame: DataFrame, column_name: str):
    if column_name in frame.columns:
        return F.col(column_name).cast("int")
    return F.lit(None).cast("int")


def build_silver_matches(
    spark: SparkSession,
    bronze_csv_glob: str,
    league_name_to_code: Mapping[str, str],
) -> DataFrame:
    bronze = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(bronze_csv_glob)
    )
    league_code_map = _build_league_code_expression(league_name_to_code)
    half_time_home_goals = _optional_integer_column(bronze, "HTHG")
    half_time_away_goals = _optional_integer_column(bronze, "HTAG")
    home_shots = _optional_integer_column(bronze, "HS")
    away_shots = _optional_integer_column(bronze, "AS")
    home_shots_on_target = _optional_integer_column(bronze, "HST")
    away_shots_on_target = _optional_integer_column(bronze, "AST")
    home_corners = _optional_integer_column(bronze, "HC")
    away_corners = _optional_integer_column(bronze, "AC")
    home_fouls = _optional_integer_column(bronze, "HF")
    away_fouls = _optional_integer_column(bronze, "AF")
    home_yellow_cards = _optional_integer_column(bronze, "HY")
    away_yellow_cards = _optional_integer_column(bronze, "AY")
    home_red_cards = _optional_integer_column(bronze, "HR")
    away_red_cards = _optional_integer_column(bronze, "AR")

    return (
        bronze.withColumn("source_file", F.input_file_name())
        .withColumn("league_name", F.regexp_extract("source_file", r"([^/\\\\]+)[/\\\\][^/\\\\]+\.csv$", 1))
        .withColumn("league_code", F.element_at(league_code_map, F.col("league_name")))
        .withColumn("season", F.regexp_extract("source_file", r"([^/\\\\]+)\.csv$", 1))
        .withColumn("match_date", F.coalesce(
            F.to_date(F.col("Date"), "dd/MM/yy"),
            F.to_date(F.col("Date"), "dd/MM/yyyy"),
            F.to_date(F.col("Date"), "yyyy-MM-dd"),
        ))
        .withColumn("home_team", F.col("HomeTeam"))
        .withColumn("away_team", F.col("AwayTeam"))
        .withColumn("full_time_home_goals", F.coalesce(F.col("FTHG").cast("int"), F.lit(0)))
        .withColumn("full_time_away_goals", F.coalesce(F.col("FTAG").cast("int"), F.lit(0)))
        .withColumn("half_time_home_goals", half_time_home_goals)
        .withColumn("half_time_away_goals", half_time_away_goals)
        .withColumn("full_time_result", F.coalesce(F.col("FTR"), F.lit("U")))
        .withColumn("home_shots", home_shots)
        .withColumn("away_shots", away_shots)
        .withColumn("home_shots_on_target", home_shots_on_target)
        .withColumn("away_shots_on_target", away_shots_on_target)
        .withColumn("home_corners", home_corners)
        .withColumn("away_corners", away_corners)
        .withColumn("home_fouls", home_fouls)
        .withColumn("away_fouls", away_fouls)
        .withColumn("home_yellow_cards", home_yellow_cards)
        .withColumn("away_yellow_cards", away_yellow_cards)
        .withColumn("home_red_cards", home_red_cards)
        .withColumn("away_red_cards", away_red_cards)
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
            "half_time_home_goals",
            "half_time_away_goals",
            "full_time_result",
            "home_shots",
            "away_shots",
            "home_shots_on_target",
            "away_shots_on_target",
            "home_corners",
            "away_corners",
            "home_fouls",
            "away_fouls",
            "home_yellow_cards",
            "away_yellow_cards",
            "home_red_cards",
            "away_red_cards",
        )
        .where(
            F.col("home_team").isNotNull()
            & F.col("away_team").isNotNull()
            & F.col("league_code").isNotNull()
            & F.col("season").isNotNull()
        )
    )


def write_silver_layer(silver_matches: DataFrame, output_dir: Path) -> Path:
    destination = silver_root(output_dir) / "matches"
    silver_matches.write.mode("overwrite").partitionBy("league_code", "season").parquet(str(destination))
    return destination


def run_silver_layer(config: Mapping[str, object]) -> Path:
    """Read raw CSV files and materialize the silver matches table."""

    raw_root = Path(config["output_dir"])
    bronze_csv_glob = str(raw_root / "*" / "*.csv")
    league_codes = cast(Mapping[str, str], config["league_codes"])
    league_name_to_code = {
        league_name: league_code
        for league_code, league_name in league_codes.items()
    }

    spark = (
        SparkSession.builder.appName("football-data-silver")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    try:
        silver_matches = build_silver_matches(spark, bronze_csv_glob, league_name_to_code)
        destination = write_silver_layer(silver_matches, raw_root.parent)
        LOGGER.info("Silver layer escrita en %s", destination)
        return destination
    finally:
        spark.stop()
