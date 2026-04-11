import logging
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from .config import load_config_from_env
from .silver import get_spark_session

LOGGER = logging.getLogger(__name__)


def calculate_league_summary(matches: DataFrame) -> DataFrame:
    """Calculate summary statistics per league and season."""
    # matches schema: League, Season, Date, HomeTeam, AwayTeam, HomeGoals, AwayGoals, Result
    
    summary = matches.groupBy("League", "Season").agg(
        F.count("*").alias("TotalMatches"),
        F.sum(F.col("HomeGoals") + F.col("AwayGoals")).alias("TotalGoals"),
        F.avg(F.col("HomeGoals") + F.col("AwayGoals")).alias("AvgGoalsPerGame"),
        (F.sum(F.when(F.col("Result") == "H", 1).otherwise(0)) / F.count("*")).alias("HomeWinPct"),
        (F.sum(F.when(F.col("Result") == "A", 1).otherwise(0)) / F.count("*")).alias("AwayWinPct"),
        (F.sum(F.when(F.col("Result") == "D", 1).otherwise(0)) / F.count("*")).alias("DrawPct")
    )
    
    return summary


def calculate_team_history(standings: DataFrame) -> DataFrame:
    """Calculate historical stats for teams across all seasons."""
    # standings schema: League, Season, Team, Played, Points, Won, Drawn, Lost, GF, GA, GD
    
    history = standings.groupBy("Team").agg(
        F.countDistinct("Season").alias("SeasonsPlayed"),
        F.sum("Played").alias("TotalMatches"),
        F.sum("Points").alias("TotalPoints"),
        F.sum("Won").alias("TotalWins"),
        F.sum("Drawn").alias("TotalDraws"),
        F.sum("Lost").alias("TotalLosses"),
        F.sum("GF").alias("TotalGF"),
        F.sum("GA").alias("TotalGA"),
        F.max("Points").alias("BestSeasonPoints")
    )
    
    return history


def run_gold_layer(config: Optional[Dict[str, object]] = None) -> None:
    """Main entry point for Gold layer processing."""
    if config is None:
        config = load_config_from_env()
        
    raw_dir = Path(config["output_dir"]) 
    # data/raw -> data/silver -> data/gold
    silver_dir = raw_dir.parent.parent / "silver"
    gold_dir = raw_dir.parent.parent / "gold"
    
    LOGGER.info(f"Starting Gold layer processing. Silver: {silver_dir}, Gold: {gold_dir}")
    
    spark = get_spark_session()
    
    try:
        matches_path = silver_dir / "matches"
        standings_path = silver_dir / "standings"
        
        if not matches_path.exists() or not standings_path.exists():
            LOGGER.warning("Silver layer data not found. Skipping Gold layer.")
            return

        matches = spark.read.parquet(str(matches_path))
        standings = spark.read.parquet(str(standings_path))
        
        # 1. League Summary
        league_summary = calculate_league_summary(matches)
        league_summary_path = gold_dir / "league_summary"
        LOGGER.info(f"Saving league summary to {league_summary_path}")
        league_summary.write.mode("overwrite").parquet(str(league_summary_path))
        
        # 2. Team History
        team_history = calculate_team_history(standings)
        team_history_path = gold_dir / "team_history"
        LOGGER.info(f"Saving team history to {team_history_path}")
        team_history.write.mode("overwrite").parquet(str(team_history_path))
        
        LOGGER.info("Gold layer processing completed successfully.")
        
    finally:
        spark.stop()
