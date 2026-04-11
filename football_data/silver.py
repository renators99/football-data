import logging
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from .config import DEFAULT_OUTPUT_DIR, load_config_from_env

LOGGER = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """Get or create a Spark session."""
    return (
        SparkSession.builder.appName("football-data-silver")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def load_raw_data(spark: SparkSession, raw_dir: Path) -> DataFrame:
    """Load all CSV files from the raw directory."""
    # Pattern to match all CSVs in subdirectories: data/raw/football-data/*/*.csv
    # We assume the structure is {league_name}/{season}.csv based on the scraper
    path_pattern = str(raw_dir / "*" / "*.csv")
    
    LOGGER.info(f"Loading data from {path_pattern}")
    
    # Read CSVs with header
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path_pattern)
    
    # Add input filename to extract metadata
    df = df.withColumn("source_file", F.input_file_name())
    
    # Extract League and Season from path
    # Path format: .../data/raw/football-data/{league}/{season}.csv
    # We can split by "/" and get the last two parts
    
    # Note: input_file_name returns the full path, which might be file:///... or just /...
    # We'll use regexp_extract or split. Split is safer if we don't know the full prefix length.
    # However, since we know the structure, let's try to parse it.
    
    # Let's assume standard unix separators for now, Spark usually normalizes them.
    # But on Windows it might be tricky. Let's use a robust regex.
    # We want the folder name before the file, and the filename without extension.
    
    # Regex explanation:
    # ([^/|\\]+) matches the league folder (not slash or backslash)
    # [/\\] matches the separator
    # ([^/|\\]+)\.csv matches the season filename
    # $ matches end of string
    
    # We need to be careful about the full path.
    # Let's just take the last two components.
    
    df = df.withColumn("filename_season", F.regexp_extract("source_file", r"([^/|\\]+)\.csv$", 1))
    df = df.withColumn("path_parts", F.split("source_file", r"[/\\]"))
    df = df.withColumn("league_slug", F.element_at("path_parts", -2))
    
    # Map league_slug to a cleaner name if needed, but the slug is fine for now.
    # We can also map the season filename (e.g. "2324") to a full year if we want, 
    # but "2324" is what we have.
    
    return df


def clean_matches(df: DataFrame) -> DataFrame:
    """Select and clean core columns for matches."""
    # Standard columns usually present in football-data.co.uk
    # Date, HomeTeam, AwayTeam, FTHG, FTAG, FTR
    
    # Filter out rows where essential data is missing
    df = df.filter(F.col("Date").isNotNull() & F.col("HomeTeam").isNotNull() & F.col("AwayTeam").isNotNull())
    
    # Select and rename
    matches = df.select(
        F.col("league_slug").alias("League"),
        F.col("filename_season").alias("Season"),
        F.col("Date"),
        F.col("HomeTeam"),
        F.col("AwayTeam"),
        F.col("FTHG").cast(IntegerType()).alias("HomeGoals"),
        F.col("FTAG").cast(IntegerType()).alias("AwayGoals"),
        F.col("FTR").alias("Result")
    )
    
    return matches


def calculate_standings(matches: DataFrame) -> DataFrame:
    """Calculate standings from matches."""
    # We need to explode matches into two rows per match: one for Home, one for Away
    
    # Home stats
    home = matches.select(
        "League", "Season",
        F.col("HomeTeam").alias("Team"),
        F.col("HomeGoals").alias("GF"),
        F.col("AwayGoals").alias("GA"),
        F.when(F.col("Result") == "H", 3).when(F.col("Result") == "D", 1).otherwise(0).alias("Points"),
        F.when(F.col("Result") == "H", 1).otherwise(0).alias("Win"),
        F.when(F.col("Result") == "D", 1).otherwise(0).alias("Draw"),
        F.when(F.col("Result") == "A", 1).otherwise(0).alias("Loss")
    )
    
    # Away stats
    away = matches.select(
        "League", "Season",
        F.col("AwayTeam").alias("Team"),
        F.col("AwayGoals").alias("GF"),
        F.col("HomeGoals").alias("GA"),
        F.when(F.col("Result") == "A", 3).when(F.col("Result") == "D", 1).otherwise(0).alias("Points"),
        F.when(F.col("Result") == "A", 1).otherwise(0).alias("Win"),
        F.when(F.col("Result") == "D", 1).otherwise(0).alias("Draw"),
        F.when(F.col("Result") == "H", 1).otherwise(0).alias("Loss")
    )
    
    # Union
    all_team_matches = home.unionByName(away)
    
    # Aggregate
    standings = all_team_matches.groupBy("League", "Season", "Team").agg(
        F.count("*").alias("Played"),
        F.sum("Points").alias("Points"),
        F.sum("Win").alias("Won"),
        F.sum("Draw").alias("Drawn"),
        F.sum("Loss").alias("Lost"),
        F.sum("GF").alias("GF"),
        F.sum("GA").alias("GA")
    )
    
    # Calculate Goal Difference
    standings = standings.withColumn("GD", F.col("GF") - F.col("GA"))
    
    # Sort (optional, but good for viewing) - usually by Points desc, GD desc, GF desc
    # Note: Spark sorting is within partitions unless we do a global sort, 
    # but for saving it doesn't matter much.
    
    return standings


def run_silver_layer(config: Optional[Dict[str, object]] = None) -> None:
    """Main entry point for Silver layer processing."""
    if config is None:
        config = load_config_from_env()
        
    raw_dir = Path(config["output_dir"]) # This points to data/raw/football-data
    # We want to save to data/silver
    # Assuming standard structure: data/raw -> data/silver
    silver_dir = raw_dir.parent.parent / "silver"
    
    LOGGER.info(f"Starting Silver layer processing. Raw: {raw_dir}, Silver: {silver_dir}")
    
    spark = get_spark_session()
    
    try:
        raw_df = load_raw_data(spark, raw_dir)
        
        # 1. Matches
        matches = clean_matches(raw_df)
        matches_path = silver_dir / "matches"
        LOGGER.info(f"Saving matches to {matches_path}")
        matches.write.mode("overwrite").parquet(str(matches_path))
        
        # 2. Standings
        standings = calculate_standings(matches)
        standings_path = silver_dir / "standings"
        LOGGER.info(f"Saving standings to {standings_path}")
        standings.write.mode("overwrite").parquet(str(standings_path))
        
        LOGGER.info("Silver layer processing completed successfully.")
        
    finally:
        spark.stop()
