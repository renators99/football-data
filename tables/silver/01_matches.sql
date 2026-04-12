-- Databricks SQL
-- Replace <catalog> and <silver_matches_path> before running.
-- Example path:
-- abfss://lakehouse@storageaccount.dfs.core.windows.net/football-data/raw/silver/matches
--
-- Table: <catalog>.silver.matches
-- Partition columns:
--   league_code STRING
--   season STRING
-- Columns:
--   season_start_year INT
--   match_date DATE
--   home_team STRING
--   away_team STRING
--   full_time_home_goals INT
--   full_time_away_goals INT
--   half_time_home_goals INT
--   half_time_away_goals INT
--   full_time_result STRING
--   home_shots INT
--   away_shots INT
--   home_shots_on_target INT
--   away_shots_on_target INT
--   home_corners INT
--   away_corners INT
--   home_fouls INT
--   away_fouls INT
--   home_yellow_cards INT
--   away_yellow_cards INT
--   home_red_cards INT
--   away_red_cards INT

CREATE SCHEMA IF NOT EXISTS `<catalog>`.silver;

CREATE OR REPLACE TABLE `<catalog>`.silver.matches
(
  season_start_year INT,
  match_date DATE,
  home_team STRING,
  away_team STRING,
  full_time_home_goals INT,
  full_time_away_goals INT,
  half_time_home_goals INT,
  half_time_away_goals INT,
  full_time_result STRING,
  home_shots INT,
  away_shots INT,
  home_shots_on_target INT,
  away_shots_on_target INT,
  home_corners INT,
  away_corners INT,
  home_fouls INT,
  away_fouls INT,
  home_yellow_cards INT,
  away_yellow_cards INT,
  home_red_cards INT,
  away_red_cards INT,
  league_code STRING,
  season STRING
)
USING PARQUET
PARTITIONED BY (league_code, season)
LOCATION '<silver_matches_path>';
