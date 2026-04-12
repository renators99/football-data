-- Databricks SQL
-- Replace <catalog> and <gold_league_season_summary_path> before running.
--
-- Table: <catalog>.gold.league_season_summary
-- Partition columns:
--   league_code STRING
--   season STRING
-- Columns:
--   matches BIGINT
--   home_wins BIGINT
--   draws BIGINT
--   away_wins BIGINT
--   home_goals BIGINT
--   away_goals BIGINT
--   avg_goals_per_match DOUBLE
--   total_goals BIGINT
--   both_teams_scored_matches BIGINT
--   over_1_5_matches BIGINT
--   over_2_5_matches BIGINT
--   over_3_5_matches BIGINT
--   avg_shots_per_match DOUBLE
--   avg_shots_on_target_per_match DOUBLE
--   avg_corners_per_match DOUBLE
--   avg_fouls_per_match DOUBLE
--   avg_yellow_cards_per_match DOUBLE
--   avg_red_cards_per_match DOUBLE
--   home_win_rate DOUBLE
--   draw_rate DOUBLE
--   away_win_rate DOUBLE
--   avg_home_goals DOUBLE
--   avg_away_goals DOUBLE
--   both_teams_scored_rate DOUBLE
--   over_1_5_rate DOUBLE
--   over_2_5_rate DOUBLE
--   over_3_5_rate DOUBLE

CREATE SCHEMA IF NOT EXISTS `<catalog>`.gold;

CREATE OR REPLACE TABLE `<catalog>`.gold.league_season_summary
(
  matches BIGINT,
  home_wins BIGINT,
  draws BIGINT,
  away_wins BIGINT,
  home_goals BIGINT,
  away_goals BIGINT,
  avg_goals_per_match DOUBLE,
  total_goals BIGINT,
  both_teams_scored_matches BIGINT,
  over_1_5_matches BIGINT,
  over_2_5_matches BIGINT,
  over_3_5_matches BIGINT,
  avg_shots_per_match DOUBLE,
  avg_shots_on_target_per_match DOUBLE,
  avg_corners_per_match DOUBLE,
  avg_fouls_per_match DOUBLE,
  avg_yellow_cards_per_match DOUBLE,
  avg_red_cards_per_match DOUBLE,
  home_win_rate DOUBLE,
  draw_rate DOUBLE,
  away_win_rate DOUBLE,
  avg_home_goals DOUBLE,
  avg_away_goals DOUBLE,
  both_teams_scored_rate DOUBLE,
  over_1_5_rate DOUBLE,
  over_2_5_rate DOUBLE,
  over_3_5_rate DOUBLE,
  league_code STRING,
  season STRING
)
USING PARQUET
PARTITIONED BY (league_code, season)
LOCATION '<gold_league_season_summary_path>';
