-- Databricks SQL
-- Enriched normalized match view.

CREATE SCHEMA IF NOT EXISTS `<catalog>`.silver_v;

CREATE OR REPLACE VIEW `<catalog>`.silver_v.matches_enriched AS
SELECT
  league_code,
  season,
  season_start_year,
  match_date,
  home_team,
  away_team,
  full_time_home_goals,
  full_time_away_goals,
  half_time_home_goals,
  half_time_away_goals,
  full_time_result,
  home_shots,
  away_shots,
  home_shots_on_target,
  away_shots_on_target,
  home_corners,
  away_corners,
  home_fouls,
  away_fouls,
  home_yellow_cards,
  away_yellow_cards,
  home_red_cards,
  away_red_cards,
  full_time_home_goals + full_time_away_goals AS total_goals,
  half_time_home_goals + half_time_away_goals AS first_half_goals,
  (full_time_home_goals - half_time_home_goals) + (full_time_away_goals - half_time_away_goals) AS second_half_goals,
  CASE
    WHEN full_time_result = 'H' THEN home_team
    WHEN full_time_result = 'A' THEN away_team
    ELSE NULL
  END AS winning_team,
  CASE
    WHEN full_time_result = 'D' THEN TRUE
    ELSE FALSE
  END AS is_draw
FROM `<catalog>`.silver.matches;
