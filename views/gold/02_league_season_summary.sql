-- Databricks SQL
-- League-level summary view over the gold aggregate table.

CREATE SCHEMA IF NOT EXISTS `<catalog>`.gold_v;

CREATE OR REPLACE VIEW `<catalog>`.gold_v.league_season_summary AS
SELECT
  league_code,
  season,
  matches,
  home_wins,
  draws,
  away_wins,
  total_goals,
  avg_goals_per_match,
  avg_home_goals,
  avg_away_goals,
  home_win_rate,
  draw_rate,
  away_win_rate,
  both_teams_scored_matches,
  both_teams_scored_rate,
  over_1_5_matches,
  over_1_5_rate,
  over_2_5_matches,
  over_2_5_rate,
  over_3_5_matches,
  over_3_5_rate,
  avg_shots_per_match,
  avg_shots_on_target_per_match,
  avg_corners_per_match,
  avg_fouls_per_match,
  avg_yellow_cards_per_match,
  avg_red_cards_per_match,
  avg_home_goals - avg_away_goals AS home_goal_advantage
FROM `<catalog>`.gold.league_season_summary;
