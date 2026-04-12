-- Databricks SQL
-- Ranking and analytics view over the gold team table.

CREATE SCHEMA IF NOT EXISTS `<catalog>`.gold_v;

CREATE OR REPLACE VIEW `<catalog>`.gold_v.team_season_table_ranked AS
SELECT
  league_code,
  season,
  team,
  matches_played,
  home_matches,
  away_matches,
  wins,
  draws,
  losses,
  points,
  goal_difference,
  goals_for,
  goals_against,
  win_rate,
  points_per_match,
  goals_for_per_match,
  goals_against_per_match,
  clean_sheet_rate,
  both_teams_scored_rate,
  over_2_5_rate,
  shot_dominance_index,
  on_target_dominance_index,
  territorial_dominance_proxy,
  net_efficiency,
  pressure_without_payoff,
  opponent_suppression_rate,
  fast_start_rate,
  second_half_surge_index,
  second_half_collapse_index,
  comeback_rate,
  blown_lead_rate,
  first_half_control_index,
  shot_accuracy,
  shot_conversion_rate,
  discipline_points_per_match,
  DENSE_RANK() OVER (
    PARTITION BY league_code, season
    ORDER BY points DESC, goal_difference DESC, goals_for DESC, team ASC
  ) AS league_position
FROM `<catalog>`.gold.team_season_table;
