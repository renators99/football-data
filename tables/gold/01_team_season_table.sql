-- Databricks SQL
-- Replace <catalog> and <gold_team_season_table_path> before running.
--
-- Table: <catalog>.gold.team_season_table
-- Partition columns:
--   league_code STRING
--   season STRING
-- Columns:
--   team STRING
--   matches_played BIGINT
--   home_matches BIGINT
--   away_matches BIGINT
--   wins BIGINT
--   draws BIGINT
--   losses BIGINT
--   goals_for BIGINT
--   goals_against BIGINT
--   first_half_goals_for BIGINT
--   first_half_goals_against BIGINT
--   second_half_goals_for BIGINT
--   second_half_goals_against BIGINT
--   shots_for BIGINT
--   shots_against BIGINT
--   shots_on_target_for BIGINT
--   shots_on_target_against BIGINT
--   corners_for BIGINT
--   corners_against BIGINT
--   fouls_for BIGINT
--   fouls_against BIGINT
--   yellow_cards BIGINT
--   red_cards BIGINT
--   clean_sheets BIGINT
--   failed_to_score BIGINT
--   both_teams_scored_matches BIGINT
--   over_1_5_matches BIGINT
--   over_2_5_matches BIGINT
--   over_3_5_matches BIGINT
--   leading_at_half BIGINT
--   trailing_at_half BIGINT
--   comeback_results BIGINT
--   blown_leads BIGINT
--   goal_difference BIGINT
--   points BIGINT
--   win_rate DOUBLE
--   draw_rate DOUBLE
--   loss_rate DOUBLE
--   points_per_match DOUBLE
--   goals_for_per_match DOUBLE
--   goals_against_per_match DOUBLE
--   goal_difference_per_match DOUBLE
--   clean_sheet_rate DOUBLE
--   failed_to_score_rate DOUBLE
--   both_teams_scored_rate DOUBLE
--   over_1_5_rate DOUBLE
--   over_2_5_rate DOUBLE
--   over_3_5_rate DOUBLE
--   first_half_goals_for_per_match DOUBLE
--   first_half_goals_against_per_match DOUBLE
--   second_half_goals_for_per_match DOUBLE
--   second_half_goals_against_per_match DOUBLE
--   avg_shots_for DOUBLE
--   avg_shots_against DOUBLE
--   avg_shots_on_target_for DOUBLE
--   avg_shots_on_target_against DOUBLE
--   shot_dominance_index DOUBLE
--   on_target_dominance_index DOUBLE
--   territorial_dominance_proxy DOUBLE
--   shot_accuracy DOUBLE
--   shot_conversion_rate DOUBLE
--   net_efficiency DOUBLE
--   pressure_without_payoff DOUBLE
--   opponent_suppression_rate DOUBLE
--   avg_corners_for DOUBLE
--   avg_corners_against DOUBLE
--   avg_fouls_for DOUBLE
--   avg_fouls_against DOUBLE
--   avg_yellow_cards DOUBLE
--   avg_red_cards DOUBLE
--   fast_start_rate DOUBLE
--   second_half_surge_index DOUBLE
--   second_half_collapse_index DOUBLE
--   comeback_rate DOUBLE
--   blown_lead_rate DOUBLE
--   first_half_control_index DOUBLE
--   discipline_points BIGINT
--   discipline_points_per_match DOUBLE

CREATE SCHEMA IF NOT EXISTS `<catalog>`.gold;

CREATE OR REPLACE TABLE `<catalog>`.gold.team_season_table
(
  team STRING,
  matches_played BIGINT,
  home_matches BIGINT,
  away_matches BIGINT,
  wins BIGINT,
  draws BIGINT,
  losses BIGINT,
  goals_for BIGINT,
  goals_against BIGINT,
  first_half_goals_for BIGINT,
  first_half_goals_against BIGINT,
  second_half_goals_for BIGINT,
  second_half_goals_against BIGINT,
  shots_for BIGINT,
  shots_against BIGINT,
  shots_on_target_for BIGINT,
  shots_on_target_against BIGINT,
  corners_for BIGINT,
  corners_against BIGINT,
  fouls_for BIGINT,
  fouls_against BIGINT,
  yellow_cards BIGINT,
  red_cards BIGINT,
  clean_sheets BIGINT,
  failed_to_score BIGINT,
  both_teams_scored_matches BIGINT,
  over_1_5_matches BIGINT,
  over_2_5_matches BIGINT,
  over_3_5_matches BIGINT,
  leading_at_half BIGINT,
  trailing_at_half BIGINT,
  comeback_results BIGINT,
  blown_leads BIGINT,
  goal_difference BIGINT,
  points BIGINT,
  win_rate DOUBLE,
  draw_rate DOUBLE,
  loss_rate DOUBLE,
  points_per_match DOUBLE,
  goals_for_per_match DOUBLE,
  goals_against_per_match DOUBLE,
  goal_difference_per_match DOUBLE,
  clean_sheet_rate DOUBLE,
  failed_to_score_rate DOUBLE,
  both_teams_scored_rate DOUBLE,
  over_1_5_rate DOUBLE,
  over_2_5_rate DOUBLE,
  over_3_5_rate DOUBLE,
  first_half_goals_for_per_match DOUBLE,
  first_half_goals_against_per_match DOUBLE,
  second_half_goals_for_per_match DOUBLE,
  second_half_goals_against_per_match DOUBLE,
  avg_shots_for DOUBLE,
  avg_shots_against DOUBLE,
  avg_shots_on_target_for DOUBLE,
  avg_shots_on_target_against DOUBLE,
  shot_dominance_index DOUBLE,
  on_target_dominance_index DOUBLE,
  territorial_dominance_proxy DOUBLE,
  shot_accuracy DOUBLE,
  shot_conversion_rate DOUBLE,
  net_efficiency DOUBLE,
  pressure_without_payoff DOUBLE,
  opponent_suppression_rate DOUBLE,
  avg_corners_for DOUBLE,
  avg_corners_against DOUBLE,
  avg_fouls_for DOUBLE,
  avg_fouls_against DOUBLE,
  avg_yellow_cards DOUBLE,
  avg_red_cards DOUBLE,
  fast_start_rate DOUBLE,
  second_half_surge_index DOUBLE,
  second_half_collapse_index DOUBLE,
  comeback_rate DOUBLE,
  blown_lead_rate DOUBLE,
  first_half_control_index DOUBLE,
  discipline_points BIGINT,
  discipline_points_per_match DOUBLE,
  league_code STRING,
  season STRING
)
USING PARQUET
PARTITIONED BY (league_code, season)
LOCATION '<gold_team_season_table_path>';
