-- Databricks SQL
-- Raw bronze view over the external parquet table.

CREATE SCHEMA IF NOT EXISTS `<catalog>`.bronze_v;

CREATE OR REPLACE VIEW `<catalog>`.bronze_v.matches AS
SELECT
  ingestion_run_id,
  league_code,
  season,
  source_file,
  Div,
  Date,
  HomeTeam,
  AwayTeam,
  FTHG,
  FTAG,
  FTR,
  HTHG,
  HTAG,
  HTR,
  HS,
  AS,
  HST,
  AST,
  HC,
  AC,
  HF,
  AF,
  HY,
  AY,
  HR,
  AR
FROM `<catalog>`.bronze.matches;
