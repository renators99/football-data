-- Databricks SQL
-- Replace <catalog> and <bronze_matches_path> before running.
-- Example path:
-- abfss://lakehouse@storageaccount.dfs.core.windows.net/football-data/raw/bronze/matches
--
-- Table: <catalog>.bronze.matches
-- Partition columns:
--   league_code STRING
--   season STRING
-- Common columns:
--   ingestion_run_id STRING
--   source_file STRING
--   league_name STRING
--   Div STRING
--   Date STRING
--   HomeTeam STRING
--   AwayTeam STRING
--   FTHG STRING
--   FTAG STRING
--   FTR STRING
--   HTHG STRING
--   HTAG STRING
--   HTR STRING
--   Attendance STRING
--   Referee STRING
--   HS STRING
--   AS STRING
--   HST STRING
--   AST STRING
--   HHW STRING
--   AHW STRING
--   HC STRING
--   AC STRING
--   HF STRING
--   AF STRING
--   HO STRING
--   AO STRING
--   HY STRING
--   AY STRING
--   HR STRING
--   AR STRING
--   HBP STRING
--   ABP STRING
--   GBH STRING
--   GBD STRING
--   GBA STRING
--   IWH STRING
--   IWD STRING
--   IWA STRING
--   LBH STRING
--   LBD STRING
--   LBA STRING
--   SBH STRING
--   SBD STRING
--   SBA STRING
--   WHH STRING
--   WHD STRING
--   WHA STRING
-- Note:
--   bronze.matches keeps raw columns from football-data.co.uk, so some extra
--   fields can appear depending on season/source.

CREATE SCHEMA IF NOT EXISTS `<catalog>`.bronze;

CREATE OR REPLACE TABLE `<catalog>`.bronze.matches
(
  ingestion_run_id STRING,
  source_file STRING,
  league_name STRING,
  Div STRING,
  Date STRING,
  HomeTeam STRING,
  AwayTeam STRING,
  FTHG STRING,
  FTAG STRING,
  FTR STRING,
  HTHG STRING,
  HTAG STRING,
  HTR STRING,
  Attendance STRING,
  Referee STRING,
  HS STRING,
  AS STRING,
  HST STRING,
  AST STRING,
  HHW STRING,
  AHW STRING,
  HC STRING,
  AC STRING,
  HF STRING,
  AF STRING,
  HO STRING,
  AO STRING,
  HY STRING,
  AY STRING,
  HR STRING,
  AR STRING,
  HBP STRING,
  ABP STRING,
  GBH STRING,
  GBD STRING,
  GBA STRING,
  IWH STRING,
  IWD STRING,
  IWA STRING,
  LBH STRING,
  LBD STRING,
  LBA STRING,
  SBH STRING,
  SBD STRING,
  SBA STRING,
  WHH STRING,
  WHD STRING,
  WHA STRING,
  league_code STRING,
  season STRING
)
USING PARQUET
PARTITIONED BY (league_code, season)
LOCATION '<bronze_matches_path>';
