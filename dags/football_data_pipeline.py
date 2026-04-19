"""Airflow DAG for the football-data lakehouse pipeline."""

from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

LOGGER = logging.getLogger(__name__)
PROJECT_DIR = Path(os.getenv("FOOTBALL_DATA_PROJECT_DIR", "/opt/airflow/project"))


def _ensure_project_importable() -> None:
    project_path = str(PROJECT_DIR)
    if project_path not in sys.path:
        sys.path.insert(0, project_path)


def run_bronze_layer() -> None:
    _ensure_project_importable()

    from football_data.bronze import run_spark_job
    from football_data.utils.config import DEFAULT_START_YEAR
    from football_data.utils.seasons import build_season_list
    from football_data_scraper import build_config

    config = build_config()
    seasons = build_season_list(int(config.get("start_year", DEFAULT_START_YEAR)))

    LOGGER.info("Running bronze layer for %d seasons", len(seasons))
    run_spark_job(config, seasons)


def run_silver_layer() -> None:
    _ensure_project_importable()

    from football_data.silver import run_silver_layer as build_silver
    from football_data_scraper import build_config

    LOGGER.info("Running silver layer")
    build_silver(build_config())


def run_gold_layer() -> None:
    _ensure_project_importable()

    from football_data.gold import run_gold_layer as build_gold
    from football_data_scraper import build_config

    LOGGER.info("Running gold layer")
    build_gold(build_config())


default_args = {
    "owner": "football-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="football_data_pipeline",
    default_args=default_args,
    description="Download football-data.co.uk CSVs and build bronze, silver and gold parquet layers.",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["football-data", "pyspark", "lakehouse"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze_download_matches",
        python_callable=run_bronze_layer,
    )

    silver = PythonOperator(
        task_id="silver_normalize_matches",
        python_callable=run_silver_layer,
    )

    gold = PythonOperator(
        task_id="gold_build_aggregates",
        python_callable=run_gold_layer,
    )

    bronze >> silver >> gold
