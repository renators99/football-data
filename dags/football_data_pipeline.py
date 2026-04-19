"""Airflow DAG for the football-data lakehouse pipeline on Google Cloud Dataproc."""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.utils.trigger_rule import TriggerRule

LOGGER = logging.getLogger(__name__)
PROJECT_DIR = Path(os.getenv("FOOTBALL_DATA_PROJECT_DIR", "/opt/airflow/project"))
GCP_PROJECT = os.getenv("GCP_PROJECT", "your-gcp-project")
GCP_REGION = os.getenv("GCP_REGION", "us-central1")
DATAPROC_CLUSTER = os.getenv("DATAPROC_CLUSTER", "football-cluster")
BQ_DATASET = os.getenv("BQ_DATASET", "football_data")
GCS_BUCKET = os.getenv("GCS_BUCKET", "your-bucket")


# Queries para crear tablas externas en BigQuery
CREATE_BRONZE_TABLE = f"""
CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT}.{BQ_DATASET}.bronze_matches`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{GCS_BUCKET}/football-data/bronze/matches/*.parquet']
);
"""

CREATE_SILVER_TABLE = f"""
CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT}.{BQ_DATASET}.silver_matches`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{GCS_BUCKET}/football-data/silver/matches/*.parquet']
);
"""

CREATE_GOLD_TABLE = f"""
CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT}.{BQ_DATASET}.gold_team_season`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{GCS_BUCKET}/football-data/gold/team_season/*.parquet']
);
"""


def _ensure_project_importable() -> None:
    project_path = str(PROJECT_DIR)
    if project_path not in sys.path:
        sys.path.insert(0, project_path)


def create_tables_and_views() -> None:
    _ensure_project_importable()

    LOGGER.info("Tables and views will be created via BigQuery operators in the DAG")


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
    description="Download football-data.co.uk CSVs and build bronze, silver and gold parquet layers on Dataproc.",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["football-data", "pyspark", "lakehouse", "gcp"],
) as dag:
    check_bronze_table = BigQueryTableExistenceSensor(
        task_id="check_bronze_table_exists",
        project_id=GCP_PROJECT,
        dataset_id=BQ_DATASET,
        table_id="bronze_matches",
        gcp_conn_id="google_cloud_default",
        poke_interval=30,
        timeout=60,
    )

    create_bronze_table = BigQueryInsertJobOperator(
        task_id="create_bronze_table",
        configuration={
            "query": {
                "query": CREATE_BRONZE_TABLE,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    check_silver_table = BigQueryTableExistenceSensor(
        task_id="check_silver_table_exists",
        project_id=GCP_PROJECT,
        dataset_id=BQ_DATASET,
        table_id="silver_matches",
        gcp_conn_id="google_cloud_default",
        poke_interval=30,
        timeout=60,
    )

    create_silver_table = BigQueryInsertJobOperator(
        task_id="create_silver_table",
        configuration={
            "query": {
                "query": CREATE_SILVER_TABLE,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    check_gold_table = BigQueryTableExistenceSensor(
        task_id="check_gold_table_exists",
        project_id=GCP_PROJECT,
        dataset_id=BQ_DATASET,
        table_id="gold_team_season",
        gcp_conn_id="google_cloud_default",
        poke_interval=30,
        timeout=60,
    )

    create_gold_table = BigQueryInsertJobOperator(
        task_id="create_gold_table",
        configuration={
            "query": {
                "query": CREATE_GOLD_TABLE,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    bronze_job = {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/football_data/bronze/run_bronze.py",
            "args": [],
        },
    }

    bronze = DataprocSubmitJobOperator(
        task_id="bronze_download_matches",
        job=bronze_job,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    silver_job = {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/football_data/silver/run_silver.py",
            "args": [],
        },
    }

    silver = DataprocSubmitJobOperator(
        task_id="silver_normalize_matches",
        job=silver_job,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    gold_job = {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/football_data/gold/run_gold.py",
            "args": [],
        },
    }

    gold = DataprocSubmitJobOperator(
        task_id="gold_build_aggregates",
        job=gold_job,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    ml_job = {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/football_data/ml/run_ml.py",
            "args": [],
        },
    }

    ml = DataprocSubmitJobOperator(
        task_id="ml_train_model",
        job=ml_job,
        region=GCP_REGION,
        gcp_conn_id="google_cloud_default",
    )

    check_bronze_table >> create_bronze_table >> bronze
    check_silver_table >> create_silver_table >> silver
    check_gold_table >> create_gold_table >> gold

    bronze >> silver >> gold >> ml
