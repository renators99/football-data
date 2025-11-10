"""PySpark job to download football-data.co.uk CSV files."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List, Sequence, Tuple

import requests
from pyspark.sql import SparkSession


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE_URL = "https://www.football-data.co.uk/mmz4281/{season}/{league_code}.csv"
DEFAULT_OUTPUT_DIR = Path(os.getenv("FOOTBALL_DATA_OUTPUT_DIR", "data/raw/football-data"))
DEFAULT_START_YEAR = int(os.getenv("FOOTBALL_DATA_START_YEAR", "1993"))
DEFAULT_PARTITIONS = int(os.getenv("FOOTBALL_DATA_PARTITIONS", "24"))
DEFAULT_GCS_BUCKET = os.getenv("FOOTBALL_DATA_GCS_BUCKET")
DEFAULT_GCS_PREFIX = os.getenv("FOOTBALL_DATA_GCS_PREFIX", "lakehouse/football-data")

LEAGUE_CODES = {
    "E0": "england_premier_league",
    "E1": "england_championship",
    "SP1": "spain_la_liga",
    "SP2": "spain_segunda_division",
    "I1": "italy_serie_a",
    "I2": "italy_serie_b",
    "F1": "france_ligue_1",
    "F2": "france_ligue_2",
    "D1": "germany_bundesliga_1",
    "D2": "germany_bundesliga_2",
    "N1": "netherlands_eredivisie",
    "P1": "portugal_primeira_liga",
}


Task = Tuple[str, str, str]
Result = Tuple[str, str, str, bool, str]


def build_season_codes(start_year: int, end_year: int | None = None) -> List[str]:
    """Return season codes used by football-data.co.uk (e.g. "2324")."""

    if end_year is None:
        end_year = datetime.utcnow().year

    if start_year > end_year:
        msg = f"start_year {start_year} must be <= end_year {end_year}"
        raise ValueError(msg)

    seasons: List[str] = []
    for year in range(start_year, end_year + 1):
        next_year = year + 1
        seasons.append(f"{str(year)[-2:]}{str(next_year)[-2:]}")
    return seasons


def build_tasks(seasons: Sequence[str], output_dir: Path = DEFAULT_OUTPUT_DIR) -> List[Task]:
    """Return download tasks as tuples of (season, league_code, output_path)."""

    tasks: List[Task] = []
    for league_code, league_name in LEAGUE_CODES.items():
        league_dir = output_dir / league_name
        for season in seasons:
            tasks.append((season, league_code, str(league_dir / f"{season}.csv")))
    return tasks


def download_csv(task: Task) -> Result:
    """Download a single CSV and report the outcome."""

    season, league_code, output_path_str = task
    output_path = Path(output_path_str)
    url = BASE_URL.format(season=season, league_code=league_code)

    try:
        response = requests.get(url, timeout=30)
    except Exception as exc:  # noqa: BLE001
        message = f"Error downloading {league_code} season {season}: {exc}"
        LOGGER.error(message)
        return season, league_code, output_path_str, False, message

    if response.status_code == 200 and response.content.strip():
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(response.content)
        message = f"Downloaded {league_code} season {season}"
        LOGGER.info(message)
        return season, league_code, output_path_str, True, message

    message = (
        f"Missing data for {league_code} season {season}: HTTP {response.status_code}"
    )
    LOGGER.warning(message)
    return season, league_code, output_path_str, False, message


def upload_successful_results_to_gcs(
    results: Sequence[Result],
    output_dir: Path,
    *,
    bucket_name: str,
    prefix: str | None = None,
) -> None:
    """Upload downloaded CSVs to Google Cloud Storage."""

    try:
        from google.cloud import storage
    except ModuleNotFoundError as exc:  # pragma: no cover - guard for optional dep
        msg = "google-cloud-storage must be installed to enable GCS uploads"
        raise RuntimeError(msg) from exc

    client = storage.Client()

    normalized_prefix = (prefix or "").strip("/")
    output_dir = output_dir.resolve()
    bucket = client.bucket(bucket_name)

    for season, league_code, output_path_str, ok, _ in results:
        if not ok:
            continue

        local_path = Path(output_path_str)

        try:
            relative_path = local_path.resolve().relative_to(output_dir)
            blob_name = (
                f"{normalized_prefix}/{relative_path.as_posix()}"
                if normalized_prefix
                else relative_path.as_posix()
            )
        except ValueError:
            # Fallback in the unlikely event the file is outside output_dir.
            blob_name = (
                f"{normalized_prefix}/{local_path.name}" if normalized_prefix else local_path.name
            )

        try:
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(local_path)
            LOGGER.info(
                "Uploaded %s season %s to gs://%s/%s",
                league_code,
                season,
                bucket_name,
                blob_name,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.error(
                "Failed to upload %s season %s to gs://%s/%s: %s",
                league_code,
                season,
                bucket_name,
                blob_name,
                exc,
            )


def run_scraper(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    start_year: int = DEFAULT_START_YEAR,
    partitions: int = DEFAULT_PARTITIONS,
    gcs_bucket: str | None = DEFAULT_GCS_BUCKET,
    gcs_prefix: str | None = DEFAULT_GCS_PREFIX,
) -> None:
    """Execute the download job inside a Spark session."""

    LOGGER.info("Starting football-data.co.uk ingestion job")
    seasons = build_season_codes(start_year)
    tasks = build_tasks(seasons, output_dir)
    LOGGER.info("Prepared %d download tasks", len(tasks))

    spark = (
        SparkSession.builder.appName("football-data-scraper")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    try:
        rdd = spark.sparkContext.parallelize(tasks, numSlices=partitions)
        results = rdd.map(download_csv).collect()
    finally:
        spark.stop()

    successes = sum(1 for *_, ok, _ in results if ok)
    LOGGER.info("Finished downloads: %d successful of %d", successes, len(results))

    if gcs_bucket:
        LOGGER.info(
            "Uploading successful downloads to gs://%s with prefix '%s'",
            gcs_bucket,
            gcs_prefix,
        )
        upload_successful_results_to_gcs(
            results,
            output_dir=output_dir,
            bucket_name=gcs_bucket,
            prefix=gcs_prefix,
        )


def spark_main() -> None:
    """Entrypoint used by serverless PySpark runners."""

    run_scraper()


if __name__ == "__main__":
    run_scraper()
