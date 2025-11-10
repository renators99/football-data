"""Entrypoint for the football-data.co.uk PySpark scraper job."""

from __future__ import annotations

import logging
from typing import Optional, Sequence

from football_data.config import ScraperConfig
from football_data.downloader import FootballDataDownloader
from football_data.seasons import SeasonCodeGenerator
from football_data.spark_job import FootballDataSparkJob
from football_data.uploader import GCSUploader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)


def build_job(config: Optional[ScraperConfig] = None) -> FootballDataSparkJob:
    """Create a job instance using configuration from the environment by default."""

    config = config or ScraperConfig.from_env()
    downloader = FootballDataDownloader(config)
    uploader = None
    if config.gcs_bucket:
        uploader = GCSUploader(config.gcs_bucket, config.gcs_prefix)
    return FootballDataSparkJob(config=config, downloader=downloader, uploader=uploader)


def run_scraper(
    *,
    config: Optional[ScraperConfig] = None,
    seasons: Optional[Sequence[str]] = None,
) -> None:
    """Execute the football-data download job."""

    config = config or ScraperConfig.from_env()
    season_generator = SeasonCodeGenerator(start_year=config.start_year)
    resolved_seasons = list(seasons or season_generator.generate())
    job = build_job(config)
    job.run(resolved_seasons)


def spark_main() -> None:
    """Entrypoint used by serverless PySpark runners."""

    run_scraper()


if __name__ == "__main__":
    run_scraper()
