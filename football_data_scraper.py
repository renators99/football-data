"""Script principal muy sencillo para bajar los CSV de football-data.co.uk."""

import logging
from typing import Dict, Iterable, Optional, Sequence

from football_data.config import DEFAULT_START_YEAR, load_config_from_env
from football_data.seasons import build_season_list
from football_data.spark_job import run_spark_job

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)


def choose_seasons(start_year: int, *, end_year: Optional[int] = None) -> Sequence[str]:
    return build_season_list(start_year, end_year=end_year)


def build_config(extra_values: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    config = load_config_from_env()
    if extra_values:
        config.update(extra_values)
    return config


def run_scraper(
    *,
    config: Optional[Dict[str, object]] = None,
    seasons: Optional[Iterable[str]] = None,
) -> None:
    """Run the PySpark downloads with a very small wrapper."""

    config = config or build_config()
    chosen_seasons = list(seasons or choose_seasons(int(config.get("start_year", DEFAULT_START_YEAR))))
    run_spark_job(config, chosen_seasons)


def spark_main() -> None:
    """Entry point for serverless runners."""

    run_scraper()


if __name__ == "__main__":
    run_scraper()
