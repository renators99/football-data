"""Entry point for Dataproc bronze layer."""

import sys
from pathlib import Path

# Agregar el proyecto al path si es necesario
PROJECT_DIR = Path("/opt/dataproc/project")  # Ajustar según el setup
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from football_data.bronze import run_spark_job
from football_data.utils.config import DEFAULT_START_YEAR
from football_data.utils.seasons import build_season_list
from football_data_scraper import build_config

if __name__ == "__main__":
    config = build_config()
    seasons = build_season_list(int(config.get("start_year", DEFAULT_START_YEAR)))
    run_spark_job(config, seasons)