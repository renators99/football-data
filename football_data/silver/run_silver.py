"""Entry point for Dataproc silver layer."""

import sys
from pathlib import Path

PROJECT_DIR = Path("/opt/dataproc/project")
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from football_data.silver import run_silver_layer
from football_data_scraper import build_config

if __name__ == "__main__":
    run_silver_layer(build_config())