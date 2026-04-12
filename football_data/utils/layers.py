"""Helpers to build bronze, silver and gold data lake layers."""

from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame


def bronze_root(output_dir: Path) -> Path:
    return Path(output_dir) / "bronze"


def silver_root(output_dir: Path) -> Path:
    return Path(output_dir) / "silver"


def gold_root(output_dir: Path) -> Path:
    return Path(output_dir) / "gold"


def raw_download_root(output_dir: Path) -> Path:
    """Keep compatibility with current layout (raw/bronze same level)."""

    return bronze_root(output_dir) / "football-data"


def write_partitioned_parquet(
    frame: DataFrame,
    destination: Path,
    partitions: Iterable[str],
) -> None:
    frame.write.mode("overwrite").partitionBy(*partitions).parquet(str(destination))
