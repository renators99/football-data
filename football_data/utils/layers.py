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


def bronze_matches_root(output_dir: Path) -> Path:
    """Persistent bronze dataset built from downloaded CSV files."""

    return Path(output_dir).parent / "bronze" / "matches"


def bronze_temp_csv_root(output_dir: Path, run_id: str) -> Path:
    """Temporary CSV staging area for one execution."""

    return Path(output_dir) / "_runs" / run_id


def write_partitioned_parquet(
    frame: DataFrame,
    destination: Path,
    partitions: Iterable[str],
) -> None:
    frame.write.mode("overwrite").partitionBy(*partitions).parquet(str(destination))
