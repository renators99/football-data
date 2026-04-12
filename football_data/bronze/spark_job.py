"""Plain functions to run the PySpark downloads."""

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .downloader import build_download_tasks, download_csv
from ..utils.layers import bronze_matches_root, bronze_temp_csv_root

LOGGER = logging.getLogger(__name__)


def _build_league_code_expression(config: Mapping[str, object]):
    mapping_items = []
    for league_code, league_name in config["league_codes"].items():  # type: ignore[union-attr]
        mapping_items.extend((F.lit(league_name), F.lit(league_code)))
    return F.create_map(*mapping_items)


def write_bronze_layer(
    spark: SparkSession,
    config: Mapping[str, object],
    results,
    *,
    run_id: str,
) -> Path | None:
    """Materialize the raw bronze dataset as parquet."""

    successful_results = [item for item in results if item.get("success")]
    if not successful_results:
        LOGGER.warning("No hubo descargas exitosas; se omite la escritura de bronze parquet")
        return None

    csv_paths = [str(Path(item["task"]["output_path"])) for item in successful_results]
    bronze_csv = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(csv_paths)
    )
    league_code_map = _build_league_code_expression(config)
    destination = bronze_matches_root(Path(config["output_dir"]))

    (
        bronze_csv.withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_run_id", F.lit(run_id))
        .withColumn("league_name", F.regexp_extract("source_file", r"([^/\\\\]+)[/\\\\][^/\\\\]+\.csv$", 1))
        .withColumn("league_code", F.element_at(league_code_map, F.col("league_name")))
        .withColumn("season", F.regexp_extract("source_file", r"([^/\\\\]+)\.csv$", 1))
        .write.mode("overwrite")
        .partitionBy("league_code", "season")
        .parquet(str(destination))
    )

    LOGGER.info("Bronze parquet escrito en %s", destination)
    return destination


def run_spark_job(config: Mapping[str, object], seasons: Sequence[str]):
    """Start Spark, download everything and maybe upload to GCS."""

    LOGGER.info("Iniciando job de football-data.co.uk")
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    temp_csv_root = bronze_temp_csv_root(Path(config["output_dir"]), run_id)
    runtime_config = dict(config)
    runtime_config["run_output_dir"] = temp_csv_root

    tasks = build_download_tasks(runtime_config, seasons)
    LOGGER.info("Se prepararon %d descargas", len(tasks))

    base_url = str(config["base_url"])
    partitions = int(config["partitions"])

    spark = (
        SparkSession.builder.appName("football-data-scraper")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    try:
        rdd = spark.sparkContext.parallelize(tasks, numSlices=partitions)
        results = rdd.map(lambda task: download_csv(task, base_url)).collect()
        bronze_destination = write_bronze_layer(spark, config, results, run_id=run_id)
    finally:
        spark.stop()

    successes = sum(1 for item in results if item.get("success"))
    LOGGER.info("Descargas terminadas: %d exitosas de %d", successes, len(results))
    if successes and bronze_destination is not None:
        shutil.rmtree(temp_csv_root, ignore_errors=True)
        LOGGER.info("CSV temporales eliminados en %s", temp_csv_root)
    elif temp_csv_root.exists():
        LOGGER.warning("Se conservaron los CSV temporales en %s para revisión", temp_csv_root)

    return results
