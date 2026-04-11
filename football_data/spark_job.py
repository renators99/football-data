"""Plain functions to run the PySpark downloads and lakehouse layers."""

import logging
from pathlib import Path
from typing import Mapping, Sequence

from pyspark.sql import SparkSession

from .downloader import build_download_tasks, download_csv
from .gold import build_gold_league_summary, build_gold_team_table, write_gold_layer
from .layers import raw_download_root
from .silver import build_silver_matches, write_silver_layer
from .uploader import upload_results_to_gcs

LOGGER = logging.getLogger(__name__)


def run_spark_job(config: Mapping[str, object], seasons: Sequence[str]):
    """Start Spark, download everything and build silver/gold layers."""

    LOGGER.info("Iniciando job de football-data.co.uk")
    tasks = build_download_tasks(config, seasons)
    LOGGER.info("Se prepararon %d descargas", len(tasks))

    base_url = str(config["base_url"])
    partitions = int(config["partitions"])
    output_dir = Path(config["output_dir"])  # type: ignore[arg-type]

    spark = (
        SparkSession.builder.appName("football-data-scraper")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    try:
        rdd = spark.sparkContext.parallelize(tasks, numSlices=partitions)
        results = rdd.map(lambda task: download_csv(task, base_url)).collect()

        successes = sum(1 for item in results if item.get("success"))
        LOGGER.info("Descargas terminadas: %d exitosas de %d", successes, len(results))

        if successes > 0:
            bronze_glob = str(raw_download_root(output_dir) / "league_code=*" / "league_name=*" / "season=*" / "data.csv")
            silver_matches = build_silver_matches(spark, bronze_glob)
            silver_path = write_silver_layer(silver_matches, output_dir)
            LOGGER.info("Capa silver generada en %s", silver_path)

            gold_team_table = build_gold_team_table(silver_matches)
            gold_summary = build_gold_league_summary(silver_matches)
            team_path, summary_path = write_gold_layer(gold_team_table, gold_summary, output_dir)
            LOGGER.info("Capa gold generada en %s y %s", team_path, summary_path)
    finally:
        spark.stop()

    bucket = config.get("gcs_bucket")
    if bucket:
        prefix = config.get("gcs_prefix")
        LOGGER.info("Subiendo archivos a gs://%s con prefijo '%s'", bucket, prefix)
        upload_results_to_gcs(
            str(bucket),
            None if prefix is None else str(prefix),
            results,
            base_dir=output_dir,
        )

    return results
