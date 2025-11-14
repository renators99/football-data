"""Plain functions to run the PySpark downloads."""

import logging
from typing import Mapping, Sequence

from pyspark.sql import SparkSession

from .downloader import build_download_tasks, download_csv
from .uploader import upload_results_to_gcs

LOGGER = logging.getLogger(__name__)


def run_spark_job(config: Mapping[str, object], seasons: Sequence[str]):
    """Start Spark, download everything and maybe upload to GCS."""

    LOGGER.info("Iniciando job de football-data.co.uk")
    tasks = build_download_tasks(config, seasons)
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
    finally:
        spark.stop()

    successes = sum(1 for item in results if item.get("success"))
    LOGGER.info("Descargas terminadas: %d exitosas de %d", successes, len(results))

    bucket = config.get("gcs_bucket")
    if bucket:
        prefix = config.get("gcs_prefix")
        LOGGER.info("Subiendo archivos a gs://%s con prefijo '%s'", bucket, prefix)
        upload_results_to_gcs(
            str(bucket),
            None if prefix is None else str(prefix),
            results,
            base_dir=config["output_dir"],
        )

    return results
