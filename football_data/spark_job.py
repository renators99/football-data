"""PySpark job orchestration for downloading and uploading football data."""

from __future__ import annotations

import logging
from typing import Iterable, Optional, Sequence

from pyspark.sql import SparkSession

from .config import ScraperConfig
from .downloader import DownloadResult, DownloadTask, FootballDataDownloader
from .uploader import GCSUploader

LOGGER = logging.getLogger(__name__)


class FootballDataSparkJob:
    """Coordinate Spark execution for football-data downloads."""

    def __init__(
        self,
        config: ScraperConfig,
        downloader: FootballDataDownloader,
        uploader: Optional[GCSUploader] = None,
    ) -> None:
        self._config = config
        self._downloader = downloader
        self._uploader = uploader

    def run(self, seasons: Sequence[str]) -> Sequence[DownloadResult]:
        """Execute the Spark job for the provided season codes."""

        LOGGER.info("Starting football-data.co.uk ingestion job")
        tasks = self._downloader.build_tasks(seasons)
        LOGGER.info("Prepared %d download tasks", len(tasks))

        spark = (
            SparkSession.builder.appName("football-data-scraper")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )

        try:
            rdd = spark.sparkContext.parallelize(tasks, numSlices=self._config.partitions)
            results = rdd.map(self._downloader.download).collect()
        finally:
            spark.stop()

        successes = sum(1 for result in results if result.success)
        LOGGER.info("Finished downloads: %d successful of %d", successes, len(results))

        if self._uploader:
            LOGGER.info(
                "Uploading successful downloads to gs://%s with prefix '%s'",
                self._uploader._bucket_name,  # pragma: no cover - log helper
                self._uploader._prefix,
            )
            self._uploader.upload(results, base_dir=self._config.output_dir)

        return results
