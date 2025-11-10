"""Football-data.co.uk scraping package."""

from .config import ScraperConfig
from .downloader import DownloadResult, DownloadTask, FootballDataDownloader
from .spark_job import FootballDataSparkJob
from .uploader import GCSUploader

__all__ = [
    "ScraperConfig",
    "DownloadResult",
    "DownloadTask",
    "FootballDataDownloader",
    "FootballDataSparkJob",
    "GCSUploader",
]
