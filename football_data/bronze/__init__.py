"""Bronze layer package for downloads and ingestion jobs."""

from .spark_job import run_spark_job

__all__ = ["run_spark_job"]
