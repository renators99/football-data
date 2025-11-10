"""GCS upload support for football-data downloads."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Optional

from .downloader import DownloadResult

LOGGER = logging.getLogger(__name__)


class GCSUploader:
    """Upload downloaded CSV files into a Google Cloud Storage bucket."""

    def __init__(self, bucket_name: str, prefix: Optional[str] = None) -> None:
        self._bucket_name = bucket_name
        self._prefix = (prefix or "").strip("/")
        self._client = None

    @property
    def client(self):  # pragma: no cover - lazy import guarded by runtime
        if self._client is None:
            try:
                from google.cloud import storage
            except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
                msg = "google-cloud-storage must be installed to enable GCS uploads"
                raise RuntimeError(msg) from exc
            self._client = storage.Client()
        return self._client

    def upload(self, results: Iterable[DownloadResult], base_dir: Path) -> None:
        bucket = self.client.bucket(self._bucket_name)
        base_dir = base_dir.resolve()

        for result in results:
            if not result.success:
                continue

            local_path = result.task.output_path.resolve()
            try:
                relative_path = local_path.relative_to(base_dir)
                blob_name = relative_path.as_posix()
            except ValueError:
                blob_name = local_path.name

            if self._prefix:
                blob_name = f"{self._prefix}/{blob_name}"

            try:
                blob = bucket.blob(blob_name)
                blob.upload_from_filename(local_path)
                LOGGER.info(
                    "Uploaded %s season %s to gs://%s/%s",
                    result.task.league_code,
                    result.task.season,
                    self._bucket_name,
                    blob_name,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.error(
                    "Failed to upload %s season %s to gs://%s/%s: %s",
                    result.task.league_code,
                    result.task.season,
                    self._bucket_name,
                    blob_name,
                    exc,
                )
