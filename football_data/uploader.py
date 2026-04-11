"""Utility to send the downloaded CSV files to Google Cloud Storage."""

import logging
from pathlib import Path
from typing import Iterable, Optional

LOGGER = logging.getLogger(__name__)


def _load_storage_client():  # pragma: no cover - needs google-cloud-storage installed
    try:
        from google.cloud import storage
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        msg = "Instala google-cloud-storage para subir archivos a GCS"
        raise RuntimeError(msg) from exc
    return storage.Client()


def upload_results_to_gcs(
    bucket_name: str,
    prefix: Optional[str],
    results: Iterable[dict],
    *,
    base_dir: Path,
) -> None:
    """Upload every successful download to the given bucket."""

    client = _load_storage_client()
    bucket = client.bucket(bucket_name)
    clean_prefix = (prefix or "").strip("/")
    base_dir = base_dir.resolve()

    for result in results:
        if not result.get("success"):
            continue

        task = result["task"]
        local_path = Path(task["output_path"]).resolve()  # type: ignore[arg-type]
        try:
            relative_path = local_path.relative_to(base_dir)
            blob_name = relative_path.as_posix()
        except ValueError:
            blob_name = local_path.name

        if clean_prefix:
            blob_name = f"{clean_prefix}/{blob_name}"

        try:
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(local_path)
            LOGGER.info(
                "Subido %s temporada %s a gs://%s/%s",
                task["league_code"],
                task["season"],
                bucket_name,
                blob_name,
            )
        except Exception as exc:  # noqa: BLE001 - keep logging simple
            LOGGER.error(
                "Error subiendo %s temporada %s a gs://%s/%s: %s",
                task["league_code"],
                task["season"],
                bucket_name,
                blob_name,
                exc,
            )
