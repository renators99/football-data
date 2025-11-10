"""Download orchestration for football-data CSVs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping, Sequence

import requests

from .config import ScraperConfig

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DownloadTask:
    """Descriptor for a single CSV download."""

    season: str
    league_code: str
    output_path: Path

    def build_url(self, base_url: str) -> str:
        return base_url.format(season=self.season, league_code=self.league_code)


@dataclass(frozen=True)
class DownloadResult:
    """Result of a download attempt."""

    task: DownloadTask
    success: bool
    message: str


class FootballDataDownloader:
    """Create download tasks and fetch remote CSVs."""

    def __init__(self, config: ScraperConfig) -> None:
        self._config = config

    def build_tasks(self, seasons: Sequence[str]) -> List[DownloadTask]:
        tasks: List[DownloadTask] = []
        for league_code in self._config.league_codes:
            league_dir = self._config.resolve_league_dir(league_code)
            for season in seasons:
                tasks.append(
                    DownloadTask(
                        season=season,
                        league_code=league_code,
                        output_path=league_dir / f"{season}.csv",
                    )
                )
        return tasks

    def download(self, task: DownloadTask) -> DownloadResult:
        url = task.build_url(self._config.base_url)
        try:
            response = requests.get(url, timeout=30)
        except Exception as exc:  # noqa: BLE001
            message = f"Error downloading {task.league_code} season {task.season}: {exc}"
            LOGGER.error(message)
            return DownloadResult(task=task, success=False, message=message)

        if response.status_code == 200 and response.content.strip():
            task.output_path.parent.mkdir(parents=True, exist_ok=True)
            task.output_path.write_bytes(response.content)
            message = f"Downloaded {task.league_code} season {task.season}"
            LOGGER.info(message)
            return DownloadResult(task=task, success=True, message=message)

        message = (
            f"Missing data for {task.league_code} season {task.season}: HTTP {response.status_code}"
        )
        LOGGER.warning(message)
        return DownloadResult(task=task, success=False, message=message)

    @staticmethod
    def summarize(results: Iterable[DownloadResult]) -> tuple[int, int]:
        """Return (successes, total) counts for results."""

        results_list = list(results)
        successes = sum(1 for result in results_list if result.success)
        return successes, len(results_list)
