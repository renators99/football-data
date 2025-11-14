"""Tiny download helpers with plain dictionaries to keep things simple."""

import logging
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence

import requests

from .config import resolve_league_dir

LOGGER = logging.getLogger(__name__)

DownloadTask = Dict[str, object]
DownloadResult = Dict[str, object]


def build_download_tasks(config: Mapping[str, object], seasons: Sequence[str]) -> List[DownloadTask]:
    """Create the combinations of league and season we want to fetch."""

    tasks: List[DownloadTask] = []
    for league_code in config["league_codes"]:  # type: ignore[index]
        league_dir = resolve_league_dir(config, league_code)
        for season in seasons:
            tasks.append(
                {
                    "season": season,
                    "league_code": league_code,
                    "output_path": Path(league_dir) / f"{season}.csv",
                }
            )
    return tasks


def download_csv(task: DownloadTask, base_url: str) -> DownloadResult:
    """Download a single CSV file."""

    season = task["season"]
    league_code = task["league_code"]
    output_path: Path = Path(task["output_path"])  # type: ignore[arg-type]
    url = base_url.format(season=season, league_code=league_code)

    try:
        response = requests.get(url, timeout=30)
    except Exception as exc:  # noqa: BLE001 - keep it easy to read
        message = f"Error al bajar {league_code} temporada {season}: {exc}"
        LOGGER.error(message)
        return {"task": task, "success": False, "message": message}

    if response.status_code == 200 and response.content.strip():
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(response.content)
        message = f"Descarga OK {league_code} temporada {season}"
        LOGGER.info(message)
        return {"task": task, "success": True, "message": message}

    message = f"No hay datos para {league_code} temporada {season}: HTTP {response.status_code}"
    LOGGER.warning(message)
    return {"task": task, "success": False, "message": message}


def summarize_results(results: Iterable[DownloadResult]) -> tuple[int, int]:
    """Count how many downloads worked."""

    results_list = list(results)
    successes = sum(1 for item in results_list if item.get("success"))
    return successes, len(results_list)
