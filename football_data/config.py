"""Very small helpers to build the scraper configuration."""

import os
from pathlib import Path
from typing import Dict, Mapping, Optional

DEFAULT_BASE_URL = "https://www.football-data.co.uk/mmz4281/{season}/{league_code}.csv"
DEFAULT_OUTPUT_DIR = Path("data/raw/football-data")
DEFAULT_START_YEAR = 1993
DEFAULT_PARTITIONS = 24
DEFAULT_GCS_PREFIX = "lakehouse/football-data"
DEFAULT_LEAGUE_CODES: Dict[str, str] = {
    "E0": "england_premier_league",
    "E1": "england_championship",
    "SP1": "spain_la_liga",
    "SP2": "spain_segunda_division",
    "I1": "italy_serie_a",
    "I2": "italy_serie_b",
    "F1": "france_ligue_1",
    "F2": "france_ligue_2",
    "D1": "germany_bundesliga_1",
    "D2": "germany_bundesliga_2",
    "N1": "netherlands_eredivisie",
    "P1": "portugal_primeira_liga",
}


def _load_env_path(var_name: str, default: Path) -> Path:
    value = os.getenv(var_name)
    return Path(value) if value else default


def _load_env_int(var_name: str, default: int) -> int:
    value = os.getenv(var_name)
    return int(value) if value else default


def _parse_league_codes(raw: str) -> Dict[str, str]:
    """Turn "code=name" pairs into a dictionary."""

    codes: Dict[str, str] = {}
    for item in raw.split(","):
        if not item.strip():
            continue
        if "=" not in item:
            raise ValueError(
                "FOOTBALL_DATA_LEAGUE_CODES debe usar el formato codigo=nombre separados por comas"
            )
        code, name = item.split("=", 1)
        codes[code.strip()] = name.strip()
    if not codes:
        raise ValueError("FOOTBALL_DATA_LEAGUE_CODES no puede quedar vacío")
    return codes


def load_config_from_env(
    *,
    league_codes: Optional[Mapping[str, str]] = None,
) -> Dict[str, object]:
    """Collect the configuration in a simple dictionary."""

    output_dir = _load_env_path("FOOTBALL_DATA_OUTPUT_DIR", DEFAULT_OUTPUT_DIR)
    start_year = _load_env_int("FOOTBALL_DATA_START_YEAR", DEFAULT_START_YEAR)
    partitions = _load_env_int("FOOTBALL_DATA_PARTITIONS", DEFAULT_PARTITIONS)
    gcs_bucket = os.getenv("FOOTBALL_DATA_GCS_BUCKET")
    gcs_prefix = os.getenv("FOOTBALL_DATA_GCS_PREFIX", DEFAULT_GCS_PREFIX)

    if league_codes is None:
        override = os.getenv("FOOTBALL_DATA_LEAGUE_CODES")
        if override:
            league_codes = _parse_league_codes(override)
        else:
            league_codes = DEFAULT_LEAGUE_CODES.copy()
    else:
        league_codes = dict(league_codes)

    return {
        "base_url": DEFAULT_BASE_URL,
        "output_dir": Path(output_dir),
        "start_year": start_year,
        "partitions": partitions,
        "gcs_bucket": gcs_bucket,
        "gcs_prefix": gcs_prefix,
        "league_codes": league_codes,
    }


def resolve_league_dir(config: Mapping[str, object], league_code: str) -> Path:
    """Helper to know where to save the CSV for one league."""

    league_name = config["league_codes"][league_code]  # type: ignore[index]
    return Path(config["output_dir"]) / league_name  # type: ignore[arg-type]
