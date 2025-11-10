"""Configuration helpers for the football-data scraper."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Mapping, Optional

DEFAULT_BASE_URL = "https://www.football-data.co.uk/mmz4281/{season}/{league_code}.csv"
DEFAULT_OUTPUT_DIR = Path("data/raw/football-data")
DEFAULT_START_YEAR = 1993
DEFAULT_PARTITIONS = 24
DEFAULT_GCS_PREFIX = "lakehouse/football-data"


def _load_env_path(var_name: str, default: Path) -> Path:
    value = os.getenv(var_name)
    return Path(value) if value else default


def _load_env_int(var_name: str, default: int) -> int:
    value = os.getenv(var_name)
    return int(value) if value else default


@dataclass(frozen=True)
class ScraperConfig:
    """Runtime configuration for the scraper job."""

    base_url: str = DEFAULT_BASE_URL
    output_dir: Path = DEFAULT_OUTPUT_DIR
    start_year: int = DEFAULT_START_YEAR
    partitions: int = DEFAULT_PARTITIONS
    gcs_bucket: Optional[str] = None
    gcs_prefix: Optional[str] = DEFAULT_GCS_PREFIX
    league_codes: Mapping[str, str] = field(default_factory=lambda: {
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
    })

    @classmethod
    def from_env(cls) -> "ScraperConfig":
        """Construct configuration by reading supported environment variables."""

        output_dir = _load_env_path("FOOTBALL_DATA_OUTPUT_DIR", DEFAULT_OUTPUT_DIR)
        start_year = _load_env_int("FOOTBALL_DATA_START_YEAR", DEFAULT_START_YEAR)
        partitions = _load_env_int("FOOTBALL_DATA_PARTITIONS", DEFAULT_PARTITIONS)
        gcs_bucket = os.getenv("FOOTBALL_DATA_GCS_BUCKET")
        gcs_prefix = os.getenv("FOOTBALL_DATA_GCS_PREFIX", DEFAULT_GCS_PREFIX)

        league_codes_override = os.getenv("FOOTBALL_DATA_LEAGUE_CODES")
        league_codes: Mapping[str, str]
        if league_codes_override:
            league_codes = cls._parse_league_codes(league_codes_override)
        else:
            league_codes = cls().league_codes  # type: ignore[assignment]

        return cls(
            output_dir=output_dir,
            start_year=start_year,
            partitions=partitions,
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_prefix,
            league_codes=league_codes,
        )

    @staticmethod
    def _parse_league_codes(raw: str) -> Mapping[str, str]:
        """Parse a comma separated list of code=name pairs."""

        codes: Dict[str, str] = {}
        for item in raw.split(","):
            if not item.strip():
                continue
            try:
                code, name = item.split("=")
            except ValueError as exc:  # pragma: no cover - guard rails
                raise ValueError(
                    "FOOTBALL_DATA_LEAGUE_CODES must be a comma separated list of code=name pairs"
                ) from exc
            codes[code.strip()] = name.strip()
        if not codes:
            raise ValueError("FOOTBALL_DATA_LEAGUE_CODES produced an empty mapping")
        return codes

    def resolve_league_dir(self, league_code: str) -> Path:
        """Return the output directory for a league code."""

        league_name = self.league_codes[league_code]
        return self.output_dir / league_name
