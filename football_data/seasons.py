"""Very small helper to build football-data season codes."""

from datetime import datetime
from typing import List, Optional


def build_season_list(start_year: int, end_year: Optional[int] = None) -> List[str]:
    """Return values like "2324" covering every season between the years given."""

    final_year = end_year or datetime.utcnow().year
    if start_year > final_year:
        raise ValueError(f"start_year {start_year} no puede ser mayor que {final_year}")

    seasons: List[str] = []
    for year in range(start_year, final_year + 1):
        next_year = year + 1
        seasons.append(f"{str(year)[-2:]}{str(next_year)[-2:]}")
    return seasons
