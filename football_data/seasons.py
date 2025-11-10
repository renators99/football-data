"""Season code generation utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional


@dataclass(frozen=True)
class SeasonCodeGenerator:
    """Generate season codes like '2324' used by football-data.co.uk."""

    start_year: int
    end_year: Optional[int] = None

    def generate(self) -> List[str]:
        """Return the list of season codes from ``start_year`` to ``end_year``."""

        end_year = self.end_year or datetime.utcnow().year
        if self.start_year > end_year:
            msg = f"start_year {self.start_year} must be <= end_year {end_year}"
            raise ValueError(msg)

        seasons: List[str] = []
        for year in range(self.start_year, end_year + 1):
            next_year = year + 1
            seasons.append(f"{str(year)[-2:]}{str(next_year)[-2:]}")
        return seasons

    def __iter__(self) -> Iterable[str]:  # pragma: no cover - convenience method
        return iter(self.generate())
