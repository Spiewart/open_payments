"""Schema types for the institution → location lookup pipeline.

A ``CandidateLocation`` is the canonical output of every backend
(NPPES, manual review, LLM) and the cache. Carrying provenance
(``source``) and a per-call timestamp lets analysts trace where a
``(city, state)`` came from when a downstream match looks wrong, and
lets the cache distinguish stale data from authoritative data.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ..citystates import CityState

CandidateLocationSource = Literal["nppes", "manual", "llm", "miss"]


class CandidateLocation(BaseModel):
    """One ``(city, state)`` candidate for a given institution name.

    Multi-campus institutions produce multiple ``CandidateLocation``
    instances. ``source=miss`` is recorded when a backend tried and
    couldn't resolve — caching the miss prevents repeated dead lookups.
    """

    model_config = ConfigDict(use_enum_values=True)

    institution: str = Field(
        description="The source institution string the backend was asked to resolve."
    )
    city: str | None = None
    state: str | None = None
    """USPS state abbreviation (e.g. 'MD'). Aligns with the
    :class:`open_payments.choices.States` enum's member names so the
    downstream matcher's ``citystates`` filter can consume it directly."""
    source: CandidateLocationSource
    """Which backend produced this candidate. Use to filter out
    ``miss`` entries in downstream consumption; use to audit which
    backend chose wrongly when a CMS match looks off."""
    confidence: float | None = None
    """0..1 confidence, when the backend supplies one (LLM only).
    NPPES exact matches are implicitly confidence=1.0; NPPES fuzzy
    matches carry the rapidfuzz ratio. Manual entries are None
    (an analyst either knew the answer or didn't)."""
    looked_up_at: datetime = Field(default_factory=datetime.utcnow)
    """When this candidate was produced. The cache uses this for
    freshness decisions if a TTL is configured."""

    def to_citystate(self) -> CityState | None:
        """Project to the matcher's ``CityState`` shape. Returns None when
        this is a miss (no city/state to project)."""
        if self.city is None and self.state is None:
            return None
        return CityState(city=self.city, state=self.state)


def flatten_to_citystates(
    results: dict[str, list[CandidateLocation]],
) -> list[CityState]:
    """Flatten a ``locate_batch`` result dict into the deduplicated
    ``list[CityState]`` shape the matcher's ``citystates`` filter expects.

    Drops ``source=miss`` entries (nothing to match against). Deduplicates
    on the ``(city, state)`` tuple — multiple institutions resolving to
    the same campus only count once.
    """
    seen: set[tuple[str | None, str | None]] = set()
    out: list[CityState] = []
    for candidates in results.values():
        for candidate in candidates:
            citystate = candidate.to_citystate()
            if citystate is None:
                continue
            key = (citystate.city, citystate.state)
            if key in seen:
                continue
            seen.add(key)
            out.append(citystate)
    return out
