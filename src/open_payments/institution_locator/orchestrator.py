"""Cascade orchestrator for institution → location lookup.

The flow
--------
1. **Cache.** Hit the on-disk JSON cache. Any name with a hit (including
   prior ``source='miss'`` entries) is resolved immediately.
2. **NPPES.** For cache misses, call ``NPPESBackend``. Results
   (including misses) are cached.
3. **Manual review.** The residual (institutions NPPES couldn't
   resolve) is handed to the ``ManualReviewBackend`` for an xlsx
   round-trip. Multi-campus institutions can be expressed as
   multiple rows in the spreadsheet.

The cascade is deliberately interactive at the residual step: the
orchestrator's ``locate_batch`` returns *after* steps 1–2, and gives
the caller a ``residual_institutions(...)`` view. The CLI in the
child app drives the manual round-trip (export → analyst fills →
import), so the library just exposes the building blocks.

Why this shape
--------------
- Backends are pluggable. Pass ``nppes_backend=None`` to disable NPPES
  (test environments without internet).
- The cache lives outside the orchestrator (``cache_path``) so two
  child apps can share lookups.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .cache import DiskCache
from .manual import ManualReviewBackend
from .nppes import NPPESBackend
from .types import CandidateLocation

logger = logging.getLogger(__name__)


def _all_misses(candidates: list[CandidateLocation]) -> bool:
    return bool(candidates) and all(c.source == "miss" for c in candidates)


class InstitutionLocator:
    """Cascade orchestrator: Cache → NPPES → Manual review.

    Construction is dependency-injectable so tests can supply mocked
    backends. Production callers typically use the defaults.
    """

    def __init__(
        self,
        *,
        cache_path: Path,
        nppes_backend: NPPESBackend | None = None,
        manual_backend: ManualReviewBackend | None = None,
    ):
        self.cache = DiskCache(Path(cache_path))
        self.nppes_backend = nppes_backend if nppes_backend is not None else NPPESBackend()
        self.manual_backend = manual_backend if manual_backend is not None else ManualReviewBackend()

    # ------------------------------------------------------------------
    # Step 1+2: cache + NPPES
    # ------------------------------------------------------------------

    def locate_batch(
        self,
        institutions: list[str],
    ) -> dict[str, list[CandidateLocation]]:
        """Run cache → NPPES. Returns the per-institution candidate dict.

        Does NOT advance to the manual step automatically. Use
        ``residual_institutions(result)`` to see what didn't resolve,
        then call ``export_for_manual_review`` / ``import_manual_review``
        to round-trip with an analyst.
        """
        if not institutions:
            return {}

        hits, misses = self.cache.get_many(institutions)
        if not misses:
            return hits

        logger.info(
            "institution lookup: %d cache hits, %d misses → querying NPPES",
            len(hits),
            len(misses),
        )
        nppes_results: dict[str, list[CandidateLocation]] = {}
        for institution in misses:
            nppes_results[institution] = self.nppes_backend.locate(institution)
        self.cache.put_many(nppes_results)

        # Merge cache hits + new NPPES results in the input order.
        merged: dict[str, list[CandidateLocation]] = {}
        for institution in institutions:
            if institution in hits:
                merged[institution] = hits[institution]
            else:
                merged[institution] = nppes_results[institution]
        return merged

    def residual_institutions(
        self,
        results: dict[str, list[CandidateLocation]],
    ) -> list[str]:
        """Return institutions whose only resolved candidate is a miss.
        These are the candidates for the manual review step."""
        return [
            institution
            for institution, candidates in results.items()
            if _all_misses(candidates)
        ]

    # ------------------------------------------------------------------
    # Step 3: manual review
    # ------------------------------------------------------------------

    def export_for_manual_review(
        self,
        institutions: list[str],
        path: Path,
    ) -> Path:
        """Write an xlsx for the analyst to fill in."""
        return self.manual_backend.export(institutions, Path(path))

    def import_manual_review(
        self,
        path: Path,
    ) -> dict[str, list[CandidateLocation]]:
        """Read a filled xlsx, write results to cache, return them."""
        results = self.manual_backend.import_(Path(path))
        if results:
            self.cache.put_many(results)
        return results
