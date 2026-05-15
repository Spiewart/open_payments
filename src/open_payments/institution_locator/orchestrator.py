"""Cascade orchestrator for institution → location lookup.

The flow
--------
1. **Cache.** Hit the on-disk JSON cache. Any name with a hit (including
   prior ``source='miss'`` entries) is resolved immediately.
2. **NPPES.** For cache misses, call ``NPPESBackend``. Results
   (including misses) are cached.
3. **Residual gate.** Count the institutions whose only resolved
   candidate is ``source='miss'``. If that count is ≤
   ``manual_threshold`` (default 50), the orchestrator hands those
   institutions to the ``ManualReviewBackend`` for an xlsx round-trip.
   Above the threshold, the orchestrator routes them to the
   ``ClaudeAPIBackend`` (if configured).

The cascade is deliberately interactive at the residual step: the
orchestrator's ``locate_batch`` returns *after* steps 1–2, and gives
the caller a ``residual_institutions(...)`` view. Callers decide
whether to invoke ``resolve_via_manual`` or ``resolve_via_llm`` —
each writes its results back to the cache.

Why this shape
--------------
- Backends are pluggable. Pass ``nppes_backend=None`` to disable NPPES
  (test environments without internet). Pass ``llm_backend`` only when
  ``ANTHROPIC_API_KEY`` is set.
- The cascade is a *menu*, not a pipeline. The CLI in the child app
  decides which steps to run; the library just exposes the building
  blocks.
- The cache lives outside the orchestrator (``cache_path``) so two
  child apps can share lookups.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .cache import DiskCache
from .llm import ClaudeAPIBackend
from .manual import ManualReviewBackend
from .nppes import NPPESBackend
from .types import CandidateLocation

logger = logging.getLogger(__name__)


def _all_misses(candidates: list[CandidateLocation]) -> bool:
    return bool(candidates) and all(c.source == "miss" for c in candidates)


class InstitutionLocator:
    """Cascade orchestrator: Cache → NPPES → (Manual OR LLM).

    Construction is dependency-injectable so tests can supply mocked
    backends. Production callers typically use the defaults.
    """

    def __init__(
        self,
        *,
        cache_path: Path,
        manual_threshold: int = 50,
        nppes_backend: NPPESBackend | None = None,
        manual_backend: ManualReviewBackend | None = None,
        llm_backend: ClaudeAPIBackend | None = None,
    ):
        self.cache = DiskCache(Path(cache_path))
        self.manual_threshold = manual_threshold
        self.nppes_backend = nppes_backend if nppes_backend is not None else NPPESBackend()
        self.manual_backend = manual_backend if manual_backend is not None else ManualReviewBackend()
        self.llm_backend = llm_backend  # None means LLM step is disabled

    # ------------------------------------------------------------------
    # Step 1+2: cache + NPPES
    # ------------------------------------------------------------------

    def locate_batch(
        self,
        institutions: list[str],
    ) -> dict[str, list[CandidateLocation]]:
        """Run cache → NPPES. Returns the per-institution candidate dict.

        Does NOT advance to the manual / LLM step automatically. Use
        ``residual_institutions(result)`` to see what didn't resolve,
        then call ``resolve_via_manual`` or ``resolve_via_llm`` as
        appropriate.
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
        These are the candidates for the manual / LLM step."""
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

    # ------------------------------------------------------------------
    # Step 3 (alt): LLM
    # ------------------------------------------------------------------

    def resolve_via_llm(
        self,
        institutions: list[str],
    ) -> dict[str, list[CandidateLocation]]:
        """Hand the residual to the LLM backend and cache the results."""
        if self.llm_backend is None:
            raise RuntimeError(
                "InstitutionLocator has no llm_backend configured; "
                "construct with llm_backend=ClaudeAPIBackend(api_key=...) "
                "to enable the LLM step."
            )
        results = self.llm_backend.locate_batch(institutions)
        if results:
            self.cache.put_many(results)
        return results

    # ------------------------------------------------------------------
    # Convenience: choose a route based on the threshold
    # ------------------------------------------------------------------

    def recommend_residual_strategy(
        self,
        results: dict[str, list[CandidateLocation]],
    ) -> tuple[str, list[str]]:
        """Return ``(strategy, residual_institutions)`` based on the
        threshold and whether an LLM backend is configured.

        Strategies:
        - ``"none"`` — no residual to resolve
        - ``"manual"`` — len(residual) ≤ threshold; hand to analyst
        - ``"llm"`` — len(residual) > threshold AND llm_backend is set
        - ``"llm_unavailable"`` — > threshold but no LLM configured;
          caller must either configure one, raise the threshold, or
          accept the misses
        """
        residual = self.residual_institutions(results)
        if not residual:
            return "none", residual
        if len(residual) <= self.manual_threshold:
            return "manual", residual
        if self.llm_backend is not None:
            return "llm", residual
        return "llm_unavailable", residual
