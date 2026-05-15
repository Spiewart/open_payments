"""NPPES (CMS NPI Registry) backend for institution → location lookup.

NPPES is the free, authoritative directory of US healthcare providers
(both individual and organizational). The public REST API allows
organization-name search and returns one record per registered NPI,
each with a primary practice address — exactly what we need.

API: https://npiregistry.cms.hhs.gov/api/?organization_name=...

Strategy
--------
1. **Exact-name pass.** Search NPPES with the institution string as
   ``organization_name``. NPPES does a token-prefix search itself,
   so "Johns Hopkins University" returns all rows whose org name
   starts with those tokens.

2. **Fuzzy fallback.** When the exact pass yields no rows, retry with
   the leading 2-3 tokens of the input (e.g. "Brigham and Women's
   Hospital" → "Brigham") and apply ``rapidfuzz.token_sort_ratio``
   filtering against the returned org names to find the best match.
   Only matches above ``FUZZY_THRESHOLD`` are kept.

3. **Top-N campuses.** Multi-campus institutions return many rows
   (Johns Hopkins → multiple Baltimore campuses + Middle River etc.).
   We deduplicate on ``(city, state)`` and keep up to ``MAX_CAMPUSES``
   per institution, ranked by frequency (most common campus first).

What we deliberately don't do
-----------------------------
- Don't cache here — that's the orchestrator's DiskCache. This backend
  is stateless: same input always produces the same output for the
  current state of NPPES.
- Don't rate-limit aggressively. NPPES doesn't publish a rate limit,
  but a 200ms inter-request sleep keeps batch runs polite without
  being slow. Configurable via ``request_interval_s``.
"""

from __future__ import annotations

import logging
import time
from collections import Counter
from typing import Any

import requests
from rapidfuzz import fuzz

from .types import CandidateLocation

logger = logging.getLogger(__name__)

NPPES_ENDPOINT = "https://npiregistry.cms.hhs.gov/api/"
FUZZY_THRESHOLD = 88
"""Minimum rapidfuzz.token_sort_ratio for a fuzzy match to count.
Calibrated against typical name-variant pairs:

- 'Johns Hopkins University School of Medicine' vs
  'JOHNS HOPKINS UNIVERSITY'                       → ratio ~85
- 'Cleveland Clinic Foundation' vs
  'CLEVELAND CLINIC'                                → ratio ~88
- 'University of Washington' vs
  'UNIVERSITY OF WASHINGTON MEDICAL CENTER'         → ratio ~74

We err toward false-negatives (let it fall through to LLM) over false-
positives (wrong city pinned to the wrong provider). Tune downward
only after auditing real misses on your dataset.
"""

MAX_CAMPUSES = 5
"""Cap on distinct (city, state) pairs per institution. Top-N by
how often that pair appears in the NPPES result set — the assumption
being that the most-frequently-registered campus is the canonical one."""


class NPPESBackend:
    """Query CMS NPI Registry for organizational addresses.

    Stateless. The orchestrator owns retry/cache concerns; this class
    just maps ``institution_name -> list[CandidateLocation]``.
    """

    def __init__(
        self,
        *,
        endpoint: str = NPPES_ENDPOINT,
        request_timeout_s: float = 10.0,
        request_interval_s: float = 0.2,
        max_results: int = 50,
        session: requests.Session | None = None,
    ):
        self.endpoint = endpoint
        self.request_timeout_s = request_timeout_s
        self.request_interval_s = request_interval_s
        self.max_results = max_results
        self._session = session or requests.Session()
        self._last_request_at: float = 0.0

    def locate(self, institution: str) -> list[CandidateLocation]:
        """Return up to ``MAX_CAMPUSES`` ``CandidateLocation`` for one institution.

        Returns ``[CandidateLocation(source='miss', ...)]`` when neither
        the exact-name pass nor the fuzzy fallback yields any result. The
        single miss sentinel preserves the contract that callers always
        get a non-empty list per institution (so the cache can store the
        attempt and not redundantly re-query).
        """
        # Exact-name pass.
        results = self._query(organization_name=institution)
        candidates = self._build_candidates(institution, results, confidence=1.0)
        if candidates:
            return candidates

        # Fuzzy fallback with truncated query.
        fuzzy_query = self._fuzzy_query_form(institution)
        if fuzzy_query and fuzzy_query.lower() != institution.lower():
            results = self._query(organization_name=fuzzy_query)
            candidates = self._build_fuzzy_candidates(institution, results)
            if candidates:
                return candidates

        return [
            CandidateLocation(
                institution=institution,
                source="miss",
            )
        ]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _query(self, *, organization_name: str) -> list[dict[str, Any]]:
        """One HTTP call with rate-limit pacing. Returns raw NPPES ``results``."""
        delta = time.monotonic() - self._last_request_at
        if delta < self.request_interval_s:
            time.sleep(self.request_interval_s - delta)
        params = {
            "version": "2.1",
            "enumeration_type": "NPI-2",
            "organization_name": organization_name,
            "limit": self.max_results,
        }
        try:
            resp = self._session.get(
                self.endpoint,
                params=params,
                timeout=self.request_timeout_s,
            )
            self._last_request_at = time.monotonic()
            resp.raise_for_status()
        except requests.RequestException:
            logger.exception("NPPES query failed for %r", organization_name)
            return []
        payload = resp.json()
        return payload.get("results", []) or []

    def _build_candidates(
        self,
        institution: str,
        results: list[dict[str, Any]],
        *,
        confidence: float,
    ) -> list[CandidateLocation]:
        """Build top-N CandidateLocations from NPPES rows.

        Dedup on ``(city.upper(), state.upper())`` so case variants don't
        produce phantom campuses; rank by frequency of that pair in the
        NPPES result set.
        """
        pair_counter: Counter[tuple[str, str]] = Counter()
        for row in results:
            for addr in row.get("addresses", []) or []:
                # NPPES returns both LOCATION and MAILING addresses per row.
                # Mailing is often a PO box; prefer LOCATION when available.
                if addr.get("address_purpose") not in (None, "LOCATION", "PRIMARY"):
                    continue
                city = (addr.get("city") or "").strip().upper()
                state = (addr.get("state") or "").strip().upper()
                if not city or not state:
                    continue
                pair_counter[(city, state)] += 1
                # NPPES rows have multiple address records; one LOCATION per
                # row is enough — we don't need to count every address subtype.
                break
        return [
            CandidateLocation(
                institution=institution,
                # Title-case city to keep cache outputs readable; state stays
                # uppercase because it's a USPS abbreviation.
                city=city.title(),
                state=state,
                source="nppes",
                confidence=confidence,
            )
            for (city, state), _count in pair_counter.most_common(MAX_CAMPUSES)
        ]

    def _build_fuzzy_candidates(
        self,
        institution: str,
        results: list[dict[str, Any]],
    ) -> list[CandidateLocation]:
        """Filter ``results`` to rows whose org-name matches ``institution``
        above ``FUZZY_THRESHOLD``, then build candidates with the rapidfuzz
        ratio as the confidence value."""
        passing: list[tuple[float, dict[str, Any]]] = []
        # NPPES always returns SHOUTING-CASE org names; rapidfuzz token_sort_ratio
        # is case-sensitive. Lowercase both before comparing or the ratio
        # collapses to ~0 even when the names are otherwise identical.
        institution_lc = institution.lower()
        for row in results:
            org_name = (row.get("basic", {}) or {}).get("organization_name", "")
            ratio = fuzz.token_sort_ratio(institution_lc, org_name.lower())
            if ratio >= FUZZY_THRESHOLD:
                passing.append((ratio, row))
        if not passing:
            return []
        # Confidence for the batch = the BEST ratio we found (so the cache
        # records the strongest evidence). Multi-campus dedup still applies.
        best_ratio = max(r for r, _ in passing)
        return self._build_candidates(
            institution,
            [row for _, row in passing],
            confidence=best_ratio / 100.0,
        )

    @staticmethod
    def _fuzzy_query_form(institution: str) -> str | None:
        """Return a truncated form for the fallback query.

        Strategy: take the first 2 capital-letter-prefixed tokens. For
        "Brigham and Women's Hospital" that's "Brigham" (single-token
        result after dropping lowercase "and"). For "Johns Hopkins
        University..." it's "Johns Hopkins".

        Returns None when the strategy can't produce something useful
        (e.g. the institution is one token already, or has no capitalized
        tokens to anchor on).
        """
        tokens = [t for t in institution.split() if t and t[0].isupper()]
        if len(tokens) < 1:
            return None
        # Two tokens is the sweet spot — disambiguates "Johns Hopkins"
        # from "Johns" alone (which has poor NPPES recall), without
        # forcing too much specificity that the truncation never helps.
        return " ".join(tokens[:2])
