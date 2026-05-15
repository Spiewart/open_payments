"""Institution → location lookup for the CMS Open Payments matcher.

Child apps (``abim_conflicts``, ``deans_conflicts``) parse free-text
institution strings out of provider profiles ("Johns Hopkins
University School of Medicine", "VA Puget Sound Health Care System").
The matcher's ``citystates`` filter needs ``(city, state)`` pairs, not
free text. This package is the resolver.

Cascade (in order)
------------------
1. **DiskCache.** JSON-backed, shared across runs and across child apps.
2. **NPPESBackend.** Queries CMS NPI Registry (``registry.cms.hhs.gov``)
   for the organization name. Free, no auth, purpose-built for
   medical institutions. Includes a fuzzy fallback for name variants.
3. **Residual gate.** Below the threshold (default 50), call
   ``ManualReviewBackend`` for an xlsx round-trip with an analyst.
   Above it, call ``ClaudeAPIBackend`` (requires
   ``open_payments[llm]`` extra and ``ANTHROPIC_API_KEY``).

Quickstart
----------
.. code-block:: python

    from pathlib import Path
    from open_payments import InstitutionLocator, ClaudeAPIBackend

    locator = InstitutionLocator(
        cache_path=Path("~/.cache/institutions.json").expanduser(),
        manual_threshold=50,
        llm_backend=ClaudeAPIBackend(api_key=os.environ["ANTHROPIC_API_KEY"]),
    )

    # Step 1+2: cache + NPPES. Returns whatever resolved so far.
    results = locator.locate_batch(["Johns Hopkins University", "Cleveland Clinic"])

    # Step 3: gate residual
    strategy, residual = locator.recommend_residual_strategy(results)
    if strategy == "manual":
        locator.export_for_manual_review(residual, Path("review.xlsx"))
        # ... analyst fills review.xlsx ...
        locator.import_manual_review(Path("review.xlsx"))
    elif strategy == "llm":
        locator.resolve_via_llm(residual)

    # results dict reflects cache state after each step; re-query
    # to fold in any new resolutions.

Design notes
------------
- Backends are pluggable. Pass ``nppes_backend=None`` or
  ``llm_backend=None`` to disable steps. The orchestrator's
  ``recommend_residual_strategy`` returns ``"llm_unavailable"`` when
  the residual is above threshold but no LLM is configured.
- Cache key is the institution string normalized via
  ``cache.canonical_key`` (lowercase + collapse whitespace). Two
  inputs with different capitalization / spacing hit the same entry.
- Misses are cached (with ``source='miss'``) so a backend that
  doesn't know an institution isn't asked again on every run. Re-
  resolve by manually editing the cache file.
"""

from .cache import DiskCache, canonical_key
from .llm import ClaudeAPIBackend
from .manual import ManualReviewBackend
from .nppes import NPPESBackend
from .orchestrator import InstitutionLocator
from .types import CandidateLocation, flatten_to_citystates

__all__ = [
    "InstitutionLocator",
    "CandidateLocation",
    "flatten_to_citystates",
    "DiskCache",
    "canonical_key",
    "NPPESBackend",
    "ManualReviewBackend",
    "ClaudeAPIBackend",
]
