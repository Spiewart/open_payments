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
3. **ManualReviewBackend.** For the residual (NPPES couldn't resolve),
   exports an xlsx for an analyst to fill in; re-imports back to the
   cache. Multi-campus institutions can be expressed as multiple rows
   in the spreadsheet.

Quickstart
----------
.. code-block:: python

    from pathlib import Path
    from open_payments import InstitutionLocator

    locator = InstitutionLocator(
        cache_path=Path("~/.cache/institutions.json").expanduser(),
    )

    # Step 1+2: cache + NPPES. Returns whatever resolved so far.
    results = locator.locate_batch(["Johns Hopkins University", "Cleveland Clinic"])

    # Step 3: any residual goes to manual review.
    residual = locator.residual_institutions(results)
    if residual:
        locator.export_for_manual_review(residual, Path("review.xlsx"))
        # ... analyst fills review.xlsx ...
        locator.import_manual_review(Path("review.xlsx"))

Design notes
------------
- Backends are pluggable. Pass ``nppes_backend=None`` to disable the
  NPPES step (e.g. test environments without internet).
- Cache key is the institution string normalized via
  ``cache.canonical_key`` (lowercase + collapse whitespace). Two
  inputs with different capitalization / spacing hit the same entry.
- Misses are cached (with ``source='miss'``) so a backend that
  doesn't know an institution isn't asked again on every run. Re-
  resolve by manually editing the cache file.
"""

from .cache import DiskCache, canonical_key
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
]
