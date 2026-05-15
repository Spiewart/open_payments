"""Stub: resolve an institution name to a ``(city, state)`` pair.

Why this module exists
----------------------
Child apps (``abim_conflicts``, ``deans_conflicts``) parse provider-side
institution strings from their respective source data — e.g. ABIM HTML
bio paragraphs name "Johns Hopkins University School of Medicine",
"VA Puget Sound Health Care System", "Cleveland Clinic". These are a
**secondary identity signal** for the CMS matcher: a provider's home
institution narrows down which CMS profile_id is theirs when multiple
candidates share a name.

The matcher's existing ``citystates`` filter wants ``(city, state)``
pairs, NOT free-text institution names. So we need a lookup layer that
maps "Johns Hopkins University School of Medicine" → ``("Baltimore",
"MD")``. Two production-ready strategies, neither implemented yet:

1. **LLM lookup.** Pass the institution string + a small system prompt
   ("Where is this institution headquartered? Respond with a single
   city, state.") to a hosted LLM. Cache results to disk; institutions
   rarely move. Faster to iterate on than option 2, but each lookup
   costs cents.

2. **Static gazetteer.** Build a curated CSV mapping the ~500 most
   common medical institution strings to canonical (city, state). One-
   time labor but no per-lookup cost. Tooling to *generate* the
   gazetteer (e.g. by running option 1 across the universe of seen
   institution strings) is natural.

Cross-app reuse
---------------
Both abim and deans benefit identically — same external substrate
(CMS), same matcher (open_payments), same need for citystates
enrichment. Keeping this here prevents the two child apps from drifting
on what counts as "the same institution" (e.g. should
``"Johns Hopkins University"`` and ``"Johns Hopkins University School
of Medicine"`` resolve to the same coordinates? — yes, but only if a
single canonical mapping says so).

TODO
----
The implementation below is a placeholder. The contract is fixed
(``InstitutionLocator.locate(name) -> list[CityState]``), but every
non-trivial path raises ``NotImplementedError`` until either an LLM
backend or a gazetteer file is wired.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .citystates import CityState


class InstitutionLocator:
    """Map institution names to ``CityState`` pairs.

    Stateless interface. A future LLM-backed implementation can subclass
    or replace this with a cache and an external client; the contract
    stays the same so callers don't need to know which backend is in use.

    Returns ``list[CityState]`` rather than a single value because some
    institutions span multiple campuses (``University of California``
    → ``[Berkeley CA, San Francisco CA, Los Angeles CA, ...]``). The
    matcher's ``citystates`` filter already takes lists.
    """

    def locate(self, institution_name: str) -> list["CityState"]:
        """Return the list of ``CityState`` pairs an institution maps to.

        TODO: wire to LLM or gazetteer. Currently returns ``[]`` for
        unknown inputs so callers can degrade gracefully (the matcher
        treats an empty ``citystates`` filter as "no city/state signal
        available" and falls back to name + credential matching).
        """
        # TODO(institution_locator): replace with real lookup. See module
        # docstring for the two strategies under consideration.
        return []

    def locate_many(self, institution_names: list[str]) -> list["CityState"]:
        """Resolve a list of institution names and return the union of
        their ``CityState`` pairs, deduplicated.

        Useful when a single ``ProviderRecord.institutions`` list carries
        multiple institutions (current + previous affiliations); the
        matcher cares about the *union* of locations the provider could
        plausibly appear under in CMS data.
        """
        seen: set[tuple[str | None, str | None]] = set()
        out: list[CityState] = []
        for name in institution_names:
            for cs in self.locate(name):
                key = (cs.city, cs.state)
                if key in seen:
                    continue
                seen.add(key)
                out.append(cs)
        return out
