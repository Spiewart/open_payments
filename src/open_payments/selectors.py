"""Match selection strategies — the layer that decides WHICH payment row is
the match for a conflicted provider, given the filter-application results.

Architectural split (see plan Section 5.7):

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Filter APPLICATION  (this repo's filter_by_* methods)                  │
  │     For each payment row in the merged frame, evaluate every defined    │
  │     filter and accumulate which ones matched into a                     │
  │     `filters: list[PaymentFilters]` column.                             │
  │                                                                         │
  │  Filter SELECTION  (this module's MatchSelector)                        │
  │     Given the populated `filters` column, decide which row wins         │
  │     (or that no unique winner exists).                                  │
  └─────────────────────────────────────────────────────────────────────────┘

Selection rules vary by study:

- **Default cascade** (preserved verbatim from the legacy in-line method):
  firstname → middlename → citystate → highest-filter-count →
  city/specialty tiebreaker → unmatched_options + UNFILTERABLE.

- **Identifier-wins** (deans pattern): if any row's filters contain an
  identifier (NPI is the canonical example), that row wins immediately —
  unique identifiers should short-circuit the cascade.

- (deferred to Section 5.8 follow-on) **Tier-based confidence**: weight
  positive AND negative filter signals to assign HIGH / MEDIUM / LOW match
  confidence per deans's `match_confidence.py` rules. Requires the
  `negative_filters` column from Section 5.8 to be meaningful.

USAGE
-----

::

    from open_payments.ids import ConflictedPaymentIDs
    from open_payments.selectors import IdentifierWinsSelector

    matcher = ConflictedPaymentIDs(
        conflicteds=df,
        payments=payments,
        selector=IdentifierWinsSelector(),  # deans NPI-wins pattern
    )
    matcher.search_for_conflicteds_ids()

EXTENDING
---------

Three increasingly-intrusive override patterns:

1. **Replace whole cascade** — subclass `MatchSelector` directly and
   implement `select()`. See `IdentifierWinsSelector` for a 30-line example.

2. **Reuse default cascade with pre-step** — subclass `DefaultMatchSelector`,
   override `select()` to do custom work before/after calling
   `super().select()`. Example: a study that wants to reject rows missing
   certain filters before the cascade runs.

3. **Reuse default cascade with custom tiebreaker** — subclass
   `DefaultMatchSelector` and override `_resolve_highest_tiebreak()` to
   change how multi-match-highest ties are broken. The cascade is split
   into named phases so subclasses can pick one.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, Literal, Protocol

import pandas as pd

from .choices import PaymentFilters, Unmatcheds

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SelectorResult:
    """The decision a `MatchSelector` returns to the matcher.

    Two kinds:

    - ``kind="unique"``: a single match was identified. ``match`` is a
      1-row DataFrame containing the winning payment row (with its full
      payments-x-conflicted columns intact). The matcher adds it to
      ``unique_ids``.

    - ``kind="unmatched_options"``: no unique winner; ``unmatched_options``
      is a multi-row DataFrame of the remaining candidates that the matcher
      should surface for manual review. ``unmatched_reason`` is the
      ``Unmatcheds`` enum value to record (typically `UNFILTERABLE`).

    ``representative_filters`` is the filter list to record on the matched
    or unmatched row — used by `add_unique_id` / `add_unmatched` to populate
    the audit-trail `filters` column. For unique results it's the winning
    row's filters; for unmatched_options results it's typically the filters
    of the representative (highest-match-count) row.
    """

    kind: Literal["unique", "unmatched_options"]
    match: pd.DataFrame | None = None
    unmatched_options: pd.DataFrame | None = None
    unmatched_reason: Unmatcheds | None = None
    representative_filters: list[PaymentFilters] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.kind == "unique":
            if self.match is None or len(self.match) != 1:
                raise ValueError(
                    "SelectorResult(kind='unique') requires a 1-row `match` DataFrame; "
                    f"got match={None if self.match is None else f'{len(self.match)}-row'}"
                )
        elif self.kind == "unmatched_options":
            if self.unmatched_options is None or self.unmatched_options.empty:
                raise ValueError(
                    "SelectorResult(kind='unmatched_options') requires a non-empty "
                    "`unmatched_options` DataFrame"
                )
            if self.unmatched_reason is None:
                raise ValueError(
                    "SelectorResult(kind='unmatched_options') requires `unmatched_reason`"
                )
        else:
            raise ValueError(f"Unknown SelectorResult kind: {self.kind!r}")

    @classmethod
    def unique(cls, match: pd.DataFrame) -> SelectorResult:
        """Convenience constructor for a unique match. ``representative_filters``
        is auto-populated from the winning row's `filters` column."""
        if len(match) != 1:
            raise ValueError(f"unique() requires a 1-row DataFrame; got {len(match)}")
        return cls(
            kind="unique",
            match=match,
            representative_filters=list(match.iloc[0]["filters"]),
        )

    @classmethod
    def unmatched_options_from(
        cls,
        options: pd.DataFrame,
        reason: Unmatcheds = Unmatcheds.UNFILTERABLE,
        representative_filters: list[PaymentFilters] | None = None,
    ) -> SelectorResult:
        """Convenience constructor for an unmatched-options result.

        ``representative_filters`` defaults to the filters list of the first
        row in ``options`` (typically the highest-match-count row, matching
        the legacy behavior).
        """
        if representative_filters is None:
            representative_filters = list(options.iloc[0]["filters"])
        return cls(
            kind="unmatched_options",
            unmatched_options=options,
            unmatched_reason=reason,
            representative_filters=representative_filters,
        )


# ---------------------------------------------------------------------------
# Protocol the selector relies on (already satisfied by ConflictedPaymentIDs)
# ---------------------------------------------------------------------------


class MatcherContext(Protocol):
    """The matcher methods a selector reads. ``ConflictedPaymentIDs`` already
    implements all of these via its existing domain mixins
    (`PaymentIDsNamesMixin`, `PaymentIDsCityStatesMixin`,
    `PaymentIDsSpecialtysMixin`), so no new interface needs to be implemented
    on the matcher side — the typing.Protocol just makes the dependency
    explicit and testable in isolation.
    """

    def get_firstname_matches(self, payments_x_conflicteds: pd.DataFrame) -> pd.DataFrame: ...
    def get_middlename_matches(self, payments_x_conflicteds: pd.DataFrame) -> pd.DataFrame: ...
    def get_full_citystate_matches(self, payments_x_conflicteds: pd.DataFrame) -> pd.DataFrame: ...
    def get_highest_matches(self, payments_x_conflicteds: pd.DataFrame) -> pd.DataFrame: ...
    def get_citystate_matches(self, payments_x_conflicteds: pd.DataFrame) -> pd.DataFrame: ...
    def get_specialty_matches(self, payments_x_conflicteds: pd.DataFrame) -> pd.DataFrame: ...


# ---------------------------------------------------------------------------
# MatchSelector base + DefaultMatchSelector
# ---------------------------------------------------------------------------


class MatchSelector(ABC):
    """Abstract strategy: decide which row of the filtered, deduped
    payments-x-conflicted frame wins for a given conflicted provider."""

    @abstractmethod
    def select(
        self,
        payments_x_conflicted: pd.DataFrame,
        matcher: MatcherContext,
    ) -> SelectorResult:
        """Given the post-filter-application frame (with `filters` column
        populated), return a `SelectorResult`. Pure: no mutation of the
        matcher's internal state.
        """


class DefaultMatchSelector(MatchSelector):
    """The legacy cascade, extracted verbatim from
    `ConflictedPaymentIDs.process_filtered_payments_x_conflicteds` so that
    behavior is bit-for-bit preserved.

    Cascade phases (each can be individually overridden by a subclass):

    1. ``_resolve_firstname``  — narrow to rows with FIRSTNAME / FIRSTNAME_PARTIAL
       / FIRST_MIDDLE_NAME filters.
    2. ``_resolve_middlename`` — further narrow to rows with MIDDLENAME /
       MIDDLE_INITIAL filters (applied over the firstname narrow).
    3. ``_resolve_citystate``  — further narrow to rows with CITYSTATE filter
       (applied over the firstname narrow).
    4. ``_resolve_highest``    — fall back to rows with the most filters
       applied; the source is the most-narrowed non-empty cascade level.
    5. ``_resolve_tiebreak``   — when highest still has >1 candidate, break
       by citystate then by specialty.

    Each phase is documented to return whichever's smallest non-empty
    narrowing or fall through.
    """

    def select(
        self,
        payments_x_conflicted: pd.DataFrame,
        matcher: MatcherContext,
    ) -> SelectorResult:
        # Phase 1: firstname narrow.
        first_name_matches = matcher.get_firstname_matches(payments_x_conflicted)
        if len(first_name_matches) == 1:
            return SelectorResult.unique(first_name_matches)

        # Phase 2: middlename narrow (over phase-1 result).
        middle_name_matches = matcher.get_middlename_matches(first_name_matches)
        if len(middle_name_matches) == 1:
            return SelectorResult.unique(middle_name_matches)

        # Phase 3: citystate narrow (over phase-1 result, NOT phase 2 — this
        # matches the legacy behavior where citystate is a peer of middlename
        # rather than a sub-narrow).
        citystate_matches = matcher.get_full_citystate_matches(first_name_matches)
        if len(citystate_matches) == 1:
            return SelectorResult.unique(citystate_matches)

        # Phase 4: highest matches — pick the most-filter-narrowed non-empty
        # source as the basis. The order (middle > citystate > firstname >
        # everything) preserves legacy precedence.
        if not middle_name_matches.empty:
            base_for_highest = middle_name_matches
        elif not citystate_matches.empty:
            base_for_highest = citystate_matches
        elif not first_name_matches.empty:
            base_for_highest = first_name_matches
        else:
            base_for_highest = payments_x_conflicted

        highest_matches = matcher.get_highest_matches(base_for_highest)
        if len(highest_matches) == 1:
            return SelectorResult.unique(highest_matches)

        # Phase 5: tiebreaker — citystate then specialty.
        best = self._resolve_highest_tiebreak(highest_matches, matcher)
        if not best.empty and len(best) == 1:
            return SelectorResult.unique(best)

        # No unique winner. Surface options + UNFILTERABLE.
        options = best if not best.empty else highest_matches
        return SelectorResult.unmatched_options_from(
            options=options,
            reason=Unmatcheds.UNFILTERABLE,
            representative_filters=list(highest_matches.iloc[0]["filters"]),
        )

    @staticmethod
    def _resolve_highest_tiebreak(
        highest_matches: pd.DataFrame, matcher: MatcherContext
    ) -> pd.DataFrame:
        """Break ties among highest-filter-count rows: citystate first, then
        specialty. Mirrors legacy logic at the bottom of
        `process_filtered_payments_x_conflicteds`.
        """
        best = matcher.get_citystate_matches(highest_matches)
        if best.empty:
            return matcher.get_specialty_matches(highest_matches)
        if len(best) > 1:
            return matcher.get_specialty_matches(best)
        return best


# ---------------------------------------------------------------------------
# IdentifierWinsSelector — deans NPI-wins pattern
# ---------------------------------------------------------------------------


class IdentifierWinsSelector(MatchSelector):
    """If any row's `filters` list contains an entry from ``IDENTIFIER_FILTERS``
    AND that produces a unique row, return it as the match. Otherwise delegate
    to a fallback selector (default `DefaultMatchSelector`).

    Use case: the deans study has NPIs on its conflicted side, and NPI is a
    globally unique provider identifier — an NPI match should short-circuit
    the cascade. Generalized via a class-var set so other studies can add
    their own identifier filters (e.g. a future `PaymentFilters.MEDICARE_ID`).

    Subclass and override `IDENTIFIER_FILTERS` to expand the set::

        class MyStudySelector(IdentifierWinsSelector):
            IDENTIFIER_FILTERS = {PaymentFilters.NPI, PaymentFilters.MEDICARE_ID}
    """

    IDENTIFIER_FILTERS: ClassVar[set[PaymentFilters]] = {PaymentFilters.NPI}

    def __init__(self, fallback: MatchSelector | None = None):
        self.fallback: MatchSelector = fallback if fallback is not None else DefaultMatchSelector()

    def select(
        self,
        payments_x_conflicted: pd.DataFrame,
        matcher: MatcherContext,
    ) -> SelectorResult:
        identifiers = self.IDENTIFIER_FILTERS
        # Rows whose filter list intersects the configured identifier set.
        identifier_hits = payments_x_conflicted[
            payments_x_conflicted["filters"].apply(
                lambda fs: bool(set(fs) & identifiers) if fs is not None else False
            )
        ]
        if len(identifier_hits) == 1:
            return SelectorResult.unique(identifier_hits)
        # 0 hits, or >1 hits (ambiguous identifier match — unlikely but
        # possible if data is bad). Defer to the fallback selector either way.
        return self.fallback.select(payments_x_conflicted, matcher)
