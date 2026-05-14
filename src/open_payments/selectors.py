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

- **Tier-based confidence** (`TieredConfidenceSelector`): assign each
  candidate a confidence tier (HIGH_NPI / MEDIUM_HIGH / ... / VERY_LOW_BARE)
  using rules that read both `filters` AND `negative_filters` — the latter
  lets the tier rules distinguish "firstname absent" from "firstname
  actively disagrees" (a Section 5.8 capability). The selector picks the
  row(s) at the highest tier and delegates ties to a fallback selector.

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

4. **Customize tier-based scoring** — subclass `TieredConfidenceSelector`
   and override one of:
   - ``TIER_RULES`` class var to redefine the predicates / order.
   - ``MIN_ACCEPTABLE_TIER_RANK`` class var to set a stricter cutoff for
     unmatched_options.
   - ``FALLBACK_TIER`` class var to relabel the catch-all tier.
   - ``select()`` for full override.
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


# ---------------------------------------------------------------------------
# TieredConfidenceSelector — tier-based scoring using positive + negative
# filter signals (Section 5.7 + 5.8). Ports the rule pattern from deans's
# match_confidence.py with the addition of Section 5.8 negative-filter awareness.
# ---------------------------------------------------------------------------

# Tier names. Externalized as module-level constants so subclasses can
# reference / extend them without re-typing strings.

TIER_HIGH_NPI = "HIGH_NPI"
TIER_MEDIUM_HIGH_NAME_PLUS = "MEDIUM_HIGH_NAME_PLUS"
TIER_MEDIUM_NAME_PARTIAL = "MEDIUM_NAME_PARTIAL"
TIER_LOW_NAME_DISAGREE = "LOW_NAME_DISAGREE"
TIER_LOW_LASTNAME_PLUS_ONE = "LOW_LASTNAME_PLUS_ONE"
TIER_LOW_NAME_ONLY = "LOW_NAME_ONLY"
TIER_VERY_LOW_LASTNAME_DISAGREE = "VERY_LOW_LASTNAME_DISAGREE"
TIER_VERY_LOW_LASTNAME_BARE = "VERY_LOW_LASTNAME_BARE"
TIER_VERY_LOW_OTHER = "VERY_LOW_OTHER"


# Convenience filter sets reused by tier predicates. Use PaymentFilters enum
# values (not strings) so a typo at predicate-write time raises AttributeError
# rather than silently never firing.

ANY_FIRSTNAME: set[PaymentFilters] = {
    PaymentFilters.FIRSTNAME,
    PaymentFilters.FIRSTNAME_PARTIAL,
    PaymentFilters.FIRST_MIDDLE_NAME,
}
ANY_MIDDLENAME: set[PaymentFilters] = {
    PaymentFilters.MIDDLENAME,
    PaymentFilters.MIDDLE_INITIAL,
    PaymentFilters.FIRST_MIDDLE_NAME,
}
ANY_SPECIALTY: set[PaymentFilters] = {
    PaymentFilters.SPECIALTY,
    PaymentFilters.SUBSPECIALTY,
    PaymentFilters.FULLSPECIALTY,
}
ANY_LOCATION: set[PaymentFilters] = {
    PaymentFilters.STATE,
    PaymentFilters.CITY,
    PaymentFilters.CITYSTATE,
}

# "Strong disambiguators" narrow provider identity beyond name match itself.
# Note: CREDENTIAL is excluded by default because most studies pre-filter
# their conflicteds to a specific credential class (e.g. MD/DO only), so a
# CREDENTIAL filter is more sanity-check than disambiguator. Subclass with
# a different STRONG_DISAMBIGUATORS class var if your study uses heterogeneous
# credentials.
STRONG_DISAMBIGUATORS: set[PaymentFilters] = ANY_SPECIALTY | ANY_LOCATION | ANY_MIDDLENAME


# Tier predicates. Each takes (positive_filters, negative_filters) and
# returns True iff the row should land in that tier. Rules are evaluated
# top-to-bottom; first match wins.

TierPredicate = "callable[[set[PaymentFilters], set[PaymentFilters]], bool]"
TierRule = tuple[str, "TierPredicate"]


def _is_high_npi(f: set[PaymentFilters], n: set[PaymentFilters]) -> bool:
    """HIGH_NPI: NPI matched. Unique identifier — effectively verified."""
    return PaymentFilters.NPI in f


def _is_medium_high_name_plus(f: set[PaymentFilters], n: set[PaymentFilters]) -> bool:
    """MEDIUM_HIGH_NAME_PLUS: full name (last + first) + 2+ strong
    disambiguators AND no active middle-name disagreement.

    Section 5.8 guard: if a middle-name signal disagrees, this row is
    demoted — fall through to ``LOW_NAME_DISAGREE`` further down the rules.
    """
    if bool(n & ANY_MIDDLENAME):
        return False
    has_full_name = PaymentFilters.LASTNAME in f and bool(f & ANY_FIRSTNAME)
    n_strong = len(f & STRONG_DISAMBIGUATORS)
    return has_full_name and n_strong >= 2


def _is_medium_name_partial(f: set[PaymentFilters], n: set[PaymentFilters]) -> bool:
    """MEDIUM_NAME_PARTIAL: full name + 1 strong disambiguator, OR
    lastname-only + 2+ strong disambiguators. Same Section 5.8 demotion
    guard as MEDIUM_HIGH — middle-name disagreement falls through to
    ``LOW_NAME_DISAGREE``."""
    if bool(n & ANY_MIDDLENAME):
        return False
    has_full_name = PaymentFilters.LASTNAME in f and bool(f & ANY_FIRSTNAME)
    n_strong = len(f & STRONG_DISAMBIGUATORS)
    return (has_full_name and n_strong >= 1) or (PaymentFilters.LASTNAME in f and n_strong >= 2)


def _is_low_name_disagree(f: set[PaymentFilters], n: set[PaymentFilters]) -> bool:
    """LOW_NAME_DISAGREE (Section 5.8 negative-aware): full name matched
    BUT a middle-name signal actively disagrees. Catches rows that the
    cleaner MEDIUM_HIGH / MEDIUM_NAME_PARTIAL predicates rejected for having
    middle-name disagreement."""
    has_full_name = PaymentFilters.LASTNAME in f and bool(f & ANY_FIRSTNAME)
    middle_disagrees = bool(n & ANY_MIDDLENAME)
    return has_full_name and middle_disagrees


def _is_lastname_plus_one(f: set[PaymentFilters], n: set[PaymentFilters]) -> bool:
    """LOW_LASTNAME_PLUS_ONE: lastname-only (no firstname matched) + exactly
    1 strong disambiguator. Section 5.8 guard: if firstname *actively
    disagrees* (not just absent), this falls through to
    ``VERY_LOW_LASTNAME_DISAGREE`` — active disagreement is worse than
    absence."""
    if PaymentFilters.FIRSTNAME in n:
        return False
    has_full_name = PaymentFilters.LASTNAME in f and bool(f & ANY_FIRSTNAME)
    if has_full_name:
        return False
    n_strong = len(f & STRONG_DISAMBIGUATORS)
    return PaymentFilters.LASTNAME in f and n_strong == 1


def _is_name_only(f: set[PaymentFilters], n: set[PaymentFilters]) -> bool:
    """LOW_NAME_ONLY: full name + 0 disambiguators. The "Chris Anderson MD"
    problem — many providers share common names. Same middle-disagree guard
    as MEDIUM tiers so a middle-name disagreement gets the ``LOW_NAME_DISAGREE``
    tier instead."""
    if bool(n & ANY_MIDDLENAME):
        return False
    has_full_name = PaymentFilters.LASTNAME in f and bool(f & ANY_FIRSTNAME)
    n_strong = len(f & STRONG_DISAMBIGUATORS)
    return has_full_name and n_strong == 0


def _is_lastname_disagree(f: set[PaymentFilters], n: set[PaymentFilters]) -> bool:
    """VERY_LOW_LASTNAME_DISAGREE (Section 5.8 negative-aware): lastname
    matched AND firstname *actively disagrees* (in negative_filters, not
    just absent). Highest false-positive risk — the matcher landed on a
    same-lastname record whose first name conflicts with the conflicted's."""
    return PaymentFilters.LASTNAME in f and PaymentFilters.FIRSTNAME in n


def _is_lastname_bare(f: set[PaymentFilters], n: set[PaymentFilters]) -> bool:
    """VERY_LOW_LASTNAME_BARE: lastname only, no firstname, no disambiguators.
    Firstname was absent on either side (active disagreement is captured by
    VERY_LOW_LASTNAME_DISAGREE above, which is evaluated first)."""
    if PaymentFilters.FIRSTNAME in n:
        return False
    has_full_name = PaymentFilters.LASTNAME in f and bool(f & ANY_FIRSTNAME)
    if has_full_name:
        return False
    n_strong = len(f & STRONG_DISAMBIGUATORS)
    return PaymentFilters.LASTNAME in f and n_strong == 0


# Rule order matters — earlier predicates win on overlap. Demotion is handled
# inside each predicate via negative-signal guards (e.g. MEDIUM_HIGH rejects
# rows with middle disagreement, letting LOW_NAME_DISAGREE further down catch
# them). This keeps tier rank index = confidence rank (lower = better) while
# still expressing the Section 5.8 demotion semantics.
DEFAULT_TIER_RULES: list[TierRule] = [
    (TIER_HIGH_NPI, _is_high_npi),
    (TIER_MEDIUM_HIGH_NAME_PLUS, _is_medium_high_name_plus),
    (TIER_MEDIUM_NAME_PARTIAL, _is_medium_name_partial),
    (TIER_LOW_NAME_DISAGREE, _is_low_name_disagree),
    (TIER_LOW_LASTNAME_PLUS_ONE, _is_lastname_plus_one),
    (TIER_LOW_NAME_ONLY, _is_name_only),
    (TIER_VERY_LOW_LASTNAME_DISAGREE, _is_lastname_disagree),
    (TIER_VERY_LOW_LASTNAME_BARE, _is_lastname_bare),
]
DEFAULT_FALLBACK_TIER = TIER_VERY_LOW_OTHER


def assign_tier(
    positive_filters: set[PaymentFilters],
    negative_filters: set[PaymentFilters],
    tier_rules: list[TierRule] = DEFAULT_TIER_RULES,
    fallback: str = DEFAULT_FALLBACK_TIER,
) -> str:
    """Apply tier rules in order; return the first matching tier name (or
    ``fallback`` if none match). Pure — no DataFrame manipulation."""
    for tier_name, predicate in tier_rules:
        if predicate(positive_filters, negative_filters):
            return tier_name
    return fallback


class TieredConfidenceSelector(MatchSelector):
    """Tier-based selection: assign each candidate row a confidence tier from
    ``TIER_RULES`` (read both ``filters`` AND ``negative_filters``), pick the
    row(s) at the highest tier, delegate ties to ``fallback``.

    Behavior:

    1. Compute tier rank (0 = best) for every row in the deduped frame.
    2. Filter to rows at the minimum (best) rank.
    3. If 1 such row → unique match.
    4. If >1 → call ``self.fallback.select(...)`` for tiebreaking.
    5. If the best-tier rank is worse than ``MIN_ACCEPTABLE_TIER_RANK``,
       surface as ``unmatched_options`` (the matcher couldn't reach an
       acceptable confidence level). The selected unmatched_reason is
       ``Unmatcheds.UNFILTERABLE``.

    Class vars (override per study):

    - ``TIER_RULES`` — list of (name, predicate) tuples. Defaults to
      ``DEFAULT_TIER_RULES`` which ports deans's rules + adds Section 5.8
      negative-aware tiers (``LOW_NAME_DISAGREE``,
      ``VERY_LOW_LASTNAME_DISAGREE``).
    - ``FALLBACK_TIER`` — tier name assigned when no rule fires.
    - ``MIN_ACCEPTABLE_TIER_RANK`` — rows below this rank are surfaced as
      unmatched. Defaults to ``len(DEFAULT_TIER_RULES) + 1`` (i.e. accept
      anything including the fallback). Set to e.g. ``3`` (rank of
      ``TIER_LOW_NAME_DISAGREE``) to reject everything LOW_* or worse.

    Example — stricter selector that rejects low-confidence tiers::

        class StrictTieredSelector(TieredConfidenceSelector):
            MIN_ACCEPTABLE_TIER_RANK = 2  # reject rank >= LOW_NAME_DISAGREE
    """

    TIER_RULES: ClassVar[list[TierRule]] = DEFAULT_TIER_RULES
    FALLBACK_TIER: ClassVar[str] = DEFAULT_FALLBACK_TIER
    MIN_ACCEPTABLE_TIER_RANK: ClassVar[int] = len(DEFAULT_TIER_RULES) + 1

    def __init__(self, fallback: MatchSelector | None = None):
        self.fallback: MatchSelector = fallback if fallback is not None else DefaultMatchSelector()

    def _tier_rank(self, tier: str) -> int:
        """Rank lookup — 0 is best, fallback is last."""
        for i, (name, _) in enumerate(self.TIER_RULES):
            if name == tier:
                return i
        return len(self.TIER_RULES)  # fallback rank

    def _assign_row_tier(self, row: pd.Series) -> str:
        positive = set(row["filters"] or [])
        negative = set(row.get("negative_filters") or [])
        return assign_tier(positive, negative, self.TIER_RULES, self.FALLBACK_TIER)

    def select(
        self,
        payments_x_conflicted: pd.DataFrame,
        matcher: MatcherContext,
    ) -> SelectorResult:
        tiers = payments_x_conflicted.apply(self._assign_row_tier, axis=1)
        ranks = tiers.map(self._tier_rank)
        best_rank = ranks.min()

        if best_rank > self.MIN_ACCEPTABLE_TIER_RANK:
            # Even the best candidate is below acceptable confidence —
            # surface everything for manual review.
            return SelectorResult.unmatched_options_from(
                options=payments_x_conflicted,
                reason=Unmatcheds.UNFILTERABLE,
                representative_filters=list(payments_x_conflicted.iloc[0]["filters"]),
            )

        best_rows = payments_x_conflicted[ranks == best_rank]
        if len(best_rows) == 1:
            return SelectorResult.unique(best_rows)

        # Multiple rows at the best tier — delegate tiebreak to fallback.
        return self.fallback.select(best_rows, matcher)
