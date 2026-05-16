"""Tests for the MatchSelector strategy layer (Section 5.7)."""

from __future__ import annotations

import pandas as pd
import pytest

from ..choices import PaymentFilters, Unmatcheds
from ..conflicteds import Conflicteds
from ..ids import ConflictedPaymentIDs, PaymentIDs
from ..selectors import (
    DEFAULT_TIER_RULES,
    TIER_HIGH_NPI,
    TIER_LOW_LASTNAME_PLUS_ONE,
    TIER_LOW_NAME_ONLY,
    TIER_MEDIUM_HIGH_NAME_PLUS,
    TIER_MEDIUM_NAME_PARTIAL,
    TIER_VERY_LOW_LASTNAME_BARE,
    TIER_VERY_LOW_LASTNAME_PARTIAL,
    TIER_VERY_LOW_OTHER,
    DefaultMatchSelector,
    IdentifierWinsSelector,
    MatchSelector,
    SelectorResult,
    TieredConfidenceSelector,
    TiesAreUnmatchedSelector,
    assign_tier,
)
from .factories import make_raw_conflicted_row

# ---------------------------------------------------------------------------
# SelectorResult invariants
# ---------------------------------------------------------------------------


def test__selector_result_unique_requires_one_row():
    row = pd.DataFrame([{"filters": [PaymentFilters.LASTNAME]}])
    result = SelectorResult.unique(row)
    assert result.kind == "unique"
    assert result.match is row
    assert result.representative_filters == [PaymentFilters.LASTNAME]


def test__selector_result_unique_rejects_zero_or_multi_row():
    with pytest.raises(ValueError, match="1-row"):
        SelectorResult.unique(pd.DataFrame())
    with pytest.raises(ValueError, match="1-row"):
        SelectorResult.unique(pd.DataFrame([{"filters": [PaymentFilters.LASTNAME]}] * 2))


def test__selector_result_unique_via_constructor_validates():
    # Direct construction with mismatched kind/data should raise.
    with pytest.raises(ValueError):
        SelectorResult(kind="unique", match=None)
    with pytest.raises(ValueError):
        SelectorResult(kind="unmatched_options", unmatched_options=None)
    with pytest.raises(ValueError):
        # unmatched_options but missing reason
        SelectorResult(
            kind="unmatched_options",
            unmatched_options=pd.DataFrame([{"filters": [PaymentFilters.LASTNAME]}]),
        )


def test__selector_result_unmatched_options_from_uses_first_row_filters_by_default():
    df = pd.DataFrame(
        [
            {"filters": [PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME], "profile_id": 1},
            {"filters": [PaymentFilters.LASTNAME], "profile_id": 2},
        ]
    )
    result = SelectorResult.unmatched_options_from(df)
    assert result.kind == "unmatched_options"
    assert result.representative_filters == [PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME]
    assert result.unmatched_reason == Unmatcheds.UNFILTERABLE


# ---------------------------------------------------------------------------
# DefaultMatchSelector — behavior preservation against the canonical fixture
# ---------------------------------------------------------------------------


def _raw_scenarios() -> pd.DataFrame:
    """Same 6-scenario raw input as test_end_to_end.py — A/B/C/D unique
    matches, E ambiguous, X no-match."""
    return pd.DataFrame(
        [
            make_raw_conflicted_row(
                name="John M. Adams, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Manhattan, NY",
            ),
            make_raw_conflicted_row(
                name="Jane Marie Brown, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Boston, MA",
            ),
            make_raw_conflicted_row(
                name="David A. Smith, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Seattle, WA",
            ),
            make_raw_conflicted_row(
                name="Hannah Lee Smith-Jones, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="San Diego, CA",
            ),
            make_raw_conflicted_row(
                name="Emily White, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Chicago, IL",
            ),
            make_raw_conflicted_row(
                name="Nobody Nonexistent, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Nowheresville, NY",
            ),
        ]
    )


def _load_payments(cms_data_dir, fixture_years) -> pd.DataFrame:
    return PaymentIDs(
        years=fixture_years,
        payment_classes="general",
        payments_folder=str(cms_data_dir),
        nrows=None,
        MD_DO_only=True,
    ).all_payments()


def test__default_selector_explicit_construction_matches_implicit_default(
    cms_data_dir, fixture_years
):
    """ConflictedPaymentIDs without a `selector=` arg should behave identically
    to one constructed with an explicit `DefaultMatchSelector()`."""
    conflicteds = Conflicteds(_raw_scenarios()).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    implicit = ConflictedPaymentIDs(conflicteds=conflicteds, payments=payments)
    implicit.search_for_conflicteds_ids()

    explicit = ConflictedPaymentIDs(
        conflicteds=conflicteds.copy(),
        payments=payments,
        selector=DefaultMatchSelector(),
    )
    explicit.search_for_conflicteds_ids()

    # Same unique matches.
    pk_to_profile_implicit = dict(
        zip(implicit.unique_ids["provider_pk"], implicit.unique_ids["profile_id"], strict=True)
    )
    pk_to_profile_explicit = dict(
        zip(explicit.unique_ids["provider_pk"], explicit.unique_ids["profile_id"], strict=True)
    )
    assert pk_to_profile_implicit == pk_to_profile_explicit

    # Same unmatched pks + reasons.
    assert set(implicit.unmatched["provider_pk"]) == set(explicit.unmatched["provider_pk"])


# ---------------------------------------------------------------------------
# IdentifierWinsSelector — NPI-wins semantics
# ---------------------------------------------------------------------------


def _make_payments_x_conflicted_row(
    profile_id: int,
    filters: list[PaymentFilters],
    first_name: str = "EMILY",
    last_name: str = "WHITE",
    middle_name=None,
    payment_id: int = 0,
) -> dict:
    """Build one row of the payments-x-conflicted frame as the matcher would
    see it post-filter-application."""
    return {
        "profile_id": profile_id,
        "filters": filters,
        "first_name": first_name,
        "last_name": last_name,
        "middle_name": middle_name,
        "payment_id": payment_id,
        "conflict_first_name": first_name,
        # Add other columns the selector might touch via matcher context
        # (the matcher methods are no-ops for empty/missing data).
        "citystates": [],
        "credentials": [],
        "specialtys": [],
        "conflict_citystates": [],
        "conflict_credentials": [],
        "conflict_specialtys": [],
        "city": None,
        "state": None,
    }


class _RecordingFallback(MatchSelector):
    """A minimal fallback that records calls and returns a marker result."""

    def __init__(self):
        self.called = False
        self.received_df = None

    def select(self, payments_x_conflicted, matcher):
        self.called = True
        self.received_df = payments_x_conflicted
        # Return an unmatched_options result so SelectorResult validation passes.
        return SelectorResult.unmatched_options_from(payments_x_conflicted)


def _stub_matcher_context():
    """A trivial MatcherContext stand-in. IdentifierWinsSelector doesn't
    consult the matcher at all in the unique-hit case, and the fallback path
    is satisfied by _RecordingFallback. Stub returns the input unchanged for
    every helper method.
    """

    class _Stub:
        def get_firstname_matches(self, df):
            return df

        def get_middlename_matches(self, df):
            return df

        def get_full_citystate_matches(self, df):
            return df

        def get_highest_matches(self, df):
            return df

        def get_citystate_matches(self, df):
            return df

        def get_specialty_matches(self, df):
            return df

    return _Stub()


def test__identifier_wins_returns_unique_when_single_npi_hit():
    """A single row with PaymentFilters.NPI in its filters → unique match,
    even if other rows have more filters total."""
    df = pd.DataFrame(
        [
            # NPI-bearing row — should win.
            _make_payments_x_conflicted_row(
                profile_id=101, filters=[PaymentFilters.LASTNAME, PaymentFilters.NPI]
            ),
            # Higher filter count but no NPI — should NOT win.
            _make_payments_x_conflicted_row(
                profile_id=102,
                filters=[
                    PaymentFilters.LASTNAME,
                    PaymentFilters.FIRSTNAME,
                    PaymentFilters.CITYSTATE,
                    PaymentFilters.SPECIALTY,
                ],
            ),
        ]
    )
    selector = IdentifierWinsSelector(fallback=_RecordingFallback())
    result = selector.select(df, matcher=_stub_matcher_context())
    assert result.kind == "unique"
    assert result.match.iloc[0]["profile_id"] == 101
    assert not selector.fallback.called


def test__identifier_wins_delegates_to_fallback_when_no_identifier_hits():
    """No row has NPI in filters → fallback selector is used."""
    df = pd.DataFrame(
        [
            _make_payments_x_conflicted_row(profile_id=101, filters=[PaymentFilters.LASTNAME]),
            _make_payments_x_conflicted_row(
                profile_id=102, filters=[PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME]
            ),
        ]
    )
    fallback = _RecordingFallback()
    selector = IdentifierWinsSelector(fallback=fallback)
    result = selector.select(df, matcher=_stub_matcher_context())
    assert fallback.called
    assert result.kind == "unmatched_options"  # marker from _RecordingFallback


def test__identifier_wins_delegates_to_fallback_when_multiple_identifier_hits():
    """Two rows BOTH have NPI (data anomaly — same NPI appearing on multiple
    profile_ids) → no unique winner from identifier path, fall back."""
    df = pd.DataFrame(
        [
            _make_payments_x_conflicted_row(
                profile_id=101, filters=[PaymentFilters.LASTNAME, PaymentFilters.NPI]
            ),
            _make_payments_x_conflicted_row(
                profile_id=102, filters=[PaymentFilters.LASTNAME, PaymentFilters.NPI]
            ),
        ]
    )
    fallback = _RecordingFallback()
    selector = IdentifierWinsSelector(fallback=fallback)
    selector.select(df, matcher=_stub_matcher_context())
    assert fallback.called


def test__identifier_wins_custom_identifier_set_via_subclass():
    """Override `IDENTIFIER_FILTERS` to expand the set of identifier filters."""

    class _CustomSelector(IdentifierWinsSelector):
        # Treat FULLSPECIALTY as a unique identifier in this hypothetical study.
        IDENTIFIER_FILTERS = {PaymentFilters.NPI, PaymentFilters.FULLSPECIALTY}

    df = pd.DataFrame(
        [
            # No NPI, but has FULLSPECIALTY — should win under the custom rule.
            _make_payments_x_conflicted_row(
                profile_id=101, filters=[PaymentFilters.LASTNAME, PaymentFilters.FULLSPECIALTY]
            ),
            _make_payments_x_conflicted_row(
                profile_id=102, filters=[PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME]
            ),
        ]
    )
    selector = _CustomSelector()
    result = selector.select(df, matcher=_stub_matcher_context())
    assert result.kind == "unique"
    assert result.match.iloc[0]["profile_id"] == 101


def test__identifier_wins_default_fallback_is_default_match_selector():
    """No-arg construction wires DefaultMatchSelector as the fallback."""
    selector = IdentifierWinsSelector()
    assert isinstance(selector.fallback, DefaultMatchSelector)


# ---------------------------------------------------------------------------
# End-to-end: IdentifierWinsSelector on a real-ish fixture
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# TieredConfidenceSelector — tier rule predicates (pure helpers)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "positive,negative,expected_tier",
    [
        # HIGH_NPI: NPI alone is enough.
        ({PaymentFilters.NPI}, set(), TIER_HIGH_NPI),
        # MEDIUM_HIGH: last + first + 2 strong disambiguators (citystate + middle).
        (
            {
                PaymentFilters.LASTNAME,
                PaymentFilters.FIRSTNAME,
                PaymentFilters.CITYSTATE,
                PaymentFilters.MIDDLENAME,
            },
            set(),
            TIER_MEDIUM_HIGH_NAME_PLUS,
        ),
        # MEDIUM: last + first + 1 strong disambiguator.
        (
            {PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME, PaymentFilters.CITYSTATE},
            set(),
            TIER_MEDIUM_NAME_PARTIAL,
        ),
        # Negative-signal info does NOT demote tier — same MEDIUM_HIGH_NAME_PLUS
        # with active middle disagreement stays at MEDIUM_HIGH; the disagreement
        # is preserved on the parallel n_negative_filters column for analyst
        # review (and is used as a tiebreak by the selector).
        (
            {
                PaymentFilters.LASTNAME,
                PaymentFilters.FIRSTNAME,
                PaymentFilters.CITYSTATE,
                PaymentFilters.MIDDLENAME,
            },
            {PaymentFilters.MIDDLE_INITIAL},
            TIER_MEDIUM_HIGH_NAME_PLUS,
        ),
        # LOW_LASTNAME_PLUS_ONE: lastname + 1 disambiguator, no firstname.
        (
            {PaymentFilters.LASTNAME, PaymentFilters.CITYSTATE},
            set(),
            TIER_LOW_LASTNAME_PLUS_ONE,
        ),
        # LOW_NAME_ONLY: full name, no strong disambiguators.
        (
            {PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME},
            set(),
            TIER_LOW_NAME_ONLY,
        ),
        # VERY_LOW_LASTNAME_BARE: lastname only, no other agreement.
        (
            {PaymentFilters.LASTNAME},
            set(),
            TIER_VERY_LOW_LASTNAME_BARE,
        ),
        # VERY_LOW_LASTNAME_BARE applies even when firstname is in negative_filters
        # (the disagreement is preserved on the negative_filters output column;
        # the tier itself only reflects positive evidence).
        (
            {PaymentFilters.LASTNAME},
            {PaymentFilters.FIRSTNAME},
            TIER_VERY_LOW_LASTNAME_BARE,
        ),
        # VERY_LOW_LASTNAME_PARTIAL: lastname matched only via 1-edit partial.
        # No exact-LASTNAME tag, so all exact-lastname tiers reject it.
        (
            {PaymentFilters.LASTNAME_PARTIAL},
            set(),
            TIER_VERY_LOW_LASTNAME_PARTIAL,
        ),
        # Partial + firstname + disambiguators still lands in LASTNAME_PARTIAL
        # (intentional demotion): the underlying name signal is weaker than
        # an exact match would have been, so we group all partial hits at the
        # same low tier rather than promote a partial-but-corroborated row to
        # MEDIUM_HIGH. Within-tier ranking by n_filters still rewards rows
        # with more corroborating signals.
        (
            {
                PaymentFilters.LASTNAME_PARTIAL,
                PaymentFilters.FIRSTNAME,
                PaymentFilters.CITYSTATE,
                PaymentFilters.MIDDLENAME,
            },
            set(),
            TIER_VERY_LOW_LASTNAME_PARTIAL,
        ),
        # Partial lastname + NPI: NPI is a unique identifier and outranks the
        # name signal — row lands in HIGH_NPI, not LASTNAME_PARTIAL.
        (
            {PaymentFilters.LASTNAME_PARTIAL, PaymentFilters.NPI},
            set(),
            TIER_HIGH_NPI,
        ),
        # Fallback: empty filters → VERY_LOW_OTHER.
        (set(), set(), TIER_VERY_LOW_OTHER),
    ],
)
def test__assign_tier_default_rules(positive, negative, expected_tier):
    assert assign_tier(positive, negative) == expected_tier


def test__assign_tier_rule_order_first_match_wins():
    """If filters satisfy both HIGH_NPI and MEDIUM_HIGH_NAME_PLUS, HIGH_NPI wins
    because it's first in DEFAULT_TIER_RULES."""
    filters = {
        PaymentFilters.NPI,
        PaymentFilters.LASTNAME,
        PaymentFilters.FIRSTNAME,
        PaymentFilters.CITYSTATE,
        PaymentFilters.MIDDLENAME,
    }
    assert assign_tier(filters, set()) == TIER_HIGH_NPI


def test__assign_tier_default_rules_ignore_negative_signal():
    """Per the redesign: default tier rules are positive-signal only. A row
    with the same positive evidence stays at the same tier regardless of
    negative_filters. Negative info lives on a parallel output column."""
    positive = {PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME, PaymentFilters.CITYSTATE}
    assert assign_tier(positive, set()) == TIER_MEDIUM_NAME_PARTIAL
    assert assign_tier(positive, {PaymentFilters.MIDDLENAME}) == TIER_MEDIUM_NAME_PARTIAL
    assert assign_tier(positive, {PaymentFilters.MIDDLE_INITIAL}) == TIER_MEDIUM_NAME_PARTIAL


# ---------------------------------------------------------------------------
# TieredConfidenceSelector — DataFrame-level selection
# ---------------------------------------------------------------------------


def _tier_row(
    profile_id: int,
    filters: list[PaymentFilters],
    negative_filters: list[PaymentFilters] | None = None,
    payment_id: int = 0,
) -> dict:
    row = _make_payments_x_conflicted_row(
        profile_id=profile_id, filters=filters, payment_id=payment_id
    )
    row["negative_filters"] = negative_filters or []
    return row


def test__tiered_selector_picks_single_highest_tier_row():
    """One row at HIGH_NPI, one at MEDIUM_NAME_PARTIAL → HIGH_NPI wins."""
    df = pd.DataFrame(
        [
            _tier_row(profile_id=101, filters=[PaymentFilters.LASTNAME, PaymentFilters.NPI]),
            _tier_row(
                profile_id=102,
                filters=[
                    PaymentFilters.LASTNAME,
                    PaymentFilters.FIRSTNAME,
                    PaymentFilters.CITYSTATE,
                ],
            ),
        ]
    )
    result = TieredConfidenceSelector().select(df, matcher=_stub_matcher_context())
    assert result.kind == "unique"
    assert result.match.iloc[0]["profile_id"] == 101


def test__tiered_selector_same_tier_tiebreak_by_fewest_negative_filters():
    """Real-world deans case: a MEDIUM_HIGH_NAME_PLUS row with MIDDLE_INITIAL
    in negative_filters loses to the same-tier clean alternative. The tier
    rules themselves are positive-only — selection-time tiebreak (not tier
    demotion) is how the negative signal influences the winner."""
    df = pd.DataFrame(
        [
            # Same-tier (MEDIUM_HIGH_NAME_PLUS) but with a negative signal —
            # loses on the negative-count tiebreak.
            _tier_row(
                profile_id=302,
                filters=[
                    PaymentFilters.LASTNAME,
                    PaymentFilters.FIRSTNAME,
                    PaymentFilters.CITYSTATE,
                    PaymentFilters.MIDDLENAME,
                ],
                negative_filters=[PaymentFilters.MIDDLE_INITIAL],
            ),
            # Clean candidate at the same tier — wins because n_negative=0.
            _tier_row(
                profile_id=301,
                filters=[
                    PaymentFilters.LASTNAME,
                    PaymentFilters.FIRSTNAME,
                    PaymentFilters.CITYSTATE,
                    PaymentFilters.MIDDLENAME,
                ],
            ),
        ]
    )
    result = TieredConfidenceSelector().select(df, matcher=_stub_matcher_context())
    assert result.kind == "unique"
    assert result.match.iloc[0]["profile_id"] == 301
    assert result.confidence_tier == TIER_MEDIUM_HIGH_NAME_PLUS


def test__tiered_selector_keeps_confident_match_when_no_clean_alternative():
    """If the ONLY MEDIUM_HIGH row has a negative signal, it should still be
    returned as the unique match — its tier is still MEDIUM_HIGH_NAME_PLUS.
    The negative_filters info travels via SelectorResult so the analyst can
    review it; we don't drop the match just because it has a negative signal."""
    df = pd.DataFrame(
        [
            _tier_row(
                profile_id=302,
                filters=[
                    PaymentFilters.LASTNAME,
                    PaymentFilters.FIRSTNAME,
                    PaymentFilters.CITYSTATE,
                    PaymentFilters.MIDDLENAME,
                ],
                negative_filters=[PaymentFilters.MIDDLE_INITIAL],
            ),
        ]
    )
    result = TieredConfidenceSelector().select(df, matcher=_stub_matcher_context())
    assert result.kind == "unique"
    assert result.match.iloc[0]["profile_id"] == 302
    assert result.confidence_tier == TIER_MEDIUM_HIGH_NAME_PLUS
    assert result.representative_negative_filters == [PaymentFilters.MIDDLE_INITIAL]


def test__tiered_selector_ties_at_top_delegate_to_fallback():
    """Two rows both at HIGH_NPI → fallback handles tiebreak."""
    df = pd.DataFrame(
        [
            _tier_row(profile_id=101, filters=[PaymentFilters.LASTNAME, PaymentFilters.NPI]),
            _tier_row(profile_id=102, filters=[PaymentFilters.LASTNAME, PaymentFilters.NPI]),
        ]
    )
    fallback = _RecordingFallback()
    TieredConfidenceSelector(fallback=fallback).select(df, matcher=_stub_matcher_context())
    assert fallback.called
    assert len(fallback.received_df) == 2  # both top-tier rows passed through


def test__tiered_selector_below_min_acceptable_rank_yields_unmatched_options():
    """Set MIN_ACCEPTABLE_TIER_RANK to reject everything LOW_* or worse.
    A row at LOW_NAME_ONLY is then below the threshold → unmatched_options."""

    class _Strict(TieredConfidenceSelector):
        # Rank 3 is LOW_NAME_DISAGREE; only ranks 0..2 (HIGH/MEDIUM_HIGH/MEDIUM) accepted.
        MIN_ACCEPTABLE_TIER_RANK = 2

    df = pd.DataFrame(
        [
            # LOW_NAME_ONLY (rank 5) — below threshold.
            _tier_row(
                profile_id=101,
                filters=[PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME],
            ),
        ]
    )
    result = _Strict().select(df, matcher=_stub_matcher_context())
    assert result.kind == "unmatched_options"
    assert result.unmatched_reason == Unmatcheds.UNFILTERABLE


def test__tiered_selector_custom_tier_rules_via_class_var():
    """Override TIER_RULES with a custom list — selector should respect it."""

    def _is_credential_only(f, n):
        return f == {PaymentFilters.LASTNAME, PaymentFilters.CREDENTIAL}

    class _CustomRules(TieredConfidenceSelector):
        TIER_RULES = [("CREDENTIAL_ONLY", _is_credential_only)]
        FALLBACK_TIER = "EVERYTHING_ELSE"

    df = pd.DataFrame(
        [
            _tier_row(
                profile_id=101,
                filters=[PaymentFilters.LASTNAME, PaymentFilters.CREDENTIAL],
            ),
            _tier_row(
                profile_id=102,
                filters=[PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME],
            ),
        ]
    )
    result = _CustomRules().select(df, matcher=_stub_matcher_context())
    assert result.kind == "unique"
    assert result.match.iloc[0]["profile_id"] == 101  # the CREDENTIAL_ONLY row wins


def test__tiered_selector_default_fallback_is_ties_are_unmatched():
    """The default fallback is precision-favored: when tier + negative-filter
    tiebreak can't disambiguate, surface tied candidates as unmatched_options
    instead of letting the recall-favored cascade silently narrow further.

    Studies wanting the legacy recall-favored behavior can opt in via
    ``TieredConfidenceSelector(fallback=DefaultMatchSelector())``.
    """
    selector = TieredConfidenceSelector()
    assert isinstance(selector.fallback, TiesAreUnmatchedSelector)


def test__tiered_selector_accepts_explicit_default_match_selector_fallback():
    """Legacy recall-favored cascade is still available via explicit opt-in."""
    selector = TieredConfidenceSelector(fallback=DefaultMatchSelector())
    assert isinstance(selector.fallback, DefaultMatchSelector)


def test__tiered_selector_default_rule_list_pins_deans_compat():
    """Default rule list ports deans's match_confidence.py rules verbatim
    plus a LASTNAME_PARTIAL tier appended at the bottom. Negative-aware
    behavior happens at the selector level via tiebreaking, not via
    tier-list extension."""
    assert len(DEFAULT_TIER_RULES) == 7
    assert [name for name, _ in DEFAULT_TIER_RULES] == [
        TIER_HIGH_NPI,
        TIER_MEDIUM_HIGH_NAME_PLUS,
        TIER_MEDIUM_NAME_PARTIAL,
        TIER_LOW_LASTNAME_PLUS_ONE,
        TIER_LOW_NAME_ONLY,
        TIER_VERY_LOW_LASTNAME_BARE,
        TIER_VERY_LOW_LASTNAME_PARTIAL,
    ]


# ---------------------------------------------------------------------------
# TieredConfidenceSelector — end-to-end against the synthetic fixture
# ---------------------------------------------------------------------------


def test__tiered_selector_end_to_end_matches_clean_scenarios(cms_data_dir, fixture_years):
    """Scenarios A/B/C/D should resolve to the same unique matches as the
    DefaultMatchSelector — the tiering should still arrive at HIGH-confidence
    matches for these scenarios since they have multiple strong disambiguators."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    matcher = ConflictedPaymentIDs(
        conflicteds=conflicteds,
        payments=payments,
        selector=TieredConfidenceSelector(),
    )
    matcher.search_for_conflicteds_ids()

    pk_to_profile = dict(
        zip(matcher.unique_ids["provider_pk"], matcher.unique_ids["profile_id"], strict=True)
    )
    # A/B/C/D should all resolve to their expected CMS profiles (same as
    # DefaultMatchSelector). Scenario E (Emily White ambiguous, no
    # disambiguators) and X (no last-name match) remain unmatched.
    assert pk_to_profile == {0: 101, 1: 201, 2: 301, 3: 401}


def test__identifier_wins_end_to_end_with_npi_in_conflicted(cms_data_dir, fixture_years):
    """When a conflicted has an NPI matching a CMS row, IdentifierWinsSelector
    should resolve it via NPI even if the default cascade would also match.

    Scenario: conflicted is Jane Brown with NPI 1000000201 (CMS profile 201's
    NPI from the fixture). Both selectors should produce the same profile_id
    match, but the IdentifierWinsSelector's path is short-circuited by NPI.
    """
    raw = pd.DataFrame(
        [
            make_raw_conflicted_row(
                name="Jane Marie Brown, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Boston, MA",
                npi=1000000201,  # matches the fixture's CMS row
            ),
        ]
    )
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    matcher = ConflictedPaymentIDs(
        conflicteds=conflicteds,
        payments=payments,
        selector=IdentifierWinsSelector(),
    )
    matcher.search_for_conflicteds_ids()

    assert len(matcher.unique_ids) == 1
    matched = matcher.unique_ids.iloc[0]
    assert matched["profile_id"] == 201
    # The matched row's filter list must include NPI (this is the whole
    # point of the IdentifierWins path).
    assert PaymentFilters.NPI in matched["filters"]


# ---------------------------------------------------------------------------
# TiesAreUnmatchedSelector tests
# ---------------------------------------------------------------------------


def test__ties_are_unmatched_surfaces_all_candidates_as_unmatched_options():
    """Multiple candidates → all surfaced as unmatched_options, none picked."""
    df = pd.DataFrame(
        [
            _make_payments_x_conflicted_row(
                profile_id=101,
                filters=[PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME, PaymentFilters.STATE],
            ),
            _make_payments_x_conflicted_row(
                profile_id=102,
                filters=[
                    PaymentFilters.LASTNAME,
                    PaymentFilters.FIRSTNAME,
                    PaymentFilters.SPECIALTY,
                ],
            ),
        ]
    )
    selector = TiesAreUnmatchedSelector()
    result = selector.select(df, matcher=_stub_matcher_context())

    assert result.kind == "unmatched_options"
    assert result.unmatched_reason == Unmatcheds.UNFILTERABLE
    # Both candidates carried through for human review.
    assert len(result.unmatched_options) == 2
    assert set(result.unmatched_options["profile_id"]) == {101, 102}


def test__ties_are_unmatched_single_row_still_unmatched():
    """Defensive behavior: a 1-row input also surfaces as unmatched_options.

    In documented use (as a fallback for TieredConfidenceSelector) this case
    doesn't arise — the parent selector handles the 1-row branch before
    delegating. But the selector is conservative if called directly: the
    caller's decision to invoke it is taken as authoritative.
    """
    df = pd.DataFrame(
        [
            _make_payments_x_conflicted_row(
                profile_id=101,
                filters=[PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME],
            ),
        ]
    )
    selector = TiesAreUnmatchedSelector()
    result = selector.select(df, matcher=_stub_matcher_context())

    assert result.kind == "unmatched_options"
    assert len(result.unmatched_options) == 1


def test__ties_are_unmatched_used_as_tiered_fallback_keeps_ties_unmatched():
    """End-to-end: TieredConfidenceSelector(fallback=TiesAreUnmatchedSelector())
    surfaces tier-tied candidates as unmatched_options rather than picking one.

    Setup: 2 candidates that both tier at MEDIUM_NAME_PARTIAL and both have
    the same n_negative_filters (zero in this case). The parent selector
    reaches its fallback path; with TiesAreUnmatchedSelector that path
    produces unmatched_options, NOT a forced pick from the legacy cascade.
    """
    df = pd.DataFrame(
        [
            _make_payments_x_conflicted_row(
                profile_id=201,
                filters=[
                    PaymentFilters.LASTNAME,
                    PaymentFilters.FIRSTNAME,
                    PaymentFilters.STATE,
                ],
            ),
            _make_payments_x_conflicted_row(
                profile_id=202,
                filters=[
                    PaymentFilters.LASTNAME,
                    PaymentFilters.FIRSTNAME,
                    PaymentFilters.SPECIALTY,
                ],
            ),
        ]
    )
    # Both rows: full name + 1 strong disambiguator → MEDIUM_NAME_PARTIAL
    # Both have 0 negative filters → tiebreaker can't differentiate → fallback.
    selector = TieredConfidenceSelector(fallback=TiesAreUnmatchedSelector())
    result = selector.select(df, matcher=_stub_matcher_context())

    assert result.kind == "unmatched_options"
    assert len(result.unmatched_options) == 2


def test__ties_are_unmatched_does_not_intercept_clear_winners():
    """End-to-end safety: a clear winner (one row at best tier) is still
    picked as unique. TiesAreUnmatchedSelector only fires when the parent
    selector delegates, which the parent does only when there's an actual
    tie.
    """
    df = pd.DataFrame(
        [
            # Best tier candidate — only this row has NPI.
            _make_payments_x_conflicted_row(
                profile_id=301,
                filters=[PaymentFilters.LASTNAME, PaymentFilters.NPI],
            ),
            # Lower-tier candidate.
            _make_payments_x_conflicted_row(
                profile_id=302,
                filters=[PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME],
            ),
        ]
    )
    selector = TieredConfidenceSelector(fallback=TiesAreUnmatchedSelector())
    result = selector.select(df, matcher=_stub_matcher_context())

    assert result.kind == "unique"
    assert result.match.iloc[0]["profile_id"] == 301
    assert result.confidence_tier == TIER_HIGH_NPI
