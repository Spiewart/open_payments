"""Tests for the MatchSelector strategy layer (Section 5.7)."""

from __future__ import annotations

import pandas as pd
import pytest

from ..choices import PaymentFilters, Unmatcheds
from ..conflicteds import Conflicteds
from ..ids import ConflictedPaymentIDs, PaymentIDs
from ..selectors import (
    DefaultMatchSelector,
    IdentifierWinsSelector,
    MatchSelector,
    SelectorResult,
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
