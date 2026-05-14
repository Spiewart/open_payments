"""Smoke + regression tests against the synthetic CMS fixtures.

These tests run without any local CMS data — they exist precisely to make sure
the rest of the suite is portable. If any of them break, the fixture and the
matching pipeline have diverged and the rest of the suite is suspect.
"""

from __future__ import annotations

import pandas as pd

from ..ids import ConflictedPaymentIDs, PaymentIDs


def test__read_payments_filters_non_physicians_and_drops_na_profile_id(cms_data_dir, fixture_years):
    """ReadPayments pipeline against fixture: physician filter drops the
    Physician Assistant row; CSV chunk filter drops the NaN profile_id row.
    Net: 8 physician rows from the 10-row fixture."""
    p = PaymentIDs(
        years=fixture_years,
        payment_classes="general",
        payments_folder=str(cms_data_dir),
        nrows=None,
        MD_DO_only=True,
    )
    df = p.all_payments()

    assert len(df) == 8, "expected 8 physician rows after filtering"
    assert 999 not in df["profile_id"].values, "Wilson PA must be filtered out"
    assert df["profile_id"].notna().all(), "NaN profile_id row must be dropped"
    assert {"profile_id", "first_name", "last_name", "middle_name"}.issubset(df.columns)


def test__canonical_matcher_unique_ids(cms_data_dir, fixture_years, canonical_conflicteds_df):
    """End-to-end: the canonical 6-row conflicted DF resolves to exactly 4
    unique matches (A, B, C, D) against the general fixture."""
    p = PaymentIDs(
        years=fixture_years,
        payment_classes="general",
        payments_folder=str(cms_data_dir),
        nrows=None,
        MD_DO_only=True,
    )
    payments = p.all_payments()

    matcher = ConflictedPaymentIDs(conflicteds=canonical_conflicteds_df, payments=payments)
    matcher.search_for_conflicteds_ids()

    assert len(matcher.unique_ids) == 4

    pk_to_profile = dict(
        zip(matcher.unique_ids["provider_pk"], matcher.unique_ids["profile_id"], strict=True)
    )
    assert pk_to_profile == {1: 101, 2: 201, 3: 301, 4: 401}


def test__canonical_matcher_unmatched_and_options(
    cms_data_dir, fixture_years, canonical_conflicteds_df
):
    """The ambiguous Emily White (pk 5) lands in unmatched with UNFILTERABLE
    plus both candidates in unmatched_options; the non-existent provider
    (pk 6) lands in unmatched with NOLASTNAME."""
    p = PaymentIDs(
        years=fixture_years,
        payment_classes="general",
        payments_folder=str(cms_data_dir),
        nrows=None,
        MD_DO_only=True,
    )
    payments = p.all_payments()

    matcher = ConflictedPaymentIDs(conflicteds=canonical_conflicteds_df, payments=payments)
    matcher.search_for_conflicteds_ids()

    unmatched_pks = set(matcher.unmatched["provider_pk"].tolist())
    assert unmatched_pks == {5, 6}

    pk5_row = matcher.unmatched[matcher.unmatched["provider_pk"] == 5].iloc[0]
    assert pk5_row["unmatched"] == "UNFILTERABLE"

    pk6_row = matcher.unmatched[matcher.unmatched["provider_pk"] == 6].iloc[0]
    assert pk6_row["unmatched"] == "NOLASTNAME"

    # Both Emily Whites should be in unmatched_options
    options_profiles = set(matcher.unmatched_options["profile_id"].tolist())
    assert options_profiles == {501, 502}


def test__convenience_read_methods_work(cms_data_dir, fixture_years):
    """Regression test for Section 5 bug 0b. The
    `read_general_payments_csvs` / `read_ownership_payments_csvs` /
    `read_research_payments_csvs` convenience wrappers must exist on
    ReadPayments-derived classes and produce non-empty frames against the
    synthetic fixture.
    """
    from ..read import ReadPayments

    reader = ReadPayments(
        years=fixture_years,
        payment_classes=["general", "ownership", "research"],
        payments_folder=str(cms_data_dir),
        nrows=None,
        MD_DO_only=False,  # raw read; physician filter is tested elsewhere
    )
    general = reader.read_general_payments_csvs()
    ownership = reader.read_ownership_payments_csvs()
    research = reader.read_research_payments_csvs()
    assert len(general) > 0
    assert len(ownership) > 0
    assert len(research) > 0


def test__ownership_and_research_csvs_readable(cms_data_dir, fixture_years):
    """Smoke: ownership and research fixtures load without error and the
    physician filter applies to them too (same physician-only logic)."""
    for payment_class in ("ownership", "research"):
        p = PaymentIDs(
            years=fixture_years,
            payment_classes=payment_class,
            payments_folder=str(cms_data_dir),
            nrows=None,
            MD_DO_only=True,
        )
        df = p.all_payments()
        assert isinstance(df, pd.DataFrame)
        assert df["profile_id"].notna().all(), (
            f"{payment_class}: NaN profile_id row should be dropped"
        )
