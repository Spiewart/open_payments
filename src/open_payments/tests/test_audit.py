"""Tests for ``open_payments.audit``.

Covers the existing match-audit helpers (filter_prevalence,
filter_combination_breakdown, tier_summary, profile_id_collisions) that
operate on a matched-providers DataFrame, plus the payment-magnitude
aggregations (aggregate_payments, summary_by_*, top_*_by_amount) that
operate on a payments DataFrame.
"""
from __future__ import annotations

import pandas as pd
import pytest

from ..audit import (
    _na_safe_float,
    aggregate_payments,
    summary_by_payment_class,
    summary_by_payment_type,
    summary_by_year,
    summary_overall,
    top_payers_by_amount,
    top_providers_by_amount,
)


def _payments(rows: list[dict]) -> pd.DataFrame:
    """Build a payments DataFrame with PaymentsSearch's column shape.

    Default-fills the columns the aggregations read so each test only
    has to specify the field it's exercising.
    """
    defaults = {
        "profile_id": 1,
        "amount": 0.0,
        "year": 2024,
        "payment_class": "general",
        "payment_entity": "Acme Pharma",
        "nature": "Consulting Fee",
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


# ---------------------------------------------------------------------------
# _na_safe_float
# ---------------------------------------------------------------------------


def test__na_safe_float_coerces_pd_na():
    """``float(pd.NA)`` raises; the wrapper coerces to 0.0."""
    assert _na_safe_float(pd.NA) == 0.0
    assert _na_safe_float(float("nan")) == 0.0
    # Real numbers pass through unchanged.
    assert _na_safe_float(3.14) == 3.14
    assert _na_safe_float(100) == 100.0


# ---------------------------------------------------------------------------
# aggregate_payments
# ---------------------------------------------------------------------------


def test__aggregate_handles_empty_input():
    stats = aggregate_payments(pd.DataFrame())
    assert stats["n_payments"] == 0
    assert stats["n_providers_with_payment"] == 0
    assert stats["total_amount"] == 0.0


def test__aggregate_basic_stats():
    df = _payments(
        [
            {"profile_id": 1, "amount": 100.0},
            {"profile_id": 1, "amount": 200.0},
            {"profile_id": 2, "amount": 300.0},
        ]
    )
    s = aggregate_payments(df)
    assert s["n_payments"] == 3
    assert s["n_providers_with_payment"] == 2
    assert s["total_amount"] == 600.0
    assert s["mean_per_payment"] == 200.0
    assert s["median_per_payment"] == 200.0


def test__aggregate_p90_is_per_provider_not_per_payment():
    """The p90 stat is across PER-PROVIDER totals (a meaningful
    'magnitude' signal), not across individual payment amounts."""
    df = _payments(
        [
            {"profile_id": 1, "amount": 50.0},
            {"profile_id": 1, "amount": 50.0},  # provider 1 total: 100
            {"profile_id": 2, "amount": 200.0},  # provider 2 total: 200
            {"profile_id": 3, "amount": 1000.0},  # provider 3 total: 1000
        ]
    )
    s = aggregate_payments(df)
    # pandas linear interpolation between 200 and 1000 at q=0.9 → 840.
    assert s["p90_per_provider"] == pytest.approx(840.0, abs=1.0)


def test__aggregate_handles_all_na_amount_column():
    """Regression: a payments slice where ``amount`` is entirely NA
    must not raise (e.g. a payment_class with all-NULL amounts)."""
    df = pd.DataFrame(
        {
            "profile_id": [1, 2],
            "amount": pd.array([pd.NA, pd.NA], dtype="Float64"),
        }
    )
    s = aggregate_payments(df)
    assert s["total_amount"] == 0.0
    assert s["mean_per_payment"] == 0.0


# ---------------------------------------------------------------------------
# summary_overall
# ---------------------------------------------------------------------------


def test__summary_overall_reports_pct_matched_with_payment():
    df = _payments([{"profile_id": 1, "amount": 50.0}])
    # 1 provider has a payment; matched_n=4 → 25%.
    out = summary_overall(df, matched_n=4)
    assert out.iloc[0]["pct_matched_with_payment"] == 25.0
    assert out.iloc[0]["matched_providers"] == 4


def test__summary_overall_zero_matched_handles_division():
    df = _payments([{"profile_id": 1, "amount": 50.0}])
    out = summary_overall(df, matched_n=0)
    assert out.iloc[0]["pct_matched_with_payment"] == 0.0


# ---------------------------------------------------------------------------
# summary_by_year / summary_by_payment_class
# ---------------------------------------------------------------------------


def test__summary_by_year_sorted_ascending():
    df = _payments(
        [
            {"year": 2024, "amount": 100.0},
            {"year": 2023, "amount": 50.0},
            {"year": 2024, "amount": 200.0},
        ]
    )
    out = summary_by_year(df)
    assert out["year"].tolist() == [2023, 2024]
    assert out[out["year"] == 2024]["total_amount"].iloc[0] == 300.0


def test__summary_by_year_handles_missing_column():
    """Defensive: empty / no-year-column input → empty DataFrame, no raise."""
    assert summary_by_year(pd.DataFrame()).empty


def test__summary_by_payment_class_separates_classes():
    df = _payments(
        [
            {"payment_class": "general", "amount": 50.0},
            {"payment_class": "research", "amount": 5000.0},
            {"payment_class": "ownership", "amount": 100000.0},
        ]
    )
    out = summary_by_payment_class(df)
    assert set(out["payment_class"]) == {"general", "research", "ownership"}
    for cls, expected in [
        ("general", 50.0),
        ("research", 5000.0),
        ("ownership", 100000.0),
    ]:
        assert (
            out[out["payment_class"] == cls]["total_amount"].iloc[0] == expected
        )


# ---------------------------------------------------------------------------
# summary_by_payment_type
# ---------------------------------------------------------------------------


def test__summary_by_payment_type_sorts_by_total_amount_desc():
    """Consulting fees should outrank food/beverage on dollars even when
    food/beverage has more rows — dollar magnitude is the signal."""
    df = _payments(
        [
            {"nature": "Food and Beverage", "amount": 20.0},
            {"nature": "Food and Beverage", "amount": 30.0},
            {"nature": "Consulting Fee", "amount": 5000.0},
            {"nature": "Travel and Lodging", "amount": 500.0},
        ]
    )
    out = summary_by_payment_type(df)
    assert out["nature"].tolist() == [
        "Consulting Fee",
        "Travel and Lodging",
        "Food and Beverage",
    ]


def test__summary_by_payment_type_buckets_null_nature_as_unspecified():
    """NaN/None nature rows go into '(unspecified)' so they don't disappear."""
    df = _payments(
        [
            {"nature": "Consulting Fee", "amount": 100.0},
            {"nature": None, "amount": 50.0},
            {"nature": float("nan"), "amount": 25.0},
        ]
    )
    out = summary_by_payment_type(df)
    unspec = out[out["nature"] == "(unspecified)"].iloc[0]
    assert unspec["n_payments"] == 2
    assert unspec["total_amount"] == 75.0


def test__summary_by_payment_type_handles_missing_nature_column():
    """Ownership-only DataFrames have no 'nature' column → empty frame."""
    df = pd.DataFrame({"profile_id": [1], "amount": [100.0]})
    out = summary_by_payment_type(df)
    assert out.empty
    assert "nature" in out.columns


# ---------------------------------------------------------------------------
# top_providers_by_amount / top_payers_by_amount
# ---------------------------------------------------------------------------


def test__top_providers_ranks_by_dollars_and_joins_matched_df():
    df = _payments(
        [
            {"profile_id": 1, "amount": 50.0},
            {"profile_id": 2, "amount": 1000.0},
            {"profile_id": 3, "amount": 200.0},
        ]
    )
    matched = pd.DataFrame(
        [
            {"profile_id": 1, "provider_pk": 10, "confidence_tier": "MEDIUM_HIGH_NAME_PLUS"},
            {"profile_id": 2, "provider_pk": 11, "confidence_tier": "LOW_NAME_ONLY"},
            {"profile_id": 3, "provider_pk": 12, "confidence_tier": "MEDIUM_HIGH_NAME_PLUS"},
        ]
    )
    out = top_providers_by_amount(df, matched, top_n=2)
    assert out["profile_id"].tolist() == [2, 3]
    # Joined column shows up via the default keep_cols.
    assert "confidence_tier" in out.columns


def test__top_providers_keep_cols_filters_to_what_matched_df_carries():
    """Missing columns in matched_df should be silently dropped — the
    helper shouldn't raise just because a study's matched DataFrame
    lacks a particular optional field."""
    df = _payments([{"profile_id": 1, "amount": 100.0}])
    matched = pd.DataFrame([{"profile_id": 1, "provider_pk": 5}])
    out = top_providers_by_amount(
        df,
        matched,
        top_n=10,
        keep_cols=["provider_pk", "nonexistent_column"],
    )
    assert "provider_pk" in out.columns
    assert "nonexistent_column" not in out.columns


def test__top_providers_handles_empty_payments():
    assert top_providers_by_amount(pd.DataFrame(), pd.DataFrame(), top_n=10).empty


def test__top_payers_aggregates_per_entity_sorted_desc():
    df = _payments(
        [
            {"profile_id": 1, "amount": 100.0, "payment_entity": "BigPharma"},
            {"profile_id": 2, "amount": 50.0, "payment_entity": "BigPharma"},
            {"profile_id": 3, "amount": 200.0, "payment_entity": "SmallPharma"},
        ]
    )
    out = top_payers_by_amount(df, top_n=10)
    bp = out[out["payment_entity"] == "BigPharma"].iloc[0]
    assert bp["n_payments"] == 2
    assert bp["n_providers"] == 2
    assert bp["total_amount"] == 150.0
    # SmallPharma comes first (higher total).
    assert out["payment_entity"].tolist()[0] == "SmallPharma"


def test__top_payers_handles_empty_or_missing_column():
    assert top_payers_by_amount(pd.DataFrame()).empty
    # Has amount but no payment_entity column.
    assert top_payers_by_amount(pd.DataFrame({"amount": [1.0]})).empty
