"""Tests for NPI parsing + ConflictNPI mixin + filter_by_npi application."""

from __future__ import annotations

import pandas as pd
import pytest

from ..choices import FilterOutcome, PaymentFilters
from ..npi import (
    ConflictNPI,
    PaymentIDsNPIMixin,
    is_valid_npi,
    parse_npi,
)

# ---------------------------------------------------------------------------
# is_valid_npi / parse_npi (pure helpers)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1234567890", 1234567890),
        (1234567890, 1234567890),
        (1234567890.0, 1234567890),  # Excel-stored-as-float
        ("  1234567890  ", 1234567890),  # whitespace
        ("1234-567-890", 1234567890),  # decorative hyphens
        ("1234.567.890", 1234567890),  # decorative periods
        ("9999999999", 9999999999),
    ],
)
def test__parse_npi_valid_forms(value, expected):
    assert parse_npi(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "   ",
        float("nan"),
        pd.NA,
        "123",  # too short
        "12345678901",  # too long
        "abcdefghij",  # non-numeric
        "12345 67890 12",  # too long after strip
        True,  # bool subclass of int - explicitly rejected
        [1234567890],  # wrong type
        {"npi": 1234567890},
    ],
)
def test__parse_npi_invalid_returns_none(value):
    assert parse_npi(value) is None


def test__is_valid_npi_distinguishes_lengths():
    assert is_valid_npi("1234567890")
    assert not is_valid_npi("123456789")
    assert not is_valid_npi("12345678901")


# ---------------------------------------------------------------------------
# ConflictNPI default mixin
# ---------------------------------------------------------------------------


def test__conflict_npi_parses_and_replaces_column():
    df = pd.DataFrame({"npi": ["1234567890", None, "9876543210"], "other": [1, 2, 3]})
    out = ConflictNPI(df).conflict_npi()
    assert str(out["npi"].dtype) == "Int64"
    assert out.iloc[0]["npi"] == 1234567890
    assert pd.isna(out.iloc[1]["npi"])
    assert out.iloc[2]["npi"] == 9876543210
    assert "other" in out.columns


def test__conflict_npi_coerces_excel_floats():
    # Excel exports NPIs as floats (1234567890.0) — parser must round-trip.
    df = pd.DataFrame({"npi": [1234567890.0, 9876543210.0]})
    out = ConflictNPI(df).conflict_npi()
    assert out.iloc[0]["npi"] == 1234567890
    assert out.iloc[1]["npi"] == 9876543210


def test__conflict_npi_drops_renamed_source_column():
    class _UpperCaseSource(ConflictNPI):
        NPI_COLUMN = "NPI"

    df = pd.DataFrame({"NPI": ["1234567890"], "other": [1]})
    out = _UpperCaseSource(df).conflict_npi()
    assert "NPI" not in out.columns
    assert "npi" in out.columns
    assert out.iloc[0]["npi"] == 1234567890


def test__conflict_npi_handles_missing_source_column():
    # Child app with no NPI input — adds empty Int64 npi column.
    df = pd.DataFrame({"other": [1, 2, 3]})
    out = ConflictNPI(df).conflict_npi()
    assert "npi" in out.columns
    assert str(out["npi"].dtype) == "Int64"
    assert out["npi"].isna().all()


def test__conflict_npi_invalid_values_become_na():
    df = pd.DataFrame({"npi": ["1234567890", "not-an-npi", "123", ""]})
    out = ConflictNPI(df).conflict_npi()
    assert out.iloc[0]["npi"] == 1234567890
    assert pd.isna(out.iloc[1]["npi"])
    assert pd.isna(out.iloc[2]["npi"])
    assert pd.isna(out.iloc[3]["npi"])


# ---------------------------------------------------------------------------
# filter_by_npi (application layer)
# ---------------------------------------------------------------------------


def _row(payment_npi, conflict_npi):
    return pd.Series({"npi": payment_npi, "conflict_npi": conflict_npi, "filters": []})


def test__filter_by_npi_matches_equal_ints():
    assert PaymentIDsNPIMixin.filter_by_npi(_row(1234567890, 1234567890)) == FilterOutcome.MATCH


def test__filter_by_npi_disagree_on_unequal():
    # Two valid 10-digit NPIs that differ → DISAGREE (different providers).
    assert PaymentIDsNPIMixin.filter_by_npi(_row(1234567890, 9876543210)) == FilterOutcome.DISAGREE


def test__filter_by_npi_no_data_on_null_payment():
    assert PaymentIDsNPIMixin.filter_by_npi(_row(pd.NA, 1234567890)) == FilterOutcome.NO_DATA


def test__filter_by_npi_no_data_on_null_conflict():
    assert PaymentIDsNPIMixin.filter_by_npi(_row(1234567890, pd.NA)) == FilterOutcome.NO_DATA


def test__filter_by_npi_no_data_on_both_null():
    # Two missing NPIs are NO_DATA, not a match.
    assert PaymentIDsNPIMixin.filter_by_npi(_row(pd.NA, pd.NA)) == FilterOutcome.NO_DATA


# ---------------------------------------------------------------------------
# PaymentFilters.NPI enum entry (closes bug 3b)
# ---------------------------------------------------------------------------


def test__paymentfilters_npi_exists():
    # Was a dangling reference at helpers.py:213 — now a valid enum entry.
    assert PaymentFilters.NPI is not None
    assert PaymentFilters.NPI.value == "NPI"


def test__paymentidsnpimixin_adds_npi_to_filters_property():
    # Sanity that the mixin's filters property contributes PaymentFilters.NPI.
    class _Stub:
        @property
        def filters(self):
            return []

    class _Probe(PaymentIDsNPIMixin, _Stub):
        pass

    instance = _Probe()
    assert PaymentFilters.NPI in instance.filters
