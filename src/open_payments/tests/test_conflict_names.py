"""Tests for the conflicted-side name parsers and the ConflictNames mixin.

Covers:
  - strip_name_suffixes: comma split, roman numerals, Jr/Sr, Col/Colonel,
    leading-initial trimming, trailing MD/DO/PhD
  - parse_middle_parts: single initial, two initials, multi-period form
    ("A.B."), full middle name with auto-initial, full + initial mix, errors
  - parse_full_name: end-to-end on a wide variety of name shapes including
    "de"/"van"/"von" particles for multi-word last names
  - ConflictNames: applies parse_full_name across a DataFrame, drops the
    source column, handles None gracefully; subclass override example
"""

from __future__ import annotations

import pandas as pd
import pytest

from ..choices import FilterOutcome
from ..names import (
    NAME_SUFFIX_VALID_VALUES,
    ConflictNames,
    ParsedMiddleName,
    ParsedName,
    PaymentIDsNamesMixin,
    extract_name_suffix,
    parse_full_name,
    parse_middle_parts,
    parse_name_suffix,
    strip_name_suffixes,
)

# ---------------------------------------------------------------------------
# strip_name_suffixes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("John Smith", "John Smith"),
        ("John Smith, MD", "John Smith"),
        ("John Smith, MD, FACP", "John Smith"),
        ("John Smith III", "John Smith"),
        ("John Smith IV", "John Smith"),
        ("John Smith Jr", "John Smith"),
        ("John Smith Jr.", "John Smith"),
        ("John Smith Sr.", "John Smith"),
        ("Col. John Smith", "John Smith"),
        ("Colonel John Smith", "John Smith"),
        ("J. Smith Jones", "Smith Jones"),  # leading single-letter trimmed
        ("John Smith MD", "John Smith"),  # trailing MD without comma
        ("John Smith DO", "John Smith"),
        ("  John   Smith  ", "John Smith"),  # whitespace normalized
        ("", ""),
        (None, ""),
    ],
)
def test__strip_name_suffixes(raw, expected):
    assert strip_name_suffixes(raw) == expected


# ---------------------------------------------------------------------------
# parse_middle_parts
# ---------------------------------------------------------------------------


def test__parse_middle_parts_empty():
    result = parse_middle_parts([])
    assert result == ParsedMiddleName()


def test__parse_middle_parts_single_initial():
    result = parse_middle_parts(["Q"])
    assert result.middle_initial_1 == "Q"
    assert result.middle_initial_2 is None
    assert result.middle_name_1 is None


def test__parse_middle_parts_initial_with_period():
    result = parse_middle_parts(["Q."])
    assert result.middle_initial_1 == "Q"


def test__parse_middle_parts_two_initials_separate_tokens():
    result = parse_middle_parts(["Q.", "M."])
    assert result.middle_initial_1 == "Q"
    assert result.middle_initial_2 == "M"


def test__parse_middle_parts_multi_period_form():
    # "A.B." in a single token -> two initials.
    result = parse_middle_parts(["A.B."])
    assert result.middle_initial_1 == "A"
    assert result.middle_initial_2 == "B"


def test__parse_middle_parts_full_middle_name():
    result = parse_middle_parts(["Quincy"])
    assert result.middle_name_1 == "Quincy"
    assert result.middle_initial_1 == "Q"  # initial auto-extracted into same slot


def test__parse_middle_parts_two_full_names():
    result = parse_middle_parts(["Quincy", "Michael"])
    assert result.middle_name_1 == "Quincy"
    assert result.middle_name_2 == "Michael"
    assert result.middle_initial_1 == "Q"
    assert result.middle_initial_2 == "M"


def test__parse_middle_parts_too_many_initials_raises():
    with pytest.raises(ValueError, match="Too many middle initials"):
        parse_middle_parts(["A", "B", "C"])


def test__parse_middle_parts_too_many_names_raises():
    with pytest.raises(ValueError, match="Too many middle names"):
        parse_middle_parts(["Anne", "Beth", "Cara"])


# ---------------------------------------------------------------------------
# parse_full_name
# ---------------------------------------------------------------------------


def test__parse_full_name_basic():
    p = parse_full_name("John Smith")
    assert p.first_name == "John"
    assert p.last_name == "Smith"
    assert p.middle_initial_1 is None
    assert p.middle_name_1 is None


def test__parse_full_name_with_middle_initial():
    p = parse_full_name("John Q. Smith")
    assert (p.first_name, p.middle_initial_1, p.last_name) == ("John", "Q", "Smith")


def test__parse_full_name_with_full_middle_name():
    p = parse_full_name("John Quincy Smith")
    assert p.first_name == "John"
    assert p.middle_name_1 == "Quincy"
    assert p.middle_initial_1 == "Q"
    assert p.last_name == "Smith"


def test__parse_full_name_two_middle_names():
    p = parse_full_name("John Quincy Michael Smith")
    assert p.first_name == "John"
    assert p.middle_name_1 == "Quincy"
    assert p.middle_name_2 == "Michael"
    assert p.last_name == "Smith"


def test__parse_full_name_strips_comma_credentials():
    p = parse_full_name("John Q. Smith, MD, FACP")
    assert (p.first_name, p.middle_initial_1, p.last_name) == ("John", "Q", "Smith")


def test__parse_full_name_strips_roman_numerals():
    p = parse_full_name("John Smith III")
    assert (p.first_name, p.last_name) == ("John", "Smith")


def test__parse_full_name_strips_jr():
    p = parse_full_name("John Smith Jr.")
    assert (p.first_name, p.last_name) == ("John", "Smith")


def test__parse_full_name_strips_trailing_md_without_comma():
    # Edge case from uptodate: MD/DO sometimes appears without a comma.
    p = parse_full_name("John Smith MD")
    assert (p.first_name, p.last_name) == ("John", "Smith")


def test__parse_full_name_strips_col():
    p = parse_full_name("Col. John Smith")
    assert (p.first_name, p.last_name) == ("John", "Smith")


def test__parse_full_name_de_souza():
    # "de" particle => multi-word last name.
    p = parse_full_name("John de Souza")
    assert (p.first_name, p.last_name) == ("John", "de Souza")
    assert p.middle_initial_1 is None


def test__parse_full_name_van_der_berg():
    # Two-particle last name.
    p = parse_full_name("Hans van der Berg")
    assert p.first_name == "Hans"
    assert p.last_name == "van der Berg"


def test__parse_full_name_multi_period_initials():
    p = parse_full_name("John A.B. Smith")
    assert p.first_name == "John"
    assert p.middle_initial_1 == "A"
    assert p.middle_initial_2 == "B"
    assert p.last_name == "Smith"


def test__parse_full_name_blank_raises():
    with pytest.raises(ValueError):
        parse_full_name("")


def test__parse_full_name_single_token_raises():
    with pytest.raises(ValueError):
        parse_full_name("Solo")


# ---------------------------------------------------------------------------
# ConflictNames mixin (default uptodate-shape input)
# ---------------------------------------------------------------------------


def test__conflict_names_default_pipeline():
    df = pd.DataFrame(
        {
            "name": [
                "John Q. Smith, MD",
                "Jane Marie Brown",
                "Hans van der Berg",
                "Alice Smith-Jones III",
            ],
            "other": [1, 2, 3, 4],
        }
    )
    out = ConflictNames(df).conflict_names()

    # Source column dropped
    assert "name" not in out.columns
    # Canonical columns added
    for col in ConflictNames.OUTPUT_COLUMNS:
        assert col in out.columns
    # Other unrelated columns survive
    assert "other" in out.columns

    # Spot-check row content
    row0 = out.iloc[0]
    assert (row0["first_name"], row0["middle_initial_1"], row0["last_name"]) == (
        "John",
        "Q",
        "Smith",
    )
    row1 = out.iloc[1]
    assert row1["middle_name_1"] == "Marie"
    row2 = out.iloc[2]
    assert row2["last_name"] == "van der Berg"


def test__conflict_names_handles_blank_input_gracefully():
    df = pd.DataFrame({"name": ["John Smith", None, ""], "other": [1, 2, 3]})
    out = ConflictNames(df).conflict_names()
    # First row parses normally
    assert out.iloc[0]["first_name"] == "John"
    # Blank rows produce None-filled name columns rather than raising
    for col in ConflictNames.OUTPUT_COLUMNS:
        assert pd.isna(out.iloc[1][col]) or out.iloc[1][col] is None
        assert pd.isna(out.iloc[2][col]) or out.iloc[2][col] is None


def test__parse_one_blank_returns_none_dict():
    result = ConflictNames.parse_one(None)
    assert all(v is None for v in result.values())
    assert set(result.keys()) == set(ConflictNames.OUTPUT_COLUMNS)


def test__parse_one_valid_returns_parsed_dict():
    result = ConflictNames.parse_one("John Q. Smith")
    assert result["first_name"] == "John"
    assert result["middle_initial_1"] == "Q"
    assert result["last_name"] == "Smith"


# ---------------------------------------------------------------------------
# Subclass override — deans-shape input (already-split columns)
# ---------------------------------------------------------------------------


class _DeansLikeConflictNames(ConflictNames):
    """deans dataset has pre-split columns: ``First Name``, ``Last Name``,
    ``Middle Name (or initial)``. Override the whole pipeline because the
    default's single-string parsing doesn't apply.
    """

    def conflict_names(self) -> pd.DataFrame:
        self.conflicts = self.conflicts.copy()
        self.conflicts["first_name"] = self.conflicts["First Name"].str.strip()
        self.conflicts["last_name"] = self.conflicts["Last Name"].str.strip()

        middle_series = self.conflicts["Middle Name (or initial)"].apply(self._parse_middle)
        middle_df = pd.DataFrame(middle_series.tolist(), index=self.conflicts.index)
        for col in (
            "middle_initial_1",
            "middle_initial_2",
            "middle_name_1",
            "middle_name_2",
        ):
            self.conflicts[col] = middle_df[col]

        self.conflicts = self.conflicts.drop(
            columns=["First Name", "Last Name", "Middle Name (or initial)"]
        )
        return self.conflicts

    @staticmethod
    def _parse_middle(value) -> dict:
        if pd.isna(value):
            return dict.fromkeys(
                ("middle_initial_1", "middle_initial_2", "middle_name_1", "middle_name_2"),
                None,
            )
        # Reuse the same pure helper as the default — the only thing that's
        # different is the source column layout, not the middle-name semantics.
        return parse_middle_parts([str(value)]).model_dump()


def test__subclass_override_for_deans_shape():
    df = pd.DataFrame(
        {
            "First Name": ["John", "Jane"],
            "Last Name": ["Smith", "Brown"],
            "Middle Name (or initial)": ["Q", "Marie"],
        }
    )
    out = _DeansLikeConflictNames(df).conflict_names()
    assert "First Name" not in out.columns
    assert (
        out.iloc[0]["first_name"],
        out.iloc[0]["middle_initial_1"],
        out.iloc[0]["last_name"],
    ) == (
        "John",
        "Q",
        "Smith",
    )
    assert out.iloc[1]["middle_name_1"] == "Marie"


# ---------------------------------------------------------------------------
# Pydantic model sanity
# ---------------------------------------------------------------------------


def test__parsed_name_model_serialization():
    p = ParsedName(first_name="John", last_name="Smith", middle_initial_1="Q")
    d = p.model_dump()
    assert d["first_name"] == "John"
    assert d["middle_initial_2"] is None  # default
    assert d["name_suffix"] is None  # default


# ---------------------------------------------------------------------------
# Name suffix: parse_name_suffix, extract_name_suffix, filter_by_name_suffix
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Canonical
        ("JR", "JR"),
        ("Jr", "JR"),
        ("jr", "JR"),
        ("JR.", "JR"),
        ("jr.", "JR"),
        ("Sr.", "SR"),
        ("II", "II"),
        ("iii", "III"),
        ("IV", "IV"),
        ("v", "V"),
        ("  Jr.  ", "JR"),  # whitespace
        # Credential leaks (CMS data quality issues observed at 19.6% of populated rows)
        ("MD", None),
        ("DDS", None),
        ("DO", None),
        ("APRN", None),
        ("PA", None),
        ("M.D.", None),
        ("OD", None),
        # Prefixes
        ("DR.", None),
        ("Dr", None),
        ("MRS.", None),
        # Numeric / garbage
        ("10", None),
        ("I", None),  # too ambiguous with middle initial; excluded per design
        ("", None),
        (None, None),
        ("   ", None),
    ],
)
def test__parse_name_suffix(raw, expected):
    assert parse_name_suffix(raw) == expected


def test__name_suffix_whitelist_membership():
    # Lock the closed set so future expansions are explicit.
    assert NAME_SUFFIX_VALID_VALUES == frozenset({"JR", "SR", "II", "III", "IV", "V"})


@pytest.mark.parametrize(
    "name,expected",
    [
        ("John Smith, Jr.", "JR"),
        ("John Smith III", "III"),
        ("John Smith, Sr.", "SR"),
        ("John Smith IV", "IV"),
        ("John Smith", None),  # no suffix
        ("John Smith, MD", None),  # MD is a credential, not a name suffix
        ("John Smith, MD, Jr.", "JR"),  # finds the legitimate suffix even with creds present
        ("", None),
        (None, None),
    ],
)
def test__extract_name_suffix(name, expected):
    assert extract_name_suffix(name) == expected


def test__parse_full_name_extracts_suffix():
    p = parse_full_name("John Q. Smith, Jr., MD")
    assert p.first_name == "John"
    assert p.middle_initial_1 == "Q"
    assert p.last_name == "Smith"
    assert p.name_suffix == "JR"


def test__parse_full_name_no_suffix():
    p = parse_full_name("John Q. Smith")
    assert p.name_suffix is None


def test__conflict_names_populates_name_suffix_column():
    df = pd.DataFrame(
        {
            "name": ["John Smith Jr.", "Jane Brown", "Hans van der Berg III"],
        }
    )
    out = ConflictNames(df).conflict_names()
    assert "name_suffix" in out.columns
    assert out.iloc[0]["name_suffix"] == "JR"
    assert out.iloc[1]["name_suffix"] is None
    assert out.iloc[2]["name_suffix"] == "III"


# filter_by_name_suffix — hit-only, strict equality after normalization
def _name_suffix_row(payment_suffix, conflict_suffix):
    return pd.Series(
        {"name_suffix": payment_suffix, "conflict_name_suffix": conflict_suffix, "filters": []}
    )


def test__filter_by_name_suffix_strict_equality_hit():
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row("JR", "JR"))
        == FilterOutcome.MATCH
    )
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row("Jr.", "JR"))
        == FilterOutcome.MATCH
    )
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row("jr", "JR."))
        == FilterOutcome.MATCH
    )


def test__filter_by_name_suffix_disagree_on_different_valid_suffixes():
    # Both whitelisted, different values → DISAGREE (negative signal).
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row("JR", "SR"))
        == FilterOutcome.DISAGREE
    )
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row("II", "III"))
        == FilterOutcome.DISAGREE
    )


def test__filter_by_name_suffix_no_data_on_non_whitelisted_tokens():
    # User constraint: strict matching means "j != JC". The whitelist rejects
    # both single-letter tokens, so this is NO_DATA, not DISAGREE.
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row("J", "JC"))
        == FilterOutcome.NO_DATA
    )


def test__filter_by_name_suffix_no_data_when_either_side_missing():
    # Hit-only: missing values produce no signal.
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row(None, "JR"))
        == FilterOutcome.NO_DATA
    )
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row("JR", None))
        == FilterOutcome.NO_DATA
    )
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row(None, None))
        == FilterOutcome.NO_DATA
    )


def test__filter_by_name_suffix_credential_leaks_are_no_data_not_disagree():
    # Two credential-leak values agreeing must NOT produce MATCH (the
    # whitelist rejects them on both sides → NO_DATA). And they shouldn't
    # produce DISAGREE either when the values literally match — the
    # whitelist guard fires before the equality check.
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row("MD", "MD"))
        == FilterOutcome.NO_DATA
    )
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row("DDS", "DDS"))
        == FilterOutcome.NO_DATA
    )
    assert (
        PaymentIDsNamesMixin.filter_by_name_suffix(_name_suffix_row("APRN", "APRN"))
        == FilterOutcome.NO_DATA
    )
