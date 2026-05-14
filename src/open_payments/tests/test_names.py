"""Pure-function tests for name normalization + matching helpers.

These tests target the module-level functions in `open_payments.names`, not
the DataFrame-aware mixin classes. They cover the cases the matcher pipeline
hits in practice — hyphenated last names, empty/None/NaN middles, comma-vs-
hyphen separators, case insensitivity.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ..names import (
    first_initial,
    has_lastname_overlap,
    is_blank,
    middle_initial_match,
    middlename_match,
    normalize,
    split_last_name,
    within_one_edit_substring,
)

# ---------------------------------------------------------------------------
# is_blank
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [None, "", "   ", "\t", np.nan, pd.NA, float("nan")],
)
def test__is_blank_true(value):
    assert is_blank(value)


@pytest.mark.parametrize(
    "value",
    ["a", " A ", "John", "M", "0"],
)
def test__is_blank_false(value):
    assert not is_blank(value)


# ---------------------------------------------------------------------------
# normalize + first_initial
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("John", "john"),
        ("  Jane  ", "jane"),
        ("SMITH-JONES", "smith-jones"),
        ("", None),
        (None, None),
        ("   ", None),
    ],
)
def test__normalize(raw, expected):
    assert normalize(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("John", "j"),
        ("M", "m"),
        ("", None),
        (None, None),
        (np.nan, None),
    ],
)
def test__first_initial(raw, expected):
    assert first_initial(raw) == expected


# ---------------------------------------------------------------------------
# split_last_name + has_lastname_overlap
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Smith", ["smith"]),
        ("Smith-Jones", ["smith", "jones"]),
        ("van der Berg", ["van", "der", "berg"]),
        ("O'Brien", ["o'brien"]),
        ("", []),
        ("   ", []),
    ],
)
def test__split_last_name(raw, expected):
    assert split_last_name(raw) == expected


def test__has_lastname_overlap_exact():
    assert has_lastname_overlap("Smith", "Smith")
    assert has_lastname_overlap("smith", "SMITH")


def test__has_lastname_overlap_hyphenated_payment_side():
    # Payment row has "Smith-Jones"; conflicted just has "Jones".
    assert has_lastname_overlap("Jones", "Smith-Jones")


def test__has_lastname_overlap_hyphenated_conflicted_side():
    assert has_lastname_overlap("Smith-Jones", "Smith")


def test__has_lastname_overlap_no_match():
    assert not has_lastname_overlap("Smith", "Adams")
    assert not has_lastname_overlap("Smith-Jones", "Adams-Wilson")


def test__has_lastname_overlap_blank_inputs():
    assert not has_lastname_overlap(None, "Smith")  # type: ignore[arg-type]
    assert not has_lastname_overlap("Smith", "")
    assert not has_lastname_overlap("", "")


# ---------------------------------------------------------------------------
# within_one_edit_substring
# ---------------------------------------------------------------------------


def test__exact_substring():
    assert within_one_edit_substring("Jon", "Jonathan")
    assert within_one_edit_substring("PHIL", "philip")  # case-insensitive default


def test__single_substitution():
    # "Catherine" vs "Katherine" - one char differs
    assert within_one_edit_substring("Catherine", "Katherine")


def test__single_insertion():
    # "Jon" -> "John" requires inserting 'h' (1 insertion)
    assert within_one_edit_substring("Jon", "John")


def test__strips_parens_and_brackets_from_needle():
    assert within_one_edit_substring("(Bob)", "Bobby")
    assert within_one_edit_substring("[Sam]", "Samuel")


def test__rejects_too_different():
    assert not within_one_edit_substring("Adams", "Wilson")


def test__blank_inputs():
    assert not within_one_edit_substring("", "Anything")
    assert not within_one_edit_substring("Something", "")
    assert not within_one_edit_substring(None, "Anything")  # type: ignore[arg-type]


def test__case_sensitive_when_requested():
    assert not within_one_edit_substring("PHIL", "philip", ignore_case=False)


# ---------------------------------------------------------------------------
# middle_initial_match  — regression tests for Section 5 bug 0c
# ---------------------------------------------------------------------------


def test__middle_initial_match_initial_to_initial():
    # Conflicted has initial "M", payment has full name "Michael"
    assert middle_initial_match(
        conflicted_middle_initial_1="M",
        conflicted_middle_initial_2=None,
        conflicted_middle_name_1=None,
        conflicted_middle_name_2=None,
        payment_middle_name="Michael",
    )


def test__middle_initial_match_full_name_to_full_name():
    # Bug 0c regression: legacy compared payment full "MICHAEL" to
    # conflicted_middle_name_1[0] = "M" (asymmetric), so this returned False.
    # The fixed version compares INITIALS on both sides -> True.
    assert middle_initial_match(
        conflicted_middle_initial_1=None,
        conflicted_middle_initial_2=None,
        conflicted_middle_name_1="Michael",
        conflicted_middle_name_2=None,
        payment_middle_name="Michael",
    )


def test__middle_initial_match_empty_string_does_not_crash():
    # Bug 0c regression: pd.notna("") is True, so the legacy guard let ""
    # through and crashed on `value[0]`. The fixed is_blank() rejects "".
    assert not middle_initial_match(
        conflicted_middle_initial_1="",
        conflicted_middle_initial_2="",
        conflicted_middle_name_1="",
        conflicted_middle_name_2="",
        payment_middle_name="Michael",
    )


def test__middle_initial_match_no_match():
    assert not middle_initial_match(
        conflicted_middle_initial_1="A",
        conflicted_middle_initial_2=None,
        conflicted_middle_name_1=None,
        conflicted_middle_name_2=None,
        payment_middle_name="Michael",
    )


def test__middle_initial_match_blank_payment_middle():
    assert not middle_initial_match(
        conflicted_middle_initial_1="M",
        conflicted_middle_initial_2=None,
        conflicted_middle_name_1=None,
        conflicted_middle_name_2=None,
        payment_middle_name=None,
    )


def test__middle_initial_match_second_initial_matches():
    assert middle_initial_match(
        conflicted_middle_initial_1="J",
        conflicted_middle_initial_2="M",
        conflicted_middle_name_1=None,
        conflicted_middle_name_2=None,
        payment_middle_name="Michael",
    )


# ---------------------------------------------------------------------------
# middlename_match
# ---------------------------------------------------------------------------


def test__middlename_match_exact():
    assert middlename_match(
        conflicted_middle_name_1="Michael",
        conflicted_middle_name_2=None,
        payment_middle_name="MICHAEL",
    )


def test__middlename_match_second_slot():
    assert middlename_match(
        conflicted_middle_name_1="Joseph",
        conflicted_middle_name_2="Michael",
        payment_middle_name="michael",
    )


def test__middlename_match_blank_payment():
    assert not middlename_match(
        conflicted_middle_name_1="Michael",
        conflicted_middle_name_2=None,
        payment_middle_name=None,
    )


def test__middlename_match_partial_does_not_count():
    # "Mike" should not match "Michael" — middlename is exact-only.
    assert not middlename_match(
        conflicted_middle_name_1="Mike",
        conflicted_middle_name_2=None,
        payment_middle_name="Michael",
    )
