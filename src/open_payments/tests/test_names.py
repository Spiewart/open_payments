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


# ---------------------------------------------------------------------------
# one_edit_regex_alts + merge_by_last_name fuzzy fallback
# ---------------------------------------------------------------------------


import re as _re

import pandas as _pd

from open_payments.choices import PaymentFilters
from open_payments.names import one_edit_regex_alts, PaymentIDsNamesMixin


class TestOneEditRegexAlts:
    def test_exact_match_always_included(self):
        regex = one_edit_regex_alts("smith")
        assert _re.fullmatch(regex, "smith") is not None

    def test_single_substitution_matches(self):
        # "smith" ↔ "smyth" (i → y)
        regex = one_edit_regex_alts("smith")
        assert _re.fullmatch(regex, "smyth") is not None

    def test_single_insertion_matches(self):
        # "philips" ↔ "phillips" (insert l)
        regex = one_edit_regex_alts("philips")
        assert _re.fullmatch(regex, "phillips") is not None

    def test_single_deletion_matches(self):
        # "ohara" ↔ "o'hara" (delete the apostrophe — though here we test
        # the simpler "muller" vs "mueller" case)
        regex = one_edit_regex_alts("mueller")
        assert _re.fullmatch(regex, "muller") is not None

    def test_apostrophe_deletion_for_ohara(self):
        # Direct case from the abim audit: "o'hara" (with apostrophe) vs
        # "ohara" (without). Deletion edit.
        regex = one_edit_regex_alts("o'hara")
        assert _re.fullmatch(regex, "ohara") is not None

    def test_two_edits_rejected(self):
        # "smith" vs "smyhh" requires 2 substitutions — should NOT match.
        regex = one_edit_regex_alts("smith")
        assert _re.fullmatch(regex, "smyhh") is None

    def test_completely_different_name_rejected(self):
        regex = one_edit_regex_alts("smith")
        assert _re.fullmatch(regex, "jones") is None

    def test_case_handled_when_caller_passes_case_insensitive(self):
        # The regex itself is lowercase; callers pair it with case=False.
        regex = one_edit_regex_alts("smith")
        assert _re.fullmatch(regex, "SMYTH", flags=_re.IGNORECASE) is not None


class TestMergeByLastNamePartial:
    """End-to-end: merge_by_last_name's third fallback finds 1-edit matches
    when exact + token-overlap come up empty. Hits from this path are
    tagged with the ``LASTNAME_PARTIAL`` filter (mirroring
    ``FIRSTNAME_PARTIAL`` on the firstname side)."""

    def _payments(self, last_names: list[str]) -> _pd.DataFrame:
        return _pd.DataFrame({"last_name": last_names})

    def test_fuzzy_apostrophe_drop(self):
        # ABIM has "O'Hara"; CMS row has the apostrophe-stripped form.
        payments = self._payments(["OHARA", "SMITH", "JONES"])
        conflicted = _pd.Series({"last_name": "O'Hara"})
        merged = PaymentIDsNamesMixin.merge_by_last_name(payments, conflicted)
        assert len(merged) == 1
        assert merged.iloc[0]["last_name"] == "OHARA"

    def test_fuzzy_single_substitution(self):
        # Smyth vs Smith (single substitution).
        payments = self._payments(["SMYTH", "JONES"])
        conflicted = _pd.Series({"last_name": "Smith"})
        merged = PaymentIDsNamesMixin.merge_by_last_name(payments, conflicted)
        assert len(merged) == 1
        assert merged.iloc[0]["last_name"] == "SMYTH"

    def test_fuzzy_falls_through_to_empty_for_unrelated_names(self):
        # No fuzzy candidates either → returns empty (NOLASTNAME outcome).
        payments = self._payments(["JONES", "WILLIAMS"])
        conflicted = _pd.Series({"last_name": "Smith"})
        merged = PaymentIDsNamesMixin.merge_by_last_name(payments, conflicted)
        assert merged.empty

    def test_exact_match_short_circuits_fuzzy(self):
        # When the exact match works, fuzzy fallback isn't invoked.
        payments = self._payments(["SMITH", "SMYTH"])
        conflicted = _pd.Series({"last_name": "Smith"})
        merged = PaymentIDsNamesMixin.merge_by_last_name(payments, conflicted)
        # Exact match returns just SMITH (not the fuzzy SMYTH match).
        assert set(merged["last_name"]) == {"SMITH"}

    # --- LASTNAME vs LASTNAME_PARTIAL tagging ---------------------------
    # The SELECTION layer demotes partial hits; the filter tag is the signal
    # that drives that demotion. These tests pin the tagging contract.

    def test_partial_path_tags_filters_lastname_partial(self):
        # Partial hit → row's `filters` column carries LASTNAME_PARTIAL only.
        # Critically NOT LASTNAME — the SELECTION-layer tier predicates
        # check `LASTNAME in filters` to land a row in the exact-lastname
        # tiers, so accidentally co-tagging would un-demote the partial row.
        payments = self._payments(["SMYTH"])
        conflicted = _pd.Series({"last_name": "Smith"})
        merged = PaymentIDsNamesMixin.merge_by_last_name(payments, conflicted)
        assert merged.iloc[0]["filters"] == [PaymentFilters.LASTNAME_PARTIAL]
        assert PaymentFilters.LASTNAME not in merged.iloc[0]["filters"]

    def test_exact_path_tags_filters_lastname(self):
        # Exact hit → unchanged behavior: LASTNAME tag.
        payments = self._payments(["SMITH"])
        conflicted = _pd.Series({"last_name": "Smith"})
        merged = PaymentIDsNamesMixin.merge_by_last_name(payments, conflicted)
        assert merged.iloc[0]["filters"] == [PaymentFilters.LASTNAME]

    def test_token_overlap_path_tags_filters_lastname(self):
        # Token-overlap (compound surname) path is treated as exact-equivalent:
        # tag stays LASTNAME because the conflicted token literally appears
        # in the payments last_name (just bundled with another token).
        payments = self._payments(["SMITH JONES"])
        conflicted = _pd.Series({"last_name": "Smith"})
        merged = PaymentIDsNamesMixin.merge_by_last_name(payments, conflicted)
        assert merged.iloc[0]["filters"] == [PaymentFilters.LASTNAME]
