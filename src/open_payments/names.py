"""Name handling for both CMS Open Payments data and conflicted-provider input.

This module is organized into three concerns, separate because they have
different stability properties:

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  1. Low-level comparison helpers  —  used by both sides                 │
  │     `is_blank`, `normalize`, `first_initial`, `split_last_name`,        │
  │     `has_lastname_overlap`, `within_one_edit_substring`,                │
  │     `middle_initial_match`, `middlename_match`                          │
  │     Pure functions that take strings/lists and return primitives.       │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  2. CMS / OpenPayments side  —  STABLE, NOT INTENDED TO BE OVERRIDDEN   │
  │     `NamesMixin`, `PaymentIDsNamesMixin`                                │
  │     CMS publishes names already split into                              │
  │     `Covered_Recipient_First_Name` / `_Last_Name` / `_Middle_Name`      │
  │     (plus `Physician_*_Name` for ownership). The library encodes that   │
  │     layout once; child apps shouldn't need to override.                 │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  3. Conflicted side  —  GENERIC DEFAULT, EXPECTED TO BE OVERRIDDEN      │
  │     `strip_name_suffixes`, `parse_full_name`, `parse_middle_parts`,     │
  │     `ConflictNames`                                                     │
  │     Conflicted input may have a single `name` column (scraped) or       │
  │     separate first/middle/last columns (structured exports). The        │
  │     default targets the single-string scraped shape (uptodate template);│
  │     child apps subclass `ConflictNames` to plug in alternatives.        │
  └─────────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import logging
import re
from typing import ClassVar, Union

import pandas as pd
from pydantic import BaseModel

from .choices import FilterOutcome, PaymentFilters
from .conflicts import Conflicts

logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Low-level helpers — used by both CMS and conflicted sides.
# ---------------------------------------------------------------------------


def is_blank(value: object) -> bool:
    """True if `value` is None, pd.NA/NaN, or an empty/whitespace-only string.

    pd.notna("") returns True, so the matcher's previous guards let empty
    strings through and then crashed on `value[0]` — see Section 5 bug 0c.
    Use this in place of `pd.notna(x)` whenever the next step would index
    or .lower() the value.
    """
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(value, str) and not value.strip():
        return True
    return False


def normalize(name: Union[str, None]) -> Union[str, None]:
    """Lowercase + strip a name string. Returns None for blank/None inputs."""
    if is_blank(name):
        return None
    return name.strip().lower()  # type: ignore[union-attr]


def first_initial(name: Union[str, None]) -> Union[str, None]:
    """Returns the lowercase first letter of `name`, or None for blank input."""
    n = normalize(name)
    return n[0] if n else None


def split_last_name(last_name: str) -> list[str]:
    """Splits a last name on hyphens and whitespace into lowercase components.

    Example: "Smith-Jones" -> ["smith", "jones"], "van der Berg" -> ["van", "der", "berg"]
    """
    if is_blank(last_name):
        return []
    parts = re.split(r"-|\s+", last_name.strip().lower())
    return [p for p in parts if p]


def has_lastname_overlap(conflict_last_name: str, payment_last_name: str) -> bool:
    """True if any token of `conflict_last_name` matches any token of
    `payment_last_name`, ignoring case. Handles hyphenated and space-separated
    multi-word last names.
    """
    if is_blank(conflict_last_name) or is_blank(payment_last_name):
        return False
    conflict_tokens = set(split_last_name(conflict_last_name))
    payment_tokens = set(split_last_name(payment_last_name))
    return bool(conflict_tokens & payment_tokens)


def within_one_edit_substring(
    needle: str,
    haystack: str,
    ignore_case: bool = True,
) -> bool:
    """True iff `needle` (or `needle` after exactly one single-character
    substitution, insertion, or deletion) appears as a substring of `haystack`.

    Replaces the legacy `helpers.str_in_str`, preserving the original
    "1-edit-distance contains" semantics that the partial-firstname matcher
    relies on (catches "Jon" in "Jonathan", "Phil" in "Philip", "Catherine"
    vs "Katherine"), while being readable and short-circuiting on the exact
    substring case.
    """
    if is_blank(needle) or is_blank(haystack):
        return False
    needle_clean = needle.translate({ord(c): None for c in "()[]"})
    if ignore_case:
        needle_clean = needle_clean.lower()
        haystack = haystack.lower()

    # Exact substring is the most common case — try it first.
    if needle_clean in haystack:
        return True

    n = len(needle_clean)
    for i in range(n):
        prefix = re.escape(needle_clean[:i])
        # Substitution: i-th char of needle replaced by any single char.
        if re.search(f"{prefix}.{re.escape(needle_clean[i + 1 :])}", haystack):
            return True
        # Insertion: an extra char inserted at position i of needle.
        if re.search(f"{prefix}.{re.escape(needle_clean[i:])}", haystack):
            return True
        # Deletion: i-th char of needle removed.
        if needle_clean[:i] + needle_clean[i + 1 :] in haystack:
            return True
    return False


def middle_initial_match(
    conflicted_middle_initial_1: Union[str, None],
    conflicted_middle_initial_2: Union[str, None],
    conflicted_middle_name_1: Union[str, None],
    conflicted_middle_name_2: Union[str, None],
    payment_middle_name: Union[str, None],
) -> bool:
    """Returns True iff any of the conflicted provider's middle-name signals
    is consistent with the payment row's middle name, compared at the
    initial-character level.

    Bug fixes from Section 5 bug 0c:
      - Tolerates empty / whitespace-only strings in conflicted_middle_*
        fields (the legacy `pd.notna` guard let "" through and crashed on `[0]`).
      - Compares INITIAL to INITIAL throughout. The legacy logic compared
        `payment_middle_name` (full) to `conflicted_middle_name_1[0]` (one
        char), so "MICHAEL" never matched "MICHAEL" — only the asymmetric
        single-letter case worked.
    """
    pay_initial = first_initial(payment_middle_name)
    if pay_initial is None:
        return False

    conflict_initials = [
        first_initial(conflicted_middle_initial_1),
        first_initial(conflicted_middle_initial_2),
        first_initial(conflicted_middle_name_1),
        first_initial(conflicted_middle_name_2),
    ]
    return pay_initial in {c for c in conflict_initials if c is not None}


def middlename_match(
    conflicted_middle_name_1: Union[str, None],
    conflicted_middle_name_2: Union[str, None],
    payment_middle_name: Union[str, None],
) -> bool:
    """Returns True iff payment's middle name fully matches either of the
    conflicted provider's two middle-name candidates (case-insensitive).
    """
    pay = normalize(payment_middle_name)
    if pay is None:
        return False
    candidates = {normalize(conflicted_middle_name_1), normalize(conflicted_middle_name_2)}
    candidates.discard(None)
    return pay in candidates


# ---------------------------------------------------------------------------
# CMS / OpenPayments side  —  STABLE, GENERIC, NOT INTENDED TO BE OVERRIDDEN.
# CMS already publishes names in three separate columns per payment class;
# the mixins below just register those column shapes and apply the low-level
# match helpers above to each row of the merged payments-x-conflicted frame.
# ---------------------------------------------------------------------------


class NamesMixin:
    @property
    def general_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols = super().general_columns
        cols.update(
            {
                "Covered_Recipient_Last_Name": ("last_name", str),
                "Covered_Recipient_First_Name": ("first_name", str),
                "Covered_Recipient_Middle_Name": ("middle_name", str),
                "Covered_Recipient_Name_Suffix": ("name_suffix", str),
            }
        )
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols = super().ownership_columns
        cols.update(
            {
                "Physician_First_Name": ("first_name", str),
                "Physician_Last_Name": ("last_name", str),
                "Physician_Middle_Name": ("middle_name", str),
                "Physician_Name_Suffix": ("name_suffix", str),
            }
        )
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols = super().research_columns
        cols.update(self.general_columns)
        return cols


class PaymentIDsNamesMixin(NamesMixin):
    """Filters OpenPayments data by first, middle, and last names."""

    @property
    def filters(self) -> list[PaymentFilters]:
        """Adds first, middle, last name, and name-suffix filters."""
        filters: list[PaymentFilters] = super().filters
        filters.append(PaymentFilters.FIRSTNAME)
        filters.append(PaymentFilters.FIRSTNAME_PARTIAL)
        filters.append(PaymentFilters.FIRST_MIDDLE_NAME)
        filters.append(PaymentFilters.MIDDLE_INITIAL)
        filters.append(PaymentFilters.MIDDLENAME)
        filters.append(PaymentFilters.NAME_SUFFIX)
        return filters

    @classmethod
    def merge_by_last_name(
        cls,
        payments: pd.DataFrame,
        conflicted: pd.Series,
    ) -> pd.DataFrame:
        """Merges the payments DataFrame with the conflicted provider
        Series by last name. Returns a DataFrame of payments
        that match the conflicted provider's last name."""

        logging.info(f"Merging Payments df with Conflicted df for {conflicted['last_name']}...")

        merged_payments = payments[
            payments["last_name"].str.lower() == conflicted["last_name"].lower()
        ]

        # If no last name matches are found, some last names contain
        # multiple last names, so we can check if the conflicted last
        # name is in the payments last name
        if merged_payments.empty:
            conflicted_last_names = split_last_name(conflicted["last_name"])

            # Check if any of potentially multiple last names
            # are in the payments last name
            merged_payments = payments[
                payments["last_name"].str.contains(
                    "|".join(conflicted_last_names),
                    na=False,
                    case=False,
                )
            ]
            if not merged_payments.empty:
                # If there are multiple last names, select payments
                # that match all the last names to avoid false positives
                double_matches = merged_payments[
                    merged_payments["last_name"].str.contains(
                        "&".join(conflicted_last_names),
                        na=False,
                        case=False,
                    )
                ]
                if not double_matches.empty:
                    merged_payments = double_matches

        if merged_payments.empty:
            return merged_payments

        conflicted_df = pd.concat(
            [conflicted.drop("last_name")] * len(merged_payments),
            axis=1,
        ).T.set_index(merged_payments.index)

        merged = pd.concat(
            [
                merged_payments,
                conflicted_df,
            ],
            axis=1,
        )

        merged.insert(0, "filters", [[PaymentFilters.LASTNAME]] * len(merged))
        # Section 5.8: parallel negative_filters column populated by
        # filter_by_* methods that return FilterOutcome.DISAGREE.
        merged.insert(1, "negative_filters", [[] for _ in range(len(merged))])

        return merged

    @staticmethod
    def get_firstname_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a DataFrame for first name matches in order of priority:
        1. First name match
        2. First name partial match
        3. First name and middle name match
        """
        refined_matches = payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(lambda x: PaymentFilters.FIRSTNAME in x)
        ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.FIRSTNAME_PARTIAL in x
                )
            ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.FIRST_MIDDLE_NAME in x
                )
            ]

        return refined_matches

    @staticmethod
    def get_middlename_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a DataFrame for middle name matches in order of priority:
        1. Middle name match
        2. Middle initial match
        """
        refined_matches = payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(lambda x: PaymentFilters.MIDDLENAME in x)
        ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.MIDDLE_INITIAL in x
                )
            ]

        return refined_matches

    @classmethod
    def filter_by_firstname(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> FilterOutcome:
        """First-name equality (case-insensitive). Returns:
        - MATCH when both first names are present and equal (also clears
          weaker FIRSTNAME_PARTIAL / FIRST_MIDDLE_NAME labels from filters
          so the row's audit trail records the strongest applicable form).
        - DISAGREE when both are present but unequal.
        - NO_DATA when either side is blank.
        """
        first_name = normalize(payments_x_conflicted["first_name"])
        conflict_first_name = normalize(payments_x_conflicted["conflict_first_name"])
        if first_name is None or conflict_first_name is None:
            return FilterOutcome.NO_DATA
        if first_name == conflict_first_name:
            # Full match supersedes weaker partial / transposition matches.
            if PaymentFilters.FIRSTNAME_PARTIAL in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(PaymentFilters.FIRSTNAME_PARTIAL)
            if PaymentFilters.FIRST_MIDDLE_NAME in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(PaymentFilters.FIRST_MIDDLE_NAME)
            return FilterOutcome.MATCH
        return FilterOutcome.DISAGREE

    @classmethod
    def filter_by_firstname_partial(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> FilterOutcome:
        """1-edit-distance partial first-name match. Returns:
        - NO_DATA when either side is blank OR FIRSTNAME already matched
          (the stronger signal supersedes this one).
        - MATCH when ``first_name`` ↔ ``conflict_first_name`` are within
          one edit (uses :func:`within_one_edit_substring`).
        - DISAGREE when both are present, FIRSTNAME didn't match, AND
          they're not within one edit either.
        """
        first_name = payments_x_conflicted["first_name"]
        conflict_first_name = payments_x_conflicted["conflict_first_name"]
        if is_blank(first_name) or is_blank(conflict_first_name):
            return FilterOutcome.NO_DATA
        if PaymentFilters.FIRSTNAME in payments_x_conflicted["filters"]:
            return FilterOutcome.NO_DATA  # superseded
        if within_one_edit_substring(first_name, conflict_first_name) or within_one_edit_substring(
            conflict_first_name, first_name
        ):
            # Partial match supersedes transposition.
            if PaymentFilters.FIRST_MIDDLE_NAME in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(PaymentFilters.FIRST_MIDDLE_NAME)
            return FilterOutcome.MATCH
        return FilterOutcome.DISAGREE

    @classmethod
    def filter_by_first_middle_name(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> FilterOutcome:
        """Transposed-name check (conflict first ↔ payment middle, or vice
        versa). Returns:
          - NO_DATA when FIRSTNAME / FIRSTNAME_PARTIAL already fired
            (transposition only matters if the direct match failed) OR when
            insufficient data on either side.
          - MATCH when any of the transposition cases agree.
          - DISAGREE when all required name fields are present AND none of
            the transposition cases match. Note: this filter is more
            tentative than firstname/middlename — a DISAGREE here just means
            "this provider's first name isn't the conflict's middle name (or
            vice versa)," which is weak negative evidence.
        """
        if (
            PaymentFilters.FIRSTNAME in payments_x_conflicted["filters"]
            or PaymentFilters.FIRSTNAME_PARTIAL in payments_x_conflicted["filters"]
        ):
            return FilterOutcome.NO_DATA  # superseded

        payment_first = normalize(payments_x_conflicted["first_name"])
        payment_middle = normalize(payments_x_conflicted["middle_name"])
        conflict_first = normalize(payments_x_conflicted["conflict_first_name"])
        conflict_middle_1 = normalize(payments_x_conflicted["conflict_middle_name_1"])
        conflict_middle_2 = normalize(payments_x_conflicted["conflict_middle_name_2"])

        # Need conflict_first plus at least one middle-side candidate to fire.
        if conflict_first is None and conflict_middle_1 is None and conflict_middle_2 is None:
            return FilterOutcome.NO_DATA
        if payment_first is None and payment_middle is None:
            return FilterOutcome.NO_DATA

        matched = (
            (payment_middle is not None and payment_middle == conflict_first)
            or (payment_first is not None and payment_first == conflict_middle_1)
            or (payment_first is not None and payment_first == conflict_middle_2)
        )
        return FilterOutcome.MATCH if matched else FilterOutcome.DISAGREE

    @classmethod
    def filter_by_middle_initial(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> FilterOutcome:
        """Middle-initial match. Returns:
        - MATCH when payment's middle initial equals any of the conflicted's
          middle initials (or first initial of middle_name_1/2).
        - DISAGREE when payment has a middle initial AND conflicted has at
          least one middle initial AND none match.
        - NO_DATA when either side lacks middle-name signal entirely.
        """
        pay_initial = first_initial(payments_x_conflicted["middle_name"])
        if pay_initial is None:
            return FilterOutcome.NO_DATA

        conflict_initials = {
            first_initial(payments_x_conflicted["conflict_middle_initial_1"]),
            first_initial(payments_x_conflicted["conflict_middle_initial_2"]),
            first_initial(payments_x_conflicted["conflict_middle_name_1"]),
            first_initial(payments_x_conflicted["conflict_middle_name_2"]),
        }
        conflict_initials.discard(None)
        if not conflict_initials:
            return FilterOutcome.NO_DATA
        return FilterOutcome.MATCH if pay_initial in conflict_initials else FilterOutcome.DISAGREE

    # `middle_initial_match` is intentionally exposed on the class as a static
    # method too — there are external callers (tests, child apps) that import
    # `PaymentIDsNamesMixin.middle_initial_match` directly.
    middle_initial_match = staticmethod(middle_initial_match)

    @classmethod
    def filter_by_middlename(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> FilterOutcome:
        """Full middle-name match. Returns:
        - MATCH when payment middle equals either conflict middle_name_1
          or _2 (case-insensitive).
        - DISAGREE when payment middle is present AND at least one conflict
          middle name is present AND none match.
        - NO_DATA when either side lacks a non-blank middle name entirely.
        """
        pay = normalize(payments_x_conflicted["middle_name"])
        if pay is None:
            return FilterOutcome.NO_DATA
        candidates = {
            normalize(payments_x_conflicted["conflict_middle_name_1"]),
            normalize(payments_x_conflicted["conflict_middle_name_2"]),
        }
        candidates.discard(None)
        if not candidates:
            return FilterOutcome.NO_DATA
        return FilterOutcome.MATCH if pay in candidates else FilterOutcome.DISAGREE

    middlename_match = staticmethod(middlename_match)

    @classmethod
    def filter_by_name_suffix(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> FilterOutcome:
        """**Hit-only filter** (CMS suffix col is junky; see
        :data:`NAME_SUFFIX_VALID_VALUES`). Returns:
          - MATCH when both sides parse to a whitelisted suffix AND they
            match strictly post-normalization.
          - DISAGREE when both parse to whitelisted suffixes that differ
            (e.g. one is JR, other is SR — strong negative signal).
          - NO_DATA when either side fails the whitelist (credential leaks
            like "MD" / "DR." land here, never producing junk-vs-junk matches).
        """
        payment_suffix = parse_name_suffix(payments_x_conflicted.get("name_suffix"))
        conflict_suffix = parse_name_suffix(payments_x_conflicted.get("conflict_name_suffix"))
        if payment_suffix is None or conflict_suffix is None:
            return FilterOutcome.NO_DATA
        return FilterOutcome.MATCH if payment_suffix == conflict_suffix else FilterOutcome.DISAGREE


# ---------------------------------------------------------------------------
# Conflicted side  —  GENERIC DEFAULT, EXPECTED TO BE OVERRIDDEN.
# Conflicted input commonly has a single ``name`` column containing scraped
# free-text like ``"John Q. Smith, MD, FACP"``. The pure helpers below parse
# such strings into canonical first/last/middle parts. The default mixin
# wires them into a DataFrame; child apps subclass when their input shape
# differs (e.g. structured First-Name / Last-Name columns -- see test for
# example).
# ---------------------------------------------------------------------------


# Roman-numeral lineage suffix pattern (II, III, IV, etc.). Borrowed from
# abim_conflicts (more correct than uptodate's [(II)(III)(IV)] bracket-class).
_ROMAN_SUFFIX_RE = re.compile(r"\b[IVXLCDM]+(?=\s|,|$)")


# Canonical name-suffix values, post-normalization (uppercase, period-stripped).
# Intentionally narrow: only lineage suffixes that are unambiguously suffixes.
# "I" is excluded because it's indistinguishable from a middle initial. The
# CMS Name_Suffix column empirically contains many credential leaks ("MD",
# "DDS", "APRN") and prefixes ("DR.", "MRS.") — those parse to None.
NAME_SUFFIX_VALID_VALUES: frozenset[str] = frozenset({"JR", "SR", "II", "III", "IV", "V"})


def parse_name_suffix(value: Union[str, None]) -> Union[str, None]:
    """**Both-side helper.** Normalize a name-suffix string to its canonical
    whitelisted form, or return None.

    Normalization:
      - Strip whitespace, uppercase, remove periods
      - Result must be in :data:`NAME_SUFFIX_VALID_VALUES`

    Examples:
      ``"JR"`` / ``"Jr"`` / ``"JR."`` / ``"jr."`` -> ``"JR"``
      ``"III"`` / ``"iii"`` -> ``"III"``
      ``"MD"`` / ``"DDS"`` / ``"APRN"`` (credential leaks) -> ``None``
      ``"DR."`` / ``"MRS."`` (prefixes) -> ``None``
      ``""`` / ``None`` / ``NaN`` -> ``None``

    The whitelist is what makes the downstream ``filter_by_name_suffix``
    safe against the junk-vs-junk false positives that real CMS data would
    otherwise produce (1% of CMS rows have suffix; many are not suffixes).
    """
    if is_blank(value):
        return None
    normalized = value.strip().upper().replace(".", "")
    return normalized if normalized in NAME_SUFFIX_VALID_VALUES else None


# TODO(NAME_SUFFIX_EXTRACTED piping, see TODO.md):
# Add a companion `extract_suffix_from_payment_name_columns(first, middle, last)`
# helper that scans CMS *payment*-side name columns for misfiled suffixes
# (Last_Name="LEYBA JR" → suffix "JR"). Survey: ~1,587 clean leaks in
# Last_Name in 14.6M-row 2023 general payments. Exclude "V" from the
# middle/first whitelist — 95% of "V" hits there are middle initials, not
# Roman-V. Pair with new PaymentFilters.NAME_SUFFIX_EXTRACTED.


def extract_name_suffix(name: Union[str, None]) -> Union[str, None]:
    """**Conflicted-side helper.** Find a name-suffix token inside a name
    string and return its canonical form (or None).

    Tokenizes on whitespace + commas; the first token that parses via
    :func:`parse_name_suffix` wins. Returns None when no suffix is found.

    Examples:
      ``"John Smith, Jr."`` -> ``"JR"``
      ``"John Smith III"`` -> ``"III"``
      ``"John Smith"`` -> ``None``
    """
    if is_blank(name):
        return None
    for token in re.split(r"[\s,]+", name.strip()):
        s = parse_name_suffix(token)
        if s is not None:
            return s
    return None


# Particles signaling a multi-word last name: "de Souza", "van der Berg", etc.
_LASTNAME_PARTICLES: frozenset[str] = frozenset(
    {"de", "del", "della", "der", "di", "du", "la", "le", "van", "von"}
)


class ParsedMiddleName(BaseModel):
    """The four-slot middle-name representation. Two initials and two full
    names, each optional. Matches the canonical conflicted-input shape used
    by the matcher (``middle_initial_1`` / ``_2`` / ``middle_name_1`` / ``_2``).
    """

    middle_initial_1: Union[str, None] = None
    middle_initial_2: Union[str, None] = None
    middle_name_1: Union[str, None] = None
    middle_name_2: Union[str, None] = None


class ParsedName(ParsedMiddleName):
    """Result of parsing a raw conflicted-provider name string."""

    first_name: str
    last_name: str
    name_suffix: Union[str, None] = None


def strip_name_suffixes(name: Union[str, None]) -> str:
    """**Conflicted-side helper.** Strip credentials, honorifics, and other
    non-name decorations from a raw scraped name string.

    Removes (case-insensitive where relevant):
      - Anything after the first comma (typically credentials/titles like
        ``", MD, FACP"``)
      - Roman-numeral lineage suffixes (``III``, ``IV``, ...)
      - ``Jr.`` / ``Sr.`` (with or without period)
      - ``Col.`` / ``Colonel`` prefixes
      - Leading single-letter prefix (e.g. ``J. `` in ``"J. Smith"``)
      - Trailing ``MD`` / ``DO`` / ``PhD`` that survived the comma split

    Returns the cleaned string. Empty or blank input returns ``""``.
    """
    if is_blank(name):
        return ""
    cleaned = name.split(",")[0]
    cleaned = _ROMAN_SUFFIX_RE.sub("", cleaned)
    cleaned = re.sub(r"\b(?:Jr|Sr)\b\.?", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bColonel\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.replace("Col.", "")
    cleaned = re.sub(r"^\w\.?\s+", "", cleaned)
    cleaned = re.sub(r"\s+(?:MD|DO|PhD)\b", "", cleaned, flags=re.IGNORECASE)
    return " ".join(cleaned.split())


def _split_first_middle_last(tokens: list[str]) -> tuple[str, list[str], str]:
    """Split tokens into (first_name, middle_tokens, last_name).

    Walks for a multi-word-last-name particle (``de``, ``van``, etc.); if
    found, everything from the particle onward joins as a single last name.
    Otherwise the last token is the last name and middle tokens fill the gap.
    """
    if len(tokens) < 2:
        raise ValueError(f"Need at least 2 tokens to parse first + last name; got {tokens!r}")

    first = tokens[0]
    rest = tokens[1:]

    for i, token in enumerate(rest):
        if token.lower() in _LASTNAME_PARTICLES:
            return first, rest[:i], " ".join(rest[i:])

    if len(rest) == 1:
        return first, [], rest[0]
    return first, rest[:-1], rest[-1]


def parse_middle_parts(middle_tokens: list[str]) -> ParsedMiddleName:
    """**Conflicted-side helper.** Parse a list of middle-name tokens into
    the canonical two-initial + two-name shape.

    Each token is interpreted as:
      - Single letter with optional trailing period (``"Q"`` / ``"Q."``)
        -> middle_initial
      - Multi-letter dotted form (``"A.B."`` or ``"A.B"``)
        -> split into successive initials
      - Anything else (``"Quincy"``) -> middle_name (with initial extracted
        into the SAME slot, mirroring uptodate's intent: a single middle
        name fills slot 1 with both `middle_name_1` and `middle_initial_1`).

    Raises ``ValueError`` if there are too many tokens to fit in the four slots.
    """
    initial_1 = initial_2 = name_1 = name_2 = None

    def _place_initial(ini: str) -> None:
        nonlocal initial_1, initial_2
        if initial_1 is None:
            initial_1 = ini
        elif initial_2 is None:
            initial_2 = ini
        else:
            raise ValueError(f"Too many middle initials in: {middle_tokens!r}")

    for token in middle_tokens:
        token = token.strip()
        if not token:
            continue
        if re.fullmatch(r"\w\.?", token):
            _place_initial(token.rstrip("."))
        elif re.fullmatch(r"(\w\.)+\w?\.?", token):
            for ch in token:
                if ch.isalpha():
                    _place_initial(ch)
        else:
            cleaned = re.sub(r"[^\w\s]", "", token)
            if not cleaned:
                continue
            if name_1 is None and initial_1 is None:
                name_1 = cleaned
                initial_1 = cleaned[0]
            elif name_2 is None and initial_2 is None:
                name_2 = cleaned
                initial_2 = cleaned[0]
            else:
                raise ValueError(f"Too many middle names in: {middle_tokens!r}")

    return ParsedMiddleName(
        middle_initial_1=initial_1,
        middle_initial_2=initial_2,
        middle_name_1=name_1,
        middle_name_2=name_2,
    )


def parse_full_name(name: Union[str, None]) -> ParsedName:
    """**Conflicted-side helper.** Parse a scraped name string into canonical
    first/last/middle parts.

    Pipeline:
      1. ``strip_name_suffixes`` to drop credentials, honorifics, lineage suffixes.
      2. Split into whitespace tokens; require >= 2.
      3. ``_split_first_middle_last`` to identify multi-word last names (``de Souza``).
      4. ``parse_middle_parts`` for any middle-position tokens.

    Examples:
      ``"John Smith"`` -> first=John, last=Smith, no middle
      ``"John Q. Smith"`` -> first=John, middle_initial_1=Q, last=Smith
      ``"John Quincy Smith"`` -> first=John, middle_name_1=Quincy, middle_initial_1=Q, last=Smith
      ``"John Smith, MD"`` -> first=John, last=Smith (MD stripped)
      ``"John Smith III"`` -> first=John, last=Smith (III stripped)
      ``"John de Souza"`` -> first=John, last="de Souza"
      ``"J. Smith"`` -> first=Smith (uptodate convention: leading single-letter is title-y noise)

    Raises ``ValueError`` for blank input or single-token names.
    """
    # Extract the suffix from the ORIGINAL name BEFORE stripping decorators —
    # strip_name_suffixes removes Jr/Sr/roman numerals, so we'd lose the signal.
    suffix = extract_name_suffix(name)

    stripped = strip_name_suffixes(name)
    if not stripped:
        raise ValueError(f"Empty name after stripping suffixes: {name!r}")

    tokens = stripped.split()
    first, middle_tokens, last = _split_first_middle_last(tokens)
    middle = parse_middle_parts(middle_tokens)
    return ParsedName(first_name=first, last_name=last, name_suffix=suffix, **middle.model_dump())


class ConflictNames(Conflicts):
    """Default names mixin for raw conflicted-provider input.

    DESIGN NOTE: This is the **conflicted-side** parser. Like
    ``ConflictCredentials``, it is generic only by virtue of covering the
    most common input shape (a single ``name`` column of relatively
    well-formed scraped or human-curated names).
    **Child apps are expected to subclass this class** when their raw input
    shape differs. Three observed shapes from real wrappers:

      - **Human-curated** (uptodate, deans): names like ``"John Q. Smith, MD"``
        are well-formed enough that the default ``parse_full_name`` handles
        them. deans uses three pre-split columns instead and overrides
        ``conflict_names()`` entirely (see test for example).
      - **Scraped HTML** (abim): names extracted from ``<h2>`` tags include
        arbitrary credentials, honorifics, and lineage suffixes interleaved
        with the name (e.g. ``"Darth Vader, Imperial Galactic Lord, FACP,
        DLoS, BS, CSP, the III"``). Child app overrides ``parse_one`` with
        a richer parser (abim's ``NameCredentialParser``) before falling
        back to the pure helpers in this module.

    Override surface:
      - Set ``NAME_COLUMN`` class var to point at a different source column.
      - Override ``parse_one(name)`` to change per-row parsing logic.
      - Override ``conflict_names()`` to replace the whole pipeline (e.g.
        when the input has pre-split name columns).

    Default output columns (added to the DataFrame):
      ``first_name``, ``last_name``, ``middle_initial_1``, ``middle_initial_2``,
      ``middle_name_1``, ``middle_name_2``. The source ``NAME_COLUMN`` is
      dropped after parsing.
    """

    NAME_COLUMN: ClassVar[str] = "name"

    OUTPUT_COLUMNS: ClassVar[tuple[str, ...]] = (
        "first_name",
        "last_name",
        "middle_initial_1",
        "middle_initial_2",
        "middle_name_1",
        "middle_name_2",
        "name_suffix",
    )

    def conflict_names(self) -> pd.DataFrame:
        """Apply ``parse_one`` to every row's name column, append the 6
        canonical name columns, and drop the source column."""

        self.conflicts = self.conflicts.copy()
        parsed = self.conflicts[self.NAME_COLUMN].apply(self.parse_one)
        parsed_df = pd.DataFrame(parsed.tolist(), index=self.conflicts.index)

        for col in self.OUTPUT_COLUMNS:
            self.conflicts[col] = parsed_df[col]

        self.conflicts = self.conflicts.drop(columns=[self.NAME_COLUMN])
        return self.conflicts

    @staticmethod
    def parse_one(name: Union[str, None]) -> dict:
        """Parse a single name string into the canonical 6-key dict. Returns
        a None-filled dict for blank input (rather than raising) so a row
        with a missing name doesn't kill the whole DataFrame apply."""
        if is_blank(name):
            return dict.fromkeys(ConflictNames.OUTPUT_COLUMNS, None)
        return parse_full_name(name).model_dump()
