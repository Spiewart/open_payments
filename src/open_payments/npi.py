"""NPI (National Provider Identifier) handling for both CMS Open Payments
data and conflicted-provider input.

Three concerns, separated by stability (same pattern as ``credentials.py``,
``names.py``, ``citystates.py``, ``specialtys.py``):

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  1. Low-level helpers  —  used by both sides                            │
  │     ``parse_npi``, ``is_valid_npi``. Pure functions over strings/ints   │
  │     that emit nullable Int64-compatible values.                         │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  2. CMS / OpenPayments side  —  STABLE, NOT INTENDED TO BE OVERRIDDEN   │
  │     ``NPIMixin``, ``PaymentNPI``, ``PaymentIDsNPIMixin``                │
  │     CMS publishes a single NPI column per payment class:                │
  │       general:   ``Covered_Recipient_NPI``                              │
  │       ownership: ``Physician_NPI``                                      │
  │       research:  ``Covered_Recipient_NPI``  (plus up to 5               │
  │                  ``Principal_Investigator_N_NPI`` columns that are      │
  │                  deferred to Section 5.9 — PI-block research handling). │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  3. Conflicted side  —  GENERIC DEFAULT, EXPECTED TO BE OVERRIDDEN      │
  │     ``ConflictNPI`` with ``NPI_COLUMN`` class var. Default reads a   │
  │     case-sensitive ``npi`` source column. Child apps with different    │
  │     casing (deans uses ``"NPI"``) flip the class var.                   │
  └─────────────────────────────────────────────────────────────────────────┘

ARCHITECTURAL NOTE — application vs. selection:

NPI is a filter, not an identifier short-circuit. ``filter_by_npi`` accumulates
``PaymentFilters.NPI`` in the row's ``filters`` list when the conflicted's
NPI equals the payment's NPI — same shape as ``filter_by_firstname`` /
``filter_by_credential`` / etc. The "NPI in filters → unique match" semantic
that deans wanted is a SELECTION-layer rule and belongs in a MatchSelector
strategy (planned in Section 5.7), not in this module.

This separation is what was missing from deans's NPI handling: there was no
filter-application layer at all, only a side-channel attribute the matcher
ignored.

EMPIRICAL DATA QUALITY (2023 general payments, 14.6M rows):

  - Non-null:      99.70% (14,566,014 / 14,609,233)
  - Null:          0.30%
  - Length:        100% of non-null values are exactly 10 digits
  - Parser misses: 0 across a 50K-row sample of distinct values

Of every match dimension surveyed (credentials, citystate, specialty, name,
name_suffix), NPI is by far the cleanest signal. When a child app has NPIs
on its conflicted side, NPI alone is enough to disambiguate ~99.7% of
candidate rows.
"""

from __future__ import annotations

import re
from typing import ClassVar, Union

import pandas as pd

from .choices import FilterOutcome, PaymentFilters
from .conflicts import Conflicts
from .names import is_blank
from .read import ReadPayments

# ---------------------------------------------------------------------------
# Low-level helpers — used by both CMS and conflicted sides.
# ---------------------------------------------------------------------------


# NPIs are 10-digit numbers (CMS / NPPES specification). They never start with
# 0, but for robustness we just check the digit count and accept the value
# as canonical regardless of leading-digit semantics.
_NPI_DIGITS_RE = re.compile(r"^\d{10}$")


def is_valid_npi(value: object) -> bool:
    """True iff `value` (after normalization) is a 10-digit NPI string."""
    if is_blank(value):
        return False
    # Accept int, str, or numeric-looking value; reject everything else.
    if isinstance(value, (int, float)):
        try:
            as_int = int(value)
        except (ValueError, OverflowError):
            return False
        return _NPI_DIGITS_RE.match(str(as_int)) is not None
    if isinstance(value, str):
        # Strip common decorative characters that occasionally appear in
        # hand-curated NPI columns (hyphens, periods, spaces).
        stripped = re.sub(r"[\s\-.]", "", value)
        return _NPI_DIGITS_RE.match(stripped) is not None
    return False


def parse_npi(value: object) -> Union[int, None]:
    """Parse anything-NPI-shaped into a canonical ``int`` (or ``None``).

    Accepts:
      - ``int`` (1234567890)
      - ``str`` of digits, optionally with hyphens / periods / whitespace
        ("1234567890", "1234-567-890", "  1234567890 ")
      - ``float`` that round-trips through int (Excel often stores NPIs as
        floats like ``1234567890.0``)
      - ``None`` / ``NaN`` / ``pd.NA`` / blank string → returns ``None``

    Returns:
      - ``int`` if the value is a valid 10-digit NPI
      - ``None`` otherwise (including invalid lengths)
    """
    if is_blank(value):
        return None
    if isinstance(value, bool):
        # bool is a subclass of int in Python; reject explicitly.
        return None
    if isinstance(value, (int, float)):
        try:
            as_int = int(value)
        except (ValueError, OverflowError):
            return None
        return as_int if _NPI_DIGITS_RE.match(str(as_int)) else None
    if isinstance(value, str):
        stripped = re.sub(r"[\s\-.]", "", value)
        if _NPI_DIGITS_RE.match(stripped):
            return int(stripped)
        return None
    return None


# ---------------------------------------------------------------------------
# CMS / OpenPayments side  —  STABLE, GENERIC, NOT INTENDED TO BE OVERRIDDEN.
# ---------------------------------------------------------------------------


class NPIMixin:
    """CMS NPI column mappings per payment class."""

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:
        cols = super().general_columns
        cols.update({"Covered_Recipient_NPI": ("npi", "Int64")})
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:
        cols = super().ownership_columns
        cols.update({"Physician_NPI": ("npi", "Int64")})
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:
        cols = super().research_columns
        # TODO(research PI block handling, see TODO.md): research CSVs also
        # have `Principal_Investigator_1..5_NPI` columns (plus full
        # name/credential/specialty blocks for each PI). A conflicted who is
        # a PI on a research payment but not the Covered_Recipient currently
        # goes silently unmatched. Major refactor; depends on selection-layer
        # extraction + vectorization first.
        cols.update({"Covered_Recipient_NPI": ("npi", "Int64")})
        return cols


class PaymentNPI(NPIMixin, ReadPayments):
    """CMS-side NPI reader. Mirrors ``PaymentCredentials`` / ``PaymentSpecialtys``
    / ``PaymentCityStates`` so child apps can read just NPIs via
    ``PaymentNPI().all_payments()`` for ad-hoc analysis."""


class PaymentIDsNPIMixin(NPIMixin):
    """Filters OpenPayments payments by NPI equality.

    This is the FILTER-APPLICATION layer for NPI: when the conflicted's NPI
    equals the payment row's NPI, ``PaymentFilters.NPI`` is added to the row's
    ``filters`` list. The SELECTION-layer interpretation ("NPI in filters →
    unique match for this conflicted") is a MatchSelector strategy concern
    and lives elsewhere.
    """

    @property
    def filters(self) -> list[PaymentFilters]:
        filters: list[PaymentFilters] = super().filters
        filters.append(PaymentFilters.NPI)
        return filters

    @classmethod
    def filter_by_npi(cls, payments_x_conflicted: pd.Series) -> FilterOutcome:
        """Returns:
          - MATCH    — both NPIs non-null and equal
          - DISAGREE — both NPIs non-null and different (strong negative signal:
            different providers)
          - NO_DATA  — either NPI is null (or unparseable)

        NPI is a particularly meaningful DISAGREE: two valid 10-digit NPIs
        that differ identify two different providers with near-certainty.
        """
        payment_npi = payments_x_conflicted.get("npi", None)
        conflict_npi = payments_x_conflicted.get("conflict_npi", None)
        if pd.isna(payment_npi) or pd.isna(conflict_npi):
            return FilterOutcome.NO_DATA
        try:
            return (
                FilterOutcome.MATCH
                if int(payment_npi) == int(conflict_npi)
                else FilterOutcome.DISAGREE
            )
        except (ValueError, TypeError):
            return FilterOutcome.NO_DATA


# ---------------------------------------------------------------------------
# Conflicted side  —  GENERIC DEFAULT, EXPECTED TO BE OVERRIDDEN.
# ---------------------------------------------------------------------------


class ConflictNPI(Conflicts):
    """Default NPI mixin for raw conflicted-provider input.

    DESIGN NOTE: **Conflicted-side** parser. The default reads from a
    ``npi`` (lowercase) source column. Child apps with different conventions
    override ``NPI_COLUMN`` (deans uses ``"NPI"`` uppercase).

    Output column: ``npi`` of nullable ``Int64`` (so missing NPIs survive as
    ``pd.NA`` rather than coercing the whole column to ``float``). When the
    source column is absent, an all-NA ``npi`` column is added so downstream
    merging logic still has a column to reference — the matcher's
    ``filter_by_npi`` then never fires for any row, which is the correct
    behavior for child apps without NPI input.

    Override surface:
      - ``NPI_COLUMN`` — name of the input column (default ``"npi"``).
      - ``get_npi(row)`` — per-row override; default delegates to
        :func:`parse_npi`.
      - ``conflict_npi()`` — full-pipeline override.
    """

    NPI_COLUMN: ClassVar[str] = "npi"

    def conflict_npi(self) -> pd.DataFrame:
        """Add a canonical ``npi`` column of nullable Int64. Drops the source
        column if it has a different name."""
        self.conflicts = self.conflicts.copy()

        if self.NPI_COLUMN not in self.conflicts.columns:
            # Tolerate child apps without NPI input — add an empty column so
            # downstream code can still reference ``npi`` without KeyError.
            self.conflicts["npi"] = pd.array([pd.NA] * len(self.conflicts), dtype="Int64")
            return self.conflicts

        parsed = self.conflicts.apply(self.get_npi, axis=1)
        # Convert object-dtype Series-of-int-or-None into nullable Int64.
        self.conflicts["npi"] = pd.array(parsed.tolist(), dtype="Int64")

        if self.NPI_COLUMN != "npi":
            self.conflicts = self.conflicts.drop(columns=[self.NPI_COLUMN])
        return self.conflicts

    @classmethod
    def get_npi(cls, row: pd.Series) -> Union[int, None]:
        """Per-row override hook. Default reads ``row[NPI_COLUMN]`` and
        delegates to :func:`parse_npi`."""
        if cls.NPI_COLUMN not in row.index:
            return None
        return parse_npi(row[cls.NPI_COLUMN])
