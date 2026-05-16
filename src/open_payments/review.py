"""Parameterized review-workbook generator for CMS profile_id matches.

Auto-matchers (e.g. :class:`open_payments.payments.PaymentsSearch` with a
:class:`open_payments.selectors.TieredConfidenceSelector`) produce candidate
matches between source-side providers and CMS OpenPayments profile_ids.
NPI-based matches are essentially certain — NPI is unique. Name-cascade
matches are candidates that warrant human review.

This module formalizes the review cycle in a study-neutral way:

  1. :func:`generate_review_template` — write a reviewer-facing Excel file
     with NPI_matches, non_NPI_matches, unmatched, collisions, and LEGEND
     sheets. Reviewer columns are blank, ready to fill.

  2. Reviewer marks each row's outcome (TRUE / FALSE + corrected_profile_id /
     blank) in their preferred tool (Excel, mostly).

  3. :func:`apply_review` — read the filled template, produce a
     ``<source>_ids_final.xlsx`` with the reviewer's decisions baked in,
     plus a refetch list of new profile_ids the review introduced.

  4. :func:`refetch_payments_for_pids` — fetch CMS payments for any new
     profile_ids and reconcile the cached payments file to the canonical
     post-review pid set.

Study-specific things (entity name, source-side column labels, extra
legend rows) are passed in via :class:`ReviewConfig`. Studies typically
expose thin wrappers that supply their own config. JDA-style migrations
(converting prior manual-review files into the new template format) are
also study-specific and stay downstream.

Color coding (from :mod:`open_payments.excel`):
  - SOURCE (light blue): source-side info (read-only for the reviewer)
  - CMS (light yellow): the matched OpenPayments record (read-only)
  - EVIDENCE (gray): matcher filters, suspicion scoring (read-only)
  - REVIEWER (light green): reviewer action zone (fill these in)
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from .excel import (
    COLOR_CMS,
    COLOR_EVIDENCE,
    COLOR_REVIEWER,
    COLOR_SOURCE,
    ColumnSpec,
    apply_data_validation,
    apply_hyperlinks,
    apply_legend_formatting,
    apply_section_styling,
    paint_warning_cell,
)
from .suspicion import (
    WARNING_TIERS,
    classify_non_npi_match_suspicion,
    classify_npi_match_suspicion,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Config dataclasses
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceField:
    """Description of one source-side field shown in the review workbook.

    The ``matched_column`` is read from the matched-providers DataFrame
    (the output of ``PaymentsSearch``, which prefixes source columns with
    ``conflict_``). The ``conflicts_column`` is the corresponding column
    name in the raw source spreadsheet (e.g. ``conflicts.xlsx``), used as
    a fallback for the unmatched sheet where the matched-df doesn't carry
    full source context.

    Attributes:
        display_label: Header text shown in Excel (e.g. ``"Dean: Last Name"``).
        matched_column: Column in the matched DataFrame.
        conflicts_column: Column in the raw conflicts/source DataFrame
            (for unmatched-sheet population). ``None`` if not needed.
        width: Excel column width.
        extractor: Optional callable to transform the raw value
            (e.g. pulling a state out of a citystates blob).
    """

    display_label: str
    matched_column: str
    conflicts_column: Optional[str] = None
    width: int = 18
    extractor: Optional[Callable[[Any], Any]] = None


@dataclass
class ReviewConfig:
    """Study-specific configuration for the review-workbook generator.

    Bundles the few bits that vary by study (entity name, source-side
    field labels, optional legend extras). The module-level functions
    take a ``ReviewConfig`` and produce a fully-formatted workbook.

    Attributes:
        entity_name: Singular noun for the source-side person
            (``"Dean"``, ``"Researcher"``, ...). Used in display labels
            and in the LEGEND.
        source_fields: Ordered tuple of :class:`SourceField`. The order
            determines the column order in the workbook.
        legend_extras: Optional study-specific legend rows. Each entry
            is ``(section_name, [(field, explanation), ...])``; the
            generic LEGEND content gets these appended at the end.
    """

    entity_name: str = "Provider"
    source_fields: Sequence[SourceField] = field(default_factory=tuple)
    legend_extras: Sequence[tuple[str, list[tuple[str, str]]]] = field(default_factory=tuple)

    @property
    def specialty_display_col(self) -> str:
        """Conventional display label for the source specialty column.

        Used as the ``source_specialty_col`` kwarg when calling the
        suspicion classifiers. Studies that label their specialty
        column differently can override this by setting a
        ``source_fields`` entry with display_label="<entity>: Specialty".
        """
        return f"{self.entity_name}: Specialty"


# --------------------------------------------------------------------------
# Universal CMS / Evidence / Reviewer column layouts
# These don't depend on the study because CMS-side columns are always
# the OpenPayments schema columns, and the evidence/reviewer columns
# are fixed across the framework.
# --------------------------------------------------------------------------


def _cms_columns() -> list[ColumnSpec]:
    return [
        ("CMS: profile_id", COLOR_CMS, 12),
        ("CMS: Last Name", COLOR_CMS, 18),
        ("CMS: First Name", COLOR_CMS, 16),
        ("CMS: Middle Name", COLOR_CMS, 14),
        ("CMS: Specialty", COLOR_CMS, 30),
        ("CMS: State", COLOR_CMS, 8),
        ("CMS: NPI", COLOR_CMS, 14),
    ]


def _evidence_columns() -> list[ColumnSpec]:
    return [
        ("confidence_tier", COLOR_EVIDENCE, 22),
        ("num_filters", COLOR_EVIDENCE, 10),
        ("positive_filters", COLOR_EVIDENCE, 60),
        ("n_negative_filters", COLOR_EVIDENCE, 10),
        ("negative_filters", COLOR_EVIDENCE, 60),
        # Each match sheet uses one of these (NPI sheet → npi_match_suspicion,
        # non-NPI sheet → non_npi_match_suspicion). Both kept in the spec so
        # styling applies regardless of which is populated on the sheet.
        ("npi_match_suspicion", COLOR_EVIDENCE, 32),
        ("non_npi_match_suspicion", COLOR_EVIDENCE, 32),
    ]


def _reviewer_columns_match() -> list[ColumnSpec]:
    return [
        ("match_correct", COLOR_REVIEWER, 14),
        ("corrected_profile_id", COLOR_REVIEWER, 14),
        ("corrected_last_name", COLOR_REVIEWER, 18),
        ("corrected_first_name", COLOR_REVIEWER, 16),
        ("corrected_npi", COLOR_REVIEWER, 14),
        ("notes", COLOR_REVIEWER, 40),
    ]


def _reviewer_columns_unmatched() -> list[ColumnSpec]:
    return [
        ("found_profile_id", COLOR_REVIEWER, 14),
        ("found_last_name", COLOR_REVIEWER, 18),
        ("found_first_name", COLOR_REVIEWER, 16),
        ("found_npi", COLOR_REVIEWER, 14),
        ("notes", COLOR_REVIEWER, 40),
    ]


def _reviewer_columns_collision() -> list[ColumnSpec]:
    return [
        ("pair_correct", COLOR_REVIEWER, 14),
        ("corrected_profile_id", COLOR_REVIEWER, 14),
        ("notes", COLOR_REVIEWER, 40),
    ]


# --------------------------------------------------------------------------
# Per-config column spec builders
# --------------------------------------------------------------------------


def _source_column_specs(config: ReviewConfig) -> list[ColumnSpec]:
    """Build (header, color, width) tuples for the entity columns."""
    return [(f.display_label, COLOR_SOURCE, f.width) for f in config.source_fields]


def _provider_pk_column(config: ReviewConfig) -> ColumnSpec:
    return ("provider_pk", COLOR_SOURCE, 12)


def _match_sheet_specs(config: ReviewConfig) -> list[ColumnSpec]:
    """Column order for NPI_matches / non_NPI_matches sheets."""
    return (
        [_provider_pk_column(config)]
        + _source_column_specs(config)
        + _cms_columns()
        + _evidence_columns()
        + _reviewer_columns_match()
    )


def _unmatched_sheet_specs(config: ReviewConfig) -> list[ColumnSpec]:
    return (
        [_provider_pk_column(config)] + _source_column_specs(config) + _reviewer_columns_unmatched()
    )


def _collisions_sheet_specs(config: ReviewConfig) -> list[ColumnSpec]:
    """Slim per-row layout for the collisions sheet."""
    src = config.source_fields
    # Pull a small set of identifying entity columns: last/first/school/
    # specialty/npi if present. Studies without a "school" column just
    # skip that entry; matches by display label.
    chosen = []
    for label_suffix in ("Last Name", "First Name", "School", "Specialty", "NPI"):
        wanted = f"{config.entity_name}: {label_suffix}"
        for f_ in src:
            if f_.display_label == wanted:
                chosen.append((f_.display_label, COLOR_SOURCE, f_.width))
                break
    return (
        [
            ("profile_id", COLOR_CMS, 12),
            ("match_count_for_this_pid", COLOR_CMS, 8),
            _provider_pk_column(config),
        ]
        + chosen
        + [
            ("CMS: Last Name", COLOR_CMS, 18),
            ("CMS: First Name", COLOR_CMS, 16),
            ("CMS: Specialty", COLOR_CMS, 30),
            ("CMS: NPI", COLOR_CMS, 14),
            ("confidence_tier", COLOR_EVIDENCE, 22),
            ("positive_filters", COLOR_EVIDENCE, 60),
        ]
        + _reviewer_columns_collision()
    )


# --------------------------------------------------------------------------
# Cell-value helpers
# --------------------------------------------------------------------------


def _filters_repr_to_str(filters_repr) -> str:
    """Convert persisted filter list-repr into a clean comma-separated string.

    Input examples:
      ``"[<PaymentFilters.LASTNAME: 'LASTNAME'>, <PaymentFilters.NPI: 'NPI'>]"``
      ``"[]"``, ``None``, ``NaN``, in-memory list of enums.
    """
    if filters_repr is None:
        return ""
    try:
        if pd.isna(filters_repr):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(filters_repr, (list, tuple, set)):
        out = []
        for v in filters_repr:
            out.append(getattr(v, "value", str(v)))
        return ", ".join(out)
    return ", ".join(re.findall(r"'([A-Z_]+)'", str(filters_repr)))


def _has_npi_in_filters(filters_repr) -> bool:
    """Return True if the persisted filter list contains the NPI filter."""
    if filters_repr is None:
        return False
    try:
        if pd.isna(filters_repr):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(filters_repr, (list, tuple, set)):
        for v in filters_repr:
            if getattr(v, "value", str(v)) == "NPI":
                return True
        return False
    return "'NPI'" in str(filters_repr)


def _cms_profile_id_url(pid) -> Optional[str]:
    """Return the OpenPayments URL for a CMS profile_id, or None."""
    if pid is None:
        return None
    try:
        if pd.isna(pid):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return f"https://openpaymentsdata.cms.gov/physician/{int(pid)}"
    except (ValueError, TypeError):
        return None


def _safe_int(v):
    """Coerce a cell value to int or None (NaN-safe)."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _date_only(v):
    """Strip time component from a datetime cell value."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return pd.to_datetime(v).date()
    except Exception:
        return v


def _normalize_outcome_to_string(value) -> Optional[str]:
    """Normalize a TRUE/FALSE outcome value to the literal string ``"TRUE"``/``"FALSE"``.

    Excel's list-validation does string comparison. pandas converts string
    "TRUE"/"FALSE" cells into booleans on round-trip, and openpyxl writes
    bool values as numeric 1/0 — which don't match the ``"TRUE,FALSE"``
    validation list and cause Excel to prompt for recovery on file open.
    Forcing the literal string before writing avoids that prompt.

    Accepts bool, int (1/0), str (any case of TRUE/FALSE/T/F/YES/NO/1/0),
    None / NaN. Anything unrecognized → None.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        if value == 1:
            return "TRUE"
        if value == 0:
            return "FALSE"
        return None
    s = str(value).strip().upper()
    if s in ("TRUE", "T", "YES", "Y", "1"):
        return "TRUE"
    if s in ("FALSE", "F", "NO", "N", "0"):
        return "FALSE"
    return None


def _coerce_review_outcome(value) -> Optional[bool]:
    """Coerce a TRUE/FALSE/blank cell value to bool/None (for apply_review)."""
    s = _normalize_outcome_to_string(value)
    if s == "TRUE":
        return True
    if s == "FALSE":
        return False
    return None


# --------------------------------------------------------------------------
# Sheet builders
# --------------------------------------------------------------------------


def _read_source_field(row, field_: SourceField, *, source: str = "matched") -> Any:
    """Pull a value from a row using the SourceField's column + extractor.

    Handles the three column-name conventions in the matcher output and
    the raw source spreadsheet:

      - ``"matched"`` — the matched sheet of the matcher's output. Source
        columns are prefixed with ``conflict_`` (e.g.
        ``conflict_last_name``); read :attr:`SourceField.matched_column`
        directly.
      - ``"unmatched"`` — the unmatched sheet of the matcher's output.
        Source columns are unprefixed (no CMS pairing means no prefix);
        strip ``conflict_`` off ``matched_column`` and read that.
      - ``"conflicts"`` — the raw source spreadsheet (e.g.
        ``conflicts.xlsx``); uses :attr:`SourceField.conflicts_column`.

    Returns the SourceField's extractor applied to the cell value (or the
    raw value if no extractor); null/NaN → ``None``.
    """
    if source == "matched":
        col = field_.matched_column
    elif source == "unmatched":
        col = field_.matched_column
        if col.startswith("conflict_"):
            col = col[len("conflict_") :]
    elif source == "conflicts":
        col = field_.conflicts_column
    else:
        col = None

    if not col:
        return None
    val = row.get(col) if hasattr(row, "get") else None
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if field_.extractor is not None:
        try:
            val = field_.extractor(val)
        except Exception:
            pass
    return val


def _build_match_rows(
    matched_df: pd.DataFrame,
    config: ReviewConfig,
    *,
    npi_matches: bool,
) -> pd.DataFrame:
    """Build the per-row content for either NPI_matches or non_NPI_matches.

    Args:
        matched_df: ``conflicteds_ids`` sheet from the auto-matcher.
        config: study config.
        npi_matches: True to filter to NPI-bearing matches, False for
            the complement.

    Returns a DataFrame whose columns follow the sheet's spec
    (source fields → CMS → evidence → reviewer).
    """
    df = matched_df[matched_df["profile_id"].notna()].copy()
    has_npi = df["filters"].apply(_has_npi_in_filters)
    df = df[has_npi] if npi_matches else df[~has_npi]

    rows = []
    for _, r in df.iterrows():
        row: dict[str, Any] = {"provider_pk": _safe_int(r.get("provider_pk"))}
        for f_ in config.source_fields:
            row[f_.display_label] = _read_source_field(r, f_, source="matched")
        row.update(
            {
                "CMS: profile_id": _safe_int(r.get("profile_id")),
                "CMS: Last Name": r.get("last_name"),
                "CMS: First Name": r.get("first_name"),
                "CMS: Middle Name": r.get("middle_name"),
                "CMS: Specialty": _extract_first_specialty(r.get("specialtys")),
                "CMS: State": _extract_first_state(r.get("citystates")),
                "CMS: NPI": _safe_int(r.get("npi")),
                "confidence_tier": r.get("confidence_tier"),
                "num_filters": _safe_int(r.get("num_filters")),
                "positive_filters": _filters_repr_to_str(r.get("filters")),
                "n_negative_filters": _safe_int(r.get("n_negative_filters")),
                "negative_filters": _filters_repr_to_str(r.get("negative_filters")),
                "npi_match_suspicion": None,
                "non_npi_match_suspicion": None,
                "match_correct": None,
                "corrected_profile_id": None,
                "corrected_last_name": None,
                "corrected_first_name": None,
                "corrected_npi": None,
                "notes": None,
            }
        )
        rows.append(row)

    cols = [c[0] for c in _match_sheet_specs(config)]
    out = pd.DataFrame(rows, columns=cols)
    if out.empty:
        return out

    # Use the positive_filters / negative_filters STRING form for suspicion
    # classification (the strings round-trip cleanly through Excel; the
    # classifier's filter-set parser handles both forms).
    if npi_matches:
        out["npi_match_suspicion"] = out.apply(
            lambda r: classify_npi_match_suspicion(
                r,
                source_specialty_col=config.specialty_display_col,
            ),
            axis=1,
        )
    else:
        out["non_npi_match_suspicion"] = out.apply(
            lambda r: classify_non_npi_match_suspicion(
                r,
                source_specialty_col=config.specialty_display_col,
            ),
            axis=1,
        )
    return out


def _build_unmatched_rows(
    unmatched_df: pd.DataFrame,
    conflicts_df: pd.DataFrame,
    config: ReviewConfig,
) -> pd.DataFrame:
    """Build the unmatched sheet: source rows the matcher couldn't place.

    Pulls term/identifying info from conflicts_df (via conflicts_column on
    each SourceField) since the matcher's unmatched sheet may not carry
    full source context.
    """
    cols = [c[0] for c in _unmatched_sheet_specs(config)]
    if unmatched_df.empty:
        return pd.DataFrame(columns=cols)

    conflicts_by_pk = (
        conflicts_df.set_index("provider_pk")
        if "provider_pk" in conflicts_df.columns
        else pd.DataFrame()
    )

    rows = []
    for _, r in unmatched_df.iterrows():
        pk = _safe_int(r.get("provider_pk"))
        if pk is not None and not conflicts_by_pk.empty and pk in conflicts_by_pk.index:
            c_row = conflicts_by_pk.loc[pk]
            if isinstance(c_row, pd.DataFrame):
                c_row = c_row.iloc[0]
        else:
            c_row = pd.Series(dtype=object)

        row: dict[str, Any] = {"provider_pk": pk}
        for f_ in config.source_fields:
            # Unmatched-sheet rows carry source columns unprefixed (no
            # CMS pairing means no "conflict_" prefix). Try that first,
            # fall back to the raw conflicts/source spreadsheet.
            val = _read_source_field(r, f_, source="unmatched")
            if val is None or (isinstance(val, float) and pd.isna(val)):
                val = _read_source_field(c_row, f_, source="conflicts")
            row[f_.display_label] = val
        row.update(
            {
                "found_profile_id": None,
                "found_last_name": None,
                "found_first_name": None,
                "found_npi": None,
                "notes": None,
            }
        )
        rows.append(row)

    return pd.DataFrame(rows, columns=cols)


def _build_collision_rows(
    matched_df: pd.DataFrame,
    config: ReviewConfig,
) -> pd.DataFrame:
    """Build the collisions sheet: one row per (pk, pid) for any colliding pid."""
    valid = matched_df[matched_df["profile_id"].notna()].copy()
    valid["profile_id"] = pd.to_numeric(valid["profile_id"], errors="coerce")
    valid = valid.dropna(subset=["profile_id"])
    counts = valid.groupby("profile_id").size()
    colliding = counts[counts > 1].index

    # Which source labels we include on this sheet (matches _collisions_sheet_specs)
    desired_labels = {
        f"{config.entity_name}: {s}"
        for s in ("Last Name", "First Name", "School", "Specialty", "NPI")
    }
    relevant_fields = [f_ for f_ in config.source_fields if f_.display_label in desired_labels]

    rows = []
    for pid in colliding:
        subset = valid[valid["profile_id"] == pid]
        for _, r in subset.iterrows():
            row: dict[str, Any] = {
                "profile_id": _safe_int(pid),
                "match_count_for_this_pid": int(len(subset)),
                "provider_pk": _safe_int(r.get("provider_pk")),
            }
            for f_ in relevant_fields:
                row[f_.display_label] = _read_source_field(r, f_, source="matched")
            row.update(
                {
                    "CMS: Last Name": r.get("last_name"),
                    "CMS: First Name": r.get("first_name"),
                    "CMS: Specialty": _extract_first_specialty(r.get("specialtys")),
                    "CMS: NPI": _safe_int(r.get("npi")),
                    "confidence_tier": r.get("confidence_tier"),
                    "positive_filters": _filters_repr_to_str(r.get("filters")),
                    "pair_correct": None,
                    "corrected_profile_id": None,
                    "notes": None,
                }
            )
            rows.append(row)

    cols = [c[0] for c in _collisions_sheet_specs(config)]
    return pd.DataFrame(rows, columns=cols)


# --------------------------------------------------------------------------
# Helpers for CMS-side fields whose persisted form is a list-repr
# --------------------------------------------------------------------------


def _extract_first_specialty(specialtys_repr) -> Optional[str]:
    """Pull a human-readable specialty from a Specialtys list-repr string."""
    if specialtys_repr is None:
        return None
    try:
        if pd.isna(specialtys_repr):
            return None
    except (TypeError, ValueError):
        pass
    s = str(specialtys_repr)
    m = re.search(r"specialty=['\"]([^'\"]+)['\"]", s)
    if not m:
        return s if len(s) < 80 else None
    spec = m.group(1).strip()
    sub_m = re.search(r"subspecialty=['\"]([^'\"]+)['\"]", s)
    if sub_m and sub_m.group(1) and sub_m.group(1).lower() != "none":
        return f"{spec} / {sub_m.group(1).strip()}"
    return spec


def _extract_first_state(citystates_repr) -> Optional[str]:
    """Pull a state code from a CityStates list-repr string."""
    if citystates_repr is None:
        return None
    try:
        if pd.isna(citystates_repr):
            return None
    except (TypeError, ValueError):
        pass
    s = str(citystates_repr)
    m = re.search(r"state=['\"]([A-Za-z]{2,})['\"]", s)
    if m:
        return m.group(1).upper()
    m2 = re.search(r"\|([A-Za-z]{2,})\]", s)
    if m2:
        return m2.group(1).upper()
    return None


# --------------------------------------------------------------------------
# LEGEND content (generic + config extras)
# --------------------------------------------------------------------------


def _build_legend_rows(config: ReviewConfig) -> pd.DataFrame:
    """Per-sheet documentation and reviewer instructions, templated to
    the study's entity name. Study-specific extras are appended at the
    end as additional sections.
    """
    rows: list[tuple[str, str]] = []

    def section(name: str) -> None:
        rows.append(("", ""))
        rows.append((f"=== {name} ===", ""))

    def row(field_: str, explanation: str) -> None:
        rows.append((field_, explanation))

    entity = config.entity_name
    entity_l = entity.lower()

    section("HOW TO USE THIS WORKBOOK")
    row(
        "Purpose",
        f"Manual review of CMS profile_id matches. The algorithm makes a "
        f"best-guess match for each {entity_l}; your job is to verify each "
        "one and correct mistakes.",
    )
    row(
        "Color coding",
        f"Blue columns: {entity_l} info from our source spreadsheet (read-only). "
        "Yellow columns: the CMS profile_id the algorithm picked, with that "
        "person's CMS-side info (read-only). "
        "Gray columns: the algorithm's evidence (which filters matched/disagreed). "
        "Green columns: YOUR input — please fill in.",
    )
    row(
        "Reviewer columns on match sheets (NPI_matches, non_NPI_matches)",
        f"match_correct: TRUE if the algorithm's CMS pick is the same person "
        f"as the {entity_l}. FALSE if it's the wrong person. "
        "If FALSE: fill corrected_profile_id with the correct CMS profile_id "
        "(look it up on openpaymentsdata.cms.gov; the CMS: profile_id column "
        "has a clickable link to the algorithm's pick for reference). "
        "If FALSE and you cannot find a correct CMS profile, leave "
        f"corrected_profile_id blank — this records that the {entity_l} has "
        "no CMS profile.",
    )
    row(
        "Reviewer columns on unmatched sheet",
        f"These are {entity_l}s the algorithm couldn't match. Search "
        f"openpaymentsdata.cms.gov for the {entity_l}. "
        "If found: fill found_profile_id with the CMS profile_id, plus "
        "optionally the CMS-side first/last/NPI for reference. "
        f"If not found: leave blank — records that the {entity_l} has no "
        "CMS profile.",
    )
    row(
        "Reviewer columns on collisions sheet",
        f"Two or more {entity_l}s were both matched to the same CMS "
        f"profile_id. For each ({entity_l}, CMS profile_id) pair: "
        f"pair_correct=TRUE if THIS {entity_l} is the right one for the "
        "CMS profile_id, FALSE otherwise. "
        f"Multiple pairs can be TRUE if the same {entity_l} appears in "
        "multiple source rows (career move — both rows refer to the same "
        "person and the same CMS profile_id). "
        "If FALSE: optionally fill corrected_profile_id if you know the "
        f"right CMS profile_id for this {entity_l} (otherwise they go back "
        "to the unmatched pool).",
    )
    row(
        "Skipping",
        "If you don't know an answer, leave match_correct blank — the row "
        "will be flagged as 'unreviewed' and the algorithm's pick will be "
        "kept by default. You can come back to it later.",
    )

    section("SHEET INVENTORY")
    row(
        "NPI_matches",
        "Algorithm matches that used NPI. NPI is unique so these are almost "
        "always correct — quick skim, mark TRUE for the obvious ones. "
        "PAY ATTENTION to the npi_match_suspicion column: rows tagged "
        "A_firstname_disagreed or B_specialty_AND_state_disagree have an NPI "
        "match BUT the matched CMS person disagrees with the source on "
        "attributes we'd expect to match — these are candidates where the "
        f"{entity_l}'s NPI in our source spreadsheet was likely entered "
        "incorrectly. "
        "C_specialty_disagrees is mild (CMS sometimes categorizes the same "
        "person under a broader specialty). "
        f"D_state_disagrees is almost always a {entity_l} career move, not a "
        "wrong NPI. E_normal is the typical clean case.",
    )
    row(
        "non_NPI_matches",
        "Algorithm matches that did NOT use NPI (matched by name, location, "
        "specialty, etc.). Main review work — these can be wrong, especially "
        "for common names ('Chris Anderson MD problem'). "
        "Check the non_npi_match_suspicion column: A_lastname_only_match "
        "flags cases where the matcher couldn't get firstname agreement "
        "(high false-positive risk); B/C flag specialty / state "
        "disagreements; D is a likely career move; E is a typical clean "
        "match. Cells in the A/B/C tiers are highlighted in warning red to "
        "draw attention.",
    )
    row(
        "unmatched",
        f"{entity}s the algorithm couldn't match at all. Manual search "
        "needed; fill found_profile_id if you locate them on CMS.",
    )
    row(
        "collisions",
        f"CMS profile_ids matched by more than one {entity_l}. Either "
        f"legitimate (same person across multiple source rows — keep both "
        "rows TRUE) or a false match (different people sharing a name).",
    )

    section("LOOKING UP CMS PROFILE IDS")
    row(
        "OpenPayments site",
        "https://openpaymentsdata.cms.gov — search by name and city/state. "
        "Profile pages show specialty, NPI, all payments. "
        "Each row's CMS: profile_id column is a clickable link to the "
        "algorithm's pick.",
    )

    # Study-specific extras
    for section_name, section_rows in config.legend_extras:
        section(section_name)
        for field_, explanation in section_rows:
            row(field_, explanation)

    return pd.DataFrame(rows, columns=["field", "explanation"])


# --------------------------------------------------------------------------
# Excel formatting wrappers
# --------------------------------------------------------------------------


def _apply_match_sheet_formatting(ws, config: ReviewConfig) -> None:
    """Section colors + dropdown + CMS hyperlink + suspicion warning cells."""
    apply_section_styling(ws, _match_sheet_specs(config))
    apply_data_validation(ws, "match_correct", ("TRUE", "FALSE"))
    apply_hyperlinks(ws, "CMS: profile_id", _cms_profile_id_url)
    _paint_suspicion_cells(ws)


def _apply_unmatched_sheet_formatting(ws, config: ReviewConfig) -> None:
    apply_section_styling(ws, _unmatched_sheet_specs(config))


def _apply_collisions_sheet_formatting(ws, config: ReviewConfig) -> None:
    apply_section_styling(ws, _collisions_sheet_specs(config))
    apply_data_validation(ws, "pair_correct", ("TRUE", "FALSE"))
    apply_hyperlinks(ws, "profile_id", _cms_profile_id_url)


def _paint_suspicion_cells(ws) -> None:
    """Paint A/B/C-tier suspicion cells warning-red (cell-level only)."""
    header_to_idx: dict[str, int] = {}
    for col_idx in range(1, ws.max_column + 1):
        h = ws.cell(row=1, column=col_idx).value
        if h is not None:
            header_to_idx[str(h)] = col_idx

    targets = [c for c in ("npi_match_suspicion", "non_npi_match_suspicion") if c in header_to_idx]
    if not targets:
        return
    for col_name in targets:
        col_idx = header_to_idx[col_name]
        for row_idx in range(2, ws.max_row + 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            if cell.value in WARNING_TIERS:
                paint_warning_cell(cell)


# --------------------------------------------------------------------------
# Main entrypoints
# --------------------------------------------------------------------------


def generate_review_template(
    matched_path: str,
    conflicts_path: str,
    output_path: str,
    config: ReviewConfig,
    *,
    existing_review_path: Optional[str] = None,
    matched_sheet_name: str = "conflicteds_ids",
    unmatched_sheet_name: str = "unmatched",
) -> None:
    """Generate a reviewer-facing Excel workbook from the auto-matcher's output.

    Args:
        matched_path: ``conflicteds_ids.xlsx`` (auto-matcher output).
        conflicts_path: raw source spreadsheet (e.g. ``conflicts.xlsx``).
        output_path: path to write the review template.
        config: study-specific :class:`ReviewConfig`.
        existing_review_path: optional path to a prior review file whose
            reviewer-filled columns should be preserved (merged on
            ``provider_pk``).
        matched_sheet_name / unmatched_sheet_name: sheet names in
            ``matched_path``. Defaults follow the PaymentsSearch convention.
    """
    logger.info(f"Reading inputs: {matched_path}, {conflicts_path}")
    matched_df = pd.read_excel(matched_path, sheet_name=matched_sheet_name)
    try:
        unmatched_df = pd.read_excel(matched_path, sheet_name=unmatched_sheet_name)
    except (ValueError, KeyError):
        unmatched_df = pd.DataFrame()
    conflicts_df = pd.read_excel(conflicts_path)

    logger.info("Building NPI_matches sheet...")
    npi_df = _build_match_rows(matched_df, config, npi_matches=True)
    logger.info("Building non_NPI_matches sheet...")
    non_npi_df = _build_match_rows(matched_df, config, npi_matches=False)
    logger.info("Building unmatched sheet...")
    unm_df = _build_unmatched_rows(unmatched_df, conflicts_df, config)
    logger.info("Building collisions sheet...")
    coll_df = _build_collision_rows(matched_df, config)
    legend_df = _build_legend_rows(config)

    # Merge in prior reviewer entries (if any)
    if existing_review_path and os.path.exists(existing_review_path):
        logger.info(f"Merging prior reviewer entries from {existing_review_path}")
        reviewer_match_cols = [c[0] for c in _reviewer_columns_match()]
        reviewer_unm_cols = [c[0] for c in _reviewer_columns_unmatched()]
        for sheet_name, current_df, keep_cols in (
            ("NPI_matches", npi_df, reviewer_match_cols),
            ("non_NPI_matches", non_npi_df, reviewer_match_cols),
            ("unmatched", unm_df, reviewer_unm_cols),
        ):
            try:
                prior = pd.read_excel(existing_review_path, sheet_name=sheet_name)
            except (ValueError, KeyError):
                continue
            if "provider_pk" not in prior.columns or prior.empty:
                continue
            keep = ["provider_pk"] + [c for c in keep_cols if c in prior.columns]
            prior = prior[keep].dropna(subset=["provider_pk"])
            prior["provider_pk"] = pd.to_numeric(prior["provider_pk"], errors="coerce").astype(
                "Int64"
            )
            dropped = current_df.drop(columns=[c for c in keep_cols if c in current_df.columns])
            dropped["provider_pk"] = pd.to_numeric(dropped["provider_pk"], errors="coerce").astype(
                "Int64"
            )
            merged = dropped.merge(prior, on="provider_pk", how="left")
            for outcome_col in ("match_correct", "pair_correct"):
                if outcome_col in merged.columns:
                    merged[outcome_col] = (
                        merged[outcome_col].apply(_normalize_outcome_to_string).astype("object")
                    )
            if sheet_name == "NPI_matches":
                npi_df = merged
            elif sheet_name == "non_NPI_matches":
                non_npi_df = merged
            else:
                unm_df = merged

    # Defensive: normalize match_correct / pair_correct to strings
    for df_ in (npi_df, non_npi_df):
        if "match_correct" in df_.columns:
            df_["match_correct"] = (
                df_["match_correct"].apply(_normalize_outcome_to_string).astype("object")
            )
    if "pair_correct" in coll_df.columns:
        coll_df["pair_correct"] = (
            coll_df["pair_correct"].apply(_normalize_outcome_to_string).astype("object")
        )

    # Write
    logger.info(f"Writing {output_path}")
    os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        legend_df.to_excel(writer, sheet_name="LEGEND", index=False)
        npi_df.to_excel(writer, sheet_name="NPI_matches", index=False)
        non_npi_df.to_excel(writer, sheet_name="non_NPI_matches", index=False)
        unm_df.to_excel(writer, sheet_name="unmatched", index=False)
        coll_df.to_excel(writer, sheet_name="collisions", index=False)

        wb = writer.book
        _apply_match_sheet_formatting(wb["NPI_matches"], config)
        _apply_match_sheet_formatting(wb["non_NPI_matches"], config)
        _apply_unmatched_sheet_formatting(wb["unmatched"], config)
        _apply_collisions_sheet_formatting(wb["collisions"], config)
        apply_legend_formatting(wb["LEGEND"])

    logger.info(
        f"Wrote review template with {len(npi_df)} NPI matches, "
        f"{len(non_npi_df)} non-NPI matches, {len(unm_df)} unmatched, "
        f"{len(coll_df)} collision rows."
    )


def apply_review(
    matched_path: str,
    review_path: str,
    output_path: str,
    refetch_list_path: str,
    config: ReviewConfig,
    *,
    matched_sheet_name: str = "conflicteds_ids",
    unmatched_sheet_name: str = "unmatched",
) -> None:
    """Apply a filled-in review to produce a final matched-providers file.

    Reads:
      - matched_path: auto-matcher output (``<study>_ids.xlsx``)
      - review_path: reviewer-filled template

    Writes:
      - output_path: ``<study>_ids_final.xlsx`` with sheets:
          * ``conflicteds_ids`` — final matched set after review
          * ``unmatched`` — providers with no CMS profile post-review
          * ``unreviewed`` — providers whose review_outcome was blank
            (algorithm pick kept by default but flagged for follow-up)
      - refetch_list_path: newline-separated list of new profile_ids the
        review introduced (need payment extraction).

    Outcome → action by sheet:

    ``NPI_matches`` / ``non_NPI_matches``:
      ``match_correct=TRUE``           → keep algorithm's pick
      ``match_correct=FALSE`` + corrected  → swap to corrected; refetch
      ``match_correct=FALSE`` + blank   → drop from matched; add to unmatched
      ``blank``                        → keep pick; flag as unreviewed

    ``unmatched``:
      ``found_profile_id`` populated   → add to matched; refetch
      ``blank``                        → confirmed unmatched

    ``collisions``:
      ``pair_correct=TRUE``            → keep pair
      ``pair_correct=FALSE`` + corrected → swap pid for this pk; refetch
      ``pair_correct=FALSE`` + blank    → drop pair; pk → unmatched
      ``blank``                        → keep (algorithm pick); unreviewed
    """
    logger.info(f"Reading auto-match output: {matched_path}")
    matched_df = pd.read_excel(matched_path, sheet_name=matched_sheet_name)

    logger.info(f"Reading review: {review_path}")
    review_sheets = {}
    for sheet_name in ("NPI_matches", "non_NPI_matches", "unmatched", "collisions"):
        try:
            review_sheets[sheet_name] = pd.read_excel(review_path, sheet_name=sheet_name)
        except (ValueError, KeyError):
            review_sheets[sheet_name] = pd.DataFrame()

    matched_by_pk = matched_df.set_index("provider_pk")
    auto_matched_pids_by_pk = {
        int(pk): int(row["profile_id"])
        for pk, row in matched_by_pk.iterrows()
        if pd.notna(row.get("profile_id"))
    }

    # Display-label of the entity's "Last Name" / "First Name" / "School"
    # — used to fill out the unmatched-after-review summary in a study-
    # neutral way. Falls back to the matched-df column where the field
    # is sourced from.
    def _display(suffix: str) -> Optional[str]:
        for f_ in config.source_fields:
            if f_.display_label == f"{config.entity_name}: {suffix}":
                return f_.display_label
        return None

    last_name_label = _display("Last Name")
    first_name_label = _display("First Name")
    school_label = _display("School")  # optional

    final: dict[int, tuple] = {}
    refetch_pids: set[int] = set()
    no_cms_match_pks: list[dict] = []
    unreviewed_rows: list[dict] = []

    def _stash_unmatched(pk: int, source_sheet: str, r) -> None:
        entry = {"provider_pk": pk, "source_sheet": source_sheet}
        if last_name_label:
            entry[last_name_label] = r.get(last_name_label)
        if first_name_label:
            entry[first_name_label] = r.get(first_name_label)
        if school_label:
            entry[school_label] = r.get(school_label)
        if "notes" in r.index if hasattr(r, "index") else False:
            entry["notes"] = r.get("notes")
        no_cms_match_pks.append(entry)

    # NPI + non-NPI sheets
    for sheet_name in ("NPI_matches", "non_NPI_matches"):
        df_ = review_sheets[sheet_name]
        if df_.empty:
            continue
        for _, r in df_.iterrows():
            pk_raw = r.get("provider_pk")
            if pk_raw is None or pd.isna(pk_raw):
                continue
            pk = int(pk_raw)
            outcome = _coerce_review_outcome(r.get("match_correct"))
            auto_pid = auto_matched_pids_by_pk.get(pk)

            if outcome is True:
                if auto_pid is not None:
                    final[pk] = ("matched", auto_pid)
            elif outcome is False:
                corrected_pid = _safe_int(r.get("corrected_profile_id"))
                if corrected_pid is not None:
                    final[pk] = ("matched", corrected_pid)
                    if auto_pid is None or corrected_pid != auto_pid:
                        refetch_pids.add(corrected_pid)
                else:
                    final[pk] = ("unmatched", "reviewer: no CMS match")
                    _stash_unmatched(pk, sheet_name, r)
            else:
                if auto_pid is not None:
                    final[pk] = ("unreviewed", auto_pid)
                    unreviewed_row = {
                        "provider_pk": pk,
                        "algorithm_profile_id_kept": auto_pid,
                        "source_sheet": sheet_name,
                    }
                    if last_name_label:
                        unreviewed_row[last_name_label] = r.get(last_name_label)
                    if first_name_label:
                        unreviewed_row[first_name_label] = r.get(first_name_label)
                    unreviewed_rows.append(unreviewed_row)

    # unmatched sheet
    unm_review = review_sheets["unmatched"]
    if not unm_review.empty:
        for _, r in unm_review.iterrows():
            pk_raw = r.get("provider_pk")
            if pk_raw is None or pd.isna(pk_raw):
                continue
            pk = int(pk_raw)
            found_pid = _safe_int(r.get("found_profile_id"))
            if found_pid is not None:
                final[pk] = ("matched", found_pid)
                refetch_pids.add(found_pid)
            else:
                final[pk] = ("unmatched", "reviewer: confirmed no CMS match")
                _stash_unmatched(pk, "unmatched", r)

    # collisions sheet (overrides if more specific)
    coll = review_sheets["collisions"]
    if not coll.empty:
        for _, r in coll.iterrows():
            pk_raw = r.get("provider_pk")
            if pk_raw is None or pd.isna(pk_raw):
                continue
            pk = int(pk_raw)
            outcome = _coerce_review_outcome(r.get("pair_correct"))
            if outcome is True:
                if pk in auto_matched_pids_by_pk:
                    final[pk] = ("matched", auto_matched_pids_by_pk[pk])
            elif outcome is False:
                corrected_pid = _safe_int(r.get("corrected_profile_id"))
                if corrected_pid is not None:
                    final[pk] = ("matched", corrected_pid)
                    refetch_pids.add(corrected_pid)
                else:
                    final[pk] = (
                        "unmatched",
                        "reviewer (collisions): wrong pair, no replacement supplied",
                    )
                    _stash_unmatched(pk, "collisions", r)

    # Build the conflicteds_ids sheet (matched + unreviewed)
    final_matched_rows = []
    for pk, (state, payload) in final.items():
        if state in ("matched", "unreviewed"):
            if pk in matched_by_pk.index:
                src = (
                    matched_by_pk.loc[pk].copy()
                    if not isinstance(matched_by_pk.loc[pk], pd.DataFrame)
                    else matched_by_pk.loc[pk].iloc[0].copy()
                )
                src["provider_pk"] = pk
                src["profile_id"] = payload
                src["reviewed"] = state == "matched"
                final_matched_rows.append(src)
            else:
                final_matched_rows.append(
                    pd.Series(
                        {
                            "provider_pk": pk,
                            "profile_id": payload,
                            "reviewed": True,
                            "source": "reviewer-supplied (was unmatched)",
                        }
                    )
                )

    final_matched_df = pd.DataFrame(final_matched_rows) if final_matched_rows else pd.DataFrame()
    unmatched_after = pd.DataFrame(no_cms_match_pks)
    unreviewed_df = pd.DataFrame(unreviewed_rows)

    logger.info(
        f"Apply outcome: matched={len(final_matched_df)} "
        f"(reviewed={sum(1 for s in final.values() if s[0] == 'matched')}, "
        f"unreviewed={len(unreviewed_df)}), "
        f"unmatched_after_review={len(unmatched_after)}, "
        f"new pids needing refetch={len(refetch_pids)}"
    )

    logger.info(f"Writing {output_path}")
    os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        if not final_matched_df.empty:
            final_matched_df.to_excel(writer, sheet_name="conflicteds_ids", index=False)
        else:
            pd.DataFrame(columns=["provider_pk", "profile_id"]).to_excel(
                writer, sheet_name="conflicteds_ids", index=False
            )
        unmatched_after.to_excel(writer, sheet_name="unmatched", index=False)
        unreviewed_df.to_excel(writer, sheet_name="unreviewed", index=False)

    with open(refetch_list_path, "w") as f:
        for pid in sorted(refetch_pids):
            f.write(f"{pid}\n")
    logger.info(f"Wrote {refetch_list_path} with {len(refetch_pids)} pids")


# --------------------------------------------------------------------------
# Refetch payments for new profile_ids
# --------------------------------------------------------------------------


def refetch_payments_for_pids(
    needs_refetch_path: str,
    payments_xlsx_path: str,
    years: Iterable[int],
    *,
    canonical_pids_path: Optional[str] = None,
    canonical_sheet_name: str = "conflicteds_ids",
    payment_class: str = "general",
    payments_sheet_name: str = "payments",
) -> None:
    """Sync a payments xlsx to a post-review canonical pid set.

    Two operations:
      1. Fetch payments for any new profile_ids in ``needs_refetch_path``
         that aren't already in the cached payments file.
      2. If ``canonical_pids_path`` is provided, DROP payments for any
         profile_ids NOT in the canonical set (e.g. auto-picks the
         reviewer overrode — their wrong-person payments shouldn't keep
         contaminating downstream aggregates).

    After running, the payments file contains exactly the payments for
    the canonical (post-review) matched profile_id set.

    Args:
        needs_refetch_path: file written by :func:`apply_review` with one
            profile_id per line.
        payments_xlsx_path: payments xlsx to update.
        years: list of years to fetch raw CSVs for.
        canonical_pids_path: ``<study>_ids_final.xlsx``. If None, no
            filter is applied (only refetch happens).
        canonical_sheet_name: sheet in canonical_pids_path holding the
            authoritative pid set.
        payment_class: "general", "research", or "ownership".
        payments_sheet_name: sheet name in the payments xlsx.
    """
    years = list(years)

    existing = pd.DataFrame()
    if os.path.exists(payments_xlsx_path):
        try:
            existing = pd.read_excel(payments_xlsx_path, sheet_name=payments_sheet_name)
        except (ValueError, KeyError):
            pass

    pids_to_fetch: set[int] = set()
    if os.path.exists(needs_refetch_path):
        with open(needs_refetch_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        pids_to_fetch.add(int(line))
                    except ValueError:
                        pass
        logger.info(f"Refetch list has {len(pids_to_fetch)} pids")

    cached_pids: set[int] = set()
    if not existing.empty and "Covered_Recipient_Profile_ID" in existing.columns:
        cached_pids = set(
            pd.to_numeric(existing["Covered_Recipient_Profile_ID"], errors="coerce")
            .dropna()
            .astype(int)
            .tolist()
        )

    canonical_pids: set[int] = set()
    if canonical_pids_path and os.path.exists(canonical_pids_path):
        canonical_df = pd.read_excel(canonical_pids_path, sheet_name=canonical_sheet_name)
        canonical_pids = set(
            pd.to_numeric(canonical_df["profile_id"], errors="coerce").dropna().astype(int).tolist()
        )
        missing_from_cache = canonical_pids - cached_pids
        if missing_from_cache:
            logger.info(
                f"Found {len(missing_from_cache)} canonical pids not in current "
                "cache; adding to refetch."
            )
            pids_to_fetch |= missing_from_cache

    pids_to_fetch = pids_to_fetch - cached_pids
    logger.info(f"Total pids to actually fetch from CMS: {len(pids_to_fetch)}")

    all_new = []
    if pids_to_fetch:
        from .payments import PaymentsSearch

        pid_df = pd.DataFrame({"profile_id": pd.Series(sorted(pids_to_fetch), dtype="Int64")})
        for year in years:
            logger.info(f"  Searching {payment_class} payments for year {year}...")
            try:
                chunk = PaymentsSearch(
                    conflicteds_ids=pid_df,
                    years=year,
                    payments=None,
                    nrows=None,
                    MD_DO_only=False,
                ).read_payments_csvs(payment_class=payment_class)
                if chunk is not None and not chunk.empty:
                    all_new.append(chunk)
                    logger.info(f"    Got {len(chunk)} rows.")
            except Exception as e:
                logger.warning(f"  Year {year} fetch failed: {e}")

    if all_new:
        new_payments = pd.concat(all_new, ignore_index=True)
        logger.info(f"Total new payment rows: {len(new_payments):,}")
        combined = (
            pd.concat([existing, new_payments], ignore_index=True)
            if not existing.empty
            else new_payments
        )
    else:
        logger.info("Refetch found no new payments to add.")
        combined = existing if not existing.empty else pd.DataFrame()

    if canonical_pids and not combined.empty and "Covered_Recipient_Profile_ID" in combined.columns:
        before = len(combined)
        combined["Covered_Recipient_Profile_ID"] = pd.to_numeric(
            combined["Covered_Recipient_Profile_ID"], errors="coerce"
        )
        combined = combined[combined["Covered_Recipient_Profile_ID"].isin(canonical_pids)].copy()
        after = len(combined)
        dropped = before - after
        if dropped > 0:
            logger.info(
                f"Dropped {dropped} payment rows for non-canonical pids "
                f"(canonical set has {len(canonical_pids)} pids)."
            )

    # Preserve other sheets when overwriting
    other_sheets = {}
    if os.path.exists(payments_xlsx_path):
        xls = pd.ExcelFile(payments_xlsx_path)
        for s in xls.sheet_names:
            if s != payments_sheet_name:
                try:
                    other_sheets[s] = pd.read_excel(payments_xlsx_path, sheet_name=s)
                except Exception:
                    pass

    with pd.ExcelWriter(payments_xlsx_path, engine="openpyxl") as writer:
        if combined.empty:
            pd.DataFrame(columns=["Covered_Recipient_Profile_ID"]).to_excel(
                writer, sheet_name=payments_sheet_name, index=False
            )
        else:
            combined.to_excel(writer, sheet_name=payments_sheet_name, index=False)
        for s, df_ in other_sheets.items():
            df_.to_excel(writer, sheet_name=s, index=False)

    final_count = len(combined) if not combined.empty else 0
    final_dollars = (
        combined["Total_Amount_of_Payment_USDollars"].sum()
        if not combined.empty and "Total_Amount_of_Payment_USDollars" in combined.columns
        else 0.0
    )
    logger.info(
        f"Updated {payments_xlsx_path}: payments now total {final_count:,} (${final_dollars:,.2f})"
    )
