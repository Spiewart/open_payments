"""Tests for specialty parsers + ConflictSpecialtys mixin.

Covers:
  - parse_cms_specialty_string: CMS pipe-delimited format (3 parts)
  - parse_specialty_freetext: 1- and 2-part free-text strings
  - parse_specialties_freetext: multi-specialty (`;`-delimited)
  - ConflictSpecialtys default: free-text parsing per row
  - SPECIALTY_MAP override (uptodate-style enum map)
  - get_specialtys override (per-row custom)
  - conflict_specialtys override (deans-style separate columns)
"""

from __future__ import annotations

import pandas as pd
import pytest

from ..specialtys import (
    ConflictSpecialtys,
    Specialtys,
    parse_cms_specialty_string,
    parse_specialties_freetext,
    parse_specialty_freetext,
)

# ---------------------------------------------------------------------------
# parse_cms_specialty_string (CMS-side helper)
# ---------------------------------------------------------------------------


def test__cms_full_three_part():
    s = parse_cms_specialty_string(
        "Allopathic & Osteopathic Physicians|Internal Medicine|Cardiology"
    )
    assert s.specialty == "Internal Medicine"
    assert s.subspecialty == "Cardiology"


def test__cms_two_part_no_subspecialty():
    s = parse_cms_specialty_string("Allopathic & Osteopathic Physicians|Family Medicine")
    assert s.specialty == "Family Medicine"
    assert s.subspecialty is None


def test__cms_drops_provider_type():
    s = parse_cms_specialty_string("Nursing Service Providers|Nurse Practitioner|Family Practice")
    # provider_type is intentionally discarded — already filtered upstream.
    assert s.specialty == "Nurse Practitioner"
    assert s.subspecialty == "Family Practice"


@pytest.mark.parametrize("value", [None, "", "   ", "OnlyProviderType"])
def test__cms_blank_or_single_part_returns_none(value):
    assert parse_cms_specialty_string(value) is None


def test__cms_real_world_sample_with_ampersand():
    # CMS strings often have & in specialty names; verify they pass through.
    s = parse_cms_specialty_string(
        "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Reproductive Endocrinology"
    )
    assert s.specialty == "Obstetrics & Gynecology"
    assert s.subspecialty == "Reproductive Endocrinology"


# ---------------------------------------------------------------------------
# parse_specialty_freetext (conflicted-side single)
# ---------------------------------------------------------------------------


def test__freetext_single_token_is_specialty():
    s = parse_specialty_freetext("Family Medicine")
    assert s.specialty == "Family Medicine"
    assert s.subspecialty is None


def test__freetext_with_subspecialty():
    s = parse_specialty_freetext("Internal Medicine | Cardiology")
    assert s.specialty == "Internal Medicine"
    assert s.subspecialty == "Cardiology"


def test__freetext_no_spaces_around_pipe():
    s = parse_specialty_freetext("Internal Medicine|Cardiology")
    assert s.specialty == "Internal Medicine"
    assert s.subspecialty == "Cardiology"


@pytest.mark.parametrize("value", [None, "", "   ", "|"])
def test__freetext_blank_returns_none(value):
    assert parse_specialty_freetext(value) is None


# ---------------------------------------------------------------------------
# parse_specialties_freetext (conflicted-side multi)
# ---------------------------------------------------------------------------


def test__multi_single_specialty():
    result = parse_specialties_freetext("Family Medicine")
    assert len(result) == 1
    assert result[0].specialty == "Family Medicine"


def test__multi_semicolon_separated():
    result = parse_specialties_freetext("Family Medicine; Internal Medicine | Cardiology")
    assert len(result) == 2
    assert result[0].specialty == "Family Medicine"
    assert result[1].specialty == "Internal Medicine"
    assert result[1].subspecialty == "Cardiology"


def test__multi_skips_blank_segments():
    result = parse_specialties_freetext("Family Medicine;; ;Surgery")
    assert len(result) == 2


def test__multi_blank_returns_empty_list():
    assert parse_specialties_freetext("") == []
    assert parse_specialties_freetext(None) == []


# ---------------------------------------------------------------------------
# ConflictSpecialtys default mixin
# ---------------------------------------------------------------------------


def test__default_freetext_single_specialty():
    df = pd.DataFrame({"specialtys": ["Family Medicine"], "other": [1]})
    out = ConflictSpecialtys(df).conflict_specialtys()
    assert out.iloc[0]["specialtys"] == [Specialtys(specialty="Family Medicine")]
    assert "other" in out.columns


def test__default_freetext_multi():
    df = pd.DataFrame({"specialtys": ["Surgery; Pediatrics"]})
    out = ConflictSpecialtys(df).conflict_specialtys()
    assert len(out.iloc[0]["specialtys"]) == 2


def test__default_handles_blank_and_none():
    df = pd.DataFrame({"specialtys": ["Family Medicine", None, "", "   "]})
    out = ConflictSpecialtys(df).conflict_specialtys()
    assert out.iloc[0]["specialtys"] == [Specialtys(specialty="Family Medicine")]
    assert out.iloc[1]["specialtys"] == []
    assert out.iloc[2]["specialtys"] == []
    assert out.iloc[3]["specialtys"] == []


def test__custom_source_column_via_classvar():
    class _CustomSource(ConflictSpecialtys):
        SPECIALTYS_COLUMN = "primary_specialty"

    df = pd.DataFrame({"primary_specialty": ["Family Medicine"], "other": [1]})
    out = _CustomSource(df).conflict_specialtys()
    assert "primary_specialty" not in out.columns
    assert out.iloc[0]["specialtys"] == [Specialtys(specialty="Family Medicine")]


# ---------------------------------------------------------------------------
# SPECIALTY_MAP override — uptodate-style closed source taxonomy
# ---------------------------------------------------------------------------


class _UptodateLikeConflictSpecialtys(ConflictSpecialtys):
    """Stand-in for uptodate_conflicts's UpToDateSpecialties enum mapping.
    The real version is a ~190-line if/elif chain over a 25-entry enum;
    SPECIALTY_MAP collapses that to a dict declaration.
    """

    SPECIALTYS_COLUMN = "section"
    SPECIALTY_MAP = {
        "Cardiovascular Medicine": [
            Specialtys(specialty="Internal Medicine", subspecialty="Cardiology")
        ],
        "Allergy and Immunology": [Specialtys(specialty="Allergy")],
        "Pulmonology and Critical Care Medicine": [
            Specialtys(specialty="Internal Medicine", subspecialty="Pulmonology"),
            Specialtys(specialty="Internal Medicine", subspecialty="Critical Care"),
        ],
    }


def test__specialty_map_lookup_used():
    df = pd.DataFrame({"section": ["Cardiovascular Medicine"]})
    out = _UptodateLikeConflictSpecialtys(df).conflict_specialtys()
    assert out.iloc[0]["specialtys"] == [
        Specialtys(specialty="Internal Medicine", subspecialty="Cardiology")
    ]


def test__specialty_map_supports_multi():
    df = pd.DataFrame({"section": ["Pulmonology and Critical Care Medicine"]})
    out = _UptodateLikeConflictSpecialtys(df).conflict_specialtys()
    assert len(out.iloc[0]["specialtys"]) == 2


def test__specialty_map_fallback_to_freetext_for_unmapped():
    # Unmapped section -> falls through to free-text parsing.
    df = pd.DataFrame({"section": ["Unrecognized Specialty"]})
    out = _UptodateLikeConflictSpecialtys(df).conflict_specialtys()
    assert out.iloc[0]["specialtys"] == [Specialtys(specialty="Unrecognized Specialty")]


# ---------------------------------------------------------------------------
# get_specialtys override — per-row custom logic
# ---------------------------------------------------------------------------


class _AbimLikeConflictSpecialtys(ConflictSpecialtys):
    """Stand-in for abim's URL-based specialty derivation. The real version
    parses an ABIM board URL hierarchy to derive specialty; here we use the
    `board` column as a stand-in for that derived value.
    """

    SPECIALTYS_COLUMN = "board"

    @classmethod
    def get_specialtys(cls, row):
        board = row.get("board") if "board" in row.index else None
        if not board:
            return []
        # Map ABIM board names to canonical specialties.
        mapping = {
            "internal-medicine": [Specialtys(specialty="Internal Medicine")],
            "cardiology-board": [
                Specialtys(specialty="Internal Medicine", subspecialty="Cardiology")
            ],
        }
        return mapping.get(board, [Specialtys(specialty=str(board))])


def test__get_specialtys_override():
    df = pd.DataFrame({"board": ["cardiology-board", "internal-medicine", "unknown"]})
    out = _AbimLikeConflictSpecialtys(df).conflict_specialtys()
    assert out.iloc[0]["specialtys"][0].subspecialty == "Cardiology"
    assert out.iloc[1]["specialtys"][0].specialty == "Internal Medicine"
    assert out.iloc[2]["specialtys"][0].specialty == "unknown"


# ---------------------------------------------------------------------------
# conflict_specialtys override — deans-style separate columns
# ---------------------------------------------------------------------------


class _DeansLikeConflictSpecialtys(ConflictSpecialtys):
    """deans has separate Specialty + Subspecialty columns — bypass the
    SPECIALTYS_COLUMN convention and override the whole pipeline.
    """

    def conflict_specialtys(self):
        self.conflicts = self.conflicts.copy()

        def _row_to_specialtys(row):
            sp = row.get("Specialty") if "Specialty" in row.index else None
            sub = row.get("Subspecialty") if "Subspecialty" in row.index else None
            if pd.isna(sp) and pd.isna(sub):
                return []
            return [
                Specialtys(
                    specialty=sp.strip() if pd.notna(sp) else None,
                    subspecialty=sub.strip() if pd.notna(sub) else None,
                )
            ]

        self.conflicts["specialtys"] = self.conflicts.apply(_row_to_specialtys, axis=1).values
        self.conflicts = self.conflicts.drop(columns=["Specialty", "Subspecialty"])
        return self.conflicts


def test__conflict_specialtys_pipeline_override():
    df = pd.DataFrame(
        {
            "Specialty": ["Internal Medicine", "Surgery", None],
            "Subspecialty": ["Cardiology", None, "General"],
        }
    )
    out = _DeansLikeConflictSpecialtys(df).conflict_specialtys()
    assert out.iloc[0]["specialtys"][0].subspecialty == "Cardiology"
    assert out.iloc[1]["specialtys"][0].specialty == "Surgery"
    assert out.iloc[1]["specialtys"][0].subspecialty is None
    assert out.iloc[2]["specialtys"][0].specialty is None
    assert out.iloc[2]["specialtys"][0].subspecialty == "General"
    assert "Specialty" not in out.columns
    assert "Subspecialty" not in out.columns


# ---------------------------------------------------------------------------
# CMS-side real-data validation (no integration mark - uses pure helper)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cms_string,expected_specialty,expected_subspecialty",
    [
        # Sampled from real 2023 CMS general payments.
        (
            "Allopathic & Osteopathic Physicians|Family Medicine",
            "Family Medicine",
            None,
        ),
        (
            "Allopathic & Osteopathic Physicians|Internal Medicine|Cardiovascular Disease",
            "Internal Medicine",
            "Cardiovascular Disease",
        ),
        (
            "Allopathic & Osteopathic Physicians|Obstetrics & Gynecology|Maternal & Fetal Medicine",
            "Obstetrics & Gynecology",
            "Maternal & Fetal Medicine",
        ),
        (
            "Nursing Service Providers|Nurse Practitioner|Family Practice",
            "Nurse Practitioner",
            "Family Practice",
        ),
        (
            "Physician Assistants & Advanced Practice Nursing Providers|Physician Assistant",
            "Physician Assistant",
            None,
        ),
    ],
)
def test__cms_real_world_samples(cms_string, expected_specialty, expected_subspecialty):
    s = parse_cms_specialty_string(cms_string)
    assert s.specialty == expected_specialty
    assert s.subspecialty == expected_subspecialty
