"""Factory helpers for building CMS-shaped DataFrames and conflicted-provider
rows in tests, without depending on real CMS data files on disk.

A factory accepts `**overrides` and returns a single row (dict). Tests stitch
rows together with `pd.DataFrame([row1, row2, ...])` so each test states only
what it cares about. The `make_general_csv_df` / `make_ownership_csv_df` /
`make_research_csv_df` helpers below assemble the canonical scenario set used
by the on-disk fixtures and conftest fixtures.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ..choices import Credentials

MD = Credentials.MEDICAL_DOCTOR.value
DO = Credentials.DOCTOR_OF_OSTEOPATHY.value
PA = Credentials.PHYSICIAN_ASSISTANT.value

ALLO_OSTEO = "Allopathic & Osteopathic Physicians"


_GENERAL_DEFAULTS: dict[str, Any] = {
    "Record_ID": 1,
    "Covered_Recipient_Profile_ID": 100,
    "Covered_Recipient_NPI": 1000000001,
    "Covered_Recipient_Last_Name": "DOE",
    "Covered_Recipient_First_Name": "JOHN",
    "Covered_Recipient_Middle_Name": "Q",
    "Covered_Recipient_Name_Suffix": "",
    "Covered_Recipient_Primary_Type_1": MD,
    "Covered_Recipient_Primary_Type_2": "",
    "Covered_Recipient_Primary_Type_3": "",
    "Covered_Recipient_Primary_Type_4": "",
    "Covered_Recipient_Primary_Type_5": "",
    "Covered_Recipient_Primary_Type_6": "",
    "Covered_Recipient_Specialty_1": f"{ALLO_OSTEO}|Family Medicine|",
    "Covered_Recipient_Specialty_2": "",
    "Covered_Recipient_Specialty_3": "",
    "Covered_Recipient_Specialty_4": "",
    "Covered_Recipient_Specialty_5": "",
    "Covered_Recipient_Specialty_6": "",
    "Recipient_City": "Manhattan",
    "Recipient_State": "NY",
    "Covered_Recipient_License_State_code1": "NY",
    "Covered_Recipient_License_State_code2": "",
    "Covered_Recipient_License_State_code3": "",
    "Covered_Recipient_License_State_code4": "",
    "Covered_Recipient_License_State_code5": "",
    "Form_of_Payment_or_Transfer_of_Value": "Cash or cash equivalent",
    "Nature_of_Payment_or_Transfer_of_Value": "Consulting Fee",
    "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name": "Acme Pharma",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name": "Acme Pharma",
    "Total_Amount_of_Payment_USDollars": 100.00,
}

_OWNERSHIP_DEFAULTS: dict[str, Any] = {
    "Physician_Profile_ID": 100,
    "Physician_NPI": 1000000001,
    "Physician_Last_Name": "DOE",
    "Physician_First_Name": "JOHN",
    "Physician_Middle_Name": "Q",
    "Physician_Name_Suffix": "",
    "Physician_Primary_Type": MD,
    "Physician_Specialty": f"{ALLO_OSTEO}|Family Medicine|",
    "Recipient_City": "Manhattan",
    "Recipient_State": "NY",
    "Total_Amount_Invested_USDollars": 500.00,
    "Value_of_Interest": 250.00,
    "Terms_of_Interest": "Stock",
    "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name": "Acme Pharma",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name": "Acme Pharma",
}

_RESEARCH_DEFAULTS: dict[str, Any] = {
    **_GENERAL_DEFAULTS,
    "Nature_of_Payment_or_Transfer_of_Value": "Research",
    "Total_Amount_of_Payment_USDollars": 25_000.00,
}


def make_general_row(**overrides: Any) -> dict[str, Any]:
    """Build a single CMS general-payments CSV row (dict). Override any column."""
    row = {**_GENERAL_DEFAULTS, **overrides}
    return row


def make_ownership_row(**overrides: Any) -> dict[str, Any]:
    """Build a single CMS ownership-payments CSV row (dict)."""
    row = {**_OWNERSHIP_DEFAULTS, **overrides}
    return row


def make_research_row(**overrides: Any) -> dict[str, Any]:
    """Build a single CMS research-payments CSV row (dict)."""
    row = {**_RESEARCH_DEFAULTS, **overrides}
    return row


# Canonical scenarios used by both the on-disk fixture CSVs and the
# in-memory conftest fixtures. Edit here once to update both surfaces.


def make_general_csv_df() -> pd.DataFrame:
    """Returns the canonical general-payments DataFrame covering the
    matching-pipeline scenarios:

    - Scenario A: single deterministic match (Adams / John / M -> profile 101)
    - Scenario B: narrowed by first name (Brown / Jane -> 201, not Jonathan 202)
    - Scenario C: narrowed by middle initial (Smith / David / A -> 301, not 302)
    - Scenario D: hyphenated last name (Smith-Jones / Hannah -> 401)
    - Scenario E: ambiguous after all filters (White / Emily -> 501 + 502, options)
    - Scenario F: non-physician must be filtered out (Wilson / Physician Assistant)
    - Scenario G: NaN profile_id row must be dropped at read time
    """
    return pd.DataFrame(
        [
            # A: single match
            make_general_row(
                Record_ID=1001,
                Covered_Recipient_Profile_ID=101,
                Covered_Recipient_NPI=1000000101,
                Covered_Recipient_Last_Name="ADAMS",
                Covered_Recipient_First_Name="JOHN",
                Covered_Recipient_Middle_Name="MICHAEL",
                Recipient_City="Manhattan",
                Recipient_State="NY",
            ),
            # B: two Browns; conflicted should pick Jane (201)
            make_general_row(
                Record_ID=1002,
                Covered_Recipient_Profile_ID=201,
                Covered_Recipient_NPI=1000000201,
                Covered_Recipient_Last_Name="BROWN",
                Covered_Recipient_First_Name="JANE",
                Covered_Recipient_Middle_Name="MARIE",
                Recipient_City="Boston",
                Recipient_State="MA",
            ),
            make_general_row(
                Record_ID=1003,
                Covered_Recipient_Profile_ID=202,
                Covered_Recipient_NPI=1000000202,
                Covered_Recipient_Last_Name="BROWN",
                Covered_Recipient_First_Name="JONATHAN",
                Covered_Recipient_Middle_Name="MICHAEL",
                Covered_Recipient_Primary_Type_1=DO,
                Recipient_City="Chicago",
                Recipient_State="IL",
            ),
            # C: same first+last, differ by middle initial
            make_general_row(
                Record_ID=1004,
                Covered_Recipient_Profile_ID=301,
                Covered_Recipient_NPI=1000000301,
                Covered_Recipient_Last_Name="SMITH",
                Covered_Recipient_First_Name="DAVID",
                Covered_Recipient_Middle_Name="ANDREW",
                Recipient_City="Seattle",
                Recipient_State="WA",
            ),
            make_general_row(
                Record_ID=1005,
                Covered_Recipient_Profile_ID=302,
                Covered_Recipient_NPI=1000000302,
                Covered_Recipient_Last_Name="SMITH",
                Covered_Recipient_First_Name="DAVID",
                Covered_Recipient_Middle_Name="BRANDON",
                Recipient_City="Seattle",
                Recipient_State="WA",
            ),
            # D: hyphenated last name
            make_general_row(
                Record_ID=1006,
                Covered_Recipient_Profile_ID=401,
                Covered_Recipient_NPI=1000000401,
                Covered_Recipient_Last_Name="SMITH-JONES",
                Covered_Recipient_First_Name="HANNAH",
                Covered_Recipient_Middle_Name="LEE",
                Recipient_City="San Diego",
                Recipient_State="CA",
            ),
            # E: ambiguous Emily Whites - both should land in unmatched_options
            make_general_row(
                Record_ID=1007,
                Covered_Recipient_Profile_ID=501,
                Covered_Recipient_Last_Name="WHITE",
                Covered_Recipient_First_Name="EMILY",
                Covered_Recipient_Middle_Name="",
                Recipient_City="Chicago",
                Recipient_State="IL",
            ),
            make_general_row(
                Record_ID=1008,
                Covered_Recipient_Profile_ID=502,
                Covered_Recipient_Last_Name="WHITE",
                Covered_Recipient_First_Name="EMILY",
                Covered_Recipient_Middle_Name="",
                Recipient_City="Chicago",
                Recipient_State="IL",
            ),
            # F: non-physician - should be filtered out by PhysicianFilter
            make_general_row(
                Record_ID=1009,
                Covered_Recipient_Profile_ID=999,
                Covered_Recipient_Last_Name="WILSON",
                Covered_Recipient_First_Name="MICHAEL",
                Covered_Recipient_Middle_Name="J",
                Covered_Recipient_Primary_Type_1=PA,
                Covered_Recipient_Specialty_1="Physician Assistants & Advanced Practice Nursing Providers|Physician Assistant|",
                Recipient_City="Manhattan",
                Recipient_State="NY",
            ),
            # G: NaN profile_id - must be dropped by filter_payment_chunk
            make_general_row(
                Record_ID=1010,
                Covered_Recipient_Profile_ID=pd.NA,
                Covered_Recipient_Last_Name="NULLY",
                Covered_Recipient_First_Name="NAME",
                Covered_Recipient_Middle_Name="Z",
            ),
        ]
    )


def make_ownership_csv_df() -> pd.DataFrame:
    """Minimal ownership CSV: one row that overlaps with general scenario A,
    plus an independent physician and a NaN row that must be dropped."""
    return pd.DataFrame(
        [
            make_ownership_row(
                Physician_Profile_ID=101,
                Physician_NPI=1000000101,
                Physician_Last_Name="ADAMS",
                Physician_First_Name="JOHN",
                Physician_Middle_Name="MICHAEL",
                Recipient_City="Manhattan",
                Recipient_State="NY",
            ),
            make_ownership_row(
                Physician_Profile_ID=601,
                Physician_NPI=1000000601,
                Physician_Last_Name="GARCIA",
                Physician_First_Name="MARIA",
                Physician_Middle_Name="LUIS",
                Physician_Primary_Type=DO,
                Recipient_City="Miami",
                Recipient_State="FL",
            ),
            make_ownership_row(
                Physician_Profile_ID=pd.NA,
                Physician_Last_Name="OWNNULL",
                Physician_First_Name="X",
            ),
        ]
    )


def make_research_csv_df() -> pd.DataFrame:
    """Minimal research CSV: one row that overlaps with general scenario A
    and a new physician."""
    return pd.DataFrame(
        [
            make_research_row(
                Record_ID=2001,
                Covered_Recipient_Profile_ID=101,
                Covered_Recipient_Last_Name="ADAMS",
                Covered_Recipient_First_Name="JOHN",
                Covered_Recipient_Middle_Name="MICHAEL",
                Recipient_City="Manhattan",
                Recipient_State="NY",
            ),
            make_research_row(
                Record_ID=2002,
                Covered_Recipient_Profile_ID=701,
                Covered_Recipient_Last_Name="NGUYEN",
                Covered_Recipient_First_Name="LINH",
                Covered_Recipient_Middle_Name="T",
                Recipient_City="San Francisco",
                Recipient_State="CA",
            ),
        ]
    )


# ---------------------------------------------------------------------------
# Conflicted-provider factories.
#
# This shape mirrors the docstring at ids.py:182-204 — once Section 7 lands a
# pydantic ConflictedProviderInput schema, this should produce instances of it.
# ---------------------------------------------------------------------------

_CONFLICTED_DEFAULTS: dict[str, Any] = {
    "provider_pk": 0,
    "first_name": "JOHN",
    "last_name": "DOE",
    "middle_initial_1": "Q",
    # Missing middle-name fields are represented as None (not ""): the matcher
    # uses pd.notna() to gate its branches, and pd.notna("") is True — so empty
    # strings would slip through and crash on [0] indexing. Section 5 fixes the
    # matcher to be empty-string safe; until then, callers must use None.
    "middle_initial_2": None,
    "middle_name_1": None,
    "middle_name_2": None,
    "credentials": [Credentials.MEDICAL_DOCTOR],
    "specialtys": [],
    "citystates": [],
}


def make_conflicted_row(**overrides: Any) -> dict[str, Any]:
    """Build a single conflicted-provider row (dict). Override any field."""
    row = {**_CONFLICTED_DEFAULTS, **overrides}
    return row


# ---------------------------------------------------------------------------
# Raw (pre-Conflicteds) input factories — what a child app's scraped/tabulated
# data looks like BEFORE running through the Conflicteds preprocessing pipeline.
# The exact column set depends on the child app; the defaults below mirror the
# uptodate_conflicts shape (closest to canonical) so subclasses only override
# the columns that differ.
# ---------------------------------------------------------------------------


_RAW_CONFLICTED_DEFAULTS: dict[str, Any] = {
    "name": "John Q. Smith, MD",
    "credential": "Physician (MD or DO)",
    "specialtys": "Family Medicine",
    "citystates": "Manhattan, NY",
    "npi": None,  # NPI is optional in conflicted input; default to missing.
    "non_us": pd.NA,
    # Provenance columns Conflicteds drops at the end; populated so the
    # current orchestrator (which drops them by name) doesn't blow up.
    "article": "Sample Article",
    "rank": 1,
    "entity": "Sample Entity",
}


def make_raw_conflicted_row(**overrides: Any) -> dict[str, Any]:
    """Build a single pre-Conflicteds raw row (dict). Override any column."""
    return {**_RAW_CONFLICTED_DEFAULTS, **overrides}


def make_canonical_conflicteds_df() -> pd.DataFrame:
    """Conflicted-providers DataFrame that exercises every scenario covered
    by the general CSV (A-F) plus one no-match case (NOLASTNAME)."""
    return pd.DataFrame(
        [
            # A: should uniquely match profile 101
            make_conflicted_row(
                provider_pk=1,
                first_name="JOHN",
                last_name="ADAMS",
                middle_initial_1="M",
                middle_name_1="MICHAEL",
            ),
            # B: should match 201 (Jane Brown), not 202 (Jonathan Brown)
            make_conflicted_row(
                provider_pk=2,
                first_name="JANE",
                last_name="BROWN",
                middle_initial_1="M",
                middle_name_1="MARIE",
            ),
            # C: should match 301 (David Andrew Smith), not 302 (David Brandon Smith)
            make_conflicted_row(
                provider_pk=3,
                first_name="DAVID",
                last_name="SMITH",
                middle_initial_1="A",
            ),
            # D: hyphenated last name
            make_conflicted_row(
                provider_pk=4,
                first_name="HANNAH",
                last_name="SMITH-JONES",
                middle_initial_1="L",
                middle_name_1="LEE",
            ),
            # E: ambiguous - two Emily Whites, no distinguishing info
            make_conflicted_row(
                provider_pk=5,
                first_name="EMILY",
                last_name="WHITE",
                middle_initial_1=None,
            ),
            # X: no match anywhere
            make_conflicted_row(
                provider_pk=6,
                first_name="NOBODY",
                last_name="NONEXISTENT",
                middle_initial_1="X",
            ),
        ]
    )
