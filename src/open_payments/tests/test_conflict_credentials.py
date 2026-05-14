"""Tests for credential parsing + the ConflictCredentials default mixin.

Covers:
  - parse_credentials_from_name: trailing-degree extraction, period tolerance,
    hyphenated aliases, deduplication
  - parse_credential_token: composite handling, alias and full-value matching,
    unknown / blank inputs
  - ConflictCredentials default behavior on the uptodate-shape input (name +
    credential columns), with a small subclass-override example mirroring
    deans's boolean MD column.
"""

from __future__ import annotations

import pandas as pd
import pytest

from ..choices import Credentials
from ..credentials import (
    CREDENTIAL_ALIASES,
    ConflictCredentials,
    parse_credential_token,
    parse_credentials_from_name,
)
from .factories import make_raw_conflicted_row

MD = Credentials.MEDICAL_DOCTOR
DO = Credentials.DOCTOR_OF_OSTEOPATHY
PA = Credentials.PHYSICIAN_ASSISTANT
NP = Credentials.NURSE_PRACTITIONER
RN = Credentials.REGISTERED_NURSE
CRNA = Credentials.CERTIFIED_REGISTERED_NURSE_ANAESTHETIST
AA = Credentials.ANESTHESIOLOGIST_ASSISTANT


# ---------------------------------------------------------------------------
# parse_credentials_from_name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Smith MD", [MD]),
        ("Smith, MD", [MD]),
        ("Smith M.D.", [MD]),
        ("Jones, M.D., FACP", [MD]),  # FACP not in alias table -> ignored
        ("Doe, MD, PhD", [MD]),
        ("Carter DO", [DO]),
        ("Wilson, PA-C", [PA]),
        ("Garcia NP", [NP]),
        ("Nguyen CRNA", [CRNA]),
        ("Patel MBBS", [MD]),  # MBBS is an MD alias
        ("Lee RN", [RN]),
        ("Anna Apple, AA", [AA]),  # token-based: "Anna" doesn't match AA
        ("Plain Name", []),
        ("", []),
        (None, []),
    ],
)
def test__parse_credentials_from_name(name, expected):
    assert parse_credentials_from_name(name) == expected


def test__parse_credentials_from_name_deduplicates():
    assert parse_credentials_from_name("Smith MD, MD") == [MD]


def test__parse_credentials_from_name_multiple_distinct():
    # Provider with two real credentials -> both reported, order preserved.
    assert parse_credentials_from_name("Smith MD, PA") == [MD, PA]


# ---------------------------------------------------------------------------
# parse_credential_token
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("MD", [MD]),
        ("M.D.", [MD]),
        ("md", [MD]),  # case-insensitive
        ("Medical Doctor", [MD]),
        ("medical doctor", [MD]),
        ("DO", [DO]),
        ("Doctor of Osteopathy", [DO]),
        ("MBBS", [MD]),
        ("RN", [RN]),
        ("Registered Nurse", [RN]),
        ("PA", [PA]),
        ("PA-C", [PA]),
        ("CRNA", [CRNA]),
        ("Certified Registered Nurse Anesthetist", [CRNA]),
        ("Physician (MD or DO)", [MD, DO]),
        ("Physician (DO or MD)", [MD, DO]),  # reversed-order composite
        ("Unknown Degree", []),
        ("", []),
        (None, []),
    ],
)
def test__parse_credential_token(raw, expected):
    assert parse_credential_token(raw) == expected


def test__all_aliases_resolve_to_their_credential():
    # Sanity: every alias in the table parses to its credential.
    for cred, aliases in CREDENTIAL_ALIASES.items():
        for alias in aliases:
            assert parse_credential_token(alias) == [cred], (
                f"{alias!r} should parse to {cred}, got {parse_credential_token(alias)}"
            )


def test__every_credentials_enum_value_parseable():
    # The full descriptive value of every Credentials enum entry round-trips.
    for cred in Credentials:
        assert parse_credential_token(cred.value) == [cred]


# ---------------------------------------------------------------------------
# ConflictCredentials default mixin (uptodate-shape input)
# ---------------------------------------------------------------------------


def _build(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([make_raw_conflicted_row(**r) for r in rows])


def test__conflict_credentials_narrows_wildcard_with_name_md():
    # "Physician (MD or DO)" + trailing "MD" -> narrow to MD only.
    df = _build([{"name": "Smith MD", "credential": "Physician (MD or DO)"}])
    out = ConflictCredentials(df).conflict_credentials()
    assert out.iloc[0]["credentials"] == [MD]
    assert "credential" not in out.columns


def test__conflict_credentials_narrows_wildcard_with_name_do():
    df = _build([{"name": "Carter DO", "credential": "Physician (MD or DO)"}])
    out = ConflictCredentials(df).conflict_credentials()
    assert out.iloc[0]["credentials"] == [DO]


def test__conflict_credentials_keeps_wildcard_when_name_ambiguous():
    # Wildcard with no trailing degree -> keep both.
    df = _build([{"name": "Plain Name", "credential": "Physician (MD or DO)"}])
    out = ConflictCredentials(df).conflict_credentials()
    assert out.iloc[0]["credentials"] == [MD, DO]


def test__conflict_credentials_specific_credential_wins():
    df = _build([{"name": "Wilson, PA-C", "credential": "PA"}])
    out = ConflictCredentials(df).conflict_credentials()
    assert out.iloc[0]["credentials"] == [PA]


def test__conflict_credentials_combines_specific_with_name_suffix():
    # Structured column is specific; name has additional credential -> union.
    df = _build([{"name": "Smith MD, PA-C", "credential": "MD"}])
    out = ConflictCredentials(df).conflict_credentials()
    assert out.iloc[0]["credentials"] == [MD, PA]


def test__conflict_credentials_falls_back_to_name_only():
    # No credential column populated, just a trailing degree in name.
    df = _build([{"name": "Jones DO", "credential": ""}])
    out = ConflictCredentials(df).conflict_credentials()
    assert out.iloc[0]["credentials"] == [DO]


def test__conflict_credentials_returns_none_when_no_signals():
    df = _build([{"name": "Anonymous", "credential": ""}])
    out = ConflictCredentials(df).conflict_credentials()
    assert out.iloc[0]["credentials"] is None


def test__conflict_credentials_drops_credential_column_when_present():
    df = _build([{"name": "Smith MD", "credential": "MD"}])
    out = ConflictCredentials(df).conflict_credentials()
    assert "credential" not in out.columns
    assert "name" in out.columns  # ConflictNames drops `name` later


def test__conflict_credentials_handles_missing_credential_column():
    # Some child apps (e.g. abim) have only `name`, no `credential` column.
    df = pd.DataFrame([{"name": "Smith MBBS"}])
    out = ConflictCredentials(df).conflict_credentials()
    assert out.iloc[0]["credentials"] == [MD]


# ---------------------------------------------------------------------------
# Subclass override example — deans-style boolean MD column
# ---------------------------------------------------------------------------


class _DeansLikeConflictCredentials(ConflictCredentials):
    """Minimal deans-style override: the input has a boolean `MD` column
    instead of a `credential` column. A populated cell -> [MD]; blank/NA -> []."""

    @classmethod
    def get_credentials(cls, conflict):
        if "MD" in conflict.index and pd.notna(conflict["MD"]) and str(conflict["MD"]).strip():
            return [Credentials.MEDICAL_DOCTOR]
        return None


def test__subclass_can_override_for_boolean_column():
    df = pd.DataFrame(
        [
            {"name": "Smith", "MD": "MD"},
            {"name": "Jones", "MD": ""},
            {"name": "Patel", "MD": pd.NA},
        ]
    )
    out = _DeansLikeConflictCredentials(df).conflict_credentials()
    assert out.iloc[0]["credentials"] == [MD]
    assert out.iloc[1]["credentials"] is None
    assert out.iloc[2]["credentials"] is None
