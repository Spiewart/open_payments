"""Payment-class column-differential audit (Section 5.5 sub-audit).

CMS publishes different column shapes per payment class:

| Payment class | Specialty cols | Credential cols | License-state cols |
|---------------|----------------|-----------------|--------------------|
| general       | 6              | 6               | 5                  |
| research      | 6              | 6               | 5                  |
| ownership     | 1              | 1               | 0                  |

Each CMS-side mixin handles the difference via an ``update_ownership_payments``
override that pads the missing columns with ``None`` before the aggregator
runs. This test file locks in that pattern by exercising the full read +
aggregate pipeline against each payment class's fixture and verifying the
post-aggregation shape is consistent.

Why this exists: the padding pattern is implicit coupling — a new mixin
author could easily forget to add the override and ownership would silently
break. These tests fail loudly if that happens. The longer-term fix (Section
5.5 TODO) is a column-count-aware aggregator that doesn't need padding; for
now, this test set guards the existing pattern.
"""

from __future__ import annotations

from ..ids import PaymentIDs


def _load(cms_data_dir, fixture_years, payment_class):
    p = PaymentIDs(
        years=fixture_years,
        payment_classes=payment_class,
        payments_folder=str(cms_data_dir),
        nrows=None,
        MD_DO_only=True,
    )
    return p.all_payments()


# ---------------------------------------------------------------------------
# General — the reference shape, 6 specialty/credential cols + 5 license states
# ---------------------------------------------------------------------------


def test__general_aggregation_produces_canonical_columns(cms_data_dir, fixture_years):
    df = _load(cms_data_dir, fixture_years, "general")
    for col in (
        "profile_id",
        "npi",
        "first_name",
        "last_name",
        "credentials",
        "specialtys",
        "citystates",
    ):
        assert col in df.columns, f"general: missing canonical column {col}"
    # Individual specialty_*/credential_*/state_license_* columns are dropped
    # by the aggregator after they're combined into the list columns.
    for dropped in ("specialty_1", "credential_1", "state_license_1"):
        assert dropped not in df.columns


# ---------------------------------------------------------------------------
# Ownership — only 1 specialty col, 1 credential col, 0 license-state cols
# in raw CMS. The padding pattern brings it to the same canonical shape.
# ---------------------------------------------------------------------------


def test__ownership_aggregation_produces_same_canonical_shape(cms_data_dir, fixture_years):
    """Ownership has 1 raw specialty col + 1 raw credential col + 0 license-state
    cols in CMS. After the padding chain (`update_ownership_payments` in each
    mixin) + the aggregators, the output shape MUST match general's
    canonical shape so the matcher can treat all payment classes uniformly.
    """
    df = _load(cms_data_dir, fixture_years, "ownership")
    for col in (
        "profile_id",
        "npi",
        "first_name",
        "last_name",
        "credentials",
        "specialtys",
        "citystates",
    ):
        assert col in df.columns, f"ownership: missing canonical column {col}"
    # Pre-aggregation columns must be cleaned up.
    for dropped in (
        "specialty_1",
        "specialty_2",
        "specialty_3",
        "specialty_4",
        "specialty_5",
        "specialty_6",
        "credential_1",
        "credential_2",
        "credential_3",
        "credential_4",
        "credential_5",
        "credential_6",
        "state_license_1",
        "state_license_2",
        "state_license_3",
        "state_license_4",
        "state_license_5",
        "city",
        "state_primary",
    ):
        assert dropped not in df.columns, (
            f"ownership: pre-aggregation column {dropped} should have been dropped"
        )


def test__ownership_npi_renamed_from_physician_npi(cms_data_dir, fixture_years):
    """The CMS ownership column is `Physician_NPI` (not `Covered_Recipient_NPI`).
    The NPIMixin's ownership_columns mapping handles the rename to `npi`. This
    test pins that contract."""
    df = _load(cms_data_dir, fixture_years, "ownership")
    assert "npi" in df.columns
    assert "Physician_NPI" not in df.columns
    # The fixture sets distinct NPIs per row; verify they came through.
    non_null = df["npi"].dropna()
    assert len(non_null) >= 1, "ownership: expected at least one row with NPI"


def test__ownership_credentials_aggregator_handles_single_column_input(cms_data_dir, fixture_years):
    """Ownership only has 1 source credential column. The aggregator (which
    iterates credential_1..6) only sees content in credential_1 — the others
    are padded with None and contribute nothing. Output should still be a
    list[Credentials]."""
    df = _load(cms_data_dir, fixture_years, "ownership")
    assert "credentials" in df.columns
    # Every row should produce a list (possibly empty).
    for value in df["credentials"]:
        assert isinstance(value, list), f"ownership: expected list[Credentials], got {type(value)}"


def test__ownership_specialtys_aggregator_handles_single_column_input(cms_data_dir, fixture_years):
    """Same as credentials but for specialties."""
    df = _load(cms_data_dir, fixture_years, "ownership")
    assert "specialtys" in df.columns
    for value in df["specialtys"]:
        assert isinstance(value, list), f"ownership: expected list[Specialtys], got {type(value)}"


def test__ownership_citystates_aggregator_handles_missing_license_state_cols(
    cms_data_dir, fixture_years
):
    """Ownership has NO `state_license_*` columns in raw CMS data. The padding
    pattern adds them as None before `create_citystates` iterates them, so
    the aggregator emits a single-element list with the `state_primary` value.
    """
    df = _load(cms_data_dir, fixture_years, "ownership")
    assert "citystates" in df.columns
    # First row in fixture is ADAMS at Manhattan, NY.
    adams = df[df["last_name"] == "ADAMS"].iloc[0]
    assert isinstance(adams["citystates"], list)
    assert len(adams["citystates"]) == 1
    assert adams["citystates"][0].city == "Manhattan"
    assert adams["citystates"][0].state == "NY"


# ---------------------------------------------------------------------------
# Research — same 6/6/5 shape as general (per-PI blocks are deferred to 5.9)
# ---------------------------------------------------------------------------


def test__research_aggregation_produces_canonical_columns(cms_data_dir, fixture_years):
    df = _load(cms_data_dir, fixture_years, "research")
    for col in (
        "profile_id",
        "npi",
        "first_name",
        "last_name",
        "credentials",
        "specialtys",
        "citystates",
    ):
        assert col in df.columns, f"research: missing canonical column {col}"


# ---------------------------------------------------------------------------
# Cross-class consistency
# ---------------------------------------------------------------------------


def test__all_three_classes_have_identical_canonical_columns(cms_data_dir, fixture_years):
    """The whole point of the padding pattern is that the matcher can iterate
    over any payment-class result without per-class branching. The post-
    aggregation column sets MUST be identical across classes (modulo
    payment_class-specific columns that legitimately differ like
    payment_amount, value_of_interest, etc.).
    """
    cols_general = set(_load(cms_data_dir, fixture_years, "general").columns)
    cols_ownership = set(_load(cms_data_dir, fixture_years, "ownership").columns)
    cols_research = set(_load(cms_data_dir, fixture_years, "research").columns)

    canonical = {
        "profile_id",
        "npi",
        "first_name",
        "last_name",
        "middle_name",
        "credentials",
        "specialtys",
        "citystates",
        "name_suffix",
        "payment_class",
    }
    assert canonical.issubset(cols_general)
    assert canonical.issubset(cols_ownership)
    assert canonical.issubset(cols_research)
