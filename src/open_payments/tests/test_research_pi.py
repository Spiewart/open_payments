"""Tests for Section 5.9 — Research-CSV Principal Investigator block handling.

What's pinned here:

  - The ``explode_research_pi_blocks`` transform produces 1 + N rows per
    input row (1 Covered_Recipient + N populated PI slots).
  - The ``person_slot`` provenance column is always populated post-explode.
  - PI block columns are renamed to their Covered_Recipient equivalents so
    downstream code sees uniform column names.
  - End-to-end: a conflicted who matches a PI (not the Covered_Recipient)
    on a research payment is now matched correctly. Previously this would
    silently miss.
  - The Trial Coordinator (a Covered_Recipient who's not in any conflicted
    list) gets correctly NOT matched, while the PIs (Sarah Kim, Raj Patel)
    DO get matched via their PI slots.
  - PI block columns are tolerant of being absent (e.g. older CMS years).

Fixture geometry (synthetic research CSV):
  - Record 2001: Adams (CR=101, no PIs)
  - Record 2002: Nguyen (CR=701, no PIs)
  - Record 2003: Trial Coordinator (CR=999) + PI_1=Sarah Kim (801) +
    PI_2=Raj Patel (802) — all three at Boston, MA, Internal Medicine
"""

from __future__ import annotations

import pandas as pd

from ..research_pi import (
    COVERED_RECIPIENT_SLOT,
    CR_TO_PI_SUFFIX,
    RESEARCH_PI_SLOTS,
    explode_research_pi_blocks,
    pi_block_cms_columns_for_dtype_dict,
)

# ---------------------------------------------------------------------------
# pi_block_cms_columns_for_dtype_dict
# ---------------------------------------------------------------------------


def test__pi_block_columns_expand_to_5_slots():
    cr_cols = {"Covered_Recipient_NPI": ("npi", "Int64")}
    pi_cols = pi_block_cms_columns_for_dtype_dict(cr_cols)
    # 5 slots × 1 CR column = 5 PI columns.
    assert len(pi_cols) == 5
    assert "Principal_Investigator_1_NPI" in pi_cols
    assert "Principal_Investigator_5_NPI" in pi_cols


def test__pi_block_columns_use_slot_prefixed_canonical():
    """Slot-prefixed canonical names avoid collision with the CR canonical
    until the explode renames them away."""
    cr_cols = {"Covered_Recipient_Last_Name": ("last_name", str)}
    pi_cols = pi_block_cms_columns_for_dtype_dict(cr_cols)
    assert pi_cols["Principal_Investigator_1_Last_Name"] == ("pi_1_last_name", str)
    assert pi_cols["Principal_Investigator_3_Last_Name"] == ("pi_3_last_name", str)


def test__pi_block_columns_handle_recipient_city_state_prefix():
    """Covered_Recipient uses bare ``Recipient_<X>`` for city/state; PI
    uses ``Principal_Investigator_N_<X>``. Verify the suffix mapping
    knows about this asymmetry."""
    cr_cols = {"Recipient_City": ("city", str), "Recipient_State": ("state", str)}
    pi_cols = pi_block_cms_columns_for_dtype_dict(cr_cols)
    assert "Principal_Investigator_1_City" in pi_cols
    assert "Principal_Investigator_1_State" in pi_cols


def test__pi_block_columns_skip_unmapped_cr_columns():
    """Mixin's research_columns may include CR columns we don't have a PI
    mapping for (theoretical case — none of the current 5 mixins hit this).
    Such columns should be silently skipped, not error."""
    cr_cols = {"Some_Unknown_Column": ("unknown", str)}
    pi_cols = pi_block_cms_columns_for_dtype_dict(cr_cols)
    assert pi_cols == {}


# ---------------------------------------------------------------------------
# explode_research_pi_blocks
# ---------------------------------------------------------------------------


def _wide_research_row(
    cr_profile_id=101,
    cr_last_name="ADAMS",
    pi_1_profile_id=None,
    pi_1_last_name=None,
    pi_2_profile_id=None,
    pi_2_last_name=None,
) -> dict:
    """Build a single wide research-CSV row dict."""
    return {
        "Covered_Recipient_Profile_ID": cr_profile_id,
        "Covered_Recipient_Last_Name": cr_last_name,
        "Principal_Investigator_1_Profile_ID": pi_1_profile_id,
        "Principal_Investigator_1_Last_Name": pi_1_last_name,
        "Principal_Investigator_2_Profile_ID": pi_2_profile_id,
        "Principal_Investigator_2_Last_Name": pi_2_last_name,
    }


def test__explode_no_pi_columns_returns_input_with_slot_label():
    """Frame with no PI columns at all (general/ownership shape) is returned
    unchanged except for the person_slot column."""
    df = pd.DataFrame(
        [{"Covered_Recipient_Profile_ID": 101, "Covered_Recipient_Last_Name": "ADAMS"}]
    )
    out = explode_research_pi_blocks(df)
    assert len(out) == 1
    assert out.iloc[0]["person_slot"] == COVERED_RECIPIENT_SLOT
    assert out.iloc[0]["Covered_Recipient_Last_Name"] == "ADAMS"


def test__explode_pi_columns_present_but_all_null_returns_one_cr_row():
    """A row with PI columns present but all null (no PI populated) explodes
    to exactly one row (Covered_Recipient slot). PI sub-rows get dropped
    because their profile_id is null."""
    df = pd.DataFrame([_wide_research_row()])
    out = explode_research_pi_blocks(df)
    assert len(out) == 1
    assert out.iloc[0]["person_slot"] == COVERED_RECIPIENT_SLOT


def test__explode_pi_1_populated_gives_cr_plus_pi_1():
    """A row with PI_1 populated → 2 sub-rows (CR + pi_1)."""
    df = pd.DataFrame([_wide_research_row(pi_1_profile_id=801, pi_1_last_name="KIM")])
    out = explode_research_pi_blocks(df)
    assert len(out) == 2
    slots = set(out["person_slot"])
    assert slots == {COVERED_RECIPIENT_SLOT, "pi_1"}


def test__explode_pi_1_and_pi_2_populated_gives_three_rows():
    """1 CR + 2 PIs → 3 sub-rows."""
    df = pd.DataFrame(
        [
            _wide_research_row(
                pi_1_profile_id=801,
                pi_1_last_name="KIM",
                pi_2_profile_id=802,
                pi_2_last_name="PATEL",
            )
        ]
    )
    out = explode_research_pi_blocks(df)
    assert len(out) == 3
    assert set(out["person_slot"]) == {COVERED_RECIPIENT_SLOT, "pi_1", "pi_2"}


def test__explode_pi_columns_renamed_to_cr_equivalents():
    """After explode, PI sub-rows use the Covered_Recipient_* CMS column
    names — the whole point is that downstream sees uniform columns."""
    df = pd.DataFrame([_wide_research_row(pi_1_profile_id=801, pi_1_last_name="KIM")])
    out = explode_research_pi_blocks(df)

    pi_row = out[out["person_slot"] == "pi_1"].iloc[0]
    # The PI sub-row's Covered_Recipient_Last_Name is "KIM" (from PI_1), NOT "ADAMS".
    assert pi_row["Covered_Recipient_Last_Name"] == "KIM"
    assert pi_row["Covered_Recipient_Profile_ID"] == 801

    cr_row = out[out["person_slot"] == COVERED_RECIPIENT_SLOT].iloc[0]
    assert cr_row["Covered_Recipient_Last_Name"] == "ADAMS"
    assert cr_row["Covered_Recipient_Profile_ID"] == 101


def test__explode_drops_unpopulated_slot_subrows():
    """PI_2 has profile_id null → no pi_2 sub-row in output."""
    df = pd.DataFrame([_wide_research_row(pi_1_profile_id=801, pi_1_last_name="KIM")])
    out = explode_research_pi_blocks(df)
    assert "pi_2" not in set(out["person_slot"])


def test__explode_constants_pinned():
    """Sanity: API constants used by other modules are stable."""
    assert RESEARCH_PI_SLOTS == (1, 2, 3, 4, 5)
    assert COVERED_RECIPIENT_SLOT == "covered_recipient"
    # Spot-check a few key entries in the suffix map.
    assert CR_TO_PI_SUFFIX["Covered_Recipient_NPI"] == "NPI"
    assert CR_TO_PI_SUFFIX["Recipient_City"] == "City"


# ---------------------------------------------------------------------------
# End-to-end: conflicted matches via PI slot
# ---------------------------------------------------------------------------


def _load_research_payments(cms_data_dir, fixture_years):
    from ..ids import PaymentIDs

    return PaymentIDs(
        years=fixture_years,
        payment_classes="research",
        payments_folder=str(cms_data_dir),
        nrows=None,
        MD_DO_only=True,
    ).all_payments()


def test__research_csv_load_explodes_per_slot(cms_data_dir, fixture_years):
    """Reading the synthetic research CSV produces 4 rows from 3 input
    rows: Adams (CR), Nguyen (CR), Trial Coordinator (CR), Sarah Kim (pi_1
    of record 2003), Raj Patel (pi_2 of record 2003)."""
    payments = _load_research_payments(cms_data_dir, fixture_years)
    assert "person_slot" in payments.columns
    counts = payments["person_slot"].value_counts()
    # 3 covered_recipient + 1 pi_1 + 1 pi_2 = 5 rows total.
    assert counts[COVERED_RECIPIENT_SLOT] == 3
    assert counts["pi_1"] == 1
    assert counts["pi_2"] == 1


def test__pi_subrows_use_pi_profile_id(cms_data_dir, fixture_years):
    """The pi_1 sub-row of record 2003 should have profile_id=801 (Kim's),
    not 999 (Trial Coordinator's)."""
    payments = _load_research_payments(cms_data_dir, fixture_years)
    pi_1 = payments[payments["person_slot"] == "pi_1"]
    assert len(pi_1) == 1
    assert pi_1.iloc[0]["profile_id"] == 801
    assert pi_1.iloc[0]["last_name"] == "KIM"


def test__conflicted_matches_via_pi_slot(cms_data_dir, fixture_years):
    """The critical Section 5.9 test: a conflicted who's a PI but not the
    Covered_Recipient now resolves to their profile_id. Previously silently
    missed."""
    from .. import Settings, find_payments_for_conflicted_providers
    from .factories import make_raw_conflicted_row

    raw = pd.DataFrame(
        [
            # Sarah Kim is PI_1 on record 2003. Should match profile_id=801.
            make_raw_conflicted_row(
                name="Sarah J. Kim, MD",
                credential="Physician (MD or DO)",
                specialtys="Internal Medicine",
                citystates="Boston, MA",
            ),
        ]
    )
    settings = Settings(data_dir=cms_data_dir, years=fixture_years, payment_classes=["research"])
    result = find_payments_for_conflicted_providers(conflicteds=raw, settings=settings)

    assert result.n_unique == 1
    assert result.unique_ids.iloc[0]["profile_id"] == 801
    # Provenance: the matching row came from the pi_1 slot, not covered_recipient.
    assert result.unique_ids.iloc[0]["person_slot"] == "pi_1"


def test__second_pi_also_matchable(cms_data_dir, fixture_years):
    """Both PI slots are independently matchable. Raj Patel = PI_2 on
    record 2003 → profile_id=802."""
    from .. import Settings, find_payments_for_conflicted_providers
    from .factories import make_raw_conflicted_row

    raw = pd.DataFrame(
        [
            make_raw_conflicted_row(
                name="Raj K. Patel, MD",
                credential="Physician (MD or DO)",
                specialtys="Internal Medicine",
                citystates="Boston, MA",
            ),
        ]
    )
    settings = Settings(data_dir=cms_data_dir, years=fixture_years, payment_classes=["research"])
    result = find_payments_for_conflicted_providers(conflicteds=raw, settings=settings)

    assert result.n_unique == 1
    assert result.unique_ids.iloc[0]["profile_id"] == 802
    assert result.unique_ids.iloc[0]["person_slot"] == "pi_2"


def test__covered_recipient_match_still_works_alongside_pi_handling(cms_data_dir, fixture_years):
    """Section 5.9 must NOT regress existing Covered_Recipient matching.
    Adams should still match profile_id=101 via the covered_recipient slot."""
    from .. import Settings, find_payments_for_conflicted_providers
    from .factories import make_raw_conflicted_row

    raw = pd.DataFrame(
        [
            make_raw_conflicted_row(
                name="John M. Adams, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Manhattan, NY",
            ),
        ]
    )
    settings = Settings(data_dir=cms_data_dir, years=fixture_years, payment_classes=["research"])
    result = find_payments_for_conflicted_providers(conflicteds=raw, settings=settings)

    assert result.n_unique == 1
    assert result.unique_ids.iloc[0]["profile_id"] == 101
    assert result.unique_ids.iloc[0]["person_slot"] == COVERED_RECIPIENT_SLOT


def test__missing_pi_columns_in_csv_handled_gracefully(cms_data_dir, fixture_years):
    """The general and ownership CSV fixtures don't have PI block columns
    (correctly — those payment classes don't have PIs). Loading them must
    work even though the read code requests PI columns via research_columns
    expansion. The usecols filter in update_csv_kwargs handles missing
    columns gracefully."""
    from ..ids import PaymentIDs

    # Loading general (no PI columns in CSV) should not raise.
    p = PaymentIDs(
        years=fixture_years,
        payment_classes=["general"],
        payments_folder=str(cms_data_dir),
        nrows=None,
        MD_DO_only=True,
    )
    general = p.read_payments_csvs("general")
    assert not general.empty


def test__matching_payment_id_in_two_slots_if_both_match(cms_data_dir, fixture_years):
    """If TWO conflicteds match the SAME research payment via different
    PI slots, both get their own match. Sarah Kim and Raj Patel are both
    PIs on record 2003; both should be matched."""
    from .. import Settings, find_payments_for_conflicted_providers
    from .factories import make_raw_conflicted_row

    raw = pd.DataFrame(
        [
            make_raw_conflicted_row(
                name="Sarah J. Kim, MD",
                credential="Physician (MD or DO)",
                specialtys="Internal Medicine",
                citystates="Boston, MA",
            ),
            make_raw_conflicted_row(
                name="Raj K. Patel, MD",
                credential="Physician (MD or DO)",
                specialtys="Internal Medicine",
                citystates="Boston, MA",
            ),
        ]
    )
    settings = Settings(data_dir=cms_data_dir, years=fixture_years, payment_classes=["research"])
    result = find_payments_for_conflicted_providers(conflicteds=raw, settings=settings)

    assert result.n_unique == 2
    pk_to_profile = dict(
        zip(result.unique_ids["provider_pk"], result.unique_ids["profile_id"], strict=True)
    )
    assert set(pk_to_profile.values()) == {801, 802}
