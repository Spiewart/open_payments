"""End-to-end smoke test: raw conflicted input -> Conflicteds orchestrator ->
ConflictedPaymentIDs matcher -> match outcomes against the synthetic fixture.

This is the first test that exercises the FULL pipeline including the
``Conflicteds`` class (Section 5 bug 0 — newly importable now that all four
ConflictX mixins plus ConflictNPI exist).

Surfaces any remaining orchestrator-level bugs:
  - bug 1: set_index discarded at conflicteds.py:70
  - bug 0d: SettingWithCopyWarning at ids.py:123-127
  - bug 2: non-deterministic drop_duplicates at ids.py:316-320
  - column-presence assumptions
  - dtype mismatches between conflicted-side parsing and CMS-side reads
"""

from __future__ import annotations

import warnings

import pandas as pd

from ..choices import PaymentFilters
from ..conflicteds import Conflicteds
from ..ids import ConflictedPaymentIDs, PaymentIDs
from .factories import make_raw_conflicted_row


def _raw_scenarios() -> pd.DataFrame:
    """Six raw conflicted-provider rows mapping to fixture scenarios A-F.

    Each row mimics what a child app would feed into ``Conflicteds`` after
    scraping / tabulating its source data.
    """
    return pd.DataFrame(
        [
            # A: should uniquely match profile 101 (Adams / John / Michael)
            make_raw_conflicted_row(
                name="John M. Adams, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Manhattan, NY",
                npi=None,
            ),
            # B: should match 201 (Jane Marie Brown), not 202 (Jonathan)
            make_raw_conflicted_row(
                name="Jane Marie Brown, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Boston, MA",
                npi=None,
            ),
            # C: should match 301 (David Andrew Smith) via middle initial A
            make_raw_conflicted_row(
                name="David A. Smith, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Seattle, WA",
                npi=None,
            ),
            # D: hyphenated last name + middle name; should match 401
            make_raw_conflicted_row(
                name="Hannah Lee Smith-Jones, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="San Diego, CA",
                npi=None,
            ),
            # E: ambiguous — two Emily Whites in CMS, no distinguishing info
            make_raw_conflicted_row(
                name="Emily White, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Chicago, IL",
                npi=None,
            ),
            # X: no CMS match anywhere
            make_raw_conflicted_row(
                name="Nobody Nonexistent, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Nowheresville, NY",
                npi=None,
            ),
        ]
    )


def _load_payments(cms_data_dir, fixture_years) -> pd.DataFrame:
    p = PaymentIDs(
        years=fixture_years,
        payment_classes="general",
        payments_folder=str(cms_data_dir),
        nrows=None,
        MD_DO_only=True,
    )
    return p.all_payments()


# ---------------------------------------------------------------------------
# Orchestrator: Conflicteds.us_conflicteds_id_search_df()
# ---------------------------------------------------------------------------


def test__conflicteds_orchestrator_produces_canonical_shape():
    """Raw input -> Conflicteds -> canonical 6-mixin output shape."""
    raw = _raw_scenarios()
    out = Conflicteds(raw).us_conflicteds_id_search_df()

    # Every mixin contributes its output column(s).
    expected = {
        "provider_pk",
        "first_name",
        "last_name",
        "middle_initial_1",
        "middle_initial_2",
        "middle_name_1",
        "middle_name_2",
        "name_suffix",
        "credentials",
        "specialtys",
        "citystates",
        "npi",
    }
    missing = expected - set(out.columns)
    assert not missing, f"missing canonical columns: {missing}"

    # Source columns dropped along the way.
    for dropped in ("name", "credential", "non_us", "article", "rank", "entity"):
        assert dropped not in out.columns, f"{dropped} should have been dropped"

    # Row count: 6 in, 6 out (no row should be filtered — all MDs).
    assert len(out) == 6


def test__conflicteds_orchestrator_parses_each_dimension_correctly():
    raw = _raw_scenarios()
    out = Conflicteds(raw).us_conflicteds_id_search_df()

    # Spot-check scenario A: Adams / John / M / Manhattan / Family Medicine.
    row_a = out.iloc[0]
    assert row_a["first_name"] == "John"
    assert row_a["last_name"] == "Adams"
    assert row_a["middle_initial_1"] == "M"
    assert len(row_a["credentials"]) > 0
    assert len(row_a["specialtys"]) == 1
    assert row_a["specialtys"][0].specialty == "Family Medicine"
    assert len(row_a["citystates"]) == 1
    assert row_a["citystates"][0].city == "Manhattan"
    assert row_a["citystates"][0].state == "NY"

    # NPI is None for all scenarios (raw input had npi=None).
    assert out["npi"].isna().all()


# ---------------------------------------------------------------------------
# Full pipeline: raw -> Conflicteds -> ConflictedPaymentIDs -> outcomes
# ---------------------------------------------------------------------------


def test__end_to_end_unique_matches_scenarios_A_B_C_D(cms_data_dir, fixture_years):
    """The four deterministic scenarios (A, B, C, D) should each resolve to
    a single unique CMS profile match."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    matcher = ConflictedPaymentIDs(conflicteds=conflicteds, payments=payments)
    matcher.search_for_conflicteds_ids()

    # 4 of 6 should land in unique_ids.
    assert len(matcher.unique_ids) == 4, (
        f"expected 4 unique matches, got {len(matcher.unique_ids)}: "
        f"{matcher.unique_ids[['provider_pk', 'profile_id']].to_dict('records')}"
    )

    # Verify each maps to the expected CMS profile.
    pk_to_profile = dict(
        zip(matcher.unique_ids["provider_pk"], matcher.unique_ids["profile_id"], strict=True)
    )
    assert pk_to_profile == {0: 101, 1: 201, 2: 301, 3: 401}


def test__end_to_end_ambiguous_and_nomatch_scenarios_E_X(cms_data_dir, fixture_years):
    """Emily White (E) is ambiguous (2 CMS candidates with same name+city).
    Nonexistent (X) has no last-name match anywhere."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    matcher = ConflictedPaymentIDs(conflicteds=conflicteds, payments=payments)
    matcher.search_for_conflicteds_ids()

    unmatched_pks = set(matcher.unmatched["provider_pk"].tolist())
    assert unmatched_pks == {4, 5}

    # E (provider_pk 4) — UNFILTERABLE: 2 candidates couldn't be narrowed.
    pk_e_row = matcher.unmatched[matcher.unmatched["provider_pk"] == 4].iloc[0]
    assert pk_e_row["unmatched"] == "UNFILTERABLE"

    # X (provider_pk 5) — NOLASTNAME: no CMS row matched on last_name.
    pk_x_row = matcher.unmatched[matcher.unmatched["provider_pk"] == 5].iloc[0]
    assert pk_x_row["unmatched"] == "NOLASTNAME"

    # Both Emily Whites land in unmatched_options.
    option_profiles = set(matcher.unmatched_options["profile_id"].tolist())
    assert option_profiles == {501, 502}


# ---------------------------------------------------------------------------
# Regression coverage for known bugs surfaced by this pipeline
# ---------------------------------------------------------------------------


def test__regression_bug_1_set_index_provider_pk_remains_a_column():
    """conflicteds.py:70 ``set_index("provider_pk")`` result is discarded
    (bug 1). Downstream code uses ``provider_pk`` as a column, so the bug is
    silent — but this test pins the current (column-not-index) invariant
    so a future "fix" to that line doesn't quietly change semantics."""
    raw = _raw_scenarios()
    out = Conflicteds(raw).us_conflicteds_id_search_df()
    assert "provider_pk" in out.columns
    # provider_pk should be a 0-indexed range over the rows that survived
    # remove_non_us + remove_non_md_do + middle-name dedupe.
    assert out["provider_pk"].tolist() == list(range(len(out)))


def test__section_5_8_scenario_c_david_brandon_accumulates_disagree(cms_data_dir, fixture_years):
    """Scenario C asks for ``David A. Smith``. CMS has two David Smiths:
      - 301 David ANDREW Smith → MIDDLE_INITIAL MATCH (A == A)
      - 302 David BRANDON Smith → MIDDLE_INITIAL DISAGREE (A != B)

    The selector picks 301 as the unique match, but the pre-dedupe merged
    frame must show ``PaymentFilters.MIDDLE_INITIAL`` in 302's
    ``negative_filters`` — that's the whole point of Section 5.8's tri-state.

    We intercept the merged frame before the selector sees it.
    """
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    captured: dict[str, pd.DataFrame] = {}

    class _Capturing(ConflictedPaymentIDs):
        def process_filtered_payments_x_conflicteds(self, payments_x_conflicted):
            if 302 in set(payments_x_conflicted.get("profile_id", pd.Series()).tolist()):
                captured["merged"] = payments_x_conflicted.copy()
            super().process_filtered_payments_x_conflicteds(payments_x_conflicted)

    matcher = _Capturing(conflicteds=conflicteds, payments=payments)
    matcher.search_for_conflicteds_ids()

    merged = captured.get("merged")
    assert merged is not None, "scenario C did not produce a merged frame containing profile_id 302"

    row_302 = merged[merged["profile_id"] == 302].iloc[0]
    assert PaymentFilters.MIDDLE_INITIAL in row_302["negative_filters"], (
        f"David Brandon Smith (302) should accumulate MIDDLE_INITIAL in "
        f"negative_filters (B != A); got {row_302['negative_filters']}"
    )

    row_301 = merged[merged["profile_id"] == 301].iloc[0]
    assert PaymentFilters.MIDDLE_INITIAL in row_301["filters"], (
        f"David Andrew Smith (301) should match MIDDLE_INITIAL (A == A); "
        f"got filters={row_301['filters']}"
    )
    assert PaymentFilters.MIDDLE_INITIAL not in row_301["negative_filters"]


def test__section_5_8_emily_white_missing_middle_name_is_no_data_not_disagree(
    cms_data_dir, fixture_years
):
    """Emily White (E) is ambiguous; both fixture rows have empty middle_name.
    The conflict input also has no middle name. Per Section 5.8, MIDDLENAME
    on these rows must yield ``FilterOutcome.NO_DATA`` (absent signal), NOT
    ``DISAGREE`` (active negative evidence). The two candidates therefore
    land in ``unmatched_options`` with empty ``negative_filters``.
    """
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    matcher = ConflictedPaymentIDs(conflicteds=conflicteds, payments=payments)
    matcher.search_for_conflicteds_ids()

    emily_options = matcher.unmatched_options[
        matcher.unmatched_options["profile_id"].isin([501, 502])
    ]
    assert len(emily_options) == 2, "expected both Emily White candidates"

    assert "negative_filters" in emily_options.columns, (
        "Section 5.8 must carry negative_filters through the dedupe+selector"
    )
    for _, row in emily_options.iterrows():
        assert row["negative_filters"] == [], (
            f"Emily White profile_id={row['profile_id']} accumulated "
            f"negative_filters={row['negative_filters']}; expected [] since "
            f"both sides lack middle name (NO_DATA, not DISAGREE)"
        )


def test__section_5_8_winning_rows_have_empty_negative_filters(cms_data_dir, fixture_years):
    """For scenarios A–D where a unique match wins outright, the winning row's
    ``negative_filters`` must be empty — the winner is the one that agreed on
    every dimension that had data on both sides."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    matcher = ConflictedPaymentIDs(conflicteds=conflicteds, payments=payments)
    matcher.search_for_conflicteds_ids()

    assert "negative_filters" in matcher.unique_ids.columns
    for _, row in matcher.unique_ids.iterrows():
        assert row["negative_filters"] == [], (
            f"unique match (provider_pk={row['provider_pk']}, "
            f"profile_id={row['profile_id']}) has negative_filters="
            f"{row['negative_filters']}; expected [] for a clean winner"
        )


def test__output_frames_carry_negative_filter_columns(cms_data_dir, fixture_years):
    """Section 5.8 + TieredConfidenceSelector v2: every output frame
    (unique_ids, unmatched, unmatched_options) must carry these columns so
    an analyst can review negative signals without re-deriving them:

      - ``negative_filters`` (list[PaymentFilters])
      - ``n_negative_filters`` (int — tally for sort/filter convenience)
      - ``confidence_tier`` (str | None — None when using the cascade
        selector; populated when using TieredConfidenceSelector)
    """
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    matcher = ConflictedPaymentIDs(conflicteds=conflicteds, payments=payments)
    matcher.search_for_conflicteds_ids()

    for frame_name, frame in [
        ("unique_ids", matcher.unique_ids),
        ("unmatched", matcher.unmatched),
        ("unmatched_options", matcher.unmatched_options),
    ]:
        for col in ("negative_filters", "n_negative_filters", "confidence_tier"):
            assert col in frame.columns, (
                f"{frame_name} is missing the {col} column required by "
                f"Section 5.8 / TieredConfidenceSelector v2"
            )

    # n_negative_filters values must agree with len(negative_filters) row-by-row.
    for _, row in matcher.unique_ids.iterrows():
        assert row["n_negative_filters"] == len(row["negative_filters"])


def test__tiered_selector_surfaces_confidence_tier_on_unique_ids(cms_data_dir, fixture_years):
    """When TieredConfidenceSelector is plugged in, every unique match gets
    a confidence_tier label."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    from ..selectors import TieredConfidenceSelector

    matcher = ConflictedPaymentIDs(
        conflicteds=conflicteds, payments=payments, selector=TieredConfidenceSelector()
    )
    matcher.search_for_conflicteds_ids()

    assert "confidence_tier" in matcher.unique_ids.columns
    # All 4 clean A/B/C/D scenarios should be at a real tier (not None).
    assert matcher.unique_ids["confidence_tier"].notna().all()


def test__regression_bug_0d_no_setting_with_copy_warnings(cms_data_dir, fixture_years):
    """Bug 0d FIXED: `add_unmatched` now copies the slice at entry, so
    SettingWithCopyWarning no longer fires on unmatched rows. End-to-end
    pipeline run must emit zero such warnings."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        matcher = ConflictedPaymentIDs(conflicteds=conflicteds, payments=payments)
        matcher.search_for_conflicteds_ids()

    scw = [w for w in caught if "SettingWithCopyWarning" in type(w.message).__name__]
    assert len(scw) == 0, (
        f"expected 0 SettingWithCopyWarning (bug 0d should stay fixed); "
        f"got {len(scw)}:\n  " + "\n  ".join(f"{w.filename}:{w.lineno}" for w in scw)
    )
