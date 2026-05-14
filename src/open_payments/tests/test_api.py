"""Tests for the public API surface (Section 7).

What's pinned here:
  - ``open_payments.find_payments_for_conflicted_providers`` accepts both
    raw and pre-parsed conflicteds DataFrames.
  - ``SearchResult`` carries the expected three DataFrames.
  - xlsx round-trip via ``SearchResult.to_excel`` / ``from_excel``.
  - ``SearchResult.update_excel`` upserts on provider_pk.
  - Schema validation raises a clear error when ``parse_conflicteds=False``
    but the input is missing required columns.
  - The top-level __init__ exports the documented public symbols.
"""

from __future__ import annotations

import pandas as pd
import pytest

from .. import (
    DefaultMatchSelector,
    IdentifierWinsSelector,
    SearchResult,
    Settings,
    TieredConfidenceSelector,
    find_payments_for_conflicted_providers,
    validate_conflicteds_df,
)
from ..choices import Unmatcheds
from .factories import make_raw_conflicted_row

# ---------------------------------------------------------------------------
# Fixtures (shared with test_end_to_end.py — duplicated here so test_api stays
# self-contained as the public-API contract test)
# ---------------------------------------------------------------------------


def _raw_scenarios() -> pd.DataFrame:
    return pd.DataFrame(
        [
            make_raw_conflicted_row(
                name="John M. Adams, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Manhattan, NY",
            ),
            make_raw_conflicted_row(
                name="Jane Marie Brown, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Boston, MA",
            ),
            make_raw_conflicted_row(
                name="Emily White, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Chicago, IL",
            ),
        ]
    )


# ---------------------------------------------------------------------------
# Top-level entrypoint: find_payments_for_conflicted_providers
# ---------------------------------------------------------------------------


def test__api_returns_searchresult_with_three_dataframes(cms_data_dir, fixture_years):
    settings = Settings(data_dir=cms_data_dir, years=fixture_years)
    result = find_payments_for_conflicted_providers(conflicteds=_raw_scenarios(), settings=settings)
    assert isinstance(result, SearchResult)
    assert isinstance(result.unique_ids, pd.DataFrame)
    assert isinstance(result.unmatched, pd.DataFrame)
    assert isinstance(result.unmatched_options, pd.DataFrame)


def test__api_auto_parses_raw_input(cms_data_dir, fixture_years):
    """Raw input (has `name`, `credential` columns) is auto-detected and
    parsed via Conflicteds."""
    settings = Settings(data_dir=cms_data_dir, years=fixture_years)
    raw = _raw_scenarios()  # raw shape
    result = find_payments_for_conflicted_providers(conflicteds=raw, settings=settings)
    # Adams/Brown should match.
    assert result.n_unique >= 2


def test__api_minimal_columns_input_matches_readme_example(cms_data_dir, fixture_years):
    """Pins the README example: a 4-column DataFrame (name / credential /
    specialtys / citystates) — no deans-style provenance columns (article,
    rank, entity, non_us) — must run end-to-end through the orchestrator.

    The Conflicteds orchestrator was historically tied to the deans schema
    and would KeyError on `non_us` and on the article/rank/entity drop.
    Section 8 made these drops tolerant of missing columns so the library
    works for non-deans child apps too.
    """
    settings = Settings(data_dir=cms_data_dir, years=fixture_years)
    minimal = pd.DataFrame(
        [
            {
                "name": "Jane M. Brown, MD",
                "credential": "Physician (MD or DO)",
                "specialtys": "Family Medicine",
                "citystates": "Boston, MA",
            }
        ]
    )
    result = find_payments_for_conflicted_providers(conflicteds=minimal, settings=settings)
    # Should match CMS profile 201 (Jane Marie Brown in fixture).
    assert result.n_unique == 1
    assert result.unique_ids.iloc[0]["profile_id"] == 201


def test__api_accepts_pre_parsed_input(cms_data_dir, fixture_years):
    """A DataFrame already matching the canonical schema is passed through
    untouched (no re-parsing)."""
    from ..conflicteds import Conflicteds

    settings = Settings(data_dir=cms_data_dir, years=fixture_years)
    pre_parsed = Conflicteds(_raw_scenarios()).us_conflicteds_id_search_df()
    result = find_payments_for_conflicted_providers(conflicteds=pre_parsed, settings=settings)
    assert result.n_unique >= 2


def test__api_parse_conflicteds_false_validates_input(cms_data_dir, fixture_years):
    """`parse_conflicteds=False` skips parsing AND runtime-validates the
    input — feeding raw data should raise."""
    settings = Settings(data_dir=cms_data_dir, years=fixture_years)
    raw = _raw_scenarios()
    with pytest.raises(ValueError, match="missing required columns"):
        find_payments_for_conflicted_providers(
            conflicteds=raw, settings=settings, parse_conflicteds=False
        )


def test__api_accepts_custom_selector(cms_data_dir, fixture_years):
    """Custom selector flows through to the matcher."""
    settings = Settings(data_dir=cms_data_dir, years=fixture_years)
    result = find_payments_for_conflicted_providers(
        conflicteds=_raw_scenarios(),
        settings=settings,
        selector=TieredConfidenceSelector(),
    )
    # confidence_tier column populated when TieredConfidenceSelector is used.
    assert "confidence_tier" in result.unique_ids.columns
    assert result.unique_ids["confidence_tier"].notna().all()


def test__api_default_selector_when_none_provided(cms_data_dir, fixture_years):
    """Omitting `selector=` uses the default cascade (confidence_tier is None)."""
    settings = Settings(data_dir=cms_data_dir, years=fixture_years)
    result = find_payments_for_conflicted_providers(conflicteds=_raw_scenarios(), settings=settings)
    assert "confidence_tier" in result.unique_ids.columns
    assert result.unique_ids["confidence_tier"].isna().all()


# ---------------------------------------------------------------------------
# SearchResult: xlsx persistence
# ---------------------------------------------------------------------------


def test__searchresult_to_excel_and_from_excel_roundtrip(cms_data_dir, fixture_years, tmp_path):
    settings = Settings(data_dir=cms_data_dir, years=fixture_years)
    result = find_payments_for_conflicted_providers(conflicteds=_raw_scenarios(), settings=settings)
    out = tmp_path / "result.xlsx"
    result.to_excel(out)
    assert out.exists()

    reloaded = SearchResult.from_excel(out)
    # Same row counts.
    assert reloaded.n_unique == result.n_unique
    assert reloaded.n_unmatched == result.n_unmatched
    # Same provider_pks in unique_ids.
    assert set(reloaded.unique_ids["provider_pk"]) == set(result.unique_ids["provider_pk"])


def test__searchresult_update_excel_creates_when_missing(cms_data_dir, fixture_years, tmp_path):
    """update_excel on a nonexistent path should behave like to_excel."""
    settings = Settings(data_dir=cms_data_dir, years=fixture_years)
    result = find_payments_for_conflicted_providers(conflicteds=_raw_scenarios(), settings=settings)
    out = tmp_path / "fresh.xlsx"
    assert not out.exists()
    result.update_excel(out)
    assert out.exists()


def test__searchresult_update_excel_upserts_on_provider_pk(cms_data_dir, fixture_years, tmp_path):
    """Re-running with the same conflicteds should not duplicate rows."""
    settings = Settings(data_dir=cms_data_dir, years=fixture_years)
    raw = _raw_scenarios()

    result1 = find_payments_for_conflicted_providers(conflicteds=raw, settings=settings)
    out = tmp_path / "growing.xlsx"
    result1.to_excel(out)

    # Run again, then update_excel. Row counts must match (no duplicates).
    result2 = find_payments_for_conflicted_providers(conflicteds=raw, settings=settings)
    result2.update_excel(out)

    reloaded = SearchResult.from_excel(out)
    assert reloaded.n_unique == result1.n_unique
    # Provider_pks unchanged.
    assert set(reloaded.unique_ids["provider_pk"]) == set(result1.unique_ids["provider_pk"])


# ---------------------------------------------------------------------------
# SearchResult: introspection
# ---------------------------------------------------------------------------


def test__searchresult_repr_summarizes_counts():
    sr = SearchResult(
        unique_ids=pd.DataFrame([{"provider_pk": 1}] * 3),
        unmatched=pd.DataFrame([{"provider_pk": 2}]),
        unmatched_options=pd.DataFrame([{"provider_pk": 3}] * 5),
    )
    assert repr(sr) == "SearchResult(unique=3, unmatched=1, unmatched_options=5)"


def test__searchresult_count_properties():
    sr = SearchResult(
        unique_ids=pd.DataFrame([{"provider_pk": i} for i in range(7)]),
        unmatched=pd.DataFrame(),
        unmatched_options=pd.DataFrame([{"provider_pk": 99}] * 2),
    )
    assert sr.n_unique == 7
    assert sr.n_unmatched == 0
    assert sr.n_unmatched_options == 2


# ---------------------------------------------------------------------------
# validate_conflicteds_df
# ---------------------------------------------------------------------------


def test__validate_conflicteds_df_passes_on_canonical_shape():
    from ..conflicteds import Conflicteds

    canonical = Conflicteds(_raw_scenarios()).us_conflicteds_id_search_df()
    validate_conflicteds_df(canonical)  # no raise


def test__validate_conflicteds_df_raises_on_missing_columns():
    bad = pd.DataFrame({"provider_pk": [1], "first_name": ["x"]})
    with pytest.raises(ValueError, match="missing required columns"):
        validate_conflicteds_df(bad)


# ---------------------------------------------------------------------------
# Public-symbol surface check
# ---------------------------------------------------------------------------


def test__package_init_exports_documented_symbols():
    import open_payments as op

    documented = {
        "find_payments_for_conflicted_providers",
        "Settings",
        "SearchResult",
        "ConflictedProviderRow",
        "validate_conflicteds_df",
        "REQUIRED_CONFLICTED_COLUMNS",
        "OPTIONAL_CONFLICTED_COLUMNS",
        "MatchSelector",
        "DefaultMatchSelector",
        "IdentifierWinsSelector",
        "TieredConfidenceSelector",
        "SelectorResult",
        "PaymentFilters",
        "FilterOutcome",
        "Unmatcheds",
    }
    missing = {name for name in documented if not hasattr(op, name)}
    assert not missing, f"Public API is missing symbols: {missing}"


def test__selectors_remain_importable_from_open_payments_namespace():
    """Sanity that the three selector classes can be constructed off the
    public namespace."""
    assert isinstance(DefaultMatchSelector(), DefaultMatchSelector)
    assert isinstance(IdentifierWinsSelector(), IdentifierWinsSelector)
    assert isinstance(TieredConfidenceSelector(), TieredConfidenceSelector)


def test__unmatcheds_enum_publicly_accessible():
    """Child apps need Unmatcheds to inspect the unmatched column."""
    assert Unmatcheds.UNFILTERABLE.value == "UNFILTERABLE"
    assert Unmatcheds.NOLASTNAME.value == "NOLASTNAME"
