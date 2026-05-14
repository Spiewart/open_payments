"""Pytest fixtures shared across the open_payments test suite.

Goal: every test in `pytest -m "not integration"` runs entirely from this
package's synthetic fixtures, with no dependency on `~/open_payments_datasets`
or any other local CMS data. Tests that DO want real CMS data should be
marked `@pytest.mark.integration`.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from .factories import (
    make_canonical_conflicteds_df,
    make_general_csv_df,
    make_ownership_csv_df,
    make_research_csv_df,
)

FIXTURE_CMS_DIR = Path(__file__).resolve().parent / "fixtures" / "cms"


@pytest.fixture
def cms_data_dir() -> Path:
    """Path to the on-disk synthetic CMS directory. Pass this as
    `payments_folder=` to any ReadPayments-derived class."""
    return FIXTURE_CMS_DIR


@pytest.fixture
def fixture_years() -> list[int]:
    """The years our committed CMS fixtures cover. Expand as scenarios grow."""
    return [2023]


@pytest.fixture
def tiny_general_df() -> pd.DataFrame:
    """In-memory CMS general-payments DataFrame (raw column names, pre-rename)."""
    return make_general_csv_df()


@pytest.fixture
def tiny_ownership_df() -> pd.DataFrame:
    """In-memory CMS ownership-payments DataFrame (raw column names)."""
    return make_ownership_csv_df()


@pytest.fixture
def tiny_research_df() -> pd.DataFrame:
    """In-memory CMS research-payments DataFrame (raw column names)."""
    return make_research_csv_df()


@pytest.fixture
def canonical_conflicteds_df() -> pd.DataFrame:
    """Six-row conflicted-providers DataFrame covering every matching-pipeline
    scenario (single match, narrowed-by-first-name, narrowed-by-middle-initial,
    hyphenated last name, ambiguous tiebreaker, no-match)."""
    return make_canonical_conflicteds_df()
