"""Tests for the xlsx manual-review backend (round-trip)."""

from __future__ import annotations

import pandas as pd
import pytest

from open_payments.institution_locator import CandidateLocation, ManualReviewBackend


@pytest.fixture
def backend():
    return ManualReviewBackend()


class TestExport:
    def test_export_creates_xlsx_with_review_columns(self, backend, tmp_path):
        path = backend.export(
            ["Johns Hopkins University", "Cleveland Clinic"], tmp_path / "review.xlsx"
        )
        assert path.exists()
        df = pd.read_excel(path, sheet_name="institutions")
        assert list(df.columns) == ["institution", "city", "state", "notes"]
        assert set(df["institution"]) == {"Johns Hopkins University", "Cleveland Clinic"}

    def test_export_prefills_known_candidates(self, backend, tmp_path):
        existing = {
            "Johns Hopkins University": [
                CandidateLocation(
                    institution="Johns Hopkins University",
                    city="Baltimore",
                    state="MD",
                    source="nppes",
                    confidence=1.0,
                )
            ]
        }
        path = backend.export(
            ["Johns Hopkins University", "Cleveland Clinic"],
            tmp_path / "review.xlsx",
            existing=existing,
        )
        df = pd.read_excel(path, sheet_name="institutions")
        # Hopkins row pre-filled; Cleveland row blank.
        hopkins_row = df[df["institution"] == "Johns Hopkins University"].iloc[0]
        assert hopkins_row["city"] == "Baltimore"
        assert hopkins_row["state"] == "MD"
        cleveland_row = df[df["institution"] == "Cleveland Clinic"].iloc[0]
        assert pd.isna(cleveland_row["city"])


class TestImport:
    def test_import_filled_rows_returns_manual_candidates(self, backend, tmp_path):
        path = tmp_path / "review.xlsx"
        df = pd.DataFrame(
            {
                "institution": ["Johns Hopkins University", "Cleveland Clinic"],
                "city": ["Baltimore", "Cleveland"],
                "state": ["MD", "OH"],
                "notes": [None, None],
            }
        )
        df.to_excel(path, sheet_name="institutions", index=False)

        results = backend.import_(path)
        assert "Johns Hopkins University" in results
        assert results["Johns Hopkins University"][0].source == "manual"
        assert results["Cleveland Clinic"][0].city == "Cleveland"
        assert results["Cleveland Clinic"][0].state == "OH"

    def test_import_blank_row_records_miss_sentinel(self, backend, tmp_path):
        path = tmp_path / "review.xlsx"
        df = pd.DataFrame(
            {
                "institution": ["Imaginary Institute"],
                "city": [None],
                "state": [None],
                "notes": ["analyst could not find"],
            }
        )
        df.to_excel(path, sheet_name="institutions", index=False)

        results = backend.import_(path)
        assert results["Imaginary Institute"][0].source == "miss"

    def test_import_uppercases_state_abbreviation(self, backend, tmp_path):
        path = tmp_path / "review.xlsx"
        df = pd.DataFrame(
            {
                "institution": ["Johns Hopkins University"],
                "city": ["Baltimore"],
                "state": ["md"],  # lowercase from analyst
                "notes": [None],
            }
        )
        df.to_excel(path, sheet_name="institutions", index=False)

        results = backend.import_(path)
        assert results["Johns Hopkins University"][0].state == "MD"

    def test_import_multi_row_per_institution_groups_into_list(self, backend, tmp_path):
        path = tmp_path / "review.xlsx"
        df = pd.DataFrame(
            {
                "institution": [
                    "University of California",
                    "University of California",
                ],
                "city": ["Berkeley", "San Francisco"],
                "state": ["CA", "CA"],
                "notes": [None, None],
            }
        )
        df.to_excel(path, sheet_name="institutions", index=False)

        results = backend.import_(path)
        assert len(results["University of California"]) == 2
        cities = {c.city for c in results["University of California"]}
        assert cities == {"Berkeley", "San Francisco"}

    def test_import_raises_on_missing_columns(self, backend, tmp_path):
        path = tmp_path / "review.xlsx"
        df = pd.DataFrame(
            {
                "institution": ["Foo"],
                "city": ["Bar"],
                # state missing!
            }
        )
        df.to_excel(path, sheet_name="institutions", index=False)

        with pytest.raises(ValueError, match="missing required columns"):
            backend.import_(path)
