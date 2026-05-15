"""Tests for the InstitutionLocator cascade orchestrator.

The orchestrator's job is to wire DiskCache + NPPES + ManualReviewBackend
together. Backends are mocked here so the tests stay fast and hermetic —
backend-specific behavior is exercised in test_institution_nppes.py /
test_institution_manual.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from open_payments.institution_locator import (
    CandidateLocation,
    InstitutionLocator,
)


def _candidate(name: str, source: str = "nppes", city: str = "Baltimore", state: str = "MD"):
    return CandidateLocation(institution=name, city=city, state=state, source=source)


def _miss(name: str):
    return CandidateLocation(institution=name, source="miss")


@pytest.fixture
def mock_nppes():
    return MagicMock()


@pytest.fixture
def locator(tmp_path, mock_nppes):
    return InstitutionLocator(
        cache_path=tmp_path / "cache.json",
        nppes_backend=mock_nppes,
    )


class TestLocateBatch:
    def test_cache_hits_skip_nppes_call(self, locator, mock_nppes):
        # Pre-populate the cache.
        locator.cache.put("Johns Hopkins University", [_candidate("Johns Hopkins University")])
        results = locator.locate_batch(["Johns Hopkins University"])
        # NPPES never called because cache hit covered everything.
        mock_nppes.locate.assert_not_called()
        assert results["Johns Hopkins University"][0].city == "Baltimore"

    def test_cache_miss_falls_through_to_nppes(self, locator, mock_nppes):
        mock_nppes.locate.return_value = [_candidate("Cleveland Clinic", city="Cleveland", state="OH")]
        results = locator.locate_batch(["Cleveland Clinic"])
        mock_nppes.locate.assert_called_once_with("Cleveland Clinic")
        assert results["Cleveland Clinic"][0].state == "OH"

    def test_mixed_cache_hits_and_misses(self, locator, mock_nppes):
        locator.cache.put("Johns Hopkins University", [_candidate("Johns Hopkins University")])
        mock_nppes.locate.return_value = [_candidate("Cleveland Clinic", city="Cleveland", state="OH")]
        results = locator.locate_batch(["Johns Hopkins University", "Cleveland Clinic"])
        # Only Cleveland needed NPPES.
        mock_nppes.locate.assert_called_once_with("Cleveland Clinic")
        assert "Johns Hopkins University" in results
        assert "Cleveland Clinic" in results

    def test_nppes_results_are_cached(self, locator, mock_nppes):
        mock_nppes.locate.return_value = [_candidate("Cleveland Clinic", city="Cleveland", state="OH")]
        locator.locate_batch(["Cleveland Clinic"])
        # Second call should not invoke NPPES again.
        mock_nppes.locate.reset_mock()
        locator.locate_batch(["Cleveland Clinic"])
        mock_nppes.locate.assert_not_called()

    def test_empty_input_returns_empty_dict(self, locator):
        assert locator.locate_batch([]) == {}


class TestResidual:
    def test_residual_excludes_resolved_institutions(self, locator):
        results = {
            "Johns Hopkins University": [_candidate("Johns Hopkins University")],
            "Imaginary Institute": [_miss("Imaginary Institute")],
        }
        assert locator.residual_institutions(results) == ["Imaginary Institute"]

    def test_residual_includes_only_all_miss_entries(self, locator):
        # A mix of miss + real → not residual (we have something).
        results = {
            "Johns Hopkins University": [
                _candidate("Johns Hopkins University"),
                _miss("Johns Hopkins University"),
            ],
        }
        assert locator.residual_institutions(results) == []


class TestManualRoute:
    def test_export_for_manual_review_delegates_to_backend(self, locator, tmp_path):
        path = tmp_path / "review.xlsx"
        called: dict = {}

        def fake_export(institutions, p, existing=None):
            called["args"] = (list(institutions), p)
            return p

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(locator.manual_backend, "export", fake_export)
            result = locator.export_for_manual_review(["Some Place"], path)
        assert result == path
        assert called["args"] == (["Some Place"], path)

    def test_import_manual_review_writes_to_cache(self, locator, tmp_path):
        path = tmp_path / "review.xlsx"
        analyst_filled = {
            "Cleveland Clinic": [
                _candidate("Cleveland Clinic", source="manual", city="Cleveland", state="OH")
            ]
        }
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(locator.manual_backend, "import_", lambda p: analyst_filled)
            locator.import_manual_review(path)
        cached = locator.cache.get("Cleveland Clinic")
        assert cached is not None
        assert cached[0].source == "manual"
        assert cached[0].city == "Cleveland"
