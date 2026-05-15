"""Tests for the InstitutionLocator cascade orchestrator.

The orchestrator's job is to wire DiskCache + NPPES + (manual or LLM)
together. Backends are mocked here so the tests stay fast and
hermetic — the backend-specific behavior is exercised in
test_institution_nppes.py / test_institution_llm.py /
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
def mock_llm():
    return MagicMock()


@pytest.fixture
def locator(tmp_path, mock_nppes, mock_llm):
    return InstitutionLocator(
        cache_path=tmp_path / "cache.json",
        manual_threshold=50,
        nppes_backend=mock_nppes,
        llm_backend=mock_llm,
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


class TestStrategy:
    def test_no_residual_returns_none_strategy(self, locator):
        strategy, residual = locator.recommend_residual_strategy(
            {"Johns Hopkins University": [_candidate("Johns Hopkins University")]}
        )
        assert strategy == "none"
        assert residual == []

    def test_below_threshold_recommends_manual(self, locator):
        # 1 institution, threshold 50 → manual.
        strategy, residual = locator.recommend_residual_strategy(
            {"Imaginary Institute": [_miss("Imaginary Institute")]}
        )
        assert strategy == "manual"
        assert residual == ["Imaginary Institute"]

    def test_above_threshold_with_llm_recommends_llm(self, tmp_path, mock_llm):
        # Threshold 2, residual 3 → above. LLM configured → llm.
        locator = InstitutionLocator(
            cache_path=tmp_path / "cache.json",
            manual_threshold=2,
            llm_backend=mock_llm,
            nppes_backend=MagicMock(),
        )
        results = {
            f"Inst {i}": [_miss(f"Inst {i}")] for i in range(3)
        }
        strategy, residual = locator.recommend_residual_strategy(results)
        assert strategy == "llm"
        assert len(residual) == 3

    def test_above_threshold_without_llm_recommends_llm_unavailable(self, tmp_path):
        locator = InstitutionLocator(
            cache_path=tmp_path / "cache.json",
            manual_threshold=2,
            llm_backend=None,
            nppes_backend=MagicMock(),
        )
        results = {f"Inst {i}": [_miss(f"Inst {i}")] for i in range(3)}
        strategy, _ = locator.recommend_residual_strategy(results)
        assert strategy == "llm_unavailable"


class TestLlmRoute:
    def test_resolve_via_llm_calls_backend_and_caches(self, locator, mock_llm):
        mock_llm.locate_batch.return_value = {
            "Cleveland Clinic": [_candidate("Cleveland Clinic", source="llm", city="Cleveland", state="OH")]
        }
        results = locator.resolve_via_llm(["Cleveland Clinic"])
        mock_llm.locate_batch.assert_called_once_with(["Cleveland Clinic"])
        # Cached → next locate_batch call won't hit NPPES.
        cached = locator.cache.get("Cleveland Clinic")
        assert cached[0].source == "llm"

    def test_resolve_via_llm_without_backend_raises(self, tmp_path):
        locator = InstitutionLocator(
            cache_path=tmp_path / "cache.json",
            llm_backend=None,
            nppes_backend=MagicMock(),
        )
        with pytest.raises(RuntimeError, match="no llm_backend configured"):
            locator.resolve_via_llm(["anything"])
