"""Tests for the Claude API backend.

The Anthropic client is mocked at the ``messages.create`` level so
tests are fully hermetic — no API key, no network, no spend. A live
test against the real API is reserved for ``-m network`` runs.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from open_payments.institution_locator.llm import ClaudeAPIBackend


def _mock_anthropic_response(text: str) -> MagicMock:
    """Build a mock that mimics the shape of ``Anthropic.messages.create``'s return."""
    block = MagicMock()
    block.text = text
    resp = MagicMock()
    resp.content = [block]
    return resp


def _backend_with_mock(text_response: str | None = None, raises=None) -> ClaudeAPIBackend:
    mock_client = MagicMock()
    if raises is not None:
        mock_client.messages.create.side_effect = raises
    else:
        mock_client.messages.create.return_value = _mock_anthropic_response(
            text_response or "{}"
        )
    return ClaudeAPIBackend(api_key="test", client=mock_client, retry_backoff_s=0.0)


class TestSuccessfulParse:
    def test_single_location_response(self):
        backend = _backend_with_mock(
            '{"locations": [{"city": "Baltimore", "state": "MD", "confidence": 0.97}]}'
        )
        candidates = backend.locate("Johns Hopkins University")
        assert len(candidates) == 1
        assert candidates[0].city == "Baltimore"
        assert candidates[0].state == "MD"
        assert candidates[0].source == "llm"
        assert candidates[0].confidence == 0.97

    def test_multi_campus_response(self):
        backend = _backend_with_mock(
            '{"locations": ['
            '{"city": "Berkeley", "state": "CA", "confidence": 0.9},'
            '{"city": "San Francisco", "state": "CA", "confidence": 0.9}'
            "]}"
        )
        candidates = backend.locate("University of California")
        assert len(candidates) == 2
        assert {c.city for c in candidates} == {"Berkeley", "San Francisco"}

    def test_state_is_uppercased(self):
        # Tolerate lowercase from model.
        backend = _backend_with_mock(
            '{"locations": [{"city": "Baltimore", "state": "md", "confidence": 0.9}]}'
        )
        candidates = backend.locate("Johns Hopkins University")
        assert candidates[0].state == "MD"


class TestFenceTolerance:
    def test_strips_markdown_json_fence(self):
        # Some models append ```json fences despite instructions to the contrary.
        backend = _backend_with_mock(
            '```json\n{"locations": [{"city": "Baltimore", "state": "MD"}]}\n```'
        )
        candidates = backend.locate("Johns Hopkins University")
        assert candidates[0].city == "Baltimore"


class TestMissHandling:
    def test_empty_locations_returns_miss(self):
        backend = _backend_with_mock('{"locations": []}')
        candidates = backend.locate("Imaginary Institute of Nothing")
        assert candidates[0].source == "miss"

    def test_invalid_json_returns_miss_without_raising(self):
        backend = _backend_with_mock("not valid json at all")
        # Critical contract: parse failures degrade to miss, never raise.
        # A 1500-call batch with one flaky response shouldn't crash.
        candidates = backend.locate("Whatever")
        assert candidates[0].source == "miss"

    def test_api_failure_after_retries_returns_miss(self):
        backend = _backend_with_mock(raises=RuntimeError("API down"))
        candidates = backend.locate("Johns Hopkins University")
        assert candidates[0].source == "miss"


class TestRetry:
    def test_transient_failure_retries_then_succeeds(self):
        # First two calls raise, third returns valid JSON.
        mock_client = MagicMock()
        good_resp = _mock_anthropic_response(
            '{"locations": [{"city": "Cleveland", "state": "OH"}]}'
        )
        mock_client.messages.create.side_effect = [
            RuntimeError("transient"),
            RuntimeError("transient"),
            good_resp,
        ]
        backend = ClaudeAPIBackend(
            api_key="test",
            client=mock_client,
            max_retries=3,
            retry_backoff_s=0.0,
        )
        candidates = backend.locate("Cleveland Clinic")
        assert candidates[0].city == "Cleveland"
        assert mock_client.messages.create.call_count == 3


class TestBatch:
    def test_locate_batch_per_institution_isolation(self):
        # One call fails, one succeeds — both should be reflected in the result.
        mock_client = MagicMock()
        responses = [
            RuntimeError("api down for hopkins"),
            RuntimeError("api down for hopkins"),
            RuntimeError("api down for hopkins"),
            _mock_anthropic_response('{"locations": [{"city": "Cleveland", "state": "OH"}]}'),
        ]
        mock_client.messages.create.side_effect = responses
        backend = ClaudeAPIBackend(
            api_key="test", client=mock_client, max_retries=3, retry_backoff_s=0.0
        )
        results = backend.locate_batch(["Johns Hopkins University", "Cleveland Clinic"])
        assert results["Johns Hopkins University"][0].source == "miss"
        assert results["Cleveland Clinic"][0].city == "Cleveland"


class TestImportError:
    def test_missing_anthropic_dep_raises_clear_error(self, monkeypatch):
        # Simulate anthropic not being installed. We can't actually uninstall
        # mid-test, but we can monkeypatch the import to raise.
        import sys

        # Force ImportError on `from anthropic import Anthropic`. Sys-modules
        # cache miss + monkeypatched __import__ is the surgical approach.
        monkeypatch.setitem(sys.modules, "anthropic", None)
        with pytest.raises(ImportError, match=r"open_payments\[llm\]"):
            ClaudeAPIBackend(api_key="test")
