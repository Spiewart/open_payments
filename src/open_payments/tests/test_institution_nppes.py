"""Tests for the NPPES backend.

HTTP calls are mocked via the ``responses`` library so tests are fully
hermetic. A live-network test that actually hits registry.cms.hhs.gov
is marked ``@pytest.mark.network``.
"""

from __future__ import annotations

import pytest
import responses

from open_payments.institution_locator.nppes import (
    FUZZY_THRESHOLD,
    NPPES_ENDPOINT,
    NPPESBackend,
)


def _nppes_payload(rows: list[dict]) -> dict:
    """Wrap rows in NPPES's standard envelope."""
    return {"result_count": len(rows), "results": rows}


def _row(org_name: str, city: str, state: str) -> dict:
    """One NPPES row with a single LOCATION address."""
    return {
        "basic": {"organization_name": org_name},
        "addresses": [
            {
                "address_purpose": "LOCATION",
                "city": city,
                "state": state,
            }
        ],
    }


@pytest.fixture
def backend():
    # Fast tests — no inter-request sleep.
    return NPPESBackend(request_interval_s=0.0)


class TestExactMatch:
    @responses.activate
    def test_exact_match_returns_single_candidate(self, backend):
        responses.add(
            responses.GET,
            NPPES_ENDPOINT,
            json=_nppes_payload([_row("JOHNS HOPKINS UNIVERSITY", "BALTIMORE", "MD")]),
            status=200,
        )
        candidates = backend.locate("Johns Hopkins University")
        assert len(candidates) == 1
        assert candidates[0].city == "Baltimore"
        assert candidates[0].state == "MD"
        assert candidates[0].source == "nppes"
        assert candidates[0].confidence == 1.0

    @responses.activate
    def test_multi_campus_returns_top_n(self, backend):
        # Multiple Baltimore campuses + one Middle River — should dedup.
        responses.add(
            responses.GET,
            NPPES_ENDPOINT,
            json=_nppes_payload(
                [
                    _row("JOHNS HOPKINS UNIVERSITY", "BALTIMORE", "MD"),
                    _row("JOHNS HOPKINS UNIVERSITY", "BALTIMORE", "MD"),
                    _row("JOHNS HOPKINS UNIVERSITY", "MIDDLE RIVER", "MD"),
                ]
            ),
            status=200,
        )
        candidates = backend.locate("Johns Hopkins University")
        cities = {c.city for c in candidates}
        assert cities == {"Baltimore", "Middle River"}

    @responses.activate
    def test_city_titlecased_state_uppercased(self, backend):
        # NPPES returns CAPS; we want pretty cities and USPS-style states.
        responses.add(
            responses.GET,
            NPPES_ENDPOINT,
            json=_nppes_payload([_row("CLEVELAND CLINIC", "CLEVELAND", "OH")]),
            status=200,
        )
        candidates = backend.locate("Cleveland Clinic")
        assert candidates[0].city == "Cleveland"
        assert candidates[0].state == "OH"


class TestMissAndFuzzyFallback:
    @responses.activate
    def test_empty_response_then_fuzzy_succeeds(self, backend):
        # First request (full name) → empty. Second request (truncated)
        # returns a row that has a TRAILING TOKEN the input doesn't, so the
        # fuzzy ratio is < 1.0 — exercising the fuzzy threshold path.
        responses.add(
            responses.GET,
            NPPES_ENDPOINT,
            json=_nppes_payload([]),
            status=200,
        )
        responses.add(
            responses.GET,
            NPPES_ENDPOINT,
            json=_nppes_payload([_row("BRIGHAM AND WOMEN'S HOSPITAL INC", "BOSTON", "MA")]),
            status=200,
        )
        candidates = backend.locate("Brigham and Women's Hospital")
        assert len(candidates) == 1
        assert candidates[0].city == "Boston"
        assert candidates[0].source == "nppes"
        # Fuzzy confidence carries the rapidfuzz ratio / 100 — must be
        # populated AND ≥ FUZZY_THRESHOLD/100 since this row passed.
        assert candidates[0].confidence is not None
        assert candidates[0].confidence >= FUZZY_THRESHOLD / 100.0

    @responses.activate
    def test_fuzzy_below_threshold_falls_through_to_miss(self, backend):
        # Both queries return junk that doesn't fuzzy-match.
        responses.add(
            responses.GET,
            NPPES_ENDPOINT,
            json=_nppes_payload([]),
            status=200,
        )
        responses.add(
            responses.GET,
            NPPES_ENDPOINT,
            json=_nppes_payload([_row("WHOLLY UNRELATED MEDICAL CENTER", "SOMEWHERE", "TX")]),
            status=200,
        )
        candidates = backend.locate("Imaginary Institute of Things")
        assert len(candidates) == 1
        assert candidates[0].source == "miss"
        assert candidates[0].city is None
        assert candidates[0].state is None

    @responses.activate
    def test_total_miss_returns_miss_sentinel(self, backend):
        # Both queries empty.
        responses.add(
            responses.GET,
            NPPES_ENDPOINT,
            json=_nppes_payload([]),
            status=200,
        )
        responses.add(
            responses.GET,
            NPPES_ENDPOINT,
            json=_nppes_payload([]),
            status=200,
        )
        candidates = backend.locate("Truly Made Up Place")
        assert candidates == [
            candidates[0],  # one entry...
        ]
        assert candidates[0].source == "miss"


class TestResilience:
    @responses.activate
    def test_http_error_returns_miss_not_raises(self, backend):
        responses.add(
            responses.GET,
            NPPES_ENDPOINT,
            json={"error": "boom"},
            status=500,
        )
        # NPPES backend swallows HTTP errors (logged) and returns miss
        # so a single flaky request doesn't crash a 500-institution batch.
        candidates = backend.locate("Johns Hopkins University")
        assert candidates[0].source == "miss"

    def test_fuzzy_threshold_is_documented_constant(self):
        # Sanity: the threshold is a module-level constant that tests pin.
        # Tuning the threshold requires updating this test, which forces
        # the analyst to think about the audit consequences.
        assert FUZZY_THRESHOLD == 88


@pytest.mark.network
def test_live_nppes_johns_hopkins():
    """Real NPPES call — runs only under ``-m network``. Not part of the
    default unit suite. Validates that the wire shape we mock matches
    what NPPES actually returns today."""
    backend = NPPESBackend(request_interval_s=0.5)
    candidates = backend.locate("Johns Hopkins University")
    assert candidates
    assert candidates[0].source == "nppes"
    # Hopkins is in Maryland, very stable institutional fact.
    assert "MD" in {c.state for c in candidates}
