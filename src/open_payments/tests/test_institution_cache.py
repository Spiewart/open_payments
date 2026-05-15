"""Tests for the JSON-backed institution cache."""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from open_payments.institution_locator import (
    CandidateLocation,
    DiskCache,
    canonical_key,
)


def _candidate(institution: str, city: str = "Baltimore", state: str = "MD") -> CandidateLocation:
    return CandidateLocation(
        institution=institution,
        city=city,
        state=state,
        source="nppes",
        confidence=1.0,
        looked_up_at=datetime(2026, 5, 15, 12, 0),
    )


class TestCanonicalKey:
    def test_lowercases(self):
        assert canonical_key("Johns Hopkins University") == "johns hopkins university"

    def test_collapses_whitespace(self):
        assert canonical_key("  Johns   Hopkins  ") == "johns hopkins"

    def test_preserves_punctuation_we_care_about(self):
        # Apostrophes and periods are load-bearing for some institutions
        # (St. Jude Children's). Don't strip them in the key.
        assert canonical_key("St. Jude Children's Hospital") == "st. jude children's hospital"


class TestDiskCacheGetPut:
    def test_get_returns_none_for_missing(self, tmp_path):
        cache = DiskCache(tmp_path / "cache.json")
        assert cache.get("Cleveland Clinic") is None

    def test_put_then_get_roundtrip(self, tmp_path):
        cache = DiskCache(tmp_path / "cache.json")
        cache.put("Johns Hopkins University", [_candidate("Johns Hopkins University")])
        result = cache.get("Johns Hopkins University")
        assert result is not None
        assert len(result) == 1
        assert result[0].city == "Baltimore"

    def test_get_matches_on_canonical_key_not_raw(self, tmp_path):
        cache = DiskCache(tmp_path / "cache.json")
        cache.put("Johns Hopkins University", [_candidate("Johns Hopkins University")])
        # Different capitalization + whitespace — should still hit.
        assert cache.get("  johns   hopkins   UNIVERSITY  ") is not None


class TestDiskCacheGetMany:
    def test_separates_hits_from_misses(self, tmp_path):
        cache = DiskCache(tmp_path / "cache.json")
        cache.put("Johns Hopkins University", [_candidate("Johns Hopkins University")])
        hits, misses = cache.get_many(["Johns Hopkins University", "Cleveland Clinic"])
        assert "Johns Hopkins University" in hits
        assert misses == ["Cleveland Clinic"]


class TestDiskCachePersistence:
    def test_writes_to_disk(self, tmp_path):
        path = tmp_path / "cache.json"
        cache = DiskCache(path)
        cache.put("Johns Hopkins University", [_candidate("Johns Hopkins University")])
        assert path.exists()
        raw = json.loads(path.read_text())
        # Key in the file is the canonical form, not the raw input.
        assert "johns hopkins university" in raw

    def test_load_from_existing_file(self, tmp_path):
        path = tmp_path / "cache.json"
        # Write a cache file directly, then load it.
        first = DiskCache(path)
        first.put("Johns Hopkins University", [_candidate("Johns Hopkins University")])
        # Fresh instance reads from disk.
        second = DiskCache(path)
        result = second.get("Johns Hopkins University")
        assert result is not None
        assert result[0].city == "Baltimore"

    def test_corrupt_file_starts_fresh_with_warning(self, tmp_path, caplog):
        import logging

        path = tmp_path / "cache.json"
        path.write_text("not valid json{{{")
        cache = DiskCache(path)
        with caplog.at_level(logging.WARNING, logger="open_payments.institution_locator.cache"):
            cache.load()
        assert any("corrupt" in r.message for r in caplog.records)
        assert cache.get("anything") is None

    def test_put_many_writes_atomically(self, tmp_path):
        cache = DiskCache(tmp_path / "cache.json")
        cache.put_many(
            {
                "Johns Hopkins University": [_candidate("Johns Hopkins University")],
                "Cleveland Clinic": [_candidate("Cleveland Clinic", "Cleveland", "OH")],
            }
        )
        # Both readable on a fresh instance.
        fresh = DiskCache(tmp_path / "cache.json")
        assert fresh.get("Cleveland Clinic")[0].state == "OH"
        assert fresh.get("Johns Hopkins University")[0].state == "MD"


class TestDiskCacheMissCaching:
    def test_storing_a_miss_sentinel_prevents_re_query(self, tmp_path):
        cache = DiskCache(tmp_path / "cache.json")
        cache.put(
            "Made Up University of Nothing",
            [CandidateLocation(institution="Made Up University of Nothing", source="miss")],
        )
        # On next call, this hits the cache — it's a "we tried, gave up" record.
        result = cache.get("Made Up University of Nothing")
        assert result is not None
        assert result[0].source == "miss"
