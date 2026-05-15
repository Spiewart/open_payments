"""Disk-backed cache for institution lookups.

JSON-backed because:

- Diff-friendly: an analyst reviewing the cache can see what was
  resolved at what time without spelunking a binary file.
- Trivially portable: copy the JSON between machines / repos / projects.
- Small enough (a few thousand entries typically). Sqlite-swap below
  ~10K rows is overkill.

Writes are atomic via the standard temp-file + ``os.replace`` pattern
so a crash mid-write doesn't corrupt the cache file.

Keying
------
We canonicalize the lookup key (lowercase, whitespace-collapsed) so
that "Johns Hopkins University" and "  Johns Hopkins  University  "
hit the same cache entry. The canonical form is what's used as the
JSON dict key; the *original* institution string is preserved inside
each ``CandidateLocation.institution`` field so downstream callers
can audit which spelling produced which result.
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from collections.abc import Iterable
from pathlib import Path

from .types import CandidateLocation

logger = logging.getLogger(__name__)


def canonical_key(institution: str) -> str:
    """Normalize an institution string into the cache key form.

    Lowercase + collapse-whitespace. We deliberately do NOT strip
    punctuation or apply the ``EntityParser`` vocabulary — that's
    payor-side normalization. Institutions like
    "St. Jude Children's Research Hospital" carry significant
    punctuation that other matchers downstream may need.
    """
    return re.sub(r"\s+", " ", institution.strip().lower())


class DiskCache:
    """JSON-backed cache mapping canonical institution key → list of
    ``CandidateLocation``.

    Read-through / write-through: callers use ``get(name) -> list | None``
    and ``put(name, candidates)``. A None return means "not in cache —
    go ask a backend".
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self._data: dict[str, list[CandidateLocation]] = {}
        self._loaded = False

    def load(self) -> None:
        """Read the cache file into memory. Idempotent."""
        if self._loaded:
            return
        if not self.path.exists():
            self._loaded = True
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning(
                "institution cache at %s is corrupt; starting fresh", self.path
            )
            self._data = {}
            self._loaded = True
            return
        self._data = {
            key: [CandidateLocation.model_validate(c) for c in entries]
            for key, entries in raw.items()
        }
        self._loaded = True

    def get(self, institution: str) -> list[CandidateLocation] | None:
        """Return cached candidates for ``institution`` or None on miss."""
        self.load()
        return self._data.get(canonical_key(institution))

    def get_many(
        self, institutions: Iterable[str]
    ) -> tuple[dict[str, list[CandidateLocation]], list[str]]:
        """Return ``(hits, misses)``.

        - ``hits`` maps the original institution string → cached candidates
        - ``misses`` is the list of original institution strings whose key
          isn't in the cache. Callers feed misses to a backend.
        """
        self.load()
        hits: dict[str, list[CandidateLocation]] = {}
        misses: list[str] = []
        for institution in institutions:
            cached = self._data.get(canonical_key(institution))
            if cached is None:
                misses.append(institution)
            else:
                hits[institution] = cached
        return hits, misses

    def put(self, institution: str, candidates: list[CandidateLocation]) -> None:
        """Insert/overwrite cache entry for ``institution``."""
        self.load()
        self._data[canonical_key(institution)] = candidates
        self._flush()

    def put_many(self, batch: dict[str, list[CandidateLocation]]) -> None:
        """Bulk insert; one flush at the end."""
        self.load()
        for institution, candidates in batch.items():
            self._data[canonical_key(institution)] = candidates
        self._flush()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _flush(self) -> None:
        """Atomic write via temp file + ``os.replace``."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            key: [c.model_dump(mode="json") for c in entries]
            for key, entries in self._data.items()
        }
        tmp_fd, tmp_path = tempfile.mkstemp(
            prefix=self.path.name + ".",
            suffix=".tmp",
            dir=str(self.path.parent),
        )
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, sort_keys=True, default=str)
            os.replace(tmp_path, self.path)
        except Exception:
            # Best-effort cleanup of the temp file on failure; ignore
            # secondary errors so the original exception surfaces.
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def __len__(self) -> int:
        self.load()
        return len(self._data)

    def __contains__(self, institution: str) -> bool:
        self.load()
        return canonical_key(institution) in self._data
