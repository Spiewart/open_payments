"""xlsx-based manual-review backend for institution → location lookup.

Workflow
--------
When the NPPES residual is small (default ≤ 50; configurable on the
orchestrator), it's often cheaper for an analyst to hand-fill the
remaining city/state pairs than to call the LLM API. This backend
implements the round-trip:

1. ``export(institutions, path)`` writes an xlsx with columns
   ``institution | city | state | notes``. The ``institution`` column
   is pre-filled; ``city``/``state``/``notes`` are blank.
2. The analyst fills the file in Excel / Google Sheets and saves it.
3. ``import_(path) -> dict[str, list[CandidateLocation]]`` reads it
   back, ignoring blank rows and rows whose city/state cells are
   still empty. Empty rows become a single ``source='miss'`` entry
   so the cache records "we asked a human and they didn't know."

A single institution can produce multiple rows in the spreadsheet
(one campus per row) — the importer groups by ``institution`` and
returns ``list[CandidateLocation]``.

This is **not** an automated backend — calls to ``export`` and
``import_`` are separated by human time. The orchestrator owns the
flow logic; this class just owns the file format.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path

import openpyxl
import pandas as pd

from .types import CandidateLocation

logger = logging.getLogger(__name__)


REVIEW_COLUMNS = ("institution", "city", "state", "notes")


class ManualReviewBackend:
    """xlsx round-trip for human-in-the-loop institution lookups."""

    def export(
        self,
        institutions: list[str],
        path: Path,
        *,
        existing: dict[str, list[CandidateLocation]] | None = None,
    ) -> Path:
        """Write a review xlsx at ``path``.

        Each institution gets one row. If ``existing`` carries known
        candidates for some institutions (e.g. a previous partial review),
        those rows come pre-filled so the analyst only has to deal with
        the truly-blank ones.
        """
        existing = existing or {}
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        rows: list[dict[str, str | None]] = []
        for institution in institutions:
            prior = existing.get(institution, [])
            usable = [c for c in prior if c.source != "miss"]
            if not usable:
                rows.append({
                    "institution": institution,
                    "city": None,
                    "state": None,
                    "notes": None,
                })
                continue
            for candidate in usable:
                rows.append({
                    "institution": institution,
                    "city": candidate.city,
                    "state": candidate.state,
                    "notes": f"prior {candidate.source}"
                    + (
                        f" conf={candidate.confidence:.2f}"
                        if candidate.confidence is not None
                        else ""
                    ),
                })

        df = pd.DataFrame(rows, columns=list(REVIEW_COLUMNS))
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="institutions", index=False)
            ws = writer.book["institutions"]
            # Widen columns so the analyst can actually read the institution names.
            ws.column_dimensions["A"].width = 60
            ws.column_dimensions["B"].width = 25
            ws.column_dimensions["C"].width = 8
            ws.column_dimensions["D"].width = 40

        logger.info(
            "wrote %d institution rows to %s for manual review",
            len(rows),
            path,
        )
        return path

    def import_(
        self,
        path: Path,
    ) -> dict[str, list[CandidateLocation]]:
        """Read a filled review xlsx back into ``CandidateLocation`` form.

        Validates the column shape (raises if the analyst destroyed the
        header). Groups rows by ``institution`` so multi-campus entries
        produce multi-element lists. Rows with blank city AND blank state
        produce a ``source='miss'`` sentinel — that's the analyst's way
        of recording "I looked and didn't find anything".
        """
        path = Path(path)
        df = pd.read_excel(path, sheet_name="institutions")
        missing_cols = set(REVIEW_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(
                f"manual-review xlsx at {path} is missing required columns: "
                f"{sorted(missing_cols)} — expected {list(REVIEW_COLUMNS)}"
            )

        grouped: dict[str, list[CandidateLocation]] = defaultdict(list)
        for _, row in df.iterrows():
            institution = self._opt_str(row.get("institution"))
            if not institution:
                continue
            city = self._opt_str(row.get("city"))
            state = self._opt_str(row.get("state"))
            if city is None and state is None:
                grouped[institution].append(
                    CandidateLocation(
                        institution=institution,
                        source="miss",
                    )
                )
                continue
            grouped[institution].append(
                CandidateLocation(
                    institution=institution,
                    city=city,
                    state=(state or "").upper() or None,
                    source="manual",
                )
            )

        # If an institution had BOTH a real candidate AND a miss sentinel
        # (e.g. analyst filled one campus and left another row blank), drop
        # the miss — the candidate wins.
        cleaned: dict[str, list[CandidateLocation]] = {}
        for institution, candidates in grouped.items():
            real = [c for c in candidates if c.source != "miss"]
            cleaned[institution] = real if real else candidates
        logger.info(
            "imported %d institutions from %s (%d with real locations)",
            len(cleaned),
            path,
            sum(1 for cs in cleaned.values() if cs and cs[0].source != "miss"),
        )
        return cleaned

    @staticmethod
    def _opt_str(value) -> str | None:
        if value is None:
            return None
        if isinstance(value, float) and pd.isna(value):
            return None
        text = str(value).strip()
        return text or None
