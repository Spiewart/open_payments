"""Unit tests for the standalone scripts/list_2024_manufacturers.py.

The script lives outside the package (``scripts/`` is sibling to ``src/``),
so we import it via a small path-mangling helper rather than as a
package member.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_SCRIPTS_DIR = Path(__file__).resolve().parents[3] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import list_2024_manufacturers as mod  # noqa: E402


PAYING = "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name"
SUBMITTING = "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name"


def _chunk(rows: list[tuple[object, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=[PAYING, SUBMITTING])


def test_unions_both_columns_across_chunks() -> None:
    chunks = [
        _chunk([("Pfizer Inc.", "Pfizer Inc."), ("Merck & Co.", "Merck & Co.")]),
        _chunk([("Roche Holding AG", "Genentech Inc."), ("Pfizer Inc.", "Pfizer Inc.")]),
        _chunk([("Novartis AG", "Sandoz Inc.")]),
    ]
    acc: set[str] = set()
    for chunk in chunks:
        mod.update_manufacturers_from_chunk(chunk, acc)

    assert acc == {
        "Pfizer Inc.",
        "Merck & Co.",
        "Roche Holding AG",
        "Genentech Inc.",
        "Novartis AG",
        "Sandoz Inc.",
    }


def test_skips_nan_and_empty_strings() -> None:
    chunk = _chunk(
        [
            ("Pfizer Inc.", np.nan),
            (np.nan, "Merck & Co."),
            ("", "Novartis AG"),
            ("Genentech Inc.", ""),
            (np.nan, np.nan),
            ("", ""),
        ]
    )
    acc: set[str] = set()
    mod.update_manufacturers_from_chunk(chunk, acc)

    assert acc == {"Pfizer Inc.", "Merck & Co.", "Novartis AG", "Genentech Inc."}


def test_preserves_raw_case_variants() -> None:
    """'Raw strings' decision: case/whitespace variants are NOT merged."""
    chunk = _chunk(
        [
            ("PFIZER INC.", "Pfizer Inc."),
            ("pfizer inc.", "PFIZER INC."),
        ]
    )
    acc: set[str] = set()
    mod.update_manufacturers_from_chunk(chunk, acc)

    assert acc == {"PFIZER INC.", "Pfizer Inc.", "pfizer inc."}


def test_accumulator_is_mutated_in_place() -> None:
    chunk = _chunk([("Pfizer Inc.", "Pfizer Inc.")])
    acc: set[str] = {"Pre-existing Co."}
    result = mod.update_manufacturers_from_chunk(chunk, acc)

    assert result is None  # mutates in place
    assert acc == {"Pre-existing Co.", "Pfizer Inc."}


def test_empty_chunk_is_a_noop() -> None:
    chunk = _chunk([])
    acc: set[str] = {"Pre-existing Co."}
    mod.update_manufacturers_from_chunk(chunk, acc)

    assert acc == {"Pre-existing Co."}
