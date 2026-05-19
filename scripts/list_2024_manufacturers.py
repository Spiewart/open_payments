"""List every unique manufacturer that appears in the 2024 CMS Open Payments
general-payments dataset, across BOTH the paying-entity and submitting-entity
columns. Writes an alphabetically sorted single-column xlsx.

Decisions (see ``docs/superpowers/specs/2026-05-19-list-2024-manufacturers-design.md``):

- Union of ``payment_entity`` + ``submitting_entity`` source columns.
- Alphabetical sort, names only (no metrics).
- All covered recipients (no MD/DO filter applied).
- Raw strings — case/whitespace/punctuation variants are NOT merged.

Path resolution reuses ``open_payments.config.Settings`` so this script
keeps working when CMS reissues the 2024 file under a new
publication-date suffix.

Usage:
    python scripts/list_2024_manufacturers.py
    python scripts/list_2024_manufacturers.py --output /some/path.xlsx
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
from pathlib import Path

import pandas as pd

# The script lives at <repo>/scripts/; the package at <repo>/src/.
# Add src/ to sys.path so we can import Settings without installing the package.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = _REPO_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from open_payments.config import Settings  # noqa: E402

PAYING_COLUMN = "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name"
SUBMITTING_COLUMN = "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name"
MANUFACTURER_COLUMNS: tuple[str, str] = (PAYING_COLUMN, SUBMITTING_COLUMN)

CHUNK_SIZE = 50_000
DEFAULT_OUTPUT = "2024_unique_manufacturers.xlsx"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def update_manufacturers_from_chunk(
    chunk: pd.DataFrame,
    manufacturers: set[str],
) -> None:
    """Union the unique non-null, non-empty manufacturer names from ``chunk``
    into the ``manufacturers`` accumulator.

    Reads both CMS manufacturer columns. Skips ``NaN`` and empty strings —
    CMS uses both for "absent". Mutates ``manufacturers`` in place; returns
    ``None``.
    """
    for column in MANUFACTURER_COLUMNS:
        series = chunk[column].dropna()
        series = series[series != ""]
        manufacturers.update(series.unique())


def resolve_2024_csv(settings: Settings | None = None) -> Path:
    """Glob-resolve the 2024 general-payments CSV via Settings.

    Mtime-sorted last-write-wins, mirroring
    ``ReadPayments.get_payment_csv_path``.
    """
    settings = settings if settings is not None else Settings()
    pattern = settings.csv_glob("general", 2024)
    matches = sorted(glob.glob(pattern), key=os.path.getmtime)
    if not matches:
        raise FileNotFoundError(f"No 2024 general-payments CSV matched: {pattern}")
    return Path(matches[-1])


def collect_unique_manufacturers(csv_path: Path) -> list[str]:
    """Stream ``csv_path`` in chunks, return the sorted list of unique
    manufacturer names across both source columns."""
    manufacturers: set[str] = set()
    n_chunks = 0
    n_rows = 0
    for chunk in pd.read_csv(
        csv_path,
        usecols=list(MANUFACTURER_COLUMNS),
        dtype=str,
        chunksize=CHUNK_SIZE,
        engine="c",
        low_memory=False,
    ):
        update_manufacturers_from_chunk(chunk, manufacturers)
        n_chunks += 1
        n_rows += len(chunk)
        if n_chunks % 20 == 0:
            log.info(
                "  %d chunks, %d rows scanned, %d unique names so far",
                n_chunks,
                n_rows,
                len(manufacturers),
            )
    log.info("Done: %d chunks, %d rows, %d unique manufacturers", n_chunks, n_rows, len(manufacturers))
    return sorted(manufacturers)


def write_xlsx(names: list[str], output_path: Path) -> None:
    """Write a single-sheet, single-column xlsx with the sorted names."""
    pd.DataFrame({"manufacturer": names}).to_excel(
        output_path, sheet_name="manufacturers", index=False
    )
    log.info("Wrote %d names to %s", len(names), output_path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(DEFAULT_OUTPUT),
        help=f"Output xlsx path (default: {DEFAULT_OUTPUT} in cwd)",
    )
    args = parser.parse_args(argv)

    csv_path = resolve_2024_csv()
    log.info("Reading %s", csv_path)
    names = collect_unique_manufacturers(csv_path)
    write_xlsx(names, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
