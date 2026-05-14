"""Regenerate the on-disk synthetic CMS fixtures used by the test suite.

The fixtures live under `cms/{year}/OP_DTL_{prefix}_PGYR{year}_*.csv` to mirror
the real CMS directory layout that `ReadPayments.get_payment_csv_path` globs
for. Whenever you change a scenario in `factories.py`, run:

    python -m open_payments.tests.fixtures.generate

from the project root to refresh the on-disk files.
"""

from __future__ import annotations

from pathlib import Path

from ..factories import (
    make_general_csv_df,
    make_ownership_csv_df,
    make_research_csv_df,
)

# Postfix mimics CMS's MMDDYYYY publication-date suffix. The exact value is
# irrelevant to the test pipeline — it's resolved by mtime in
# `ReadPayments.get_payment_csv_path` — but using a real-looking value keeps
# the fixtures honest.
POSTFIX = "P01012099"

FIXTURE_ROOT = Path(__file__).resolve().parent / "cms"


def _write(payment_class_prefix: str, year: int, df) -> Path:
    year_dir = FIXTURE_ROOT / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    path = year_dir / f"OP_DTL_{payment_class_prefix}_PGYR{year}_{POSTFIX}.csv"
    df.to_csv(path, index=False)
    return path


def regenerate(year: int = 2023) -> list[Path]:
    paths = [
        _write("GNRL", year, make_general_csv_df()),
        _write("OWNRSHP", year, make_ownership_csv_df()),
        _write("RSRCH", year, make_research_csv_df()),
    ]
    return paths


if __name__ == "__main__":
    for p in regenerate():
        print(f"wrote {p}")
