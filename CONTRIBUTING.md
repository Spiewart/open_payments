# Contributing to open_payments

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

Verify:

```bash
pytest -m "not integration" -q
ruff check src && ruff format --check src
```

493 tests should pass; ruff should be clean.

## Architectural overview

The package has two distinct layers that are easy to confuse:

### Filter APPLICATION (per-row predicates)

Each match dimension (name, credentials, citystates, specialty, NPI, name-suffix) has its own module that defines:

1. A `Conflict<Domain>` mixin parsing raw child-app input into the canonical column shape (override the conflicted-side parsing; never override the CMS-side parsing).
2. A `PaymentIDs<Domain>Mixin` defining one or more `filter_by_*` classmethods. Each `filter_by_*` returns a tri-state `FilterOutcome` (`MATCH` / `DISAGREE` / `NO_DATA`).
3. A `PaymentFilters.<NAME>` enum entry in [choices.py](src/open_payments/choices.py).

The matcher runs each filter against each candidate row. `MATCH` outcomes accumulate in the row's `filters` list; `DISAGREE` outcomes accumulate in `negative_filters`; `NO_DATA` accumulates nothing.

### Filter SELECTION (winner-picking)

[selectors.py](src/open_payments/selectors.py) defines `MatchSelector` strategies that take the post-application frame and decide which row(s) win. Three built-ins:

- `DefaultMatchSelector` — legacy cascade (firstname → middlename → citystate → highest-filter-count).
- `IdentifierWinsSelector` — NPI (or any class-var-configured identifier) wins immediately when present and unique.
- `TieredConfidenceSelector` — scores each row by tier (HIGH_NPI / MEDIUM_HIGH / ... / VERY_LOW), picks the highest tier, breaks ties by fewest negative signals.

**Application is generic; selection is study-specific.** Add a new filter when you have a new column to look at. Add a new selector when you have a new way of weighing existing columns.

## How to add a new filter

Example: adding a `filter_by_practice_state` that compares the conflicted's known practice state against the CMS row's License_State column.

### 1. Add the enum entry

Edit [src/open_payments/choices.py](src/open_payments/choices.py):

```python
class PaymentFilters(StrEnum):
    # ...existing entries...
    PRACTICE_STATE = "PRACTICE_STATE"
```

### 2. Build the filter module

Create `src/open_payments/practice_state.py`. Match the existing module structure (see [npi.py](src/open_payments/npi.py) for the simplest example):

```python
class PaymentIDsPracticeStateMixin:
    """Filter-application layer for practice state."""

    @property
    def filters(self):
        return super().filters + [PaymentFilters.PRACTICE_STATE]

    @classmethod
    def filter_by_practice_state(cls, payments_x_conflicted) -> FilterOutcome:
        """MATCH when conflicted's known state ∈ CMS license-state list,
        DISAGREE when both sides have data and there's no overlap,
        NO_DATA when either side is blank."""
        ...


class ConflictPracticeState:
    """Parses conflicted-side input. Override per study."""

    PRACTICE_STATE_COLUMN = "practice_state"

    def conflict_practice_state(self) -> pd.DataFrame:
        ...
```

### 3. Wire into the matcher

Edit [src/open_payments/ids.py](src/open_payments/ids.py) to add `PaymentIDsPracticeStateMixin` to `ConflictedPaymentIDs`'s MRO.

Edit [src/open_payments/conflicteds.py](src/open_payments/conflicteds.py) to add `ConflictPracticeState` to `Conflicteds`'s MRO.

### 4. Tests

Write `tests/test_practice_state.py` covering:

- `filter_by_practice_state` returns each outcome (`MATCH` / `DISAGREE` / `NO_DATA`).
- `ConflictPracticeState.conflict_practice_state()` parses the expected input shapes (including the override surface — show a subclass example).
- An end-to-end smoke that runs the full pipeline with the new filter active.

For tri-state coverage, follow the pattern in [tests/test_filter_outcomes.py](src/open_payments/tests/test_filter_outcomes.py) — parametrized `(positive_filters, negative_filters, expected_outcome)` tuples.

## How to add a new year's CMS publication

CMS publishes annual data with a year-suffixed filename, e.g. `OP_DTL_GNRL_PGYR2024_P01012099.csv`. The `_P01012099` suffix is CMS's publication-date marker and can change in re-issues.

### 1. Place the files

Drop the new CSVs under `data_dir/{year}/` (e.g. `~/open_payments_datasets/2024/OP_DTL_GNRL_PGYR2024_P01012099.csv`).

### 2. Expand Settings.years

If you want the new year to be in the default set, edit `DEFAULT_YEARS` in [src/open_payments/config.py](src/open_payments/config.py). Otherwise, callers can pass `Settings(years=[..., new_year])` explicitly.

### 3. No code changes needed for new years

The CSV reader uses a glob-based filename resolver (see `get_payment_csv_path` in [read.py](src/open_payments/read.py)) so re-issues with different `_PMMDDYYYY` suffixes work without code changes.

### 4. Audit the new data

CMS occasionally introduces new credential strings, specialty taxonomies, or column shifts. After loading the new year:

```python
from open_payments.credentials import unique_credentials
from open_payments.specialtys import unique_specialties

unique_credentials(year=2024)   # prints distinct credential strings
unique_specialties(year=2024)
```

If the new year introduces values not in our enums (`Credentials`, `Specialtys`), add them to [choices.py](src/open_payments/choices.py).

## How to add a new payment class

Currently three CMS payment classes are supported: `general`, `ownership`, `research`. The column-shape differential between them is handled in each `PaymentIDs<Domain>Mixin` (general/research have 6 specialty/credential columns; ownership has 1; license-state count differs too — see [tests/test_payment_class_differential.py](src/open_payments/tests/test_payment_class_differential.py)).

If CMS ever adds a fourth payment class:

1. Add it to the `Literal` type in [config.py](src/open_payments/config.py) `Settings.payment_classes`.
2. Add column-mapping properties (`<new_class>_columns`) to every relevant filter module's `Payment<Domain>` class.
3. Add an `update_<new_class>_payments` override if its column count differs from general/research.
4. Add a synthetic CSV fixture under `tests/fixtures/cms/{year}/OP_DTL_<NEW>_PGYR{year}_*.csv`.
5. Extend `tests/test_payment_class_differential.py`'s parametrized list.

## Style + conventions

- **Lint:** `ruff` (replaces flake8 + black + pylint). Configured in [pyproject.toml](pyproject.toml).
- **Test runner:** `pytest`. The `integration` marker tags tests that need real CMS data on disk; CI runs `pytest -m "not integration"`.
- **Test data:** synthetic CSVs in [tests/fixtures/cms/](src/open_payments/tests/fixtures/cms/). When you add a scenario, expand the fixture there rather than relying on local-only data.
- **Comments:** explain the *why* (a non-obvious constraint, a workaround, surprising behavior), not the *what*.
- **Docstrings:** module-top docstrings frame the file's role; class docstrings document the override surface; method docstrings document the return contract (especially `FilterOutcome` semantics).

## Releasing

(TBD — this section will fill in once the package is published.)

## License

MIT (see [LICENSE](LICENSE) when added).
