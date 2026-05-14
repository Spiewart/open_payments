# open_payments

Match conflicted-provider data against CMS [Open Payments](https://www.cms.gov/openpayments) datasets.

A child app supplies a DataFrame of providers (scraped names, license rosters, deans lists, etc.) and gets back a `SearchResult` mapping each provider to an Open Payments `profile_id` — or to an unmatched/ambiguous bucket for manual review.

## Install

```bash
pip install -e .[dev]   # editable + dev tools (pytest, ruff, mypy)
```

Or, when published, `pip install open_payments`.

## Configure

The matcher reads CMS CSVs from disk and supports a year list per run. Configure via env vars or pass a `Settings` instance:

```bash
export OPEN_PAYMENTS_DATA_DIR=~/open_payments_datasets
export OPEN_PAYMENTS_YEARS='[2022, 2023, 2024]'
```

```python
from open_payments import Settings

settings = Settings(
    data_dir="~/op_data",            # default: ~/open_payments_datasets
    years=[2022, 2023, 2024],        # default: [2020..2024]
    payment_classes=["general"],     # default: all three (general, ownership, research)
)
```

CSV filename pattern follows CMS: `OP_DTL_{GNRL,OWNRSHP,RSRCH}_PGYR{year}_*.csv`. Place the CSVs under `data_dir/{year}/`.

## Use

Single-call API:

```python
import pandas as pd
from open_payments import find_payments_for_conflicted_providers, Settings

# Your conflicted-provider input. Two acceptable shapes:
#   1. Raw — a DataFrame with a `name` column, etc. (the shape Conflicteds parses)
#   2. Pre-parsed — already conforms to schemas.REQUIRED_CONFLICTED_COLUMNS
#
# Auto-detected; pass parse_conflicteds=True/False to force.
conflicteds = pd.DataFrame([
    {"name": "Jane M. Brown, MD", "credential": "Physician (MD or DO)",
     "specialtys": "Family Medicine", "citystates": "Boston, MA"},
    # ...
])

result = find_payments_for_conflicted_providers(
    conflicteds=conflicteds,
    settings=Settings(years=[2023]),
)
print(result)
# SearchResult(unique=42, unmatched=3, unmatched_options=7)

result.to_excel("matches.xlsx")
```

`SearchResult` carries three DataFrames:

| Field | What's in it |
|---|---|
| `unique_ids` | One row per conflicted provider matched to a single CMS `profile_id`. |
| `unmatched` | One row per conflicted with no unique match. The `unmatched` column tags `NOLASTNAME` or `UNFILTERABLE`. |
| `unmatched_options` | For `UNFILTERABLE` cases, the CMS candidates the matcher couldn't disambiguate. Multi-row per `provider_pk`. |

Each row also carries audit columns: `filters` (positive signals), `negative_filters` (active disagreements), `n_negative_filters` (tally), and `confidence_tier` (assigned when using a tier-aware selector — see below).

## Customize matching: selectors

Different studies need different selection rules. Pass `selector=` to control how a winning row is picked from candidates:

```python
from open_payments import (
    find_payments_for_conflicted_providers,
    DefaultMatchSelector,
    IdentifierWinsSelector,
    TieredConfidenceSelector,
)

# 1. Legacy cascade: firstname → middlename → citystate → highest-filter-count.
result = find_payments_for_conflicted_providers(conflicteds, selector=DefaultMatchSelector())

# 2. Identifier-wins (e.g. deans NPI pattern): if any candidate row has NPI in
#    its filters, that row wins immediately. Falls back to cascade otherwise.
result = find_payments_for_conflicted_providers(conflicteds, selector=IdentifierWinsSelector())

# 3. Tier-based (deans match_confidence rules + Section 5.8 negative-signal
#    tiebreak): scores each candidate by confidence tier, picks the highest
#    tier, breaks ties by fewest negative filters. Surfaces `confidence_tier`
#    on every output row.
result = find_payments_for_conflicted_providers(conflicteds, selector=TieredConfidenceSelector())
```

Subclass `MatchSelector` for full control. The three built-in selectors are documented in [src/open_payments/selectors.py](src/open_payments/selectors.py).

## Customize parsing: ConflictX mixins

The `Conflicteds` orchestrator parses raw child-app input into the canonical column shape the matcher consumes. Each dimension has a dedicated mixin with overridable defaults:

| Mixin | What it parses |
|---|---|
| `ConflictNames` | `name` string → `first_name` / `last_name` / `middle_initial_1` / `middle_initial_2` / `middle_name_1` / `middle_name_2` / `name_suffix` |
| `ConflictCredentials` | `credential` string → `list[Credentials]` |
| `ConflictCityStates` | `citystates` string → `list[CityState]` |
| `ConflictSpecialtys` | `specialtys` string → `list[Specialtys]` |
| `ConflictNPI` | `npi` value → `Int64` |

Each mixin has two parsing layers — one for the **CMS side** (fixed taxonomy, never override) and one for the **conflicted side** (project-specific, overrideable). To plug in custom input shape, subclass the relevant mixin:

```python
from open_payments.specialtys import ConflictSpecialtys, Specialtys

class MySpecialtys(ConflictSpecialtys):
    # Lightest override: a class-var map for common cases.
    SPECIALTY_MAP = {
        "FM": [Specialtys(specialty="Family Medicine")],
        "IM": [Specialtys(specialty="Internal Medicine")],
        # ...
    }

# Or override get_specialtys(row) for per-row logic, or conflict_specialtys()
# to replace the whole pipeline.
```

## Audit columns: positive + negative signals

Every `filter_by_*` returns a tri-state `FilterOutcome`:

- `MATCH` — both sides have data and agree → filter accumulates in `filters` list.
- `DISAGREE` — both sides have data and conflict → filter accumulates in `negative_filters` list.
- `NO_DATA` — at least one side blank → nothing accumulates.

This lets analysts distinguish "middle name absent" from "middle name actively disagrees" — the latter is strong negative evidence for false positives. `n_negative_filters` is a tally column for quick sort/filter.

Example query: find suspect matches in the result:

```python
result = find_payments_for_conflicted_providers(conflicteds, selector=TieredConfidenceSelector())

# All MEDIUM_HIGH matches that had at least one disagreement.
suspicious = result.unique_ids[
    (result.unique_ids["confidence_tier"] == "MEDIUM_HIGH_NAME_PLUS")
    & (result.unique_ids["n_negative_filters"] > 0)
]
```

## Repository structure

```
src/open_payments/
  __init__.py           public exports
  api.py                find_payments_for_conflicted_providers entrypoint
  schemas.py            SearchResult dataclass + input contract
  config.py             Settings (pydantic-settings env vars)
  conflicteds.py        Conflicteds orchestrator (input normalization)
  ids.py                ConflictedPaymentIDs matcher
  selectors.py          DefaultMatchSelector / IdentifierWinsSelector / TieredConfidenceSelector
  choices.py            PaymentFilters / FilterOutcome / Unmatcheds / Credentials / States enums
  names.py              ConflictNames + name-parsing helpers + filter_by_firstname/etc.
  credentials.py        ConflictCredentials + filter_by_credential
  citystates.py         ConflictCityStates + filter_by_city/state/citystate
  specialtys.py         ConflictSpecialtys + filter_by_specialty/subspecialty/fullspecialty
  npi.py                ConflictNPI + filter_by_npi
  read.py               CMS CSV reading
  payments.py           payment-aggregation helpers
  helpers.py            legacy xlsx-persistence helpers (kept for back-compat; new code uses SearchResult)
  tests/                493 tests including synthetic fixtures
```

## Run the tests

```bash
pytest -m "not integration"      # 493 tests, no real CMS data needed
pytest -m integration            # extra checks that need ~/open_payments_datasets
ruff check src
ruff format --check src
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to add a new filter, extend payment classes, or land a new year's CMS publication.

Open work items live in [TODO.md](TODO.md).

## License

MIT.
