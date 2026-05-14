# TODO

Tracked, actionable work items pending in this repository.

For full project history, architectural rationale, and completed work, see the
session plan at `~/.claude/plans/this-repo-is-a-serene-meadow.md` (not
git-tracked). This file lives in the repo and contains only the *pending*
items, organized by area.

---

## Filterable-column follow-ups

### Name-suffix piping from name columns (`NAME_SUFFIX_EXTRACTED`)

**Motivation:** CMS empirically puts suffixes in the wrong column. A 14.6M-row
2023 general-payments survey found **1,587 rows** with a clean suffix
concatenated into `Last_Name` (e.g. `LEYBA JR`, `KERR II`, `JACKSON JR`,
`BELL III`, `GARZA, III`, `Cowley Jr`). The dedicated `Covered_Recipient_Name_Suffix`
column is populated on only 0.97% of rows; piping the leaked suffixes would
recover ~14% additional suffix signal.

**Design (recommended):**

- Add `extract_suffix_from_payment_name_columns(first, middle, last) -> str | None`
  in `names.py`. Primarily scans `last_name` (cleanest source — virtually all
  hits are real leaks). Optionally scans `first_name` / `middle_name` with
  the additional constraint that `V` must be excluded from the whitelist for
  those columns (95% false-positive rate — `V` in middle/first is almost
  always a middle initial, not Roman-V).
- New `PaymentFilters.NAME_SUFFIX_EXTRACTED` enum entry — separate from the
  existing `PaymentFilters.NAME_SUFFIX` so the selection layer can weight
  pristine-CMS-data hits vs. heuristic-recovery hits differently.
- `filter_by_name_suffix_extracted` follows the same hit-only + strict-
  equality pattern as `filter_by_name_suffix`.
- Apply the extraction at column-read time so the matcher sees both
  `name_suffix` (from CMS's dedicated column) and `name_suffix_extracted`
  (from name-column piping) as independent dimensions.

**Caveats from the data:**

- 28,176 of 33,268 middle-name hits are literal `V` / `V.` — middle initials,
  not Roman-V suffixes. Exclude `V` from the middle/first whitelist.
- First-name has only 650 hits total, mostly `V`-as-initial false positives.
- Last-name is the only clean source.

**Defer until:** Selection-layer extraction (Section 5.7) lands. The two
filters' different confidence levels are the whole point of splitting them —
without a pluggable selector, the weight difference can't be expressed.

### Additional filterable columns (if/when child apps need them)

- **Address + Zip Code** (`Recipient_Primary_Business_Street_Address_Line1/2`,
  `Recipient_Zip_Code`) — higher specificity than city/state but only useful
  if child apps have address data on their conflicted side. Defer until asked.
- **Manufacturer / GPO** — only useful as a match dimension when a child app
  knows "this conflicted has a known relationship with manufacturer X". Niche.

---

## Negative filter assessment (Section 5.8) — DONE

Filter return type changed from `bool` to tri-state
:class:`FilterOutcome` (`MATCH` / `DISAGREE` / `NO_DATA`):
- **MATCH**: both sides have data and agree → `PaymentFilters` appended to `filters`.
- **DISAGREE**: both sides have data and conflict → appended to `negative_filters`
  (new parallel column, populated by `Conflicted_x_PaymentIDs.filter_payment`).
- **NO_DATA**: at least one side is blank, or supersession (e.g. CITYSTATE
  already matched → CITY returns NO_DATA) → nothing accumulated.

13 `filter_by_*` methods refactored across:
- `names.py` (firstname, firstname_partial, first_middle_name, middle_initial,
  middlename, name_suffix)
- `credentials.py` (credential)
- `citystates.py` (city, state, citystate)
- `specialtys.py` (specialty, subspecialty, fullspecialty)
- `npi.py` (npi)

`merge_by_last_name` now inserts both `filters` and `negative_filters`
columns at row creation. `negative_filters` carries through the dedupe
+ selector pipeline into `unique_ids` / `unmatched_options`.

Test coverage:
- New `test_filter_outcomes.py` — 79 parametrized tests pinning the
  MATCH/DISAGREE/NO_DATA distinction for every filter, plus 3 tests on
  `filter_payment` routing (MATCH → filters, DISAGREE → negative_filters,
  NO_DATA → neither).
- `test_end_to_end.py` gained 3 Section 5.8 tests:
  - Scenario C (David A. Smith → 301 Andrew / 302 Brandon): captures the
    pre-dedupe merged frame and asserts profile_id 302 has
    `MIDDLE_INITIAL ∈ negative_filters` (B != A is real DISAGREE).
  - Scenario E (Emily White ambiguous, no middle name on either side):
    unmatched_options rows have empty `negative_filters` (NO_DATA, not
    DISAGREE — the canonical "absent signal" case from the plan).
  - Winning rows in unique_ids have empty `negative_filters` (the matcher
    selected the clean winner).

All 452 tests passing; ruff clean.

`DefaultMatchSelector` ignores `negative_filters` (behavior preserved).
The signal is now available to future selectors that want to use it —
see `TieredConfidenceSelector` below.

---

## Selection-layer extraction (Section 5.7) — DONE

Implemented in [src/open_payments/selectors.py](src/open_payments/selectors.py):
- `SelectorResult` (frozen dataclass) — selector return type; `unique`
  + `unmatched_options_from` convenience constructors with invariant validation.
- `MatcherContext` (typing.Protocol) — interface the selector reads from
  the matcher. ConflictedPaymentIDs satisfies it via existing mixin methods.
- `MatchSelector` (ABC) — strategy base class.
- `DefaultMatchSelector` — the legacy cascade extracted verbatim. Split
  into named phases (`_resolve_highest_tiebreak`) so subclasses can override
  one phase rather than the whole `select()`.
- `IdentifierWinsSelector` — deans NPI-wins pattern. Class-var
  `IDENTIFIER_FILTERS` defaults to `{PaymentFilters.NPI}`; expand via subclass.

`ConflictedPaymentIDs.__init__` gained `selector=None` parameter. The old
`process_filtered_payments_x_conflicteds` is now a thin wrapper:
- `_dedupe_by_profile_id(df)` extracted as a static method.
- `_apply_selector_result(result, df)` writes the SelectorResult into
  unique_ids / unmatched / unmatched_options.

11 new tests in `test_selectors.py` cover SelectorResult invariants,
DefaultMatchSelector behavior preservation, IdentifierWinsSelector
NPI-wins / fallback / custom IDENTIFIER_FILTERS / end-to-end with real
fixture. All 370 tests passing.

### TieredConfidenceSelector — next natural step now that Section 5.8 has landed

A third built-in selector porting deans's `match_confidence.py` tier rules
(HIGH_NPI / MEDIUM_HIGH_NAME_PLUS / LOW_NAME_ONLY / VERY_LOW_LASTNAME_BARE)
is now buildable — Section 5.8 added the negative-filter signal the tier
rules need to distinguish "middle name agrees" from "middle name not present"
from "middle name actively disagrees" (the deans `LOW_LASTNAME_PLUS_ONE` tier
specifically describes disagreement).

Design sketch:
- New `TieredConfidenceSelector` in `selectors.py`, subclasses
  `DefaultMatchSelector` so behavior preservation is the fallback.
- Tier rules read both `filters` (positive evidence) and `negative_filters`
  (DISAGREE signals) per row to score confidence. Example tier:
  `LOW_LASTNAME_PLUS_ONE` fires when only LASTNAME + one positive filter
  are present AND `negative_filters` is non-empty (active disagreement).
- Override surface: a `TIER_RULES: list[TierRule]` class var so child apps
  can replace / extend the deans defaults without rewriting `select()`.

Defer the build to when there's a child-app need (deans is the obvious one).

---

## Research Principal Investigator block handling (planned Section 5.9)

**Problem:** Research CSVs have **252 columns** including up to 5 Principal
Investigator blocks (`Principal_Investigator_1..5_NPI`, `_*_Name`, `_*_Specialty`,
etc.). The current matcher only scans `Covered_Recipient_*` for research
payments, so any conflicted who is a PI (not the Covered_Recipient) silently
goes unmatched.

**Defer until:** NPI lands (done), Name_Suffix lands (done), selection-layer
extraction lands. Vectorization (Section 6) is also a prerequisite —
per-row 6× scanning of the existing `.iterrows()` loop is untenable.

---

## Payment-class differential column audit — DONE

Audit verified the padding pattern is correctly applied across all 3 CMS-side
mixins that need it (credentials, specialtys, citystates). NPI doesn't need
padding (single column per class, just a rename). Names doesn't need padding
(single column, same shape post-rename).

**Test coverage added** in `tests/test_payment_class_differential.py` (8 tests)
locks in the contract that all 3 payment classes produce the same canonical
output column set (`profile_id`, `npi`, `first_name`, `last_name`,
`credentials`, `specialtys`, `citystates`, `name_suffix`, `payment_class`).

**Remaining (deferred):** the padding pattern is implicit coupling — a new
mixin author could forget to add the `update_ownership_payments` override and
ownership would silently break. The longer-term cleanup is a column-count-
aware aggregator that doesn't need padding. For now, the test set guards the
existing behavior; refactor when there's a real driver to add a 5th CMS-side
mixin that needs ownership padding.

---

## Section 5 remaining bugs

All resolved in the Section 5 bug-fix batch:

- **0d** — `SettingWithCopyWarning` in `add_unmatched` — fixed by `.copy()` at entry. Regression test `test_end_to_end.py::test__regression_bug_0d_no_setting_with_copy_warnings` asserts zero warnings.
- **1** — `set_index` discarded at `conflicteds.py:70` — removed. Provider_pk is now explicitly a column. Pinned by `test__regression_bug_1_set_index_provider_pk_remains_a_column`.
- **2** — NaN-sort non-determinism at `ids.py:316-323` — added `na_position="last"` + `payment_id` tiebreaker.
- **4** — Re-evaluated; the `.copy()` call at `payments.py:346` already mitigates the original issue. No code change needed.
- **5** — PhysicianFilter test logic flaw — `reset_index(drop=True)` in setUp; scalar `.loc[idx]` replaces the `==idx -> .any()` workaround.
- **6** — Silent profile_id drop in `filter_payment_chunk` — now emits `logging.warning` with the dropped-row count.

---

## Sections 6–8 (later)

- **Section 6 — Vectorize the matcher.** Replace `.iterrows()` at
  `ids.py:240` and `.apply(lambda)` at `ids.py:288` with a join-based flow.
  Single merge on last_name; per-filter vectorized predicate; group-by
  `provider_pk` for the narrowing. Keep the old class as
  `LegacyConflictedPaymentIDs` for one release.
- **Section 7 — Public API + typed input/output contract.** `schemas.py`
  with pydantic `ConflictedProviderInput` + `SearchResult` (replacing the
  loose-DataFrame contracts). Single high-level
  `find_payments_for_conflicted_providers(conflicteds, settings) -> SearchResult`
  in `__init__.py`.
- **Section 8 — Documentation & contribution hardening.** README example
  for child-app wrapping; `docs/architecture.md`; `CONTRIBUTING.md`;
  `py.typed` marker.
