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

### TieredConfidenceSelector — DONE

Added to [src/open_payments/selectors.py](src/open_payments/selectors.py)
alongside the existing `DefaultMatchSelector` / `IdentifierWinsSelector`.

Design (v2, after the v1 demotion-guards approach was rejected for
collapsing positive-evidence information):

- **Tier rules** are positive-signal predicates ported verbatim from
  deans's match_confidence.py: 6 tiers
  (`HIGH_NPI`, `MEDIUM_HIGH_NAME_PLUS`, `MEDIUM_NAME_PARTIAL`,
  `LOW_LASTNAME_PLUS_ONE`, `LOW_NAME_ONLY`, `VERY_LOW_LASTNAME_BARE`)
  plus a `VERY_LOW_OTHER` fallback. Negative-signal info does NOT
  affect tier assignment — a `MEDIUM_HIGH_NAME_PLUS` row stays at that
  tier even when negative_filters is non-empty.
- **Negative-signal info is preserved on parallel output columns**:
  every output frame (`unique_ids`, `unmatched`, `unmatched_options`)
  carries `negative_filters` (list), `n_negative_filters` (int tally),
  and `confidence_tier` (str | None). Analysts can re-stratify
  confident matches by negative-signal count at review time.
- **Selection-time tiebreak by negative count**: when multiple rows
  share the best tier, the selector prefers the row(s) with fewest
  `negative_filters`. Real-world motivation: a deans
  `MEDIUM_HIGH_NAME_PLUS` row with `MIDDLE_INITIAL` in negative_filters
  was empirically a false positive — the clean same-tier alternative
  should win.
- **Override surface**: subclasses customize via `TIER_RULES`,
  `FALLBACK_TIER`, `MIN_ACCEPTABLE_TIER_RANK` class vars, or override
  `select()` outright.

`SelectorResult` gained `representative_negative_filters` and
`confidence_tier` fields. `add_unique_id` and `add_unmatched` propagate
all three new columns through.

Tests:
- `test_selectors.py` — 28 selector tests including parametrized tier
  rule coverage and the real-world same-tier negative-tiebreak case.
- `test_end_to_end.py` — pins that all three output frames carry the
  new columns and that `n_negative_filters` agrees with
  `len(negative_filters)` row-by-row.

474 total tests passing; ruff clean.

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

## Section 7 — Public API + typed input/output — DONE

Single high-level entrypoint plus a `SearchResult` output bundle so child
apps can depend on a stable surface:

- [src/open_payments/api.py](src/open_payments/api.py) —
  ``find_payments_for_conflicted_providers(conflicteds, settings, selector,
  payments, parse_conflicteds) -> SearchResult``. Auto-detects whether the
  input is raw (needs ``Conflicteds`` parsing) or pre-parsed (already
  matches ``REQUIRED_CONFLICTED_COLUMNS``); ``parse_conflicteds=`` forces
  either. Settings-aware payment loading.
- [src/open_payments/schemas.py](src/open_payments/schemas.py) —
  ``SearchResult`` (frozen dataclass holding the three result frames plus
  ``to_excel`` / ``update_excel`` / ``from_excel`` methods, replacing the
  free functions in helpers.py). ``ConflictedProviderRow`` pydantic model
  documents the per-row input contract; ``validate_conflicteds_df`` is the
  cheap runtime gate.
- [src/open_payments/__init__.py](src/open_payments/__init__.py) — exports
  the documented public surface: ``find_payments_for_conflicted_providers``,
  ``Settings``, ``SearchResult``, ``ConflictedProviderRow``,
  ``validate_conflicteds_df``, the four selector classes,
  ``SelectorResult``, and the three enums (``PaymentFilters``,
  ``FilterOutcome``, ``Unmatcheds``).

xlsx merge dedup switched to a ``(provider_pk, profile_id)`` key (instead
of full-row equality), because list-valued columns get stringified on
xlsx round-trip and the resulting dtype asymmetry crashed pandas's
``.eq()`` broadcast. Key-based dedup also captures the actual identity
of an unmatched option (different filter sets for the same option are
still the same option).

16 new tests in [test_api.py](src/open_payments/tests/test_api.py). 490
total passing; ruff clean.

The older ``helpers.update_or_create_conflicteds_ids`` / etc. free
functions remain in [helpers.py](src/open_payments/helpers.py) for
backwards compatibility with anything that imports them directly, but
new code should use the SearchResult methods.

---

## Sections 6 and 8 (later)

- **Section 6 — Vectorize the matcher.** Replace `.iterrows()` at
  `ids.py:240` and `.apply(lambda)` at `ids.py:288` with a join-based flow.
  Single merge on last_name; per-filter vectorized predicate; group-by
  `provider_pk` for the narrowing. Keep the old class as
  `LegacyConflictedPaymentIDs` for one release.
- **Section 8 — Documentation & contribution hardening.** README example
  for child-app wrapping; `docs/architecture.md`; `CONTRIBUTING.md`;
  `py.typed` marker.
