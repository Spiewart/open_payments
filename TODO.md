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

## Section 5.9 — Research Principal Investigator block handling — DONE

Research CSVs publish up to 5 ``Principal_Investigator_N_*`` column blocks
per row in addition to the ``Covered_Recipient_*`` block. The matcher
historically only scanned Covered_Recipient, silently missing every
conflicted who's a PI but not the principal recipient. For ABIM
(internal-medicine board certification) this was the dominant gap on
research payments.

**Implementation — explode-on-read** (in
[src/open_payments/research_pi.py](src/open_payments/research_pi.py)):

1. Each filter mixin's ``research_columns`` extends with
   ``Principal_Investigator_N_*`` CMS columns via
   ``pi_block_cms_columns_for_dtype_dict(general_columns)``.
2. ``ReadPayments.update_csv_kwargs`` now intersects ``usecols`` with the
   actual CSV header so older CMS years (without PI blocks) and the
   general/ownership CSVs (no PI blocks at all) load cleanly.
3. ``ReadPayments.filter_payment_chunk`` calls
   ``explode_research_pi_blocks(chunk)`` for the research payment class
   after the standard MD/DO + profile_id filters.
4. ``explode_research_pi_blocks`` transforms each input row into 1 + N
   sub-rows (1 Covered_Recipient + N populated PI slots). PI block CMS
   columns are renamed to their Covered_Recipient equivalents so all
   downstream code (rename, list-column builders, the matcher itself,
   the cross-merge from Section 6) sees a uniform shape with no PI
   awareness.
5. A ``person_slot`` provenance column is added to every research row:
   ``"covered_recipient"`` or ``"pi_1"`` through ``"pi_5"``. Carries
   through into ``unique_ids`` so analysts know which slot fired.

Verified on real CMS 2023 research data: explodes cleanly, person_slot
distribution matches expectation (CR + sparse PI blocks).

**Tests** in [test_research_pi.py](src/open_payments/tests/test_research_pi.py)
— 18 new tests:
- Helper-level: column-expansion math (5 slots × N CR cols), suffix
  asymmetry handling (Recipient_City vs Principal_Investigator_N_City),
  unmapped-column tolerance.
- Explode-level: empty / partially-populated / fully-populated PI blocks,
  slot-prefixed renames produce uniform CR column names, unpopulated
  slots dropped.
- End-to-end: synthetic research fixture extended with a Trial Coordinator
  CR + Sarah Kim (PI_1) + Raj Patel (PI_2). Three end-to-end scenarios
  pin that:
  - Sarah Kim matches profile_id=801 via the ``pi_1`` slot.
  - Raj Patel matches profile_id=802 via the ``pi_2`` slot.
  - Adams (CR-only) still matches profile_id=101 via ``covered_recipient``
    (no regression).
  - Both PIs matchable simultaneously when both are in the conflicteds list.
- Edge: missing PI columns in CSVs (general/ownership shape) handled
  gracefully via the tolerant usecols filter.

521 total tests passing; ruff clean.

**Architectural note** — the explode is BEFORE the matcher's cross-merge
(Section 6), so the cross-merge sees the long-form per-slot frame and
benefits from vectorization across all (payment, person_slot) pairs.
This is what made full Section 6 vectorization a prerequisite per the
original plan.

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

## Section 6 — Matcher vectorization — DONE

Two-phase optimization landed:

### Phase 1 — Inner filter loop (scoped, already shipped earlier)

Replaced the inner ``for payment_filter in self.filters:
merged.apply(filter_payment, axis=1)`` loop with a single
``merged.apply(self.apply_all_filters_to_row, axis=1)`` that visits each
row once and runs every filter in ``self.filters`` order.

- 14 separate row-wise pandas-apply iterations → 1 iteration per merged frame.
- Benchmark (100 conflicteds, single profile per CMS row): **0.80s → 0.34s
  (~2.3× speedup)**.

### Phase 2 — Cross-merge vectorization (NEW)

Replaced the outer ``for _, conflicted in conflicteds.iterrows()`` loop
with a hybrid two-phase pipeline in
``Conflicted_x_PaymentIDs.search_for_conflicteds_ids``:

1. ``_vectorized_search`` — single cross-merge of ``self.payments``
   × ``self.conflicteds`` on lowercased ``last_name`` (case-insensitive
   equality), one ``apply`` to evaluate all filters across the joint
   frame, then ``groupby('provider_pk')`` to feed each conflicted's
   candidates to the configured selector. Handles every provider whose
   last name has an exact CMS match.
2. **Per-provider fallback** — provider_pks NOT covered by phase 1 fall
   through to the legacy ``filter_payments_for_conflicted`` path, which
   uses ``merge_by_last_name``'s multi-word ``str.contains`` fallback.
   Necessary for cases like conflicted "John Smith Jones" vs CMS
   "Smith-Jones" where exact-key match misses but the multi-word
   fallback hits. Also catches the NOLASTNAME case.

Subclasses can opt out of vectorization by overriding ``_vectorized_search``
to return ``set()``.

Benchmarks (synthetic fixture):

| Workload | Per-provider only | Vectorized | Speedup |
|----------|-------------------|-----------:|--------:|
| 600 conflicteds, all matching one profile | 1.83s | 0.87s | 2.1× |
| 600 mixed lastnames across 5 fixture profiles | — | 1.30s | — |

Real-world payoff is larger because real CMS data has more candidates per
common last name (Smith, Jones, etc.) — the vectorized path amortizes the
pandas merge/concat overhead across all conflicteds at once instead of
paying per-conflicted.

9 new tests in [test_vectorized_search.py](src/open_payments/tests/test_vectorized_search.py):
- Three behavior-parity tests (vectorized output ≡ per-provider output for
  unique_ids / unmatched / unmatched_options on the canonical fixture).
- Vectorized phase claims the right provider_pks (5 of 6 scenarios; the
  no-last-name case correctly falls through).
- Hyphenated last name handled by exact-key merge.
- NOLASTNAME path still fires through fallback.
- Empty payments / empty conflicteds edge cases.
- Subclass opt-out via ``_vectorized_search`` override.

503 total tests passing; ruff clean.

---

## Section 8 — Documentation — DONE

- [README.md](README.md) — install, configure, single-call API example,
  selector overview, ConflictX mixin override surface, audit-column
  query example, repository layout, test commands.
- [CONTRIBUTING.md](CONTRIBUTING.md) — architectural overview (filter
  application vs. selection), how-to guides for adding a new filter /
  new year / new payment class, style + conventions.
- ``src/open_payments/py.typed`` — marker file telling mypy/pyright the
  package ships type hints. Wired through
  ``[tool.hatch.build.targets.wheel]`` in pyproject.toml.

Plus a real bug-fix surfaced by writing the README example:
[conflicteds.py](src/open_payments/conflicteds.py) ``remove_non_us`` and
the ``article/rank/entity`` drop were hard-coded to the deans schema and
would ``KeyError`` for any non-deans child app. Both now tolerate
missing columns:
- ``remove_non_us`` no-ops when ``non_us`` isn't present.
- The article/rank/entity drop uses ``errors="ignore"``.

Regression test ``test__api_minimal_columns_input_matches_readme_example``
pins the minimal 4-column input path. 494 tests passing.
