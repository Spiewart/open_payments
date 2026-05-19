# List 2024 unique manufacturers — design

**Date**: 2026-05-19
**Author**: brainstormed with Claude in worktree `friendly-taussig-175d37`
**Status**: Approved, ready for implementation plan

## Goal

Produce an Excel workbook listing every unique manufacturer name that
appears in the 2024 CMS Open Payments general-payments dataset, across
both the *paying* and *submitting* manufacturer columns.

Output: `2024_unique_manufacturers.xlsx` — single sheet (`manufacturers`),
single column (`manufacturer`), alphabetically sorted, raw strings (no
normalization or aggregation).

## Decisions (resolved during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Which manufacturer columns | Union of `payment_entity` + `submitting_entity` | Most comprehensive — captures parent/subsidiary splits where the payer and submitter differ. |
| Sort | Alphabetical (case-sensitive default) | Names-only output; raw strings means case variants sit adjacent. |
| Metrics | None | Output is a name list, not a magnitude analysis. |
| Recipient scope | All covered recipients | Matches literal request ("manufacturers that made payments to providers"); no `MD_DO_only` filter. |
| Name normalization | None (raw strings) | Preserves CMS as-published; cleaning is a separate analyst pass. |
| Code location | Standalone script at `scripts/list_2024_manufacturers.py` | Minimal — no need to extend the `open_payments` package for a one-off. |

## CMS source columns

From `open_payments.payments.Payments.general_columns`:

- `Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name` → renamed to `payment_entity` in the package
- `Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name` → renamed to `submitting_entity` in the package

The script reads the **CMS source names** directly (no package rename
step needed) since we're not threading the chunks through `Payments`.

## File location & layout

```
scripts/
  list_2024_manufacturers.py
src/open_payments/tests/
  test_list_2024_manufacturers.py   # unit test for the aggregator only
```

A new top-level `scripts/` directory is acceptable for analyst-facing
one-offs. No `__init__.py` needed.

## Architecture

```
+----------------------------+      +----------------------------+
|  Settings().csv_glob(      | ---> |  glob.glob() resolves      |
|    "general", 2024)        |      |  CMS-postfix-agnostic path |
+----------------------------+      +----------------------------+
                                                |
                                                v
                              +---------------------------------+
                              | pd.read_csv(..., usecols=[two   |
                              |   manufacturer columns],        |
                              |   chunksize=50000, dtype=str)   |
                              +---------------------------------+
                                                |
                              chunk by chunk    v
                              +---------------------------------+
                              | update_manufacturers_from_chunk |
                              |   (chunk, manufacturers: set)   |
                              | unions both columns' uniques    |
                              | into the accumulator set        |
                              +---------------------------------+
                                                |
                                                v
                              +---------------------------------+
                              | sorted(manufacturers) -> 1-col  |
                              | DataFrame -> .to_excel(path)    |
                              +---------------------------------+
```

## Components

### `update_manufacturers_from_chunk(chunk, manufacturers)`

**Signature**: `(chunk: pd.DataFrame, manufacturers: set[str]) -> None`
(mutates the accumulator in place).

**Responsibility**: read the two CMS manufacturer columns from a chunk,
extract unique non-null non-empty names from each, union into the
accumulator. Pure mutation, no return value.

**This is the user-implemented function.** Trade-offs to consider:
- Empty strings (`""`) vs `NaN` — CMS uses both for "absent".
- One pass per column vs. stacking with `pd.concat`.
- The chunk-level `.unique()` returns a small numpy array; passing that
  to `set.update()` is the natural composition.

### `main()`

Resolves the CSV path via `Settings().csv_glob("general", 2024)` and
`glob.glob` (mtime-sorted, last-write-wins — same rule as
`ReadPayments.get_payment_csv_path`). Streams chunks, calls the
aggregator on each, sorts, writes xlsx. Optional `--output PATH`
argument (default `2024_unique_manufacturers.xlsx` in `cwd`).

## Error handling

- Missing CSV → `FileNotFoundError` propagates (clearer than catching).
- Multiple CSV matches (CMS reissue) → most recent by mtime wins,
  matching the package's existing rule.
- Rows where *both* manufacturer columns are NaN/empty → silently skipped
  (the aggregator's intended behavior).
- Reading errors (corrupt rows, dtype issues) → let pandas' default
  raise; we trust CMS data quality here.

## Testing

**Unit test** (`test_list_2024_manufacturers.py`):
- Construct three synthetic chunks with overlapping + distinct names,
  some NaN, some empty strings, names appearing in only one column.
- Call `update_manufacturers_from_chunk` on each in sequence.
- Assert the accumulator matches the expected union.

**Integration / smoke**: manual run of the full script on the real 8.8 GB
file. Confirm:
- xlsx is produced
- row count is sensible (~thousands to ~tens of thousands)
- no obvious dupes that should have been merged (verifies the "raw
  strings" decision is honored — variants stay separate)

The integration run is too large for CI and isn't worth automating for
a one-off analyst script.

## Out of scope

- Other years — request was 2024-specific. Adding `--year` could be a
  follow-up but is YAGNI for now.
- Other payment classes (research, ownership) — request was general only.
- Name normalization — explicitly chosen out.
- Per-manufacturer metrics (n_payments, total_amount, etc.) — explicitly
  chosen out.
- Performance optimization beyond the obvious `usecols` + `chunksize`.
  The 8.8 GB read should complete in a few minutes on a modern laptop.

## Open questions

None remaining at design time. All decisions resolved during
brainstorming.
