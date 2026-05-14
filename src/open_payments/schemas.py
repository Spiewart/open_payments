"""Public input / output contracts for the open_payments matcher.

The matcher's I/O is DataFrame-based for performance (pandas is the lingua
franca), but the *contract* — which columns must be present, what dtypes,
what each row means — needs to be explicit so child apps can depend on it
and so future internal refactors don't silently break consumers.

Two layers:

1. :class:`ConflictedProviderRow` — a pydantic model documenting the
   per-row shape of the conflicted-providers input. Used as the schema
   reference; not actually constructed per row in the hot path (the
   matcher reads the DataFrame directly). :func:`validate_conflicteds_df`
   is the runtime check that confirms a DataFrame is shaped correctly
   before the matcher consumes it.

2. :class:`SearchResult` — the output bundle. A frozen dataclass holding
   three DataFrames (``unique_ids``, ``unmatched``, ``unmatched_options``)
   plus the methods that used to live in ``helpers.py``:
   ``to_excel(path)``, ``update_excel(path)``, ``from_excel(path)``.

These are the symbols a child app should import. Internal types
(``Conflicteds``, ``ConflictedPaymentIDs``, mixin chain) are
implementation detail.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from .citystates import CityState
from .credentials import Credentials
from .specialtys import Specialtys

if TYPE_CHECKING:
    from .config import Settings


# ---------------------------------------------------------------------------
# Conflicted-providers input schema
# ---------------------------------------------------------------------------

#: Columns the matcher reads from the conflicteds DataFrame. The
#: ``Conflicteds`` orchestrator (see ``conflicteds.py``) is responsible for
#: parsing child-app raw input into these columns; child apps can either
#: feed raw data to ``Conflicteds`` first or skip the orchestrator and feed
#: a pre-parsed DataFrame matching this shape directly to the matcher.
REQUIRED_CONFLICTED_COLUMNS: tuple[str, ...] = (
    "provider_pk",
    "first_name",
    "last_name",
    "middle_initial_1",
    "middle_initial_2",
    "middle_name_1",
    "middle_name_2",
    "credentials",
    "specialtys",
    "citystates",
)

#: Columns the matcher tolerates if present (filter modules add these
#: optionally). Validation does not require them, but they're documented
#: here so child-app authors know they can populate them.
OPTIONAL_CONFLICTED_COLUMNS: tuple[str, ...] = (
    "name_suffix",
    "npi",
)


class ConflictedProviderRow(BaseModel):
    """Per-row schema of the conflicted-providers DataFrame.

    Documents the runtime contract that the matcher's mixin chain expects.
    The matcher does NOT construct a ``ConflictedProviderRow`` per input row
    — it reads the DataFrame directly for performance. Use this model as
    the source of truth for column names + types when writing a child app.

    Use :func:`validate_conflicteds_df` to runtime-check a DataFrame
    against this contract before invoking the matcher.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    provider_pk: int = Field(
        description="Stable integer identifier assigned by the child app. "
        "Used to thread results back to source rows."
    )
    first_name: str | None = None
    last_name: str
    middle_initial_1: str | None = None
    middle_initial_2: str | None = None
    middle_name_1: str | None = None
    middle_name_2: str | None = None
    credentials: list[Credentials] = Field(default_factory=list)
    specialtys: list[Specialtys] = Field(default_factory=list)
    citystates: list[CityState] = Field(default_factory=list)
    # Optional filter-side columns.
    name_suffix: str | None = None
    npi: int | None = None


def validate_conflicteds_df(df: pd.DataFrame) -> None:
    """Raise ``ValueError`` if ``df`` is missing any required column.

    Cheap structural check (does not validate dtypes or per-row contents) —
    intended as the front-door gate for child-app inputs so users get a
    clear error instead of an obscure ``KeyError`` deep in the matcher.
    """
    missing = [c for c in REQUIRED_CONFLICTED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Conflicteds DataFrame is missing required columns: {missing}. "
            f"Expected columns: {list(REQUIRED_CONFLICTED_COLUMNS)}. "
            f"See open_payments.schemas.ConflictedProviderRow for the full "
            f"contract, or feed your raw input through "
            f"open_payments.conflicteds.Conflicteds first to derive them."
        )


# ---------------------------------------------------------------------------
# Search result bundle
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SearchResult:
    """The output of :func:`find_payments_for_conflicted_providers`.

    Three DataFrames:

    - **unique_ids** — one row per conflicted provider that resolved to a
      single OpenPayments profile_id. Columns include ``provider_pk``,
      ``profile_id``, ``filters`` (list of PaymentFilters that matched),
      ``num_filters`` (count), ``negative_filters`` + ``n_negative_filters``
      (Section 5.8 disagreement signal), and ``confidence_tier`` (set by
      tier-aware selectors; None for cascade selectors).

    - **unmatched** — one row per conflicted provider with NO unique match.
      ``unmatched`` column carries an ``Unmatcheds`` enum value
      (``NOLASTNAME`` / ``UNFILTERABLE``).

    - **unmatched_options** — for ``UNFILTERABLE`` cases, the candidate
      OpenPayments rows the matcher couldn't disambiguate. Multiple rows
      per ``provider_pk``.

    Persistence methods replace the older ``helpers.update_or_create_*``
    free functions:
      - :meth:`to_excel` — write all three frames to a single xlsx
        (overwrites). Use for new files.
      - :meth:`update_excel` — merge into an existing xlsx, preserving prior
        runs (upsert by ``provider_pk``).
      - :meth:`from_excel` (classmethod) — load a previously-persisted
        SearchResult from xlsx.
    """

    unique_ids: pd.DataFrame
    unmatched: pd.DataFrame
    unmatched_options: pd.DataFrame
    settings: Settings | None = field(default=None, repr=False)

    # ---- xlsx persistence ----

    def to_excel(self, path: str | os.PathLike) -> None:
        """Write all three frames to ``path`` as a single xlsx, overwriting
        any existing file. ``unmatched_options`` is split across one sheet
        per provider_pk so the analyst can review each conflicted's
        candidates separately.
        """
        path = Path(path)
        with pd.ExcelWriter(path) as writer:
            self.unique_ids.to_excel(writer, sheet_name="conflicteds_ids", index=False)
            self.unmatched.to_excel(writer, sheet_name="unmatched", index=False)
            self._write_unmatched_options_to_sheets(writer)

    def update_excel(self, path: str | os.PathLike) -> None:
        """Merge this SearchResult into the xlsx at ``path``.

        Upserts on ``provider_pk``: a provider_pk in the new result
        replaces the persisted row of the same provider_pk; new
        provider_pks are appended; persisted provider_pks not in the
        new result are preserved.

        If ``path`` doesn't exist, behaves like :meth:`to_excel`.
        """
        path = Path(path)
        if not path.exists():
            self.to_excel(path)
            return

        existing = SearchResult.from_excel(path)
        merged_unique = _upsert_by_provider_pk(existing.unique_ids, self.unique_ids)
        merged_unmatched = _upsert_by_provider_pk(existing.unmatched, self.unmatched)
        merged_options = _merge_unmatched_options(
            new_options=self.unmatched_options,
            existing_options=existing.unmatched_options,
            matched_df=merged_unique,
        )
        SearchResult(
            unique_ids=merged_unique,
            unmatched=merged_unmatched,
            unmatched_options=merged_options,
        ).to_excel(path)

    @classmethod
    def from_excel(cls, path: str | os.PathLike) -> SearchResult:
        """Load a SearchResult previously written by :meth:`to_excel` /
        :meth:`update_excel`."""
        path = Path(path)
        with pd.ExcelFile(path) as xls:
            unique = pd.read_excel(xls, sheet_name="conflicteds_ids")
            unmatched = pd.read_excel(xls, sheet_name="unmatched")
            # All other sheets are per-provider_pk option sheets. They have
            # integer names (the provider_pk).
            option_sheets = [s for s in xls.sheet_names if _is_int_str(s)]
            options = (
                pd.concat(
                    [pd.read_excel(xls, sheet_name=s) for s in option_sheets],
                    ignore_index=True,
                )
                if option_sheets
                else pd.DataFrame()
            )
        return cls(unique_ids=unique, unmatched=unmatched, unmatched_options=options)

    # ---- introspection helpers ----

    @property
    def n_unique(self) -> int:
        return len(self.unique_ids)

    @property
    def n_unmatched(self) -> int:
        return len(self.unmatched)

    @property
    def n_unmatched_options(self) -> int:
        return len(self.unmatched_options)

    def __repr__(self) -> str:
        return (
            f"SearchResult(unique={self.n_unique}, "
            f"unmatched={self.n_unmatched}, "
            f"unmatched_options={self.n_unmatched_options})"
        )

    # ---- internal ----

    def _write_unmatched_options_to_sheets(self, writer: pd.ExcelWriter) -> None:
        if self.unmatched_options.empty:
            return
        for pk in self.unmatched_options["provider_pk"].unique():
            subset = self.unmatched_options[self.unmatched_options["provider_pk"] == pk]
            subset.to_excel(writer, sheet_name=str(pk), index=False)


# ---------------------------------------------------------------------------
# xlsx merge helpers
# ---------------------------------------------------------------------------


def _is_int_str(s: str) -> bool:
    try:
        int(s)
        return True
    except (ValueError, TypeError):
        return False


def _upsert_by_provider_pk(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Upsert ``new`` into ``existing`` keyed on ``provider_pk``.

    Rows in ``new`` replace same-provider_pk rows in ``existing``. New
    provider_pks are appended. Existing-only provider_pks are preserved.
    """
    if existing.empty:
        return new.copy()
    if new.empty:
        return existing.copy()
    new_pks = set(new["provider_pk"])
    survivors = existing[~existing["provider_pk"].isin(new_pks)]
    return pd.concat([survivors, new], ignore_index=True)


def _merge_unmatched_options(
    new_options: pd.DataFrame,
    existing_options: pd.DataFrame,
    matched_df: pd.DataFrame,
) -> pd.DataFrame:
    """Merge unmatched options. Two-step:

    1. Drop persisted options for providers that now have a match (they're
       stale — the new run resolved them).
    2. Dedup new vs. existing on the natural key ``(provider_pk, profile_id)``.
       Rows in ``new_options`` for a (pk, profile_id) pair that's already
       persisted are skipped; new pairs are prepended.

    Rationale for key-based dedup (vs. full-row equality): list-valued
    columns (filters, citystates, etc.) get stringified when xlsx
    round-trips but stay as Python lists in-memory, so ``.eq()`` between
    the two raises on dtype mismatch. The (pk, profile_id) pair is the
    natural identity of an unmatched option anyway — different filter
    sets for the same option are still the same option.
    """
    # Drop options for providers that now have matches.
    if not existing_options.empty and not matched_df.empty:
        existing_options = existing_options[
            ~existing_options["provider_pk"].isin(matched_df["provider_pk"])
        ]
    if existing_options.empty:
        return new_options.copy()
    if new_options.empty:
        return existing_options.copy()

    # Key-based dedup. Requires both sides to carry both columns; if either
    # is missing, fall back to "keep new" semantics (the merge is best-effort).
    key_cols = [
        c
        for c in ("provider_pk", "profile_id")
        if c in existing_options.columns and c in new_options.columns
    ]
    if not key_cols:
        return pd.concat([new_options, existing_options], ignore_index=True)

    existing_keys = set(map(tuple, existing_options[key_cols].itertuples(index=False, name=None)))
    is_new = new_options[key_cols].apply(lambda r: tuple(r) not in existing_keys, axis=1)
    fresh = new_options[is_new]
    if fresh.empty:
        return existing_options.copy()
    return pd.concat([fresh, existing_options], ignore_index=True)
