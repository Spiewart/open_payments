"""Public matcher entrypoint.

The single function a child app needs to call::

    from open_payments import find_payments_for_conflicted_providers

    result = find_payments_for_conflicted_providers(
        conflicteds=my_raw_df,                # raw or parsed conflicted providers
        settings=Settings(years=[2022, 2023]),  # optional override
    )
    result.to_excel("out.xlsx")
    print(result)  # SearchResult(unique=42, unmatched=3, unmatched_options=7)

Behind the scenes:

1. If the input doesn't already conform to the parsed-conflicteds schema
   (per :data:`schemas.REQUIRED_CONFLICTED_COLUMNS`), it's run through
   :class:`Conflicteds` to derive the canonical 6-mixin output.
2. Payment CSVs are loaded via :class:`PaymentIDs` with the supplied
   ``Settings`` (or the env-var defaults).
3. The matcher runs with the requested selector (defaults to
   :class:`DefaultMatchSelector`).
4. The internal ``unique_ids`` / ``unmatched`` / ``unmatched_options``
   DataFrames are wrapped in a :class:`SearchResult` and returned.

Note: the loose-DataFrame internals (``ConflictedPaymentIDs``,
``Conflicteds``, mixin chain) remain accessible for advanced use cases
that need to override behavior beyond what ``selector=`` allows. Most
callers should not need to touch them.
"""

from __future__ import annotations

import pandas as pd

from .config import Settings
from .conflicteds import Conflicteds
from .ids import ConflictedPaymentIDs, PaymentIDs
from .schemas import REQUIRED_CONFLICTED_COLUMNS, SearchResult, validate_conflicteds_df
from .selectors import MatchSelector


def find_payments_for_conflicted_providers(
    conflicteds: pd.DataFrame,
    settings: Settings | None = None,
    selector: MatchSelector | None = None,
    payments: pd.DataFrame | None = None,
    parse_conflicteds: bool | None = None,
) -> SearchResult:
    """Match a DataFrame of conflicted providers against CMS Open Payments.

    Parameters
    ----------
    conflicteds
        Either a raw child-app input (a DataFrame with a ``name`` column,
        etc. — the shape :class:`Conflicteds` knows how to parse) OR a
        pre-parsed DataFrame already matching the
        :data:`schemas.REQUIRED_CONFLICTED_COLUMNS` shape. Auto-detected
        by default; pass ``parse_conflicteds=True/False`` to force.
    settings
        Configuration. Defaults to :class:`Settings` (env-var driven).
    selector
        Selection strategy. Defaults to
        :class:`open_payments.selectors.DefaultMatchSelector`.
    payments
        Pre-loaded payments DataFrame. If None, loads via
        :class:`PaymentIDs` using the configured settings.
    parse_conflicteds
        If ``True``, always run input through :class:`Conflicteds`. If
        ``False``, never run; assume input is pre-parsed. If ``None``
        (default), auto-detect: parse if any required column is missing.

    Returns
    -------
    SearchResult
        Bundle of ``unique_ids`` / ``unmatched`` / ``unmatched_options``
        plus xlsx persistence methods.

    Raises
    ------
    ValueError
        If ``parse_conflicteds=False`` AND the input doesn't satisfy the
        :func:`schemas.validate_conflicteds_df` contract.
    """
    if settings is None:
        settings = Settings()

    if parse_conflicteds is None:
        parse_conflicteds = not _looks_pre_parsed(conflicteds)

    if parse_conflicteds:
        conflicteds = Conflicteds(conflicteds).us_conflicteds_id_search_df()
    else:
        validate_conflicteds_df(conflicteds)

    if payments is None:
        payments = PaymentIDs(
            years=settings.years,
            payment_classes=settings.payment_classes,
            payments_folder=str(settings.data_dir),
            MD_DO_only=True,
            nrows=None,
        ).all_payments()

    matcher = ConflictedPaymentIDs(
        conflicteds=conflicteds,
        payments=payments,
        selector=selector,
    )
    matcher.search_for_conflicteds_ids()

    return SearchResult(
        unique_ids=matcher.unique_ids,
        unmatched=matcher.unmatched,
        unmatched_options=matcher.unmatched_options,
        settings=settings,
    )


def _looks_pre_parsed(df: pd.DataFrame) -> bool:
    """Heuristic: a DataFrame is "pre-parsed" if it has every required
    conflicteds column. Avoids running the parsing pipeline twice when a
    caller has already parsed their input."""
    return all(col in df.columns for col in REQUIRED_CONFLICTED_COLUMNS)
