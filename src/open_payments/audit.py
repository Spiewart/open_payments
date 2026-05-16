"""Post-match audit statistics over a matched-providers DataFrame.

These functions are study-neutral: they take the ``unique_ids`` DataFrame
produced by :class:`SearchResult` (or any equivalent matched-results frame
with ``profile_id`` / ``filters`` / ``confidence_tier`` columns) and return
descriptive statistics for analyst review.

Typical usage from a child app's audit workbook generator::

    from open_payments import audit
    matched_df = pd.read_excel("conflicteds_ids_final.xlsx", sheet_name="conflicteds_ids")

    prevalence_df = audit.filter_prevalence(matched_df)
    combos_df = audit.filter_combination_breakdown(matched_df)
    collisions_df = audit.profile_id_collisions(matched_df)
    tier_df = audit.tier_summary(matched_df)

None of these functions require a study-specific config — they read only
columns that ``SearchResult.unique_ids`` always carries. Child apps that
want to add study-specific aggregates can call these and concatenate their
own.

Tier ordering: kept in sync with ``open_payments.selectors.DEFAULT_TIER_RULES``
so summary tables sort the same way as the matcher's own labels. If a
study customizes ``TieredConfidenceSelector.TIER_RULES``, pass a matching
``tier_order`` to :func:`tier_summary`.
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd

from .selectors import (
    DEFAULT_FALLBACK_TIER,
    DEFAULT_TIER_RULES,
)

# Default tier ordering for sort stability — matches DEFAULT_TIER_RULES.
DEFAULT_TIER_ORDER: list[str] = [name for name, _ in DEFAULT_TIER_RULES] + [DEFAULT_FALLBACK_TIER]


_FILTER_VALUE_RE = re.compile(r"'([A-Z_]+)'")


def parse_filters_repr(filters_repr) -> set[str]:
    """Extract PaymentFilters value strings from a persisted filter column.

    The matcher writes ``filters`` and ``negative_filters`` as a list of
    :class:`PaymentFilters` enum values. When those round-trip through
    Excel they come back as the string repr of the list, e.g.
    ``"[<PaymentFilters.LASTNAME: 'LASTNAME'>, <PaymentFilters.NPI: 'NPI'>]"``.
    This helper pulls the quoted enum-value strings out as a plain set,
    or returns an empty set for null/unparseable inputs.

    Args:
        filters_repr: The raw cell value (str, list, None, NaN).

    Returns:
        Set of filter-name strings (e.g. ``{"LASTNAME", "NPI"}``).
    """
    if filters_repr is None:
        return set()
    try:
        if pd.isna(filters_repr):
            return set()
    except (TypeError, ValueError):
        pass
    if isinstance(filters_repr, set):
        return {str(x) for x in filters_repr}
    if isinstance(filters_repr, (list, tuple)):
        # In-memory list of PaymentFilters / strings
        out: set[str] = set()
        for v in filters_repr:
            out.add(getattr(v, "value", str(v)))
        return out
    return set(_FILTER_VALUE_RE.findall(str(filters_repr)))


def filter_prevalence(matched_df: pd.DataFrame) -> pd.DataFrame:
    """Per-:class:`PaymentFilters` usage rate across all matches.

    Returns one row per filter value found in any match's positive
    ``filters`` list, sorted by descending count. Columns:

    - ``filter``: the PaymentFilters value name (e.g. ``"LASTNAME"``)
    - ``count``: number of matches whose positive filter set contains this value
    - ``pct_of_matches``: count / len(matched_df) * 100, rounded to 1 decimal

    Useful for answering: "what fraction of matches involved NPI vs name
    vs specialty vs location?"
    """
    total = len(matched_df)
    if total == 0 or "filters" not in matched_df.columns:
        return pd.DataFrame(columns=["filter", "count", "pct_of_matches"])
    counts: dict[str, int] = {}
    for filters_repr in matched_df["filters"]:
        for f in parse_filters_repr(filters_repr):
            counts[f] = counts.get(f, 0) + 1
    return (
        pd.DataFrame(
            [(f, c, round(c / total * 100, 1)) for f, c in counts.items()],
            columns=["filter", "count", "pct_of_matches"],
        )
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )


def filter_combination_breakdown(matched_df: pd.DataFrame) -> pd.DataFrame:
    """Distinct positive-filter sets across all matches, sorted by frequency.

    Each row is a unique combination of filter values with the count of
    matches that produced exactly that set. Columns:

    - ``filter_combination``: " + "-joined sorted filter names
    - ``num_filters``: cardinality of the set
    - ``count``: number of matches with that combination
    - ``pct_of_matches``: count / total

    Useful for spotting risky single-filter combinations like
    ``{LASTNAME}`` or ``{FIRSTNAME, LASTNAME}`` with no disambiguator
    (the "Chris Anderson MD" pattern).
    """
    total = len(matched_df)
    if total == 0 or "filters" not in matched_df.columns:
        return pd.DataFrame(
            columns=["filter_combination", "num_filters", "count", "pct_of_matches"]
        )
    combo_counts: dict[tuple, int] = {}
    for filters_repr in matched_df["filters"]:
        key = tuple(sorted(parse_filters_repr(filters_repr)))
        combo_counts[key] = combo_counts.get(key, 0) + 1
    rows = [
        (
            " + ".join(combo) if combo else "(none)",
            len(combo),
            count,
            round(count / total * 100, 1),
        )
        for combo, count in combo_counts.items()
    ]
    return (
        pd.DataFrame(
            rows,
            columns=["filter_combination", "num_filters", "count", "pct_of_matches"],
        )
        .sort_values(["count", "num_filters"], ascending=[False, False])
        .reset_index(drop=True)
    )


def tier_summary(
    matched_df: pd.DataFrame,
    tier_column: str = "confidence_tier",
    tier_order: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Count and percentage of matches per ``confidence_tier`` value.

    The matched_df is expected to carry a tier column populated by a
    tier-aware selector such as
    :class:`open_payments.selectors.TieredConfidenceSelector` — which
    writes ``confidence_tier`` on every unique match. Rows with missing
    tier surface as a separate row labeled ``"(no tier)"`` so the analyst
    can see gaps.

    Args:
        matched_df: matched-providers frame from ``SearchResult.unique_ids``.
        tier_column: column to summarize (default ``confidence_tier``).
        tier_order: explicit sort order for the output. Defaults to
            :data:`DEFAULT_TIER_ORDER`.
    """
    total = len(matched_df)
    if total == 0 or tier_column not in matched_df.columns:
        return pd.DataFrame(columns=[tier_column, "count", "pct_of_matches"])
    df = matched_df[[tier_column]].copy()
    df[tier_column] = df[tier_column].fillna("(no tier)")
    summary = df.groupby(tier_column).size().reset_index(name="count")
    summary["pct_of_matches"] = (summary["count"] / total * 100).round(1)
    order = list(tier_order) if tier_order is not None else list(DEFAULT_TIER_ORDER)
    if "(no tier)" not in order:
        order = order + ["(no tier)"]
    summary["_rank"] = summary[tier_column].map(lambda t: order.index(t) if t in order else 999)
    summary = summary.sort_values("_rank").drop(columns=["_rank"]).reset_index(drop=True)
    return summary


def profile_id_collisions(
    matched_df: pd.DataFrame,
    *,
    source_pk_column: str = "provider_pk",
    source_label_columns: Optional[list[str]] = None,
    tier_column: str = "confidence_tier",
    tier_order: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Profile_ids matched by more than one source primary key.

    A collision can mean either of two distinct things, and the worst-tier
    sort surfaces the latter to the top of the sheet:

      Legitimate: the same person held multiple roles in the study
      population (e.g. dean career move across two schools — both source
      rows correctly resolve to the same OpenPayments profile_id). These
      are usually all on a HIGH_NPI tier.

      False positive: the matcher picked the same OpenPayments record for
      two different source rows whose names overlap. These typically
      appear at LOW or VERY_LOW tiers.

    Args:
        matched_df: matched-providers frame.
        source_pk_column: the source-side primary key column. Defaults to
            ``"provider_pk"`` (the deans_conflicts convention).
        source_label_columns: columns to surface in the output for
            human-readable identification of each source row. Examples:
            ``["last_name", "first_name", "school"]``. If a column isn't
            present in matched_df, it's silently skipped.
        tier_column: confidence tier column. Default ``confidence_tier``.
        tier_order: tier ranking for the "worst-tier first" sort. Defaults
            to :data:`DEFAULT_TIER_ORDER`.

    Returns:
        One row per colliding ``profile_id`` with worst-tier-first sort.
    """
    if matched_df.empty or "profile_id" not in matched_df.columns:
        return pd.DataFrame()

    valid = matched_df[matched_df["profile_id"].notna()].copy()
    valid["profile_id"] = pd.to_numeric(valid["profile_id"], errors="coerce")
    valid = valid.dropna(subset=["profile_id"])
    valid["profile_id"] = valid["profile_id"].astype("Int64")

    collision_counts = valid.groupby("profile_id").size()
    colliding_ids = collision_counts[collision_counts > 1].index
    if len(colliding_ids) == 0:
        return pd.DataFrame()

    has_tier = tier_column in valid.columns
    order = list(tier_order) if tier_order is not None else list(DEFAULT_TIER_ORDER)
    rank_lookup = {t: i for i, t in enumerate(order)}
    source_label_columns = source_label_columns or []

    rows = []
    for pid in colliding_ids:
        subset = valid[valid["profile_id"] == pid]
        if has_tier:
            worst_tier = max(
                subset[tier_column].fillna("(no tier)").tolist(),
                key=lambda t: rank_lookup.get(t, 999),
            )
            worst_rank = rank_lookup.get(worst_tier, 999)
        else:
            worst_tier = "(no tier column)"
            worst_rank = 999

        row = {
            "profile_id": int(pid),
            "match_count": int(len(subset)),
            "worst_confidence_tier": worst_tier,
            f"{source_pk_column}s": ", ".join(
                map(
                    str,
                    sorted(subset[source_pk_column].dropna().astype(int).tolist()),
                )
            ),
        }
        for col in source_label_columns:
            if col in subset.columns:
                row[f"{col}s"] = " | ".join(subset[col].astype(str).fillna("").tolist())
        if "filters" in subset.columns:
            row["filters"] = " || ".join(subset["filters"].astype(str).tolist())
        row["_worst_rank"] = worst_rank
        rows.append(row)

    return (
        pd.DataFrame(rows)
        .sort_values(["_worst_rank", "match_count"], ascending=[False, False])
        .drop(columns=["_worst_rank"])
        .reset_index(drop=True)
    )
