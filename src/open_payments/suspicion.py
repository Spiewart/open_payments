"""Match-confidence suspicion classification.

Every match in :class:`SearchResult.unique_ids` carries a positive
``filters`` list (which signals matched) and a ``negative_filters`` list
(which signals had-data-and-disagreed). The :class:`TieredConfidenceSelector`
assigns a coarse ``confidence_tier`` based on positives only. This module
adds a finer-grained suspicion label that combines positives, negatives,
and per-row attributes (specialty, state) to flag matches that warrant
manual re-verification.

Two classifiers cover the two pages of a typical review workbook:

  - :func:`classify_npi_match_suspicion` — for matches whose positive
    filters include ``NPI``. The match has the strongest possible upstream
    signal (NPI is unique), so the suspicion model focuses on whether the
    downstream attributes (firstname, specialty, state) agree. When they
    disagree despite NPI agreement, the source-side NPI might have been
    entered incorrectly during data collection.

  - :func:`classify_non_npi_match_suspicion` — for matches that fell back
    to name-cascade (no NPI in positives). The suspicion model leans on
    the confidence tier (lastname-only tiers are high-risk) plus
    specialty/state disagreement on top.

Both classifiers return one of the labels in :data:`WARNING_TIERS` (high
suspicion, suitable for red cell coloring) or one of the lower-severity
labels (state-only disagreement = D, normal = E).

Custom predicates / taxonomy aliases can be passed in to adapt to a
specific study; the defaults handle most common situations.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence

import pandas as pd

# --------------------------------------------------------------------------
# Filter-name groupings (matches PaymentFilters values)
# --------------------------------------------------------------------------

FIRSTNAME_FILTERS: frozenset[str] = frozenset(
    {"FIRSTNAME", "FIRSTNAME_PARTIAL", "FIRST_MIDDLE_NAME"}
)
SPECIALTY_FILTERS: frozenset[str] = frozenset({"SPECIALTY", "SUBSPECIALTY", "FULLSPECIALTY"})
LOCATION_FILTERS: frozenset[str] = frozenset({"STATE", "CITY", "CITYSTATE"})

# Lastname-only confidence tiers — the matcher had only lastname
# agreement (possibly plus middlename / disambiguators), never firstname.
# Includes VERY_LOW_LASTNAME_PARTIAL (1-edit fuzzy lastname) because a
# row whose only name agreement is a partial-lastname match is at
# least as risky as a row with an exact-lastname-only match — the
# underlying name signal is structurally weaker.
LASTNAME_ONLY_TIERS: frozenset[str] = frozenset(
    {
        "VERY_LOW_LASTNAME_BARE",
        "VERY_LOW_LASTNAME_PARTIAL",
        "LOW_LASTNAME_PLUS_ONE",
        "LOW_NAME_ONLY",
    }
)


# --------------------------------------------------------------------------
# Suspicion tier labels
# --------------------------------------------------------------------------

# NPI-match suspicion labels
SUSPICION_NPI_FIRSTNAME_DISAGREED = "A_firstname_disagreed"
SUSPICION_NPI_SPECIALTY_AND_STATE = "B_specialty_AND_state_disagree"
SUSPICION_NPI_SPECIALTY = "C_specialty_disagrees"
SUSPICION_NPI_STATE = "D_state_disagrees"
SUSPICION_NPI_NORMAL = "E_normal"

# Non-NPI-match suspicion labels (B/C/D/E are shared with the NPI side
# for cross-sheet consistency; A is sheet-specific)
SUSPICION_NON_NPI_LASTNAME_ONLY = "A_lastname_only_match"

# Tier set that warrants red cell highlighting in review workbooks.
WARNING_TIERS: frozenset[str] = frozenset(
    {
        SUSPICION_NPI_FIRSTNAME_DISAGREED,
        SUSPICION_NON_NPI_LASTNAME_ONLY,
        SUSPICION_NPI_SPECIALTY_AND_STATE,
        SUSPICION_NPI_SPECIALTY,
    }
)


# --------------------------------------------------------------------------
# CMS taxonomy aliases
# --------------------------------------------------------------------------

# Specialty pairs that are the same field under different CMS labels —
# treated as non-disagreements when classifying suspicion. Each entry is
# (source_substrings, cms_substrings); a match on both sides means the
# disagreement is a taxonomy artifact, not a real mismatch.
DEFAULT_TAXONOMY_ALIASES: list[tuple[set[str], set[str]]] = [
    ({"neurosurgery"}, {"neurological surgery"}),
    ({"urogynecology"}, {"urogynecology", "pelvic medicine"}),
    ({"radiation oncology"}, {"radiation oncology", "radiology"}),
    ({"plastic surgery"}, {"plastic surgery"}),
    ({"general practice"}, {"general practice", "family"}),
    (
        {"maternal fetal medicine", "maternal-fetal medicine"},
        {"neonatal-perinatal medicine"},
    ),
]


def specialty_is_taxonomy_alias(
    source_specialty,
    cms_specialty,
    *,
    aliases: Sequence[tuple[set[str], set[str]]] = DEFAULT_TAXONOMY_ALIASES,
) -> bool:
    """Return True if the two specialty strings look like the same field
    under different CMS taxonomy labels.

    Substring match on both sides, case-insensitive. Pass a custom alias
    list to extend or override for a specific study.
    """
    if not isinstance(source_specialty, str) or not isinstance(cms_specialty, str):
        return False
    s = source_specialty.lower()
    c = cms_specialty.lower()
    for source_keys, cms_keys in aliases:
        if any(t in s for t in source_keys) and any(t in c for t in cms_keys):
            return True
    return False


# --------------------------------------------------------------------------
# Filter-set parsing
# --------------------------------------------------------------------------

_FILTERNAME_RE = re.compile(r"\b([A-Z][A-Z_]+)\b")


def parse_filter_set(filters_repr) -> set[str]:
    """Extract filter-name strings from a positive/negative filter cell value.

    Handles in-memory lists, sets, the persisted enum-repr string from
    Excel round-trips, and None/NaN. Returns a set of bare filter names
    (e.g. ``{"LASTNAME", "NPI"}``).
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
        out: set[str] = set()
        for v in filters_repr:
            out.add(getattr(v, "value", str(v)))
        return out
    return set(_FILTERNAME_RE.findall(str(filters_repr)))


# --------------------------------------------------------------------------
# Classifiers
# --------------------------------------------------------------------------


def classify_npi_match_suspicion(
    row: pd.Series,
    *,
    positive_col: str = "positive_filters",
    negative_col: str = "negative_filters",
    source_specialty_col: str = "Dean: Specialty",
    cms_specialty_col: str = "CMS: Specialty",
    taxonomy_aliases: Sequence[tuple[set[str], set[str]]] = DEFAULT_TAXONOMY_ALIASES,
) -> str:
    """Categorize a NPI-bearing match for re-verification priority.

    The match has NPI in its positive filters (otherwise it wouldn't be on
    the NPI page). We check whether firstname, specialty, and state agree.
    Disagreements suggest the source-side NPI may have been entered wrong.

    Returns one of:
      ``A_firstname_disagreed`` — first name in CMS differs from the source's
      ``B_specialty_AND_state_disagree`` — name agrees but both specialty
          AND state differ
      ``C_specialty_disagrees`` — same state, specialty differs (excluding
          CMS taxonomy aliases)
      ``D_state_disagrees`` — only state differs (typical career move)
      ``E_normal`` — no concerning disagreements

    Args:
        row: a row from the matched-providers frame.
        positive_col / negative_col: filter columns to inspect.
        source_specialty_col / cms_specialty_col: specialty columns used
            for taxonomy-alias detection.
        taxonomy_aliases: alias pairs (see :data:`DEFAULT_TAXONOMY_ALIASES`).
    """
    pos = parse_filter_set(row.get(positive_col))
    neg = parse_filter_set(row.get(negative_col))
    fn_disagreed = bool(neg & FIRSTNAME_FILTERS) and not bool(pos & FIRSTNAME_FILTERS)
    if fn_disagreed:
        return SUSPICION_NPI_FIRSTNAME_DISAGREED

    spec_disagreed = bool(neg & SPECIALTY_FILTERS) and not bool(pos & SPECIALTY_FILTERS)
    state_disagreed = bool(neg & LOCATION_FILTERS) and not bool(pos & LOCATION_FILTERS)

    if spec_disagreed and specialty_is_taxonomy_alias(
        row.get(source_specialty_col),
        row.get(cms_specialty_col),
        aliases=taxonomy_aliases,
    ):
        spec_disagreed = False

    if spec_disagreed and state_disagreed:
        return SUSPICION_NPI_SPECIALTY_AND_STATE
    if spec_disagreed:
        return SUSPICION_NPI_SPECIALTY
    if state_disagreed:
        return SUSPICION_NPI_STATE
    return SUSPICION_NPI_NORMAL


def classify_non_npi_match_suspicion(
    row: pd.Series,
    *,
    positive_col: str = "positive_filters",
    negative_col: str = "negative_filters",
    tier_col: str = "confidence_tier",
    source_specialty_col: str = "Dean: Specialty",
    cms_specialty_col: str = "CMS: Specialty",
    taxonomy_aliases: Sequence[tuple[set[str], set[str]]] = DEFAULT_TAXONOMY_ALIASES,
    lastname_only_tiers: Iterable[str] = LASTNAME_ONLY_TIERS,
) -> str:
    """Categorize a non-NPI match for re-verification priority.

    The match used the name-cascade fallback because NPI matching didn't
    fire (typically: the source-side has an NPI but it's not in CMS
    payments). The strongest red flag is when the matcher landed in a
    lastname-only confidence tier — no firstname agreement, high
    false-positive risk for common surnames.

    Returns one of:
      ``A_lastname_only_match`` — tier in :data:`LASTNAME_ONLY_TIERS`
      ``B_specialty_AND_state_disagree``
      ``C_specialty_disagrees``
      ``D_state_disagrees`` (typical career move)
      ``E_normal``
    """
    tier = row.get(tier_col)
    lon = set(lastname_only_tiers)
    if isinstance(tier, str) and tier in lon:
        return SUSPICION_NON_NPI_LASTNAME_ONLY

    pos = parse_filter_set(row.get(positive_col))
    neg = parse_filter_set(row.get(negative_col))
    spec_disagreed = bool(neg & SPECIALTY_FILTERS) and not bool(pos & SPECIALTY_FILTERS)
    state_disagreed = bool(neg & LOCATION_FILTERS) and not bool(pos & LOCATION_FILTERS)

    if spec_disagreed and specialty_is_taxonomy_alias(
        row.get(source_specialty_col),
        row.get(cms_specialty_col),
        aliases=taxonomy_aliases,
    ):
        spec_disagreed = False

    if spec_disagreed and state_disagreed:
        return SUSPICION_NPI_SPECIALTY_AND_STATE
    if spec_disagreed:
        return SUSPICION_NPI_SPECIALTY
    if state_disagreed:
        return SUSPICION_NPI_STATE
    return SUSPICION_NPI_NORMAL
