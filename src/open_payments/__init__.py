"""Match conflicted providers against CMS Open Payments data.

The public API is intentionally tiny. Most child apps need only::

    from open_payments import find_payments_for_conflicted_providers, Settings

    result = find_payments_for_conflicted_providers(
        conflicteds=my_raw_df,
        settings=Settings(years=[2022, 2023]),
    )
    result.to_excel("out.xlsx")

For advanced use cases (custom selectors, custom Conflict* mixins, direct
matcher access), import from the submodules:

- ``open_payments.selectors`` — selector strategies
- ``open_payments.conflicteds`` — input-normalization pipeline
- ``open_payments.ids`` — matcher class
- ``open_payments.choices`` — PaymentFilters / Unmatcheds / Credentials / States / FilterOutcome enums
- ``open_payments.schemas`` — input/output contracts

Post-match analysis and review (study-neutral; child apps wrap these with
their study-specific entity name and column layout):

- ``open_payments.audit`` — match-result statistics (filter prevalence,
  collisions, tier summary)
- ``open_payments.suspicion`` — match-confidence suspicion classification
  for NPI and non-NPI matches
- ``open_payments.excel`` — Excel formatting primitives (color palette,
  section styling, data validation, hyperlinks)
- ``open_payments.review`` — reviewer-facing workbook generator + apply
  workflow, parameterized by :class:`ReviewConfig`

Everything else (helpers, payment_types, individual filter modules) is
implementation detail.
"""

from . import audit, excel, review, suspicion
from .api import find_payments_for_conflicted_providers
from .choices import FilterOutcome, PaymentFilters, Unmatcheds
from .config import Settings
from .entity_parser import EntityParser
from .institution_locator import (
    CandidateLocation,
    InstitutionLocator,
    ManualReviewBackend,
    NPPESBackend,
    flatten_to_citystates,
)
from .review import ReviewConfig, SourceField
from .schemas import (
    OPTIONAL_CONFLICTED_COLUMNS,
    REQUIRED_CONFLICTED_COLUMNS,
    ConflictedProviderRow,
    SearchResult,
    validate_conflicteds_df,
)
from .selectors import (
    DefaultMatchSelector,
    IdentifierWinsSelector,
    MatchSelector,
    SelectorResult,
    TieredConfidenceSelector,
    TiesAreUnmatchedSelector,
)

__all__ = [
    # Main entrypoint
    "find_payments_for_conflicted_providers",
    # Configuration
    "Settings",
    # Schemas / contracts
    "SearchResult",
    "ConflictedProviderRow",
    "validate_conflicteds_df",
    "REQUIRED_CONFLICTED_COLUMNS",
    "OPTIONAL_CONFLICTED_COLUMNS",
    # Selectors
    "MatchSelector",
    "DefaultMatchSelector",
    "IdentifierWinsSelector",
    "TieredConfidenceSelector",
    "TiesAreUnmatchedSelector",
    "SelectorResult",
    # Enums
    "PaymentFilters",
    "FilterOutcome",
    "Unmatcheds",
    # Shared utilities (used by child apps post-match)
    "EntityParser",
    "InstitutionLocator",
    "CandidateLocation",
    "flatten_to_citystates",
    "NPPESBackend",
    "ManualReviewBackend",
    # Post-match analysis submodules
    "audit",
    "suspicion",
    "excel",
    "review",
    # Review config types (common to surface at top level)
    "ReviewConfig",
    "SourceField",
]
