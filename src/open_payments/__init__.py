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

Everything else (helpers, payment_types, individual filter modules) is
implementation detail.
"""

from .api import find_payments_for_conflicted_providers
from .choices import FilterOutcome, PaymentFilters, Unmatcheds
from .config import Settings
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
    "SelectorResult",
    # Enums
    "PaymentFilters",
    "FilterOutcome",
    "Unmatcheds",
]
