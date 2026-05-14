import logging
from typing import Literal, Union

import pandas as pd

from .choices import FilterOutcome, PaymentFilters, Unmatcheds
from .citystates import PaymentCityStates, PaymentIDsCityStatesMixin
from .credentials import PaymentCredentials, PaymentIDsCredentialsMixin
from .names import NamesMixin, PaymentIDsNamesMixin
from .npi import PaymentIDsNPIMixin, PaymentNPI
from .selectors import DefaultMatchSelector, MatchSelector, SelectorResult
from .specialtys import PaymentIDsSpecialtysMixin, PaymentSpecialtys

# Configure logging
logging.basicConfig(level=logging.INFO)


class IDsMixin:
    @property
    def general_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:
        cols = super().general_columns
        cols.update(
            {
                "Covered_Recipient_Profile_ID": ("profile_id", "Int32"),
            }
        )
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:
        cols = super().ownership_columns
        cols.update(
            {
                "Physician_Profile_ID": ("profile_id", "Int32"),
            }
        )
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:
        cols = super().research_columns
        cols.update({**self.general_columns})
        return cols


class PaymentIDs(
    IDsMixin,
    NamesMixin,
    PaymentSpecialtys,
    PaymentCredentials,
    PaymentCityStates,
    PaymentNPI,
):
    def update_payments(
        self,
        payment_class: Literal["general", "ownership", "research"],
    ) -> pd.DataFrame:
        """Removes duplicate IDs and renames columns for the payment class
        DataFrame."""

        payments = super().update_payments(payment_class)

        payments = self.post_update_payments_mod(payments)

        return payments

    def post_update_payments_mod(self, payments: pd.DataFrame) -> pd.DataFrame:
        """Method that is called after the update_payments method."""

        payments = self.remove_duplicate_ids(payments)
        payments = self.specialtys(payments)
        payments = self.credentials(payments)
        payments = self.citystates(payments)

        return payments

    def update_ownership_payments(self) -> pd.DataFrame:
        """Updates ownership payments and returns the updated DataFrame."""
        self.ownership_payments = super().update_ownership_payments()
        return self.ownership_payments

    @staticmethod
    def remove_duplicate_ids(df: pd.DataFrame) -> pd.DataFrame:
        """Method that removes duplicate Covered_Recipient_Profile_IDs
        from the DataFrame."""

        df.reset_index(inplace=True, drop=True)

        df = df[
            df["profile_id"].isnull()
            | ~df[df["profile_id"].notnull()].duplicated(subset="profile_id", keep="first")
        ]

        return df


class Conflicted_x_PaymentIDs:
    def __init__(
        self,
        conflicteds: pd.DataFrame,
        payments: Union[pd.DataFrame, None],
        selector: Union[MatchSelector, None] = None,
    ):
        self.conflicteds = conflicteds
        self.payments = payments
        self.unmatched = pd.DataFrame()
        self.unmatched_options = pd.DataFrame()
        self.unique_ids = pd.DataFrame()
        # Selection strategy (Section 5.7). Defaults to the legacy cascade
        # extracted verbatim into DefaultMatchSelector — behavior-preserving.
        self.selector: MatchSelector = selector if selector is not None else DefaultMatchSelector()

    @property
    def filters(self) -> list[PaymentFilters]:
        """Returns a list of PaymentFilters to filter the
        payments_x_conflicted DataFrame by."""

        return []

    def add_unmatched(
        self,
        conflicted: pd.DataFrame,
        unmatched: Unmatcheds,
        filters: list[PaymentFilters],
        num_filters: int,
        negative_filters: list[PaymentFilters] | None = None,
        confidence_tier: str | None = None,
    ) -> None:
        """Adds the unmatched conflicted provider to the unmatched
        DataFrame.

        Bug 0d fix: `conflicted` arrives as a slice of `self.conflicteds`;
        copying at entry avoids the SettingWithCopyWarning that previously
        fired three times per unmatched row.

        ``negative_filters`` / ``confidence_tier`` are propagated from
        the selector so the analyst can see active-disagreement signals
        and tier labels on unmatched rows too (Section 5.8 + tier-aware
        selectors).
        """
        conflicted = conflicted.copy()
        conflicted["unmatched"] = unmatched
        conflicted["filters"] = [filters] * len(conflicted)
        conflicted["num_filters"] = num_filters
        neg = list(negative_filters) if negative_filters is not None else []
        conflicted["negative_filters"] = [neg] * len(conflicted)
        conflicted["n_negative_filters"] = len(neg)
        conflicted["confidence_tier"] = confidence_tier

        self.unmatched = pd.concat([self.unmatched, conflicted])

    def convert_merged_dtypes(
        self,
        merged: pd.DataFrame,
    ) -> pd.DataFrame:
        """Updates payments and conflicteds columns into lists after
        they are loaded as strs in CSVs and Excel files."""

        return merged

    def add_unique_id(
        self,
        highest_matches: pd.DataFrame,
        confidence_tier: str | None = None,
    ) -> None:
        """Append a unique-match row to ``self.unique_ids``.

        Adds derived audit columns:
          - ``num_filters`` (positive filter count)
          - ``n_negative_filters`` (Section 5.8 negative-signal tally)
          - ``confidence_tier`` (set by tier-aware selectors;
            None for cascade selectors)

        ``negative_filters`` itself already propagates as a column from
        the merged frame, so it's not re-added here — just the count
        derivation and tier annotation.
        """
        highest_matches = highest_matches.copy()
        highest_matches.insert(
            0,
            "num_filters",
            highest_matches["filters"].apply(len),
        )
        if "negative_filters" in highest_matches.columns:
            highest_matches["n_negative_filters"] = highest_matches["negative_filters"].apply(
                lambda x: len(x) if x is not None else 0
            )
        else:
            highest_matches["negative_filters"] = [[] for _ in range(len(highest_matches))]
            highest_matches["n_negative_filters"] = 0
        highest_matches["confidence_tier"] = confidence_tier
        self.unique_ids = pd.concat(
            [self.unique_ids, highest_matches],
            ignore_index=True,
        )

    def filter_payment(
        self,
        payments_x_conflicted: pd.Series,
        payment_filter: PaymentFilters,
    ) -> pd.Series:
        """Apply one filter to one row, route the outcome to the right list.

        Bug 5.8: ``filter_by_*`` methods now return a tri-state
        :class:`FilterOutcome` enum so the matcher can distinguish active
        DISAGREE (negative evidence) from NO_DATA (no signal). MATCH
        appends to ``filters``; DISAGREE appends to ``negative_filters``;
        NO_DATA accumulates nothing.
        """
        if payments_x_conflicted.empty:
            return payments_x_conflicted

        outcome = getattr(self, f"filter_by_{payment_filter.lower()}")(
            payments_x_conflicted=payments_x_conflicted,
        )

        # DO NOT USE .append() — it invokes the pandas deprecated method,
        # NOT the Python list append.
        if outcome == FilterOutcome.MATCH:
            payments_x_conflicted["filters"] = payments_x_conflicted["filters"] + [payment_filter]
        elif outcome == FilterOutcome.DISAGREE:
            payments_x_conflicted["negative_filters"] = payments_x_conflicted[
                "negative_filters"
            ] + [payment_filter]
        # FilterOutcome.NO_DATA → no accumulation.

        return payments_x_conflicted

    def apply_all_filters_to_row(
        self,
        payments_x_conflicted: pd.Series,
    ) -> pd.Series:
        """Apply every filter in ``self.filters`` to a single row in one pass.

        Section 6 optimization: the legacy implementation called
        ``merged.apply(filter_payment, axis=1)`` once per filter, doing
        N_filters (~14) separate row-wise iterations over the merged
        frame. This method collapses that into a single
        ``merged.apply(apply_all_filters_to_row, axis=1)`` call so each
        row is visited once and all filters fire against it in order.

        Behavior is identical: filters are applied in ``self.filters``
        order, and supersession rules (e.g. FIRSTNAME removing
        FIRSTNAME_PARTIAL) depend only on filter order, not on
        cross-row state.
        """
        if payments_x_conflicted.empty:
            return payments_x_conflicted
        for payment_filter in self.filters:
            payments_x_conflicted = self.filter_payment(
                payments_x_conflicted=payments_x_conflicted,
                payment_filter=payment_filter,
            )
        return payments_x_conflicted


class ConflictedPaymentIDs(
    IDsMixin,
    PaymentIDsCityStatesMixin,
    PaymentIDsCredentialsMixin,
    PaymentIDsNamesMixin,
    PaymentIDsSpecialtysMixin,
    PaymentIDsNPIMixin,
    Conflicted_x_PaymentIDs,
):
    """Filters OpenPayments payments by conflicted providers
    to find unique OpenPayments IDs.

    Args:
        conflicteds[DataFrame]:
        -provider_pk: Int64
        -first_name: str
        -last_name: str
        -middle_initial_1: str
        -middle_initial_2: str
        -middle_name_1: str
        -middle_name_2: str
        -credentials: array[Credentials]
        -specialtys: array[Specialtys]
        -citystates: array[CityState]

        payments[DataFrame]:
        -profile_id: Int64
        -first_name: str
        -middle_name: str
        -last_name: str
        -specialtys: array[Specialtys]
        -credentials: array[Credentials]
        -citystates: array[CityState]

    """

    @property
    def merge_column(self) -> str:
        """Returns the column to merge on. This is used to merge
        the payments DataFrame with the conflicteds DataFrame."""

        return "last_name"

    def search_for_conflicteds_ids(
        self,
    ) -> None:
        """Searches for OpenPayments IDs for the conflicted providers and
        updates the unmatched and unique_ids attributes with search results,
        or lack thereof.

        Section 6 (full vectorization): two-phase pipeline:

        1. **Vectorized cross-merge** (``_vectorized_search``): single merge
           of all conflicteds × all payments on the merge column
           (case-insensitive equality), one ``apply`` to evaluate all
           filters across the joint frame, then ``groupby('provider_pk')``
           to run the selector once per conflicted. Handles the common case
           (single-word last names with an exact CMS match) without paying
           the per-conflicted Python overhead.

        2. **Per-provider fallback**: provider_pks NOT covered by the
           vectorized phase fall through to the legacy per-provider path
           (``filter_payments_for_conflicted``), which uses
           ``merge_by_last_name``'s multi-word ``str.contains`` fallback.
           Necessary for cases like conflicted "John Smith Jones" vs CMS
           "Smith-Jones" where exact-key match misses but the multi-word
           fallback hits.

        Subclasses can opt out of vectorization by overriding
        ``_vectorized_search`` to return ``set()``.
        """

        # Add a conflict_ prefix to the columns of the conflicteds DataFrame
        # to avoid name clashes with the payments DataFrame
        conflicteds = self.conflicteds.rename(
            columns={
                col: f"conflict_{col}"
                for col in self.conflicteds.columns
                if (col != self.merge_column and col != "provider_pk")
            }
        )

        # Phase 1: vectorized cross-merge for the common path.
        handled_pks = self._vectorized_search(conflicteds=conflicteds)

        # Phase 2: per-provider fallback for everyone the vectorized path
        # didn't handle. Most relevant for multi-word last names that miss
        # the exact-key merge but hit the str.contains fallback.
        for _, conflicted in conflicteds.iterrows():
            pk = conflicted["provider_pk"]
            if pk in handled_pks:
                continue
            # Defensive guard mirroring the legacy "skip already-processed"
            # check (relevant when subclasses pre-populate unique_ids /
            # unmatched between vectorized and per-provider phases).
            if not self.unique_ids.empty and pk in self.unique_ids["provider_pk"].values:
                continue
            if not self.unmatched.empty and pk in self.unmatched["provider_pk"].values:
                continue
            logging.info(
                f"Per-provider fallback: {conflicted.get('conflict_first_name', '?')} "
                f"{conflicted[self.merge_column]}"
            )
            self.filter_payments_for_conflicted(conflicted=conflicted)

    def _vectorized_search(
        self,
        conflicteds: pd.DataFrame,
    ) -> set:
        """Vectorized cross-merge entry point. See
        :meth:`search_for_conflicteds_ids` for the design.

        Returns the set of provider_pks handled by this phase. Provider_pks
        NOT returned fall through to the per-provider path so multi-word
        ``merge_by_last_name`` fallback can fire.

        Currently only specialized for ``merge_column == "last_name"``.
        Other merge columns return an empty set (full per-provider fallback).
        Override to add vectorization for a different merge column.
        """
        if self.merge_column != "last_name":
            return set()
        if self.payments is None or self.payments.empty or conflicteds.empty:
            return set()

        # Case-insensitive join key, computed on a copy so we don't mutate
        # self.payments / the renamed conflicteds frame.
        payments_keyed = self.payments.assign(
            _op_lname_key=self.payments[self.merge_column].astype(str).str.lower()
        )
        conflicteds_keyed = conflicteds.assign(
            _op_lname_key=conflicteds[self.merge_column].astype(str).str.lower()
        ).drop(columns=[self.merge_column])

        merged = payments_keyed.merge(conflicteds_keyed, on="_op_lname_key").drop(
            columns=["_op_lname_key"]
        )

        if merged.empty:
            return set()

        # Initialize filter columns — every merged row matched on LASTNAME
        # by construction. Same shape as merge_by_last_name produces in the
        # per-provider path.
        merged.insert(0, "filters", [[PaymentFilters.LASTNAME]] * len(merged))
        merged.insert(1, "negative_filters", [[] for _ in range(len(merged))])

        merged = self.convert_merged_dtypes(merged)
        merged = merged.apply(self.apply_all_filters_to_row, axis=1)

        # Run the selector once per provider_pk group. Selector + dedupe +
        # add_unique_id / add_unmatched all already handle a single
        # provider's candidates correctly — this just feeds them in.
        handled: set = set()
        for provider_pk, group in merged.groupby("provider_pk", sort=False):
            self.process_filtered_payments_x_conflicteds(payments_x_conflicted=group)
            handled.add(provider_pk)

        return handled

    def filter_payments_for_conflicted(
        self,
        conflicted: pd.Series,
    ) -> None:
        """Filters the payments DataFrame for the given conflicted provider."""
        merged = getattr(self, f"merge_by_{self.merge_column}")(
            payments=self.payments,
            conflicted=conflicted,
        )

        if merged.empty:
            logging.info(f"No payments found for {conflicted[self.merge_column]}.")
            self.add_unmatched(
                conflicted=self.conflicteds[
                    self.conflicteds["provider_pk"] == conflicted["provider_pk"]
                ],
                unmatched=Unmatcheds.NOLASTNAME,
                filters=[],
                num_filters=0,
            )
            return

        merged = self.convert_merged_dtypes(merged)

        # Section 6 optimization: single .apply over rows applying every
        # filter in `self.filters` order, instead of N_filters separate
        # .apply(axis=1) passes. Same behavior, ~14x fewer pandas-internal
        # row iterations.
        merged = merged.apply(self.apply_all_filters_to_row, axis=1)

        self.process_filtered_payments_x_conflicteds(
            payments_x_conflicted=merged,
        )

    def process_filtered_payments_x_conflicteds(
        self,
        payments_x_conflicted: pd.DataFrame,
    ) -> None:
        """Selection-layer entry point (Section 5.7).

        Dedupes by profile_id, then delegates to the configured
        ``self.selector`` (defaults to ``DefaultMatchSelector`` which is the
        legacy cascade extracted verbatim). The selector returns a
        :class:`SelectorResult`; this method applies the result to the
        matcher's internal state (``unique_ids`` / ``unmatched`` /
        ``unmatched_options``).

        Override the *selector*, not this method, for study-specific rules.
        See :mod:`open_payments.selectors` for examples.
        """
        payments_x_conflicted = self._dedupe_by_profile_id(payments_x_conflicted)
        result = self.selector.select(payments_x_conflicted, matcher=self)
        self._apply_selector_result(result, payments_x_conflicted)

    @staticmethod
    def _dedupe_by_profile_id(payments_x_conflicted: pd.DataFrame) -> pd.DataFrame:
        """Reduce to one row per profile_id, preferring the row with the
        richest middle-name info as a tiebreaker.

        Bug 2 fix: explicit ``na_position="last"`` ensures null middle_name
        rows sort AFTER non-null ones, so ``keep="first"`` deterministically
        picks the row with the richest middle-name info per profile_id.
        Secondary sort by ``payment_id`` (Record_ID) provides a stable
        tiebreaker when two rows for the same profile_id share the same
        middle_name value.
        """
        sort_keys = ["profile_id", "middle_name"]
        if "payment_id" in payments_x_conflicted.columns:
            sort_keys.append("payment_id")
        payments_x_conflicted = payments_x_conflicted.sort_values(
            by=sort_keys,
            ascending=True,
            na_position="last",
        )
        return payments_x_conflicted.drop_duplicates(subset="profile_id", keep="first")

    def _apply_selector_result(
        self,
        result: SelectorResult,
        payments_x_conflicted: pd.DataFrame,
    ) -> None:
        """Apply a SelectorResult to the matcher's internal state.

        For ``kind="unique"``: route to ``add_unique_id``.
        For ``kind="unmatched_options"``: append candidates to
        ``self.unmatched_options`` and record the conflicted's source row in
        ``self.unmatched`` with the selector-supplied filters + reason.
        """
        if result.kind == "unique":
            first = result.match.iloc[0]
            logging.info(
                f"Unique match selected for "
                f"{first['conflict_first_name']} {first['last_name']} "
                f"(filters={list(first['filters'])}, "
                f"negative_filters={list(first.get('negative_filters', []))}, "
                f"tier={result.confidence_tier})"
            )
            self.add_unique_id(result.match, confidence_tier=result.confidence_tier)
            return

        # kind == "unmatched_options"
        options = result.unmatched_options.copy()
        if "negative_filters" not in options.columns:
            options["negative_filters"] = [[] for _ in range(len(options))]
        options["n_negative_filters"] = options["negative_filters"].apply(
            lambda x: len(x) if x is not None else 0
        )
        options["confidence_tier"] = result.confidence_tier
        self.unmatched_options = pd.concat([self.unmatched_options, options])
        unmatched_conflict = self.conflicteds[
            self.conflicteds["provider_pk"] == payments_x_conflicted.iloc[0]["provider_pk"]
        ]
        self.add_unmatched(
            conflicted=unmatched_conflict,
            unmatched=result.unmatched_reason or Unmatcheds.UNFILTERABLE,
            filters=result.representative_filters,
            num_filters=len(result.representative_filters),
            negative_filters=result.representative_negative_filters,
            confidence_tier=result.confidence_tier,
        )

    def extract_single_match(
        self,
        matches: pd.DataFrame,
    ) -> bool:
        if matches.shape[0] == 1:
            logging.info(
                f"Found unique match for {matches['conflict_first_name'].unique()[0]}"
                f" {matches['last_name'].unique()[0]}"
            )
            self.add_unique_id(matches)
            return True
        return False

    @staticmethod
    def get_highest_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Returns the rows with the most filters applied to them."""

        return payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(
                lambda x: len(x) == max(payments_x_conflicteds["filters"].apply(len))
            )
        ]
