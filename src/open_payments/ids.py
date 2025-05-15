from typing import Literal, Union

import pandas as pd

from .choices import PaymentFilters, Unmatcheds
from .citystates import PaymentCityStates, PaymentIDsCityStates
from .credentials import PaymentCredentials, PaymentIDsCredentials
from .names import NamesMixin, PaymentIDsNames
from .physicians_only import PhysicianFilter
from .specialtys import PaymentSpecialtys, PaymentIDsSpecialtys


class IDsMixin:

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[str, int]]]:
        cols = super().general_columns
        cols.update({
                "Covered_Recipient_Profile_ID": ("profile_id", "Int32"),
            }
        )
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[str, int]]]:
        cols = super().ownership_columns
        cols.update({
                "Physician_Profile_ID": ("profile_id", "Int32"),
        })
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[str, int]]]:
        cols = super().research_columns
        cols.update({**self.general_columns})
        return cols


class PaymentIDs(
    PaymentSpecialtys,
    PaymentCredentials,
    PaymentCityStates,
    IDsMixin,
    NamesMixin,
):

    def __init__(
        self,
        MD_DO_only: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.MD_DO_only = MD_DO_only

    def update_payments(
        self,
        payment_class: Literal["general", "ownership", "research"],
    ) -> pd.DataFrame:
        """Removes duplicate IDs and renames columns for the payment class
        DataFrame."""

        if self.MD_DO_only:
            payments = PhysicianFilter(
                payments=getattr(
                    self,
                    f"{payment_class}_payments",
                )
            ).filter()

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
            | ~df[
                df['profile_id'].notnull()
            ].duplicated(subset='profile_id', keep='first')
        ]

        return df


class Conflicted_x_PaymentIDs:

    def __init__(
        self,
        conflicteds: pd.DataFrame,
        payments: Union[pd.DataFrame, None],
    ):
        self.conflicteds = conflicteds
        self.payments = payments
        self.unmatched = pd.DataFrame()
        self.unmatched_options: pd.DataFrame = pd.DataFrame()
        self.unique_ids = pd.DataFrame()

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
    ) -> None:
        """Adds the unmatched conflicted provider to the unmatched
        DataFrame."""

        conflicted.loc[:, 'unmatched'] = unmatched
        conflicted.loc[:, "filters"] = [filters]
        conflicted.loc[:, "num_filters"] = num_filters

        self.unmatched = pd.concat(
            [self.unmatched, pd.DataFrame(conflicted)]
        )

    def convert_merged_dtypes(
        self,
        merged: pd.DataFrame,
    ) -> pd.DataFrame:
        """Updates  payments and conflicteds columns into lists after
        they are loaded as strs in CSVs and Excel files."""

        return merged

    def add_unique_id(
        self,
        highest_matches: pd.DataFrame,
    ) -> None:
        highest_matches.insert(
            0,
            "num_filters",
            highest_matches["filters"].apply(len),
        )
        self.unique_ids = pd.concat(
            [self.unique_ids, highest_matches],
            ignore_index=True,
        )

    def filter_payment(
        self,
        payments_x_conflicted: pd.Series,
        payment_filter: PaymentFilters,
    ) -> pd.Series:

        if not payments_x_conflicted.empty and getattr(
            self,
            f"filter_by_{payment_filter.lower()}",
        )(
            payments_x_conflicted=payments_x_conflicted,
        ):
            # DO NOT USE .append(), as it invokes the pandas
            # deprectated method, NOT the Python list append method.
            payments_x_conflicted["filters"] = (
                payments_x_conflicted["filters"]
                + [payment_filter]
            )

        return payments_x_conflicted


class ConflictedPaymentIDs(
    IDsMixin,
    PaymentIDsCityStates,
    PaymentIDsCredentials,
    PaymentIDsNames,
    PaymentIDsSpecialtys,
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

    def search_for_conflicteds_ids(
        self,
    ) -> None:
        """Searches for OpenPayments IDs for the conflicted providers and
        updates the unmatched and unique_ids attributes with search results,
        or lack thereof."""

        # Add a conflict_ prefix to the columns of the conflicteds DataFrame
        # to avoid name clashes with the payments DataFrame
        conflicteds = self.conflicteds.rename(
            columns={
                col: f"conflict_{col}" for col in self.conflicteds.columns
                if (col != "last_name" and col != "provider_pk")
            }
        )

        # Iterate over conflicteds and filter the payments DataFrame
        # for matches
        # Will populate the unique_ids and unmatched DataFrames
        # if there is a match or no match respectively
        for _, conflicted in conflicteds.iterrows():
            # Don't re-filter provider_pks that have already been filtered.
            # This is to allow looping through the pre-loaded OpenPayments
            # dataframes without having to re-read them.
            if (
                (
                    conflicted["provider_pk"] not in self.unique_ids["provider_pk"].values
                    if not self.unique_ids.empty else True
                )
                and (
                    conflicted["provider_pk"] not in self.unmatched["provider_pk"].values
                    if not self.unmatched.empty else True
                )
            ):
                self.filter_payments_for_conflicted(
                    conflicted=conflicted,
                )

            print(f"Processing conflicted provider: {conflicted['last_name']}")

    def filter_payments_for_conflicted(
        self,
        conflicted: pd.Series,
    ) -> None:
        """Filters the payments DataFrame for the given conflicted provider."""

        merged = self.merge_by_last_name(
            conflicted=conflicted,
        )

        if merged.empty:
            print(f"No payments found for {conflicted['last_name']}.")
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

        for payment_filter in self.filters:
            merged = merged.apply(
                lambda x: self.filter_payment(
                    payments_x_conflicted=x,
                    payment_filter=payment_filter,
                ),
                axis=1,
            )

        first_name_matches = self.get_firstname_matches(merged)

        # Preferentially filter for first name matches
        # Get the rows with the most filters
        if not first_name_matches.empty:
            middle_name_matches = self.get_middlename_matches(
                first_name_matches
            )
            if not middle_name_matches.empty:
                # Get the rows with the most filters
                highest_matches = middle_name_matches[
                    middle_name_matches["filters"].apply(
                        lambda x: len(x) == max(
                            middle_name_matches["filters"].apply(len)
                        )
                    )
                ]
            else:
                # Get the rows with the most filters
                highest_matches = first_name_matches[
                    first_name_matches["filters"].apply(
                        lambda x: len(x) == max(
                            first_name_matches["filters"].apply(len)
                        )
                    )
                ]

        else:
            highest_matches = merged[
                merged["filters"].apply(
                    lambda x: len(x) == max(
                        merged["filters"].apply(len)
                    )
                )
            ]

        # Of the highest matches, reduce the DataFrame to only
        # have a single payment from each profile_id
        # Keep the rows for each profile_id that have the fewest na columns

        highest_matches = highest_matches.sort_values(
            by=["profile_id", "middle_name"],
            ascending=[True, True],
        )

        highest_matches = highest_matches.drop_duplicates(
            subset="profile_id", keep="first"
        )

        if highest_matches.shape[0] == 1:
            print(f"Found unique match for {merged['conflict_first_name'].unique()[0]} {merged['last_name'].unique()[0]}")
            self.add_unique_id(highest_matches)
        else:
            print(
                f"Multiple matches found for {merged['conflict_first_name'].unique()[0]} {merged['last_name'].unique()[0]}: {len(highest_matches)}"
            )

            print(
                "Attempting to find a single good match..."
            )

            best_highest_matches = self.get_citystate_matches(
                highest_matches
            )

            if (
                not best_highest_matches.empty
                and best_highest_matches.shape[0] == 1
            ):
                print(f"Found unique match for {merged['conflict_first_name'].unique()[0]} {merged['last_name'].unique()[0]}")
                self.add_unique_id(best_highest_matches)
            else:
                if not best_highest_matches.empty:
                    self.unmatched_options = pd.concat(
                        [self.unmatched_options, best_highest_matches]
                    )
                else:
                    self.unmatched_options = pd.concat(
                        [self.unmatched_options, highest_matches]
                    )
                unmatched_conflict = self.conflicteds[
                    self.conflicteds["provider_pk"] == conflicted["provider_pk"]
                ]

                self.add_unmatched(
                    conflicted=unmatched_conflict,
                    unmatched=Unmatcheds.UNFILTERABLE,
                    filters=highest_matches.iloc[0]["filters"],
                    num_filters=len(highest_matches.iloc[0]["filters"])
                )
