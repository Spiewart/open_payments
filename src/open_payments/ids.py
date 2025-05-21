from typing import Literal, Type, Union

import pandas as pd

from .choices import PaymentFilters, Unmatcheds
from .citystates import PaymentCityStates, PaymentIDsCityStates
from .credentials import PaymentCredentials, PaymentIDsCredentials
from .helpers import ColumnMixin
from .names import NamesMixin, PaymentIDsNames
from .physicians_only import ReadPaymentsPhysicians
from .specialtys import PaymentIDsSpecialtys, PaymentSpecialtys


class IDsMixin(ColumnMixin):

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:
        cols = super().general_columns
        cols.update({
                "Covered_Recipient_Profile_ID": ("profile_id", "Int32"),
            }
        )
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:
        cols = super().ownership_columns
        cols.update({
                "Physician_Profile_ID": ("profile_id", "Int32"),
        })
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:
        cols = super().research_columns
        cols.update({**self.general_columns})
        return cols


class PaymentIDs(
    PaymentSpecialtys,
    PaymentCredentials,
    PaymentCityStates,
    IDsMixin,
    NamesMixin,
    ReadPaymentsPhysicians,
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

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:
        """Returns the columns for the general payments DataFrame."""

        cols = super().general_columns
        cols.update({
            "Program_Year": ("payment_year", "Int16"),
        })

        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:
        """Returns the columns for the ownership payments DataFrame."""

        cols = super().ownership_columns
        cols.update({
            "Program_Year": ("payment_year", "Int16"),
        })

        return cols


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

        highest_matches.drop("payment_year", axis=1, inplace=True)

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
        -payment_year: int
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
        or lack thereof."""

        # Add a conflict_ prefix to the columns of the conflicteds DataFrame
        # to avoid name clashes with the payments DataFrame
        conflicteds = self.conflicteds.rename(
            columns={
                col: f"conflict_{col}" for col in self.conflicteds.columns
                if (col != self.merge_column and col != "provider_pk")
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

            print(
                f"Processing conflicted provider: {conflicted['conflict_first_name']}"
                f" {conflicted['last_name']}"
            )

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
            print(f"No payments found for {conflicted[self.merge_column]}.")
            self.add_unmatched(
                conflicted=self.conflicteds[
                    self.conflicteds["provider_pk"] == conflicted[
                        "provider_pk"
                    ]
                ],
                unmatched=Unmatcheds.NOLASTNAME,
                filters=[],
                num_filters=0,
            )
            return

        merged = self.convert_merged_dtypes(merged)

        merged = self.fill_middle_names(merged)
        
        for payment_filter in self.filters:
            merged = merged.apply(
                lambda x: self.filter_payment(
                    payments_x_conflicted=x,
                    payment_filter=payment_filter,
                ),
                axis=1,
            )

        self.process_filtered_payments_x_conflicteds(
            payments_x_conflicted=merged,
        )

    def process_filtered_payments_x_conflicteds(
        self,
        payments_x_conflicted: pd.DataFrame,
    ) -> None:
        """Processes a filtered payments_x_conflicted DataFrame and
        adds either the unique row to the unique_ids df or, if unmatched,
        the conflicted provider  data about which filters were
        successfully applied to the unmatched df.

        Can be overwritten for alternative functionality.
        """

        # Of the payments, reduce the DataFrame to only
        # have a single payment from each profile_id

        payments_x_conflicted = payments_x_conflicted.sort_values(
            by=["profile_id", "payment_year"],
            ascending=[True, False],
        )

        payments_x_conflicted = payments_x_conflicted.drop_duplicates(
            subset="profile_id", keep="first"
        )

        first_name_matches = self.get_firstname_matches(
            payments_x_conflicteds=payments_x_conflicted
        )

        if not first_name_matches.empty and self.extract_single_match(
            first_name_matches
        ):
            return
        # Preferentially filter for first name matches
        # Get the rows with the most filters
        else:
            middle_name_matches = self.get_middlename_matches(
                first_name_matches
            )
            print(middle_name_matches)
            if not middle_name_matches.empty and self.extract_single_match(
                middle_name_matches
            ):
                return
            else:
                # If a single ID can't be found with the middle name,
                # try to find a match with the full City and State
                citystate_matches = self.get_citystate_matches(
                    first_name_matches
                )
                print(citystate_matches)
                if not citystate_matches.empty and self.extract_single_match(
                    citystate_matches
                ):
                    return

        highest_matches = self.get_highest_matches(
            payments_x_conflicteds=(
                middle_name_matches if not middle_name_matches.empty
                else citystate_matches if not citystate_matches.empty
                else first_name_matches if not first_name_matches.empty
                else payments_x_conflicted
            )
        )

        if highest_matches.shape[0] == 1:
            print(
                f"Found unique match for {payments_x_conflicted['conflict_first_name'].unique()[0]}"
                f" {payments_x_conflicted['last_name'].unique()[0]}"
            )
            self.add_unique_id(highest_matches)
        else:
            print(
                f"Multiple matches found for {payments_x_conflicted['conflict_first_name'].unique()[0]}"
                f" {payments_x_conflicted['last_name'].unique()[0]}: {len(highest_matches)}"
            )

            print(
                "Attempting to find a single good match..."
            )

            best_highest_matches = self.get_citystate_matches(
                highest_matches
            )

            if best_highest_matches.empty:
                best_highest_matches = self.get_specialty_matches(
                    highest_matches
                )
            elif best_highest_matches.shape[0] > 1:
                best_highest_matches = self.get_specialty_matches(
                    best_highest_matches
                )

            if (
                not best_highest_matches.empty
                and best_highest_matches.shape[0] == 1
            ):
                print(
                    f"Found unique match for {payments_x_conflicted['conflict_first_name'].unique()[0]}"
                    f" {payments_x_conflicted['last_name'].unique()[0]}"
                )
                self.add_unique_id(best_highest_matches)
            else:
                print(
                    f"Could not find match for {payments_x_conflicted['conflict_first_name'].unique()[0]}"
                    f" {payments_x_conflicted['last_name'].unique()[0]}"
                )
                if not best_highest_matches.empty:
                    self.unmatched_options = pd.concat(
                        [self.unmatched_options, best_highest_matches]
                    )
                else:
                    self.unmatched_options = pd.concat(
                        [self.unmatched_options, highest_matches]
                    )
                unmatched_conflict = self.conflicteds[
                    self.conflicteds["provider_pk"] == payments_x_conflicted.iloc[0]["provider_pk"]
                ]

                self.add_unmatched(
                    conflicted=unmatched_conflict,
                    unmatched=Unmatcheds.UNFILTERABLE,
                    filters=highest_matches.iloc[0]["filters"],
                    num_filters=len(highest_matches.iloc[0]["filters"])
                )

    @staticmethod
    def fill_middle_names(merged: pd.DataFrame) -> pd.DataFrame:
        """Iterates over middle_name column for each row and
        looks for na values and then searches the df for other rows
        for the same profile_id with a middle_name and backfills
        the na values with the middle_name from the other rows. This
        is because some payments may have their middle name omitted,
        but it can be useful when searching."""

        merged["middle_name"] = merged.apply(
            lambda x: (
                x["middle_name"] if not pd.isna(x["middle_name"]) else
                next(iter(
                    merged[
                        merged["profile_id"] == x["profile_id"]
                    ]["middle_name"].unique()), None)
            ),
            axis=1,
        )

        return merged

    def extract_single_match(
        self,
        matches: pd.DataFrame,
    ) -> bool:
        if matches.shape[0] == 1:
            print(
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
                lambda x: len(x) == max(
                    payments_x_conflicteds["filters"].apply(len)
                )
            )
        ]