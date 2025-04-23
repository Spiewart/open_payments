from enum import StrEnum
from itertools import combinations
from typing import Literal, Union

import pandas as pd

from .citystates import PaymentCityStates
from .credentials import PaymentCredentials
from .helpers import get_conflicted_ids_from_file, str_in_str
from .physicians_only import PhysicianFilter
from .read import ReadPayments
from .specialtys import PaymentSpecialtys


class PaymentIDs(
    PaymentSpecialtys,
    PaymentCredentials,
    PaymentCityStates,
    ReadPayments,
):

    def __init__(
        self,
        MD_DO_only: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.MD_DO_only = MD_DO_only

    def unique_payments(self) -> pd.DataFrame:
        """Returns a DataFrame of rows from OpeyPayments payment datasets that
        have a unique provider ID (Covered_Recipient_Profile_ID)."""

        all_payments = self.all_payments()

        # Remove duplicates again because there may be duplicate IDs between
        # the three different payment types.
        all_payments = self.remove_duplicate_ids(all_payments)

        all_payments.drop("payment_class", axis=1, inplace=True)

        return all_payments

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


class PaymentFilters(StrEnum):
    """Enum class for the various stages at which a conflicted provider
    can be identified from the list of unique OpenPayment IDs."""

    LASTNAME = "LASTNAME"
    FIRSTNAME = "FIRSTNAME"
    CREDENTIAL = "CREDENTIAL"
    SPECIALTY = "SPECIALTY"
    SUBSPECIALTY = "SUBSPECIALTY"
    FULLSPECIALTY = "FULLSPECIALTY"  # Matches specialty and subspecialty
    MIDDLE_INITIAL = "MIDDLE_INITIAL"
    MIDDLENAME = "MIDDLENAME"
    CITY = "CITY"
    STATE = "STATE"
    CITYSTATE = "CITYSTATE"  # Matches city and state


class Unmatcheds(StrEnum):
    """Enum class to represent the various ways a conflicted can
    remain unmatched throughout / after the filtering process."""

    NOLASTNAME = "NOLASTNAME"  # No matches for the last name in OpenPayments
    UNFILTERABLE = "UNFILTERABLE"  # Multiple OpenPayments IDs that can't be matched


def get_list_of_combinations(input_list: list[PaymentFilters]) -> list[list[PaymentFilters]]:
    """
    Generates all combinations of PaymentFilters from the input list.

    Args:
        input_list: The list PaymetnIDFilters to generate combinations from.

    Returns:
        A list of lists of PaymentFilters, where each list is a combination.
    """
    all_combinations = []
    for r in range(1, len(input_list) + 1):
        for combination in combinations(input_list, r):
            all_combinations.append(list(combination))
    return all_combinations


class ConflictedPaymentIDs(PaymentIDs):

    def __init__(
        self,
        *args,
        conflicteds: pd.DataFrame,
        payments: Union[pd.DataFrame, None],
        filters: list[PaymentFilters] = None,
        **kwargs,
    ):
        """Filters OpenPayments profile IDs by .

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
        super().__init__(*args, **kwargs)
        self.conflicteds = conflicteds
        self.payments = payments
        self.unmatched = pd.DataFrame()
        self.unique_ids = pd.DataFrame()
        self.filters = (
            filters if filters else [
                payment_filter for payment_filter
                in PaymentFilters.__members__.values()
                if payment_filter is not PaymentFilters.LASTNAME
            ]
        )

    def search_for_conflicteds_ids(self) -> None:
        """Searches for OpenPayments IDs for the conflicted providers and
        updates the unmatched and unique_ids attributes with search results,
        or lack thereof."""

        # Add a conflict_ prefix to the columns of the conflicteds DataFrame
        # to avoid name clashes with the payments DataFrame
        conflicteds = self.conflicteds.rename(
            columns={
                col: f"conflict_{col}" for col in self.conflicteds.columns
                if col != "last_name"
            }
        )

        # Iterate over conflicteds and filter the payments DataFrame
        # for matches
        # Will populate the unique_ids and unmatched DataFrames
        # if there is a match or no match respectively
        for _, conflicted in conflicteds.iterrows():
            print(conflicted)
            self.filter_payment_for_conflicted(
                conflicted=conflicted,
            )

            print(f"Processing conflicted provider: {conflicted['last_name']}")

    def update_master(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Updates the master DataFrames for conflicted_ids and unmatched
        providers. Returns the updated DataFrames."""

        conflicteds_ids, unmatched_ids = get_conflicted_ids_from_file()

        for new_id in self.unique_ids.iterrows():
            self.update_or_insert_provider(
                provider_row=new_id,
                df=conflicteds_ids,
            )
        for new_unmatched in self.unmatched.iterrows():
            self.update_or_insert_provider(
                provider_row=new_unmatched,
                df=unmatched_ids,
            )

        return conflicteds_ids, unmatched_ids

    def filter_payment_for_conflicted(
        self,
        conflicted: pd.Series,
    ) -> None:
        """Filters the payments DataFrame for the given conflicted provider."""

        merged = self.merge_by_last_name(
            conflicted=conflicted,
        )

        if merged.empty:
            print(f"No payments found for {conflicted['last_name']}.")
            conflicted["unmatched"] = Unmatcheds.NOLASTNAME
            self.unmatched = pd.concat(
                [self.unmatched, pd.DataFrame([conflicted])]
            )
            return

        for payment_filter in self.filters:
            merged = merged.apply(
                lambda x: self.filter_payment(
                    payments_x_conflicted=x,
                    payment_filter=payment_filter,
                ),
                axis=1,
            )
        print(merged.columns)
        # Get the row with the most filter matches
        highest_match = merged[
            merged["filters"].apply(
                lambda x: len(x) == max(
                    merged["filters"].apply(len)
                )
            )
        ]

        if highest_match.shape[0] == 1:
            print(f"Found unique match for {merged[['last_name', 'first_name']]}: {highest_match}")
            self.unique_ids = pd.concat(
                [self.unique_ids, highest_match],
                ignore_index=True,
            )
        else:
            print(
                f"Multiple matches found for {merged['last_name']}: {highest_match.shape[0]}")
            unmatched_conflict = self.conflicteds[
                self.conflicteds["conflict_provider_pk"] == conflicted["conflict_provider_pk"]
            ]
            unmatched_conflict["unmatched"] = Unmatcheds.UNFILTERABLE
            self.unmatched = pd.concat(
                [self.unmatched, unmatched_conflict]
            )

    def merge_by_last_name(
        self,
        conflicted: pd.Series,
    ) -> pd.DataFrame:
        """Merges the payments DataFrame with the conflicted provider
        Series by last name. Returns a DataFrame of payments
        that match the conflicted provider's last name."""

        print(f"Merging Payments df with Conflicted df by {conflicted['last_name']}...")

        merged_payments = self.payments[
            self.payments["last_name"].str.lower()
            == conflicted["last_name"].lower()
        ]

        if merged_payments.empty:
            return merged_payments

        conflicted_df = pd.concat(
            [conflicted.drop("last_name")] * len(merged_payments),
            axis=1,
        ).T.set_index(merged_payments.index)

        merged = pd.concat(
            [
                merged_payments,
                conflicted_df,
            ],
            axis=1,
        )

        merged.insert(0, "filters", [[PaymentFilters.LASTNAME]] * len(merged))

        return merged

    def filter_payment(
        self,
        payments_x_conflicted: pd.Series,
        payment_filter: PaymentFilters,
    ) -> pd.Series:

        payments_x_conflicted = getattr(
            self,
            f"filter_by_{payment_filter.lower()}",
        )(
            payments_x_conflicted=payments_x_conflicted,
        ) if not payments_x_conflicted.empty else payments_x_conflicted

        return payments_x_conflicted

    @classmethod
    def filter_by_credential(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> pd.Series:
        """Checks if a payment_x_conflicted series has a match
        in its credentials and conflict_credentials columns and adds a
        filter to the filters column to indicate as such if so."""

        if (
            cls.empty_array_or_nan_can_be_iterated(
                payments_x_conflicted["credentials"]
            )
            and cls.empty_array_or_nan_can_be_iterated(
                payments_x_conflicted["conflict_credentials"]
            )
            and any(
                cred in payments_x_conflicted["credentials"]
                for cred in payments_x_conflicted["conflict_credentials"]
            )
        ):
            # If there is a match, append the filter to the filters list
            payments_x_conflicted["filters"].append(PaymentFilters.CREDENTIAL)

        return payments_x_conflicted

    @classmethod
    def filter_by_firstname(
        cls,
        payments_x_conflicted: pd.Series,
        strict: bool = False,
    ) -> pd.Series:
        """Checks if a payment_x_conflicted series has a match
        in its first_name and conflict_first_name columns and adds a
        filter to the filters column to indicate as such if so."""

        if (
            pd.notna(payments_x_conflicted["first_name"])
            and pd.notna(payments_x_conflicted["conflict_first_name"])
            and (
                payments_x_conflicted["first_name"]
                == payments_x_conflicted["conflict_first_name"]
                if strict
                else (
                    str_in_str(
                        payments_x_conflicted["first_name"],
                        payments_x_conflicted["conflict_first_name"]
                    )
                    or str_in_str(
                        payments_x_conflicted["conflict_first_name"],
                        payments_x_conflicted["first_name"]
                    )
                )
            )
        ):
            payments_x_conflicted["filters"].append(PaymentFilters.FIRSTNAME)

        return payments_x_conflicted

    @classmethod
    def filter_by_specialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> pd.Series:
        """Checks for specialty match between the payment and conflicted
        provider. If there is a match, it appends the filter to the
        filters list."""

        if (
            cls.payment_conflict_specialty_match(
                payment_specialtys=payments_x_conflicted["specialtys"],
                conflict_specialtys=payments_x_conflicted[
                    "conflict_specialtys"
                ],
            )
        ):
            payments_x_conflicted["filters"].append(PaymentFilters.SPECIALTY)
        return payments_x_conflicted

    @classmethod
    def payment_conflict_specialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:
        """Checks if the specialtys exist and match."""

        return any(
            spec.specialty in [
                spec.specialty for spec in payment_specialtys if pd.notna(spec)
            ] for spec in [
                spec for spec in conflict_specialtys if pd.notna(spec)
            ]
        ) if (
            cls.empty_array_or_nan_can_be_iterated(payment_specialtys)
            and cls.empty_array_or_nan_can_be_iterated(conflict_specialtys)
        ) else False

    @classmethod
    def filter_by_subspecialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> pd.Series:
        """Filters by subspecialty."""

        if (
                cls.payment_conflict_subspecialty_match(
                    payment_specialtys=payments_x_conflicted["specialtys"],
                    conflict_specialtys=payments_x_conflicted[
                        "conflict_specialtys"
                    ],
                )
        ):
            payments_x_conflicted["filters"].append(
                PaymentFilters.SUBSPECIALTY
            )
        return payments_x_conflicted

    @classmethod
    def payment_conflict_subspecialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            spec.subspecialty in [
                spec.subspecialty for spec in payment_specialtys if pd.notna(
                    spec
                )
            ] for spec in [
                spec for spec in conflict_specialtys if pd.notna(spec)
            ]
        ) if (
            cls.empty_array_or_nan_can_be_iterated(payment_specialtys)
            and cls.empty_array_or_nan_can_be_iterated(conflict_specialtys)
        ) else False

    @classmethod
    def filter_by_fullspecialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> pd.Series:
        """Filters by full specialty."""

        if (
                cls.payment_conflict_full_specialty_match(
                    payment_specialtys=payments_x_conflicted["specialtys"],
                    conflict_specialtys=payments_x_conflicted[
                        "conflict_specialtys"
                    ],
                )
        ):
            payments_x_conflicted["filters"].append(
                PaymentFilters.FULLSPECIALTY
            )
        return payments_x_conflicted

    @classmethod
    def payment_conflict_full_specialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            spec in [
                spec for spec in payment_specialtys if pd.notna(spec)
            ] for spec in [
                spec for spec in conflict_specialtys if pd.notna(spec)
            ]
        ) if (
            cls.empty_array_or_nan_can_be_iterated(payment_specialtys)
            and cls.empty_array_or_nan_can_be_iterated(conflict_specialtys)
        ) else False

    @classmethod
    def filter_by_city(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> pd.Series:
        """Filters by city."""

        if (
                cls.payment_conflict_city_match(
                    payment_citystates=payments_x_conflicted["citystates"],
                    conflict_citystates=payments_x_conflicted[
                        "conflict_citystates"
                    ],
                )
        ):
            payments_x_conflicted["filters"].append(
                PaymentFilters.CITY
            )
        return payments_x_conflicted

    @classmethod
    def payment_conflict_city_match(
        cls,
        payment_citystates: Union[list[PaymentSpecialtys], None],
        conflict_citystates: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            citystate.city in [
                citystate.city for citystate in payment_citystates
                if pd.notna(citystate)
            ] for citystate in [
                citystate for citystate in conflict_citystates
                if pd.notna(citystate)
            ]
        ) if (
            cls.empty_array_or_nan_can_be_iterated(payment_citystates)
            and cls.empty_array_or_nan_can_be_iterated(conflict_citystates)
        ) else False

    @staticmethod
    def empty_array_or_nan_can_be_iterated(
        row: Union[list, float]
    ) -> bool:
        """Checks if the citystates can be iterated over."""

        return isinstance(row, list) and row

    @classmethod
    def filter_by_state(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> pd.Series:
        """Filters by state."""

        if (
                cls.payment_conflict_state_match(
                    payment_citystates=payments_x_conflicted["citystates"],
                    conflict_citystates=payments_x_conflicted[
                        "conflict_citystates"
                    ],
                )
        ):
            payments_x_conflicted["filters"].append(
                PaymentFilters.STATE
            )
        return payments_x_conflicted

    @classmethod
    def payment_conflict_state_match(
        cls,
        payment_citystates: Union[list[PaymentSpecialtys], None],
        conflict_citystates: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            citystate.state in [
                citystate.state for citystate in payment_citystates
                if pd.notna(citystate)
            ] for citystate in [
                citystate for citystate in conflict_citystates
                if pd.notna(citystate)
            ]
        ) if (
            cls.empty_array_or_nan_can_be_iterated(payment_citystates)
            and cls.empty_array_or_nan_can_be_iterated(conflict_citystates)
        ) else False

    @classmethod
    def filter_by_citystate(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> pd.Series:
        """Filters by city and state."""

        if (
            cls.payment_conflict_citystate_match(
                payment_citystates=payments_x_conflicted["citystates"],
                conflict_citystates=payments_x_conflicted[
                    "conflict_citystates"
                ],
            )
        ):
            payments_x_conflicted["filters"].append(
                PaymentFilters.CITYSTATE
            )
        return payments_x_conflicted

    @classmethod
    def payment_conflict_citystate_match(
        cls,
        payment_citystates: Union[list[PaymentSpecialtys], None],
        conflict_citystates: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            citystate in [
                citystate for citystate in payment_citystates
                if pd.notna(citystate)
            ] for citystate in [
                citystate for citystate in conflict_citystates
                if pd.notna(citystate)
            ]
        ) if (
            cls.empty_array_or_nan_can_be_iterated(payment_citystates)
            and cls.empty_array_or_nan_can_be_iterated(conflict_citystates)
        ) else False

    @classmethod
    def filter_by_middle_initial(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> pd.Series:
        """Filters by middle initial."""

        if (
            cls.middle_initial_match(
                conflicted_middle_initial_1=payments_x_conflicted[
                    "conflict_middle_initial_1"
                    ],
                conflicted_middle_initial_2=payments_x_conflicted[
                    "conflict_middle_initial_2"
                ],
                conflicted_middle_name_1=payments_x_conflicted[
                    "conflict_middle_name_1"
                ],
                conflicted_middle_name_2=payments_x_conflicted[
                    "conflict_middle_name_2"
                ],
                payment_middle_name=payments_x_conflicted["middle_name"],
            )
        ):
            payments_x_conflicted["filters"].append(
                PaymentFilters.MIDDLE_INITIAL
            )
        return payments_x_conflicted

    @staticmethod
    def middle_initial_match(
        conflicted_middle_initial_1: Union[str, None],
        conflicted_middle_initial_2: Union[str, None],
        conflicted_middle_name_1: Union[str, None],
        conflicted_middle_name_2: Union[str, None],
        payment_middle_name: Union[str, None],
    ) -> bool:
        """Checks if the middle initial matches."""

        return pd.notna(payment_middle_name) and (
            (
                pd.notna(conflicted_middle_initial_1)
                and (
                    payment_middle_name[0].lower()
                    == conflicted_middle_initial_1.lower()
                )
            )
            or (
                pd.notna(conflicted_middle_initial_2)
                and (
                    payment_middle_name[0].lower()
                    == conflicted_middle_initial_2.lower()
                )
            )
            or (
                pd.notna(conflicted_middle_name_1)
                and (
                    payment_middle_name.lower()
                    == conflicted_middle_name_1[0].lower()
                )
            )
            or (
                pd.notna(conflicted_middle_name_2)
                and (
                    payment_middle_name.lower()
                    == conflicted_middle_name_2[0].lower()
                )
            )
        )

    @classmethod
    def filter_by_middlename(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> pd.Series:
        """Filters by middle name."""

        if (
            cls.middlename_match(
                conflicted_middle_name_1=payments_x_conflicted[
                    "conflict_middle_name_1"
                    ],
                conflicted_middle_name_2=payments_x_conflicted[
                    "conflict_middle_name_2"
                ],
                payment_middle_name=payments_x_conflicted["middle_name"],
            )
        ):
            payments_x_conflicted["filters"].append(
                PaymentFilters.MIDDLENAME
            )
        return payments_x_conflicted

    @staticmethod
    def middlename_match(
        conflicted_middle_name_1: Union[str, None],
        conflicted_middle_name_2: Union[str, None],
        payment_middle_name: Union[str, None],
    ) -> bool:
        """Checks if the middle name matches."""

        return pd.notna(payment_middle_name) and (
            (
                pd.notna(conflicted_middle_name_1)
                and (
                    payment_middle_name.lower()
                    == conflicted_middle_name_1.lower()
                )
            )
            or (
                pd.notna(conflicted_middle_name_2)
                and (
                    payment_middle_name.lower()
                    == conflicted_middle_name_2.lower()
                )
            )
        )

    @staticmethod
    def update_or_insert_provider(
        provider_row: pd.Series,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Updates or inserts a provider into the DataFrame based
        on his or her provider_pk."""

        # Check if the provider already exists in the DataFrame
        existing_index = df.index[df["provider_pk"] == provider_row["provider_pk"]]

        if not existing_index.empty:
            # Update the existing row
            df.loc[existing_index[0]] = provider_row
        else:
            # Append the new row to the DataFrame
            df = pd.concat([df, pd.DataFrame([provider_row])], ignore_index=True)

        return df
