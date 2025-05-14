import re
from enum import StrEnum
from itertools import combinations
from typing import Literal, Union

import pandas as pd

from .citystates import PaymentCityStates, convert_citystates
from .credentials import PaymentCredentials, convert_credentials
from .helpers import str_in_str
from .physicians_only import PhysicianFilter
from .read import ReadPayments
from .specialtys import PaymentSpecialtys, convert_specialtys


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
    FIRSTNAME_PARTIAL = "FIRSTNAME_PARTIAL"  # Matches first name with a partial match
    FIRST_MIDDLE_NAME = "FIRST_MIDDLE_NAME"  # Matches first name and middle name
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
        self.unmatched_options: pd.DataFrame = pd.DataFrame()
        self.unique_ids = pd.DataFrame()
        self.filters = (
            filters if filters else [
                payment_filter for payment_filter
                in PaymentFilters.__members__.values()
                if payment_filter is not PaymentFilters.LASTNAME
            ]
        )

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

        # If no last name matches are found, some last names contain
        # multiple last names, so we can check if the conflicted last
        # name is in the payments last name
        if merged_payments.empty:
            # Split the last name by hyphen and whitespace
            conflicted_last_names = re.split(
                r"-|\s+",
                conflicted["last_name"].lower()
            )

            # Check if any of potentially multiple last names
            # are in the payments last name
            merged_payments = self.payments[
                self.payments["last_name"].str.contains(
                    '|'.join(conflicted_last_names),
                    na=False,
                    case=False,
                )
            ]
            if not merged_payments.empty:
                # If there are multiple last names, select payments
                # that match all the last names to avoid false positives
                double_matches = merged_payments[
                    merged_payments["last_name"].str.contains(
                        '&'.join(conflicted_last_names),
                        na=False,
                        case=False,
                    )
                ]
                if not double_matches.empty:
                    merged_payments = double_matches

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
        """Updates citystates, credentials, and specialtys from payments
        and conflicteds columns into lists after they are loaded as strs
        in CSVs and Excel files."""

        merged["citystates"] = merged["citystates"].apply(
            lambda x: convert_citystates(x) if isinstance(x, str) else x
        )

        merged["conflict_citystates"] = merged["conflict_citystates"].apply(
            lambda x: convert_citystates(x) if isinstance(x, str) else x
        )

        merged["credentials"] = merged["credentials"].apply(
            lambda x: convert_credentials(x) if isinstance(x, str) else x
        )

        merged["conflict_credentials"] = merged["conflict_credentials"].apply(
            lambda x: convert_credentials(x) if isinstance(x, str) else x
        )

        merged["specialtys"] = merged["specialtys"].apply(
            lambda x: convert_specialtys(x) if isinstance(x, str) else x
        )

        merged["conflict_specialtys"] = merged["conflict_specialtys"].apply(
            lambda x: convert_specialtys(x) if isinstance(x, str) else x
        )
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

    @staticmethod
    def get_citystate_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a DataFrame for city/state matches in order of priority:
        1. City/State match
        2. State match
        3. City match
    """
        refined_matches = payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(
                lambda x: PaymentFilters.CITYSTATE in x
            )
        ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.STATE in x
                )
            ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.CITY in x
                )
            ]

        return refined_matches

    @staticmethod
    def get_firstname_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a DataFrame for first name matches in order of priority:
        1. First name match
        2. First name partial match
        3. First name and middle name match
    """
        refined_matches = payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(
                lambda x: PaymentFilters.FIRSTNAME in x
            )
        ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.FIRSTNAME_PARTIAL in x
                )
            ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.FIRST_MIDDLE_NAME in x
                )
            ]

        return refined_matches

    @staticmethod
    def get_middlename_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a DataFrame for middle name matches in order of priority:
        1. Middle name match
        2. Middle initial match
    """
        refined_matches = payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(
                lambda x: PaymentFilters.MIDDLENAME in x
            )
        ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.MIDDLE_INITIAL in x
                )
            ]

        return refined_matches

    @classmethod
    def filter_by_credential(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Checks if a payment_x_conflicted series has a match
        in its credentials and conflict_credentials columns and adds a
        filter to the filters column to indicate as such if so."""

        return (
            any(
                cred in payments_x_conflicted["credentials"]
                for cred in payments_x_conflicted["conflict_credentials"]
            )
        )

    @classmethod
    def filter_by_firstname(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Checks if a payment_x_conflicted series has a match
        in its first_name and conflict_first_name columns and adds a
        filter to the filters column to indicate as such if so."""

        value = (
            pd.notna(payments_x_conflicted["first_name"])
            and pd.notna(payments_x_conflicted["conflict_first_name"])
            and (
                payments_x_conflicted["first_name"].lower()
                == payments_x_conflicted["conflict_first_name"].lower()
            )
        )
        # Full match should supercede a partial match or first/middle name match
        if value:
            if PaymentFilters.FIRSTNAME_PARTIAL in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(
                    PaymentFilters.FIRSTNAME_PARTIAL
                )
            if PaymentFilters.FIRST_MIDDLE_NAME in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(
                    PaymentFilters.FIRST_MIDDLE_NAME
                )

        return value

    @classmethod
    def filter_by_firstname_partial(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Checks if a payment_x_conflicted series has a match
        in its first_name and conflict_first_name columns and adds a
        filter to the filters column to indicate as such if so."""

        value = (
            pd.notna(payments_x_conflicted["first_name"])
            and pd.notna(payments_x_conflicted["conflict_first_name"])
            and PaymentFilters.FIRSTNAME not in payments_x_conflicted[
                "filters"
            ]
            and (
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
        # Partial match should supercede first / middle name match
        if value and PaymentFilters.FIRST_MIDDLE_NAME in payments_x_conflicted["filters"]:
            payments_x_conflicted["filters"].remove(
                PaymentFilters.FIRST_MIDDLE_NAME
            )

        return value

    @classmethod
    def filter_by_first_middle_name(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Checks if a payment_x_conflicted series has a match
        in its OpenPayments first_name and any of its conflicts
        middle name columns, or vice versa (conflict_first_name
        and OpenPayments middle_name)."""

        return (
            PaymentFilters.FIRSTNAME not in payments_x_conflicted[
                "filters"
            ] and
            PaymentFilters.FIRSTNAME_PARTIAL not in payments_x_conflicted[
                "filters"
            ]
        ) and (
            # Conflict first_name and OpenPayments middle_name
            (
                pd.notna(payments_x_conflicted["middle_name"])
                and pd.notna(payments_x_conflicted["conflict_first_name"])
                and (
                    payments_x_conflicted["middle_name"].lower()
                    == payments_x_conflicted["conflict_first_name"].lower()
                )
            )
            # Conflict middle_names and OpenPayments first_name
            or (
                (
                    pd.notna(payments_x_conflicted["conflict_middle_name_1"])
                    and pd.notna(payments_x_conflicted["first_name"])
                    and (
                        payments_x_conflicted["first_name"].lower()
                        == payments_x_conflicted["conflict_middle_name_1"].lower()
                    )
                ) or (
                    pd.notna(payments_x_conflicted["conflict_middle_name_2"])
                    and pd.notna(payments_x_conflicted["first_name"])
                    and (
                        payments_x_conflicted["first_name"].lower()
                        == payments_x_conflicted["conflict_middle_name_2"].lower()
                    )
                )
            )
        )

    @classmethod
    def filter_by_specialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Checks for specialty match between the payment and conflicted
        provider. If there is a match, it appends the filter to the
        filters list."""

        return (
            cls.payment_conflict_specialty_match(
                payment_specialtys=payments_x_conflicted["specialtys"],
                conflict_specialtys=payments_x_conflicted[
                    "conflict_specialtys"
                ],
            )
        )

    @classmethod
    def payment_conflict_specialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:
        """Checks if the specialtys exist and match."""

        return any(
            cls.specialty_str_matcher(
                payment_specialty=payment_specialty.specialty,
                conflict_specialty=conflict_specialty.specialty,
            ) for payment_specialty in payment_specialtys
            for conflict_specialty in conflict_specialtys
        )

    @staticmethod
    def specialty_str_matcher(
        payment_specialty: Union[str, None],
        conflict_specialty: Union[str, None],
    ) -> bool:
        """Checks if the specialtys exist and match."""

        payment_specialty = payment_specialty.lower() if pd.notna(
            payment_specialty
        ) else None
        conflict_specialty = conflict_specialty.lower() if pd.notna(
            conflict_specialty
        ) else None

        # If either specialty is None, return False
        if payment_specialty is None or conflict_specialty is None:
            return False
        # If both specialties are the same, return True
        elif payment_specialty == conflict_specialty:
            return True

        payment_specialty_strs = payment_specialty.split(" ")

        conflict_specialty_strs = conflict_specialty.split(" ")

        # Remove "medicine" from the specialty strings, as it is non-specific
        if "medicine" in payment_specialty_strs:
            payment_specialty_strs.remove("medicine")
        if "medicine" in conflict_specialty_strs:
            conflict_specialty_strs.remove("medicine")

        return any(
            payment_str in conflict_specialty_strs
            for payment_str in payment_specialty_strs
        ) or any(
            conflict_str in payment_specialty_strs
            for conflict_str in conflict_specialty_strs
        )

    @classmethod
    def filter_by_subspecialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by subspecialty."""

        return (
                cls.payment_conflict_subspecialty_match(
                    payment_specialtys=payments_x_conflicted["specialtys"],
                    conflict_specialtys=payments_x_conflicted[
                        "conflict_specialtys"
                    ],
                )
        )

    @classmethod
    def payment_conflict_subspecialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            cls.specialty_str_matcher(
                payment_specialty=payment_specialty.subspecialty,
                conflict_specialty=conflict_specialty.subspecialty,
            ) for payment_specialty in payment_specialtys
            for conflict_specialty in conflict_specialtys
        )

    @classmethod
    def filter_by_fullspecialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by full specialty."""

        return (
            cls.payment_conflict_full_specialty_match(
                payment_specialtys=payments_x_conflicted["specialtys"],
                conflict_specialtys=payments_x_conflicted[
                    "conflict_specialtys"
                ],
            )
        )

    @classmethod
    def payment_conflict_full_specialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            (
                cls.specialty_str_matcher(
                    payment_specialty=payment_specialty.specialty,
                    conflict_specialty=conflict_specialty.specialty,
                ) and cls.specialty_str_matcher(
                    payment_specialty=payment_specialty.subspecialty,
                    conflict_specialty=conflict_specialty.subspecialty,
                )
                for payment_specialty in payment_specialtys
                for conflict_specialty in conflict_specialtys
            )
        )

    @classmethod
    def filter_by_city(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by city."""

        return (
                cls.payment_conflict_city_match(
                    payment_citystates=payments_x_conflicted["citystates"],
                    conflict_citystates=payments_x_conflicted[
                        "conflict_citystates"
                    ],
                )
        )

    @classmethod
    def payment_conflict_city_match(
        cls,
        payment_citystates: Union[list[PaymentSpecialtys], None],
        conflict_citystates: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            citystate.city in [
                citystate.city for citystate in payment_citystates
                if (
                    pd.notna(citystate)
                    and pd.notna(
                        citystate.city
                    )
                )
            ] for citystate in [
                citystate for citystate in conflict_citystates
                if (
                    pd.notna(citystate)
                    and pd.notna(
                        citystate.city
                    )
                )
            ]
        )

    @classmethod
    def filter_by_state(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by state."""

        return (
                cls.payment_conflict_state_match(
                    payment_citystates=payments_x_conflicted["citystates"],
                    conflict_citystates=payments_x_conflicted[
                        "conflict_citystates"
                    ],
                )
        )

    @classmethod
    def payment_conflict_state_match(
        cls,
        payment_citystates: Union[list[PaymentSpecialtys], None],
        conflict_citystates: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            payment_citystate.state_matches(conflict_citystate.state)
            for payment_citystate in payment_citystates
            for conflict_citystate in conflict_citystates
            if (
                pd.notna(payment_citystate)
                and pd.notna(conflict_citystate)
                and pd.notna(payment_citystate.state)
                and pd.notna(conflict_citystate.state)
            )
        )

    @classmethod
    def filter_by_citystate(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by city and state."""

        return (
            cls.payment_conflict_citystate_match(
                payment_citystates=payments_x_conflicted["citystates"],
                conflict_citystates=payments_x_conflicted[
                    "conflict_citystates"
                ],
            )
        )

    @classmethod
    def payment_conflict_citystate_match(
        cls,
        payment_citystates: Union[list[PaymentSpecialtys], None],
        conflict_citystates: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            payment_citystate.citystate_matches(
                citystate=conflict_citystate
            )
            for payment_citystate in payment_citystates
            for conflict_citystate in conflict_citystates
            if (
                pd.notna(payment_citystate)
                and pd.notna(conflict_citystate)
            )
        )

    @classmethod
    def filter_by_middle_initial(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by middle initial."""

        return (
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
        )

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
    ) -> bool:
        """Filters by middle name."""

        return (
            cls.middlename_match(
                conflicted_middle_name_1=payments_x_conflicted[
                    "conflict_middle_name_1"
                    ],
                conflicted_middle_name_2=payments_x_conflicted[
                    "conflict_middle_name_2"
                ],
                payment_middle_name=payments_x_conflicted["middle_name"],
            )
        )

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
