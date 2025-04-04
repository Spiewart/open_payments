from enum import StrEnum
from itertools import combinations
from typing import Literal, Union

import pandas as pd

from .citystates import PaymentCityStates
from .credentials import PaymentCredentials
from .helpers import get_file_suffix, open_payments_directory, str_in_str
from .read import ReadPayments
from .specialtys import PaymentSpecialtys


class PaymentIDs(
    PaymentSpecialtys,
    PaymentCredentials,
    PaymentCityStates,
    ReadPayments,
):

    def create_unique_MD_DO_payment_ids_excel(self, path: Union[str, None] = None) -> None:
        path = open_payments_directory() if path is None else path

        unique_ids = self.unique_MD_DO_payment_ids(self.unique_payment_ids())

        file_suffix = get_file_suffix(self.years, self.payment_classes)

        with pd.ExcelWriter(
            f"{path}/unique_MD_DO_payment_ids{file_suffix}.xlsx",
            engine="openpyxl",
        ) as writer:
            unique_ids.to_excel(writer, sheet_name="unique_ids")

    def unique_payment_ids(self) -> pd.DataFrame:
        """Returns a DataFrame of rows from OpeyPayments payment datasets that
        have a unique provider ID (Covered_Recipient_Profile_ID)."""

        for payment_class in self.payment_classes:

            setattr(
                self,
                f"{payment_class}_payments", getattr(self, f"read_{payment_class}_payments_csvs")(
                    usecols=getattr(self, f"{payment_class}_columns").keys(),
                    dtype={key: value[1] for key, value in getattr(self, f"{payment_class}_columns").items()},
                )
            )
            if hasattr(self, f"update_{payment_class}_payments"):
                setattr(
                    self,
                    f"{payment_class}_payments",
                    getattr(self, f"update_{payment_class}_payments")()
                )
            else:
                setattr(
                    self,
                    f"{payment_class}_payments",
                    getattr(self, "update_payments")(payment_class)
                )

        all_payments = pd.concat([
            self.general_payments, self.ownership_payments, self.research_payments
        ])

        # Remove duplicates again because there may be duplicate IDs between
        # the three different payment types.
        all_payments = self.remove_duplicate_ids(all_payments)

        all_payments.drop("payment_class", axis=1, inplace=True)

        return all_payments

    def unique_MD_DO_payment_ids(
        self,
        unique_payment_ids: Union[pd.DataFrame, None] = None,
    ) -> pd.DataFrame:
        """Returns a DataFrame of rows from OpeyPayments payment datasets that
        have a unique provider ID (Covered_Recipient_Profile_ID) and are either
        MDs or DOs."""

        if unique_payment_ids is None:
            unique_payment_ids = self.unique_payment_ids()

        MD_DO_ids = self.filter_MD_DO(unique_payment_ids)

        return MD_DO_ids

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

        df = df.drop_duplicates(
            subset="profile_id"
        )

        return df

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().general_columns
        cols.update({
                "Covered_Recipient_Profile_ID": ("profile_id", "Int64"),
                "Covered_Recipient_NPI": ("npi", "Int64"),
                "Covered_Recipient_Last_Name": ("last_name", str),
                "Covered_Recipient_First_Name": ("first_name", str),
                "Covered_Recipient_Middle_Name": ("middle_name", str),
        })
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().ownership_columns
        cols.update({
                "Physician_Profile_ID": ("profile_id", "Int64"),
                "Physician_First_Name": ("first_name", str),
                "Physician_Last_Name": ("last_name", str),
                "Physician_Middle_Name": ("middle_name", str),
                "Physician_NPI": ("npi", "Int64"),
        })
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().research_columns

        cols.update(
            self.general_columns
        )
        return cols


class PaymentIDFilters(StrEnum):
    """Enum class for the various stages at which a conflicted provider
    can be identified from the list of unique OpenPayment IDs."""

    LASTNAME = "LASTNAME"
    FIRSTNAME = "FIRSTNAME"
    CREDENTIAL = "CREDENTIAL"
    SPECIALTY = "SPECIALTY"
    SUBSPECIALTY = "SUBSPECIALTY"
    FULLSPECIALTY = "FULLSPECIALTY"  # Matches specialty and subspecialty
    MIDDLE_INITIAL = "MIDDLE_INITIAL"
    CITY = "CITY"
    STATE = "STATE"


class Unmatcheds(StrEnum):
    """Enum class to represent the various ways a conflicted can
    remain unmatched throughout / after the filtering process."""

    NOLASTNAME = "NOLASTNAME"  # No matches for the last name in OpenPayments
    UNFILTERABLE = "UNFILTERABLE"  # Multiple OpenPayments IDs that can't be matched


def get_list_of_combinations(input_list: list[PaymentIDFilters]) -> list[list[PaymentIDFilters]]:
    """
    Generates all combinations of PaymentIDFilters from the input list.

    Args:
        input_list: The list PaymetnIDFilters to generate combinations from.

    Returns:
        A list of lists of PaymentIDFilters, where each list is a combination.
    """
    all_combinations = []
    for r in range(1, len(input_list) + 1):
        for combination in combinations(input_list, r):
            all_combinations.append(list(combination))
    return all_combinations


class ConflictedPaymentIDs(PaymentIDs):

    def __init__(
        self,
        conflicteds: pd.DataFrame,
        payment_ids: Union[pd.DataFrame, None],
        *args,
        **kwargs,
    ):
        """Overwritten to add a conflicteds argument / attribute.
        conflicteds is a DataFrame with the following columns:
        -provider_pk: Int64
        -first_name: str
        -last_name: str
        -middle_initial_1: str
        -middle_initial_2: str
        -middle_name_1: str
        -middle_name_2: str
        -credentials: array[Credentials]
        -specialtys: array[Specialtys]
        -citystates: array[str]

        payment_ids is a DataFrame with the following columns:
        -profile_id: Int64
        -npi: Int64
        -first_name: str
        -middle_name: str
        -last_name: str
        -specialtys: array[Specialtys]
        -credentials: array[Credentials]
        -city: str
        -states: array[str]
        """
        super().__init__(*args, **kwargs)
        self.conflicteds = conflicteds

        self.conflicteds = self.add_conflict_prefix(self.conflicteds)
        self.num_conflicteds = self.conflicteds["conflict_provider_pk"].nunique()
        self.payment_ids = payment_ids
        self.unmatched = pd.DataFrame()
        self.unique_ids = pd.DataFrame()

    @staticmethod
    def add_conflict_prefix(
        conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Adds the conflict_ prefix to each
        conflicteds column other than last_name."""

        conflicteds = conflicteds.rename(
            columns={
                col: f"conflict_{col}" for col in conflicteds.columns
            }
        )
        return conflicteds

    def conflicteds_payments_ids(
        self,
        filters: list[PaymentIDFilters] = None,
    ) -> pd.DataFrame:
        """Method that returns a DataFrame of payment IDs for conflicteds."""

        if filters is None:
            filters = [
                PaymentIDFilters.LASTNAME,
                PaymentIDFilters.FIRSTNAME,
                PaymentIDFilters.CREDENTIAL,
                PaymentIDFilters.SPECIALTY,
                PaymentIDFilters.SUBSPECIALTY,
                PaymentIDFilters.FULLSPECIALTY,
            ]

        if self.payment_ids is None:
            self.payment_ids = self.unique_payment_ids()

        self.payment_ids = self.merge_by_last_name(
            payment_ids=self.payment_ids,
            conflicteds=self.conflicteds,
        )

        # Filter out conflicteds that have no last name match
        # in OpenPayments
        self.update_unmatched(
            unmatcheds=self.payment_ids[
                self.payment_ids["profile_id"].isna()
            ],
            unmatched=Unmatcheds.NOLASTNAME,
        )

        filter_combos = get_list_of_combinations(filters)

        for filter_combo in filter_combos:
            if self.payment_ids.empty:
                break
            self.filter_and_update_unique_ids(
                payment_ids_x_conflicteds=self.payment_ids,
                id_filters=filter_combo,
            )
            self.check_sanity()

        # TODO: Check all the LASTNAME matches for secondary matches with other columns
        return self.payment_ids

    @classmethod
    def merge_by_last_name(
        cls,
        payment_ids: pd.DataFrame,
        conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merges conflicteds with payment_ids DataFrames
        by last name. Returns tuple of DataFrames:
        - conflicteds x payment_ids matched by last name
        - conflicteds that did not match by last name"""

        return pd.merge(
            payment_ids,
            conflicteds,
            left_on=["last_name"],
            right_on=["conflict_last_name"],
            how="outer",
        )

    def filter_and_update_unique_ids(
        self,
        payment_ids_x_conflicteds: pd.DataFrame,
        id_filters: list[PaymentIDFilters],
    ) -> None:
        """Filters the payment_ids x conflicteds DataFrame
        by the given filters and updates the unique_ids DataFrame
        with the new unique IDs."""

        filtered_ids = self.id_filter(
            payment_ids_x_conflicteds=payment_ids_x_conflicteds,
            id_filters=id_filters,
        )

        self.extract_and_update_unique_ids(
            filtered_ids=filtered_ids,
            id_filters=id_filters,
        )

    def id_filter(
        self,
        payment_ids_x_conflicteds: pd.DataFrame,
        id_filters: list[PaymentIDFilters],
    ) -> pd.DataFrame:
        for id_filter in id_filters:
            payment_ids_x_conflicteds = getattr(
                self,
                f"filter_by_{id_filter.lower()}",
            )(
                payment_ids_x_conflicteds=payment_ids_x_conflicteds,
            ) if not payment_ids_x_conflicteds.empty else payment_ids_x_conflicteds

        return payment_ids_x_conflicteds

    def extract_and_update_unique_ids(
        self,
        filtered_ids: pd.DataFrame,
        id_filters: list[PaymentIDFilters],
    ) -> Union[pd.DataFrame, None]:
        """Extracts unique IDs from the payments x conflicteds DataFrame
        and updates the unique_ids DataFrame with the new unique IDs."""

        if not filtered_ids.empty:
            new_unique_ids = self.extract_unique_ids(
                payments_x_conflicteds=filtered_ids,
            )
            self.update_ids(
                new_unique_ids=new_unique_ids,
                filters=id_filters,
            )
            return filtered_ids
        else:
            return None

    @classmethod
    def extract_unique_ids(
        cls,
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Extracts unique IDs from the payments x conflicteds DataFrame
        and returns a DataFrame with the unique IDs."""

        unique_ids = pd.DataFrame()

        for provider_pk in payments_x_conflicteds["conflict_provider_pk"].unique():
            provider_rows = payments_x_conflicteds[
                payments_x_conflicteds["conflict_provider_pk"] == provider_pk
            ]

            if len(provider_rows) == 1 and provider_rows["profile_id"].notna().any():
                unique_ids = pd.concat([unique_ids, provider_rows])

        return unique_ids

    def update_ids(
        self,
        new_unique_ids: pd.DataFrame,
        filters: list[PaymentIDFilters],
    ) -> None:
        """Extracts unique provider_pks from the payment_ids DataFrame
        and adds them to the unique_ids DataFrame."""

        # https://stackoverflow.com/questions/69799736/join-an-array-to-every-row-in-the-pandas-dataframe
        new_unique_ids["filters"] = filters * len(new_unique_ids)

        self.unique_ids = pd.concat([self.unique_ids, new_unique_ids])

        if not new_unique_ids.empty:
            self.payment_ids = self.payment_ids.drop(
                self.payment_ids[
                    self.payment_ids["conflict_provider_pk"].isin(new_unique_ids["conflict_provider_pk"])
                ].index
            )

    def update_unmatched(
        self,
        unmatcheds: pd.DataFrame,
        unmatched: Unmatcheds,
    ) -> None:

        unmatcheds = self.conflicteds[
            self.conflicteds["conflict_provider_pk"].isin(unmatcheds["conflict_provider_pk"])
        ]

        unmatcheds = self.remove_conflict_prefix(unmatcheds)

        unmatcheds["reason"] = unmatched

        self.unmatched = pd.concat(
            [self.unmatched, unmatcheds]
        )

        self.payment_ids = self.payment_ids[
            self.payment_ids["profile_id"].notna()
        ]

    @staticmethod
    def remove_conflict_prefix(
        conflicteds_x_payment_ids: pd.DataFrame,
    ) -> pd.DataFrame:
        """Removes the conflict_ prefix from the columns
        of the conflicteds x payment_ids DataFrame."""

        conflicteds_x_payment_ids = conflicteds_x_payment_ids.rename(
            columns={
                col: col.replace("conflict_", "")
                for col in conflicteds_x_payment_ids.columns
                if col.startswith("conflict_")
            }
        )

        return conflicteds_x_payment_ids

    def check_sanity(self) -> None:
        # Sanity check to ensure no loss of conflicteds

        assert (
            self.num_conflicteds
            ==
            (
                (self.unmatched["provider_pk"].nunique(dropna=True) if not self.unmatched.empty else 0)
                + (len(self.unique_ids) if not self.unique_ids.empty else 0)
                + (self.payment_ids["conflict_provider_pk"].nunique(dropna=True) if not self.payment_ids.empty else 0)
            )
        )

    @classmethod
    def filter_by_lastname(
        cls,
        payment_ids_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a payment_ids DataFrame merged with a
        conflicteds DataFrame by removing rows that
        do not have a last name match."""

        return payment_ids_x_conflicteds[payment_ids_x_conflicteds.apply(
            lambda row: (
                pd.notna(row["last_name"])
                and pd.notna(row["conflict_last_name"])
                and row["last_name"] == row["conflict_last_name"]
            ),
            axis=1,
        )]

    @classmethod
    def filter_by_credential(
        cls,
        payment_ids_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a payment_ids DataFrame merged with a
        conflicteds DataFrame by removing rows that
        do not have a credential match."""

        return payment_ids_x_conflicteds[payment_ids_x_conflicteds.apply(
            lambda row: (
                pd.notna(row["credentials"])
                and pd.notna(row["conflict_credentials"])
                and any(
                    cred in row["credentials"]
                    for cred in row["conflict_credentials"]
                )
            ),
            axis=1,
        )]

    @classmethod
    def filter_by_firstname(
        cls,
        payment_ids_x_conflicteds: pd.DataFrame,
        strict: bool = False,
    ) -> pd.DataFrame:
        """Filters a payment_ids DataFrame merged with a
        conflicteds DataFrame by removing rows that
        do not have a first name match."""

        return payment_ids_x_conflicteds[payment_ids_x_conflicteds.apply(
            lambda row: (
                pd.notna(row["first_name"])
                and pd.notna(row["conflict_first_name"])
                and row["first_name"] == row["conflict_first_name"] if strict else (
                    pd.notna(row["first_name"])
                    and pd.notna(row["conflict_first_name"])
                    and (
                        str_in_str(row["first_name"], row["conflict_first_name"])
                        or str_in_str(row["conflict_first_name"], row["first_name"])
                    )
                )
            ),
            axis=1,
        )]

    @classmethod
    def filter_by_specialty(
        cls,
        payment_ids_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters by specialty."""

        return payment_ids_x_conflicteds[payment_ids_x_conflicteds.apply(
            lambda row: (
                cls.payment_conflict_specialty_match(
                    payment_specialtys=row["specialtys"],
                    conflict_specialtys=row["conflict_specialtys"],
                )
            ),
            axis=1,
        )]

    @staticmethod
    def payment_conflict_specialty_match(
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:
        """Checks if there is a specialty conflict."""

        return any(
            spec.specialty in [
                spec.specialty for spec in payment_specialtys if pd.notna(spec)
            ] for spec in [
                spec for spec in conflict_specialtys if pd.notna(spec)
            ]
        ) if pd.notna(payment_specialtys) and pd.notna(conflict_specialtys) else False

    @classmethod
    def filter_by_subspecialty(
        cls,
        payment_ids_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters by subspecialty."""

        return payment_ids_x_conflicteds[payment_ids_x_conflicteds.apply(
            lambda row: (
                cls.payment_conflict_subspecialty_match(
                    payment_specialtys=row["specialtys"],
                    conflict_specialtys=row["conflict_specialtys"],
                )
            ),
            axis=1,
        )]

    @staticmethod
    def payment_conflict_subspecialty_match(
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:
        """Checks if there is a specialty conflict."""

        return any(
            spec.subspecialty in [
                spec.subspecialty for spec in payment_specialtys if pd.notna(spec)
            ] for spec in [
                spec for spec in conflict_specialtys if pd.notna(spec)
            ]
        ) if pd.notna(payment_specialtys) and pd.notna(conflict_specialtys) else False

    @classmethod
    def filter_by_fullspecialty(
        cls,
        payment_ids_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters by full specialty."""

        return payment_ids_x_conflicteds[payment_ids_x_conflicteds.apply(
            lambda row: (
                cls.payment_conflict_full_specialty_match(
                    payment_specialtys=row["specialtys"],
                    conflict_specialtys=row["conflict_specialtys"],
                )
            ),
            axis=1,
        )]

    @staticmethod
    def payment_conflict_full_specialty_match(
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:
        """Checks if there is a specialty conflict."""

        return any(
            spec in [
                spec for spec in payment_specialtys if pd.notna(spec)
            ] for spec in [
                spec for spec in conflict_specialtys if pd.notna(spec)
            ]
        ) if pd.notna(payment_specialtys) and pd.notna(conflict_specialtys) else False

    @classmethod
    def filter_by_city(
        cls,
        payment_ids_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters by city."""

        return payment_ids_x_conflicteds[payment_ids_x_conflicteds.apply(
            lambda row: (
                pd.notna(row["city"])
                and pd.notna(row["conflict_city"])
                and row["city"] == row["conflict_city"]
            ),
            axis=1,
        )]