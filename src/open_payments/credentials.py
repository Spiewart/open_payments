import re
from typing import Type, Union

import pandas as pd

from .choices import Credentials, PaymentFilters
from .helpers import ColumnMixin, get_file_suffix, open_payments_directory
from .read import ReadPayments


class CredentialsMixin(ColumnMixin):
    """Mixin class for credentials."""

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols: dict[str, tuple[str, Union[Type[str], str]]] = super().general_columns
        cols.update({
                "Covered_Recipient_Primary_Type_1": ("credential_1", str),
                "Covered_Recipient_Primary_Type_2": ("credential_2", str),
                "Covered_Recipient_Primary_Type_3": ("credential_3", str),
                "Covered_Recipient_Primary_Type_4": ("credential_4", str),
                "Covered_Recipient_Primary_Type_5": ("credential_5", str),
                "Covered_Recipient_Primary_Type_6": ("credential_6", str),
        })
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols: dict[str, tuple[str, Union[Type[str], str]]] = super().ownership_columns
        cols.update({
                "Physician_Primary_Type": ("credential_1", str),
        })
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols: dict[str, tuple[str, Union[Type[str], str]]] = super().research_columns

        cols.update(
            self.general_columns
        )
        return cols


class PaymentCredentials(ReadPayments, CredentialsMixin):

    def create_unique_credentials_excel(self, path: Union[str, None] = None) -> None:
        path = open_payments_directory() if path is None else path

        unique_credentials = self.unique_credentials()

        file_suffix = get_file_suffix(self.years, self.payment_classes)

        with pd.ExcelWriter(
            f"{path}/unique_credentials{file_suffix}.xlsx",
            engine="openpyxl",
        ) as writer:
            unique_credentials.to_excel(writer, sheet_name="unique_credentials", index=False)

    def unique_credentials(self) -> pd.Series:
        """Returns a Series of unique credentials from OpeyPayments payment datasets."""

        self.general_payments = self.read_general_payments_csvs(
            usecols=self.general_columns.keys(),
            dtype={key: value[1] for key, value in self.general_columns.items()},
        )
        self.general_payments = self.update_payments("general")

        self.ownership_payments = self.read_ownership_payments_csvs(
            usecols=self.ownership_columns.keys(),
            dtype={key: value[1] for key, value in self.ownership_columns.items()},
        )
        self.ownership_payments = self.update_payments("ownership")

        self.research_payments = self.read_research_payments_csvs(
            usecols=self.research_columns.keys(),
            dtype={key: value[1] for key, value in self.research_columns.items()},
        )
        self.research_payments = self.update_payments("research")

        all_payments = pd.concat([
            self.general_payments, self.ownership_payments, self.research_payments
        ])

        all_credentials = self.get_all_credentials(all_payments)

        all_credentials = all_credentials.drop_duplicates()

        all_credentials = all_credentials.dropna()

        return all_credentials.reset_index(drop=True)

    @staticmethod
    def get_all_credentials(df: pd.DataFrame) -> pd.Series:
        """Method that returns all unique credentials from the DataFrame."""

        credentials = pd.concat([
            df["credential_1"],
            df["credential_2"],
            df["credential_3"],
            df["credential_4"],
            df["credential_5"],
            df["credential_6"],
        ])

        credentials.rename("credential", inplace=True)

        return credentials

    @classmethod
    def credentials(cls, payments: pd.DataFrame) -> pd.DataFrame:
        """Method that combines the credentials into a Series."""

        payments.insert(
            1,
            "credentials",
            payments.apply(cls.create_credentials, axis=1)
        )

        payments = cls.drop_individual_credentials(payments)

        return payments

    @staticmethod
    def create_credentials(payment: pd.Series) -> pd.Series:
        """Aggregates the credentials into a Series."""

        credentials = payment[
            [
                "credential_1", "credential_2", "credential_3",
                "credential_4", "credential_5", "credential_6"
            ]
        ].dropna()

        credentials = credentials.unique()

        credentials = [Credentials(cred) for cred in credentials]

        return credentials

    @staticmethod
    def drop_individual_credentials(payments: pd.DataFrame) -> pd.DataFrame:

        payments = payments.drop([
            "credential_1", "credential_2", "credential_3",
            "credential_4", "credential_5", "credential_6"
        ], axis=1)

        return payments

    def update_ownership_payments(self) -> pd.DataFrame:
        """Overwritten to add credential_2-6 to the DataFrame, as they
        won't be present after renaming pre-existing columns."""

        self.ownership_payments["credential_2"] = None
        self.ownership_payments["credential_3"] = None
        self.ownership_payments["credential_4"] = None
        self.ownership_payments["credential_5"] = None
        self.ownership_payments["credential_6"] = None
        self.ownership_payments = super().update_ownership_payments()

        self.ownership_payments["credential_7"] = None
        return self.ownership_payments


def convert_credentials(credentials: str) -> Union[list[Credentials], None]:
    """Convert a string representation of a list of Credentials objects
    to a list of Credentials objects."""

    if not credentials:
        return None

    converted = []

    credentials = re.findall(r": '(.*?)'", credentials)

    for credential in credentials:
        credential = Credentials(credential)
        converted.append(credential)

    return converted


def unique_credentials() -> None:
    """Creates an Excel file containing unique credentials."""

    PaymentCredentials(nrows=None, years=2023).create_unique_credentials_excel()


class PaymentIDsCredentials(CredentialsMixin):
    """Filters OpenPayments payments by credentials."""

    @property
    def filters(self) -> list["PaymentFilters"]:
        """Overwritten to add CREDENTIAL PaymentFilter to
        the filters property."""

        filters: list[PaymentFilters] = super().filters
        filters.append(PaymentFilters.CREDENTIAL)
        return filters

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

    def convert_merged_dtypes(
        self,
        merged: pd.DataFrame,
    ) -> pd.DataFrame:
        """Updates  payments and conflicteds columns into lists after
        they are loaded as strs in CSVs and Excel files."""

        merged: pd.DataFrame = super().convert_merged_dtypes(merged)

        merged["credentials"] = merged["credentials"].apply(
            lambda x: convert_credentials(x) if isinstance(x, str) else x
        )

        merged["conflict_credentials"] = merged["conflict_credentials"].apply(
            lambda x: convert_credentials(x) if isinstance(x, str) else x
        )

        return merged