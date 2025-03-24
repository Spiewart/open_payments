
import pandas as pd
from typing import Union

from .helpers import get_file_suffix, open_payments_directory
from .read import ReadPayments


class PaymentCredentials(ReadPayments):

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

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().general_columns
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
    def ownership_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().ownership_columns
        cols.update({
                "Physician_Primary_Type": ("credential_1", str),
        })
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().research_columns

        cols.update(
            self.general_columns
        )
        return cols

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

    @staticmethod
    def filter_MD_DO(df: pd.DataFrame) -> pd.DataFrame:
        """Method that filters the DataFrame by MD and DO credentials."""

        def credential_match(credential: Union[str, None]) -> bool:
            return pd.notna(credential) and (
                credential == "Medical Doctor" or credential == "Doctor of Osteopathy"
            )

        df = df[
            df["credential_1"].apply(credential_match)
            | df["credential_2"].apply(credential_match)
            | df["credential_3"].apply(credential_match)
            | df["credential_4"].apply(credential_match)
            | df["credential_5"].apply(credential_match)
            | df["credential_6"].apply(credential_match)
        ]
        return df

    @staticmethod
    def credentials(payment: pd.Series) -> pd.Series:
        """Aggregates the credentials into a Series."""

        credentials = payment[
            [
                "credential_1", "credential_2", "credential_3",
                "credential_4", "credential_5", "credential_6"
            ]
        ].dropna()

        credentials = credentials.unique()

        return credentials