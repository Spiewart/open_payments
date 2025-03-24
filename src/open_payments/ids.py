from typing import Literal, Union

import pandas as pd

from .credentials import PaymentCredentials
from .helpers import get_file_suffix, open_payments_directory
from .read import ReadPayments
from .specialtys import PaymentSpecialtys


class PaymentIDs(PaymentCredentials, PaymentSpecialtys, ReadPayments):

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

        # Remove duplicates again because there may be duplicate IDs between
        # the three different payment types.
        all_payments = self.remove_duplicate_ids(all_payments)

        return all_payments

    def unique_MD_DO_payment_ids(
        self,
        unique_payment_ids: pd.DataFrame,
    ) -> pd.DataFrame:

        MD_DO_ids = self.filter_MD_DO(unique_payment_ids)

        return MD_DO_ids

    def update_payments(
        self,
        payment_class: Literal["general", "ownership", "research"],
    ) -> pd.DataFrame:
        """Removes duplicate IDs and renames columns for the payment class
        DataFrame."""
        payments: pd.DataFrame = getattr(self, f"{payment_class}_payments")
        payments = super().update_payments(payment_class)
        payments = self.remove_duplicate_ids(payments)
        return payments

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
                "Recipient_City": ("city", str),
                "Recipient_State": ("state", str),
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
                "Physician_Name_Suffix": ("name_suffix", str),
                "Physician_NPI": ("npi", "Int64"),
                "Recipient_City": ("city", str),
                "Recipient_State": ("state", str),
        })
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().research_columns

        cols.update(
            self.general_columns
        )
        return cols