from typing import Type, Union

import pandas as pd

from .choices import Credentials
from .read import ReadPayments


class ReadPaymentsPhysicians(ReadPayments):

    @property
    def general_columns(self) -> dict[
        str, tuple[Union[str, None], Union[Type[str], str]]
    ]:
        cols = super().general_columns
        cols.update({
            "Covered_Recipient_Primary_Type_1": ("credential_1", str),
            "Covered_Recipient_Primary_Type_2": ("credential_2", str),
            "Covered_Recipient_Primary_Type_3": ("credential_3", str),
            "Covered_Recipient_Primary_Type_4": ("credential_4", str),
            "Covered_Recipient_Primary_Type_5": ("credential_5", str),
            "Covered_Recipient_Primary_Type_6": ("credential_6", str),
            "Covered_Recipient_Specialty_1": ("specialty_1", str),
            "Covered_Recipient_Specialty_2": ("specialty_2", str),
            "Covered_Recipient_Specialty_3": ("specialty_3", str),
            "Covered_Recipient_Specialty_4": ("specialty_4", str),
            "Covered_Recipient_Specialty_5": ("specialty_5", str),
            "Covered_Recipient_Specialty_6": ("specialty_6", str),
        })
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:
        cols = super().ownership_columns
        cols.update({
            "Physician_Primary_Type": ("credential_1", str),
            "Physician_Specialty": ("specialty_1", str),
        })
        return cols

    potential_credential_columns = [
        "Covered_Recipient_Primary_Type_1",
        "Covered_Recipient_Primary_Type_2",
        "Covered_Recipient_Primary_Type_3",
        "Covered_Recipient_Primary_Type_4",
        "Covered_Recipient_Primary_Type_5",
        "Covered_Recipient_Primary_Type_6",
        "Physician_Primary_Type",
    ]

    potential_specialty_columns = [
        "Covered_Recipient_Specialty_1", str,
        "Covered_Recipient_Specialty_2", str,
        "Covered_Recipient_Specialty_3", str,
        "Covered_Recipient_Specialty_4", str,
        "Covered_Recipient_Specialty_5", str,
        "Covered_Recipient_Specialty_6", str,
        "Physician_Specialty", str,
    ]

    def filter_payment_chunk(self, payment_chunk: pd.DataFrame) -> pd.DataFrame:
        chunk = super().filter_payment_chunk(payment_chunk)
        print("for physicians only...")
        chunk = self.filter(chunk)
        return chunk

    @classmethod
    def filter(cls, payments: pd.DataFrame) -> pd.DataFrame:
        """Method that filters unprocessed OpenPayments data
        for payments that are made to physicians only."""

        return payments[
            (cls.physician_specialty(payments) | cls.specialty_null(payments))
            & (cls.physician_credential(payments) | cls.credential_null(payments))
        ]

    @classmethod
    def physician_specialty(cls, payments: pd.DataFrame) -> pd.Series:
        """Checks the payments DataFrame's specialty columns for
        'Allopathic & Osteopathic Physicians' and returns a Series
        of boolean values indicating if so. This is used to filter
        the DataFrame for physicians only."""

        return payments[cls.get_specialty_filter_columns(payments)].apply(
            lambda specialty_columns: specialty_columns.str.contains(
                "Allopathic & Osteopathic Physicians",
                case=False,
                na=False,
                regex=False,
            ).any(),
            axis=1
        )

    @classmethod
    def specialty_null(cls, payments: pd.DataFrame) -> pd.Series:
        """Method that returns True if all of the specialty columns are null."""
        return payments[cls.get_specialty_filter_columns(payments)].isnull().all(axis=1)

    @classmethod
    def credential_null(cls, payments: pd.DataFrame) -> pd.Series:
        """Method that returns True if all of the credential columns are null."""
        return payments[cls.get_credential_filter_columns(payments)].isnull().all(axis=1)

    @classmethod
    def get_credential_filter_columns(cls, payments: pd.DataFrame) -> list[str]:
        """Method that returns the credential columns to filter by."""

        return [
            column for column in cls.potential_credential_columns
            if column in payments.columns
        ]

    @classmethod
    def physician_credential(cls, payments: pd.DataFrame) -> pd.Series:
        """Method that checks if the row contains a physician credential."""

        return payments[cls.get_credential_filter_columns(payments)].apply(
            lambda credential_columns: any(
                credential in [
                    Credentials.MEDICAL_DOCTOR,
                    Credentials.DOCTOR_OF_OSTEOPATHY,
                ] for credential in credential_columns
            ),
            axis=1
        )

    @classmethod
    def get_specialty_filter_columns(cls, payments: pd.DataFrame) -> list[str]:
        """Method that returns the specialty columns to filter by."""

        return [
            column for column in cls.potential_specialty_columns
            if column in payments.columns
        ]
