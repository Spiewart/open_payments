import pandas as pd

from .choices import Credentials


class PhysicianFilter:
    def __init__(
        self,
        payments: pd.DataFrame,
    ):
        self.payments = payments

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
        "Covered_Recipient_Specialty_1",
        "Covered_Recipient_Specialty_2",
        "Covered_Recipient_Specialty_3",
        "Covered_Recipient_Specialty_4",
        "Covered_Recipient_Specialty_5",
        "Covered_Recipient_Specialty_6",
        "Physician_Specialty",
    ]

    def filter(self) -> pd.DataFrame:
        """Method that filters unprocessed OpenPayments data
        for payments that are made to physicians only."""

        return self.payments[
            (self.physician_specialty() | self.specialty_null())
            & (self.physician_credential() | self.credential_null())
        ]

    def physician_specialty(self) -> pd.Series:
        """Checks the payments DataFrame's specialty columns for
        'Allopathic & Osteopathic Physicians' and returns a Series
        of boolean values indicating if so. This is used to filter
        the DataFrame for physicians only."""

        return self.payments[self.get_specialty_filter_columns()].apply(
            lambda specialty_columns: specialty_columns.str.contains(
                "Allopathic & Osteopathic Physicians",
                case=False,
                na=False
            ).any(),
            axis=1
        )

    def specialty_null(self) -> pd.Series:
        """Method that returns True if all of the specialty columns are null."""
        return self.payments[
            self.get_specialty_filter_columns()
        ].isnull().all(axis=1)

    def credential_null(self) -> pd.Series:
        """Method that returns True if all of the credential columns are null."""
        return self.payments[
            self.get_credential_filter_columns()
        ].isnull().all(axis=1)

    def get_credential_filter_columns(self):
        """Method that returns the credential columns to filter by."""

        return [
            column for column in self.potential_credential_columns
            if column in self.payments.columns
        ]

    def physician_credential(self) -> pd.Series:
        """Method that checks if the row contains a physician credential."""

        return self.payments[self.get_credential_filter_columns()].apply(
            lambda credential_columns: any(
                credential in [
                    Credentials.MEDICAL_DOCTOR,
                    Credentials.DOCTOR_OF_OSTEOPATHY,
                ] for credential in credential_columns
            ),
            axis=1
        )

    def get_specialty_filter_columns(self):
        """Method that returns the specialty columns to filter by."""

        return [
            column for column in self.potential_specialty_columns
            if column in self.payments.columns
        ]
