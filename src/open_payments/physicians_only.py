import logging

import pandas as pd

from .choices import Credentials

logging.basicConfig(level=logging.INFO)


class PhysicianFilter:
    """Class method to filter unprocessed OpenPayments payments for MDs and DOs.
    Uses columns pertaining to physician credentials and specialties
    to filter the payments DataFrame.

    args:
        payments (pd.DataFrame): The DataFrame containing OpenPayments payments.

    returns:
        pd.DataFrame: A DataFrame containing only the payments for MDs and DOs.
    """

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
        """Filters the payments DataFrame for MDs and DOs
        by checking the specialty and credential columns.
        Returns rows that have either:
        1. A specialty of 'Allopathic & Osteopathic Physicians'
        2. A credential of 'Medical Doctor' or 'Doctor of Osteopathy'"""

        logging.info(
            "Filtering payments for MDs and DOs based on specialty and credential columns..."
        )

        return self.payments[self.physician_specialty() | self.physician_credential()]

    def physician_specialty(self) -> pd.Series:
        """Returns True for rows that have a specialty of
        'Allopathic & Osteopathic Physicians'."""

        return self.payments[self.get_specialty_filter_columns()].apply(
            lambda specialty_columns: (
                specialty_columns.astype(str)
                .str.contains("Allopathic & Osteopathic Physicians", case=False, na=False)
                .any()
            ),
            axis=1,
        )

    def get_specialty_filter_columns(self):
        """Returns MD/DO-specialty columns that are in the DF."""

        return [
            column for column in self.potential_specialty_columns if column in self.payments.columns
        ]

    def physician_credential(self) -> pd.Series:
        """Returns True for rows that have a credential of 'Medical Doctor' or 'Doctor of Osteopathy'."""

        return self.payments[self.get_credential_filter_columns()].apply(
            lambda credential_columns: any(
                credential
                in [
                    Credentials.MEDICAL_DOCTOR,
                    Credentials.DOCTOR_OF_OSTEOPATHY,
                ]
                for credential in credential_columns
            ),
            axis=1,
        )

    def get_credential_filter_columns(self):
        """Returns MD/DO-credential columns that are in the DF."""

        return [
            column
            for column in self.potential_credential_columns
            if column in self.payments.columns
        ]
