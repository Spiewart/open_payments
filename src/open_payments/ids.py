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

        for payment_class in self.payment_classes:
            setattr(
                self,
                f"{payment_class}_payments",getattr(self, f"read_{payment_class}_payments_csvs")(
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
        payments: pd.DataFrame = getattr(self, f"{payment_class}_payments")
        payments = super().update_payments(payment_class)
        payments = self.post_update_payments_mod(payments)
        return payments

    def post_update_payments_mod(self, payments: pd.DataFrame) -> pd.DataFrame:
        """Method that is called after the update_payments method."""

        payments = self.remove_duplicate_ids(payments)
        payments.insert(1, "specialtys", payments.apply(PaymentSpecialtys.specialtys, axis=1))
        payments = self.drop_individual_specialtys(payments)
        payments = self.combine_credentials(payments)
        return payments

    def update_ownership_payments(self) -> pd.DataFrame:
        """Updates ownership payments and returns the updated DataFrame."""
        self.ownership_payments = super().update_ownership_payments()
        self.ownership_payments = self.post_update_payments_mod(self.ownership_payments)
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


class ConflictedPaymentIDs(PaymentIDs):

    def __init__(
        self,
        conflicteds: pd.DataFrame,
        payment_ids: Union[pd.DataFrame, None],
        *args,
        **kwargs,
    ):
        """Overwritten to add a conflicteds argument / attribute.
        Conflicteds is a DataFrame with the following columns:
        -first_name: str
        -last_name: str
        -middle_initial_1: str
        -middle_initial_2: str
        -middle_name_1: str
        -middle_name_2: str
        -credentials: array[str]
        -specialtys: array[str]
        -citystates: array[str]
        """
        super().__init__(*args, **kwargs)
        self.conflicteds = conflicteds
        self.payment_ids = payment_ids

    def conflicteds_payments_ids(self) -> pd.DataFrame:
        """Method that returns a DataFrame of payment IDs for conflicteds."""

        if not self.payment_ids:
            self.payment_ids = self.unique_payment_ids()