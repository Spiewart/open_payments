from typing import Literal, Union

import pandas as pd

from .helpers import get_file_suffix, open_payments_directory
from .read import ReadPayments


class PaymentTypes(ReadPayments):

    def __init__(
        self,
        **kwargs
    ):
        super().__init__(
            **kwargs
        )

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().general_columns
        cols.update({
            "Form_of_Payment_or_Transfer_of_Value": ("form", str),
            "Nature_of_Payment_or_Transfer_of_Value": ("nature", str),
        })
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().ownership_columns
        cols.update({
                "Terms_of_Interest": ("payment_type", str),
        })
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().research_columns

        cols.update(
            self.general_columns
        )
        return cols

    def create_payment_types_excel(self) -> None:
        data_directory = open_payments_directory()

        df = self.payment_types()

        file_suffix = get_file_suffix(self.years, self.payment_classes)

        file_name = f"payment_types_{file_suffix}.xlsx" if file_suffix else "payment_types.xlsx"

        with pd.ExcelWriter(
            f"{data_directory}/{file_name}",
            engine="openpyxl",
        ) as writer:
            df.to_excel(writer, sheet_name="payment_types")

            print("Successfully wrote types of payments to Excel.")

    def payment_types(self) -> pd.DataFrame:
        """Returns a DataFrame of unique types of payments from the OpenPayments datasets."""

        df = pd.DataFrame()

        for payment_class in self.payment_classes:
            types = getattr(self, f"get_types_of_{payment_class}_payments")()
            types.insert(1, "payment_class", payment_class, allow_duplicates=True)
            df = pd.concat([df, types])

        return df

    def get_types_of_general_payments(self) -> pd.DataFrame:
        if self.general_payments.empty:
            self.general_payments = self.read_payments_csvs(
                payment_class="general",
            )

        unique_nature_of_payments = self.general_payments["Nature_of_Payment_or_Transfer_of_Value"].unique()

        unique_nature_of_payments = pd.DataFrame(unique_nature_of_payments, columns=["Nature_of_Payment_or_Transfer_of_Value"])

        unique_nature_of_payments.rename(
            columns={"Nature_of_Payment_or_Transfer_of_Value": "payment_type"},
            inplace=True,
        )

        unique_nature_of_payments.dropna(inplace=True)

        unique_nature_of_payments.reset_index(drop=True, inplace=True)

        return unique_nature_of_payments

    def get_types_of_ownership_payments(self) -> pd.DataFrame:
        if self.ownership_payments.empty:
            self.ownership_payments = self.read_payments_csvs(
                payment_class="ownership",
            )

        unique_terms_of_interest = self.ownership_payments["Terms_of_Interest"].unique()

        unique_terms_of_interest = pd.DataFrame(unique_terms_of_interest, columns=["Terms_of_Interest"])

        unique_terms_of_interest.rename(
            columns={"Terms_of_Interest": "payment_type"},
            inplace=True,
        )

        unique_terms_of_interest.dropna(inplace=True)

        unique_terms_of_interest.reset_index(drop=True, inplace=True)

        return unique_terms_of_interest

    def get_types_of_research_payments(self) -> pd.DataFrame:
        if self.research_payments.empty:
            self.research_payments = self.read_payments_csvs(
                payment_class="research",
            )

        unique_forms_of_payment = self.research_payments["Form_of_Payment_or_Transfer_of_Value"].unique()

        unique_forms_of_payment = pd.DataFrame(unique_forms_of_payment, columns=["Form_of_Payment_or_Transfer_of_Value"])

        unique_forms_of_payment.rename(
            columns={"Form_of_Payment_or_Transfer_of_Value": "payment_type"},
            inplace=True,
        )

        unique_forms_of_payment.dropna(inplace=True)

        unique_forms_of_payment.reset_index(drop=True, inplace=True)

        return unique_forms_of_payment
