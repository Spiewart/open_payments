from typing import Literal, Union

import pandas as pd

from .read import ReadPayments


class Payments(ReadPayments):

    def __init__(
        self,
        years: Union[
            list[Literal[2020, 2021, 2022, 2023]],
            Literal[2020, 2021, 2022, 2023],
        ] = None,
        payment_classes: Union[
            list[Literal["general", "ownership", "research"]],
            Literal["general", "ownership", "research"],
            None,
        ] = None,
        payments_folder: Union[str, None] = None,
        nrows: Union[int, None] = 100,
        general_payments: pd.DataFrame = None,
        ownership_payments: pd.DataFrame = None,
        research_payments: pd.DataFrame = None,
    ):
        super().__init__(
            years=years,
            payment_classes=payment_classes,
            payments_folder=payments_folder,
            nrows=nrows,
            general_payments=general_payments,
            ownership_payments=ownership_payments,
            research_payments=research_payments,
        )

    def get_all_payments(self) -> pd.DataFrame:
        """Reads CSVs for each type of payment (general, ownership, research) into DataFrames,
        renames columns such that they match, merges them with the conflicteds_with_payments DataFrame,
        and concatenates them into a single DataFrame."""

        self.read_general_payments_csvs(
            usecols=self.general_columns.keys(),
            dtype={key: value[1] for key, value in self.general_columns.items()}
        )
        self.read_ownership_payments_csvs(
            usecols=self.ownership_columns.keys(),
            dtype={key: value[1] for key, value in self.ownership_columns.items()}
        )
        self.read_research_payments_csvs(
            usecols=self.research_columns.keys(),
            dtype={key: value[1] for key, value in self.research_columns.items()}
        )
        self.general_payments = self.update_payments("general")
        self.ownership_payments = self.update_ownership_payments()
        self.research_payments = self.update_payments("research")

        all_payments = pd.concat(
            [self.general_payments, self.ownership_payments, self.research_payments],
            sort=False,
            axis=0,
            ignore_index=True
        )

        return all_payments

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[str, int]]]:
        """Returns columns of interest and a tuple of the column's rename
        and dtype for reading general payments."""

        cols = super().general_columns

        cols.update(
            {
                "Covered_Recipient_Profile_ID": ("profile_id", "Int64"),
                "Form_of_Payment_or_Transfer_of_Value": ("payment_type", str),
                "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name": ("submitting_entity", str),
                "Total_Amount_of_Payment_USDollars": ("payment_amount", "Float64"),
                "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name": ("payment_entity", str),
                "Nature_of_Payment_or_Transfer_of_Value": ("payment_type", str),
                "Record_ID": ("payment_id", "Int64"),
            }
        )

        return cols

    def update_payments(
        self,
        payment_class: Literal["general", "ownership", "research"],
    ) -> pd.DataFrame:
        """Adds a payment_class column to the payment class DataFrame and renames columns."""
        payments: pd.DataFrame = getattr(self, f"{payment_class}_payments")
        payments.insert(1, "payment_class", payment_class)
        payments.rename(
            columns={key: val[0] for key, val in getattr(self, f"{payment_class}_columns").items()},
            inplace=True,
        )
        return payments

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[str, int]]]:
        """Returns columns of interest and a tuple of the column's rename
        and dtype for reading ownership payments."""

        return {
            "Physician_Profile_ID": ("profile_id", "Int64"),
            "Total_Amount_Invested_USDollars": ("payment_amount", "Float64"),
            "Value_of_Interest": ("value_of_interest", "Float64"),
            "Terms_of_Interest": ("terms_of_interest", str),
            "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name": ("submitting_entity", str),
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name": ("payment_entity", str),
        }

    def update_ownership_payments(self) -> pd.DataFrame:
        """Calls the update_payments method but also adds a null payment_id
        column and adds the value_of_interest to the payment_amount column
        and drops the value_of_interest column."""
        self.ownership_payments.insert(0, "payment_id", None)

        self.ownership_payments = self.update_payments("ownership")

        self.ownership_payments["payment_amount"] = self.ownership_payments.apply(
            lambda x: x["value_of_interest"] + x["payment_amount"],
            axis=1,
        )

        self.ownership_payments.drop(columns=["value_of_interest"], inplace=True)

        return self.ownership_payments

    @property
    def research_columns(self) -> dict[str, str]:
        """Returns columns of interest and a tuple of the column's rename
        and dtype for reading research payments."""

        return {
            "Covered_Recipient_Profile_ID": ("profile_id", "Int64"),
            "Form_of_Payment_or_Transfer_of_Value": ("payment_type", str),
            "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name": ("submitting_entity", str),
            "Total_Amount_of_Payment_USDollars": ("payment_amount", "Float64"),
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name": ("payment_entity", str),
            "Record_ID": ("payment_id", "Int64"),
        }
