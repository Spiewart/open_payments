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

        for payment_class in self.payment_classes:
            getattr(self, f"read_{payment_class}_payments_csvs")(
                usecols=getattr(self, f"{payment_class}_columns").keys(),
                dtype={key: value[1] for key, value in getattr(self, f"{payment_class}_columns").items()},
            )
            if hasattr(self, f"update_{payment_class}_payments"):
                getattr(self, f"update_{payment_class}_payments")()
            else:
                getattr(self, "update_payments")(payment_class)

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

    def update_ownership_payments(self) -> pd.DataFrame:
        """Adds a null payment_id column and adds the value_of_interest
        to the payment_amount column and drops the value_of_interest
        column. This is because the ownership OpenPayments dataset
        has different column names than the general and research datasets."""

        self.ownership_payments.insert(0, "payment_id", None)

        self.ownership_payments = super().update_payments("ownership")

        self.ownership_payments["payment_amount"] = self.ownership_payments.apply(
            lambda x: x["value_of_interest"] + x["payment_amount"],
            axis=1,
        )

        self.ownership_payments.drop(columns=["value_of_interest"], inplace=True)

        return self.ownership_payments