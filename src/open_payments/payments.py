from typing import Literal, Type, Union

import logging
import pandas as pd

from .payment_types import PaymentTypes
from .physicians_only import PhysicianFilter
from .read import ReadPayments

# Configure logging
logging.basicConfig(level=logging.INFO)


class Payments(ReadPayments):

    def __init__(
        self,
        years: Union[
            list[Literal[2020, 2021, 2022, 2023, 2024]],
            Literal[2020, 2021, 2022, 2023, 2024],
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
        MD_DO_only: bool = True,
    ):
        super().__init__(
            years=years,
            payment_classes=payment_classes,
            payments_folder=payments_folder,
            nrows=nrows,
            general_payments=general_payments,
            ownership_payments=ownership_payments,
            research_payments=research_payments,
            MD_DO_only=MD_DO_only,
        )

    def filter_payment_chunk(
        self,
        payment_chunk: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters the payment chunk for physicians only if specified."""

        logging.info(
            "Filtering payment chunk"
            f"{' for MDs and DOs' if self.MD_DO_only else ''}..."
        )

        if self.MD_DO_only:
            payment_chunk = PhysicianFilter(payment_chunk).filter()

        return payment_chunk

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:
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
    def ownership_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:
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
    def research_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:
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


class PaymentsSearch(PaymentTypes, ReadPayments):
    """Class that searches a Payments DataFrame for payments
    made to a DataFrame of OpenPayments profile IDs
    (Covered_Recipient_Profile_ID or Physician_Profile_ID).

    Args:
        payments (pd.DataFrame): DataFrame of payments
            generated by Payments class method (above).
        conflicteds_ids (pd.DataFrame): DataFrame of OpenPayments
            profile IDs generated by ConflictedPaymentIDs
            (ids.py).
    """

    def __init__(
        self,
        conflicteds_ids: pd.DataFrame,
        payments: Union[pd.DataFrame, None] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.payments = payments
        self.conflicteds_ids = conflicteds_ids

    def filter_payment_chunk(
        self,
        payment_chunk: pd.DataFrame,
        payment_class: Literal["general", "ownership", "research"] = "general",
    ) -> pd.DataFrame:

        payment_chunk = super().filter_payment_chunk(
            payment_chunk=payment_chunk,
            payment_class=payment_class,
        )

        payment_chunk = payment_chunk[
            payment_chunk[
                next(
                    iter(
                        [
                            key for key, value
                            in getattr(self, f"{payment_class}_columns").items()
                            if value[0] == "profile_id"
                        ]
                    )
                )
            ].isin(
                self.conflicteds_ids["profile_id"]
            )
        ]

        return payment_chunk

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols = super().general_columns
        cols.update({
            "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name": ("submitting_entity", str),
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name": ("payment_entity", str),
            "Total_Amount_of_Payment_USDollars": ("amount", "Float64"),
            "Date_of_Payment": ("date", str),
            "Record_ID": ("id", "Int64"),
            "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1": ("product", str),
            "Product_Category_or_Therapeutic_Area_1": ("product_category", str),
            "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1": ("product_name", str),
            "Program_Year": ("year", "Int64"),
        })
        return cols


class DescribePayments:
    """Class that generates descriptive statistics for a
    OpenPayments payments DataFrame.
    Args:
        payments (pd.DataFrame): DataFrame of payments
            generated by Payments/Search class methods (above).
        conflicteds_ids (pd.DataFrame): DataFrame of OpenPayments
            profile IDs generated by ConflictedPaymentIDs
            (ids.py).
    """
    def __init__(
        self,
        payments: pd.DataFrame,
        conflicteds_ids: Union[pd.DataFrame, None] = None,
    ):
        self.payments = payments
        self.conflicteds_ids = conflicteds_ids

    @classmethod
    def get_descriptive_stats(
        cls,
        payments: pd.DataFrame,
        conflicted_ids: Union[pd.DataFrame, None] = None,
    ) -> pd.DataFrame:
        """Generates descriptive statistics for the payments DataFrame.
        Adds extra stats if conflicteds_ids is provided."""

        stats = pd.DataFrame(
            {
                "total_payments": [payments.shape[0]],
                "total_amount": [payments["Total_Amount_of_Payment_USDollars"].sum()],
                "mean_amount": [payments["Total_Amount_of_Payment_USDollars"].mean()],
                "median_amount": [payments["Total_Amount_of_Payment_USDollars"].median()],
            }
        )

        conflicted_with_ids = conflicted_ids[conflicted_ids["profile_id"].notna()] if conflicted_ids is not None else pd.DataFrame()

        if conflicted_ids is not None:
            stats["pct_w_pmt"] = conflicted_ids["profile_id"].isin(
                payments["Covered_Recipient_Profile_ID"]
            ).sum() / conflicted_with_ids.shape[0]
            stats["avg_pmts_per_conflicted"] = [
                payments[
                    payments["Covered_Recipient_Profile_ID"].isin(
                        conflicted_ids["profile_id"]
                    )
                ].shape[0] / conflicted_with_ids.shape[0]
            ]
            stats["avg_dol_pmt_per_conflicted"] = [
                payments[
                    payments["Covered_Recipient_Profile_ID"].isin(
                        conflicted_with_ids["profile_id"]
                    )
                ]["Total_Amount_of_Payment_USDollars"].mean()
            ]

        return stats

    def get_all_descriptive_stats(self) -> pd.DataFrame:
        """Generates descriptive statistics for the entire
        payments DataFrame."""

        return self.get_descriptive_stats(
            self.payments,
            self.conflicteds_ids,
        )

    def get_descriptive_stats_by_years(self) -> pd.DataFrame:
        """Generates descriptive statistics for all
        years in the payments DataFrame."""

        stats = pd.DataFrame()

        for year in self.payments["Program_Year"].unique():
            year_payments = self.payments[
                self.payments["Program_Year"] == year
            ]

            year_stats = self.get_descriptive_stats(
                year_payments,
                self.conflicteds_ids,
            )

            year_stats["year"] = year
            # Required to suppress FutureWarning about dropping columns
            year_stats.dropna(axis=1, how="all", inplace=True)
            stats = pd.concat([stats, year_stats])

        return stats

    def get_descriptive_stats_by_payment_type(
        self,
    ) -> pd.DataFrame:
        """Generates descriptive statistics for a specific
        payment type in the payments DataFrame."""

        stats = pd.DataFrame()

        for payment_type in self.payments["Nature_of_Payment_or_Transfer_of_Value"].unique():
            for year in self.payments["Program_Year"].unique():
                year_payments = self.payments[
                    (self.payments["Program_Year"] == year) &
                    (self.payments["Nature_of_Payment_or_Transfer_of_Value"] == payment_type)
                ]

                year_stats = self.get_descriptive_stats(
                    year_payments,
                    self.conflicteds_ids,
                )

                year_stats["year"] = year
                year_stats["payment_type"] = payment_type
                # Required to suppress FutureWarning about dropping columns
                year_stats.dropna(axis=1, how="all", inplace=True)
                stats = pd.concat([stats, year_stats])

        return stats

    def get_payments_by_conflicted(self) -> pd.DataFrame:
        """Returns a DataFrame of payments made to conflicted profile IDs.
        Organized by conflicted provider_pk in ascending order. Inserts a blank
        row for conflicted profile IDs with no payments."""

        logging.info("Getting payments by conflicted...")

        payments_by_conflicted = pd.DataFrame()

        for _, row in self.conflicteds_ids.iterrows():
            conflicted_payments = self.payments[
                self.payments["Covered_Recipient_Profile_ID"] == row["profile_id"]
            ].copy()

            if conflicted_payments.empty:
                conflicted_payments = pd.DataFrame(
                    {
                        col: [None] for col in self.payments.columns
                    }
                )
                conflicted_payments["Covered_Recipient_Profile_ID"] = row["profile_id"]

            conflicted_payments["provider_pk"] = row["provider_pk"]

            conflicted_payments.dropna(axis=1, how="all", inplace=True)
            payments_by_conflicted = pd.concat(
                [payments_by_conflicted, conflicted_payments]
            )

        payments_by_conflicted.sort_values(
            by=["provider_pk"],
            inplace=True,
        )
        payments_by_conflicted.reset_index(drop=True, inplace=True)

        return payments_by_conflicted