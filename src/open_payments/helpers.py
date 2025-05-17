import os
import re
from typing import Literal, Type, Union

import pandas as pd

from .choices import PaymentFilters


class ColumnMixin:

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols = super().general_columns
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols = super().ownership_columns
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols = super().research_columns
        return cols

    @property
    def filters(self) -> list["PaymentFilters"]:
        """Overwritten to add CREDENTIAL PaymentFilter to
        the filters property."""

        filters: list[PaymentFilters] = super().filters
        return filters

    def convert_merged_dtypes(
        self,
        merged: pd.DataFrame,
    ) -> pd.DataFrame:
        """Updates  payments and conflicteds columns into lists after
        they are loaded as strs in CSVs and Excel files."""

        merged: pd.DataFrame = super().convert_merged_dtypes(merged)

        return merged


def get_file_suffix(
    years: Union[
            list[Literal[2020, 2021, 2022, 2023]],
            Literal[2020, 2021, 2022, 2023],
        ],
    payment_classes: Union[
        list[Literal["general", "ownership", "research"]],
        Literal["general", "ownership", "research"],
        None,
    ],
) -> str:
    if not isinstance(payment_classes, list):
        payment_classes = [payment_classes] if payment_classes is not None else []

    return (
            f"_{'_'.join(payment_classes)}_{('_'.join([str(year) for year in years] if isinstance(years, list) else [str(years)]))}"
            if (
                isinstance(years, list) and any(
                    year not in years for year in [2020, 2021, 2022, 2023]
                ) or isinstance(
                    years,
                    int,
                ) and years not in [2020, 2021, 2022, 2023]
            )
            or (
                payment_classes is not None
                and any(
                    payment_class not in payment_classes
                    for payment_class in ["general", "research", "ownership"]
                )
            )
            else ""
        )


def load_all_MD_DO_payments_csvs() -> pd.DataFrame:
    """Imports all OpenPayments payments for MDs and DOs
    for the years 2020-2023 for all payment types (general, ownership,
    and research) into a single dataframe."""
    path = open_payments_directory()

    all_payments = pd.DataFrame()

    for payment_class in ["general", "ownership", "research"]:
        for year in [2020, 2021, 2022, 2023]:
            file_suffix = get_file_suffix(years=year, payment_classes=payment_class)
            file_name = f"MD_DO_payments{file_suffix}.csv"

            if file_name not in os.listdir(path):
                print(
                    f"File {path}/{file_name} does not exist. "
                    "Please create the file first."
                )
                continue

            payments = pd.read_csv(
                f"{path}/{file_name}",
                dtype={
                    "first_name": str,
                    "middle_name": str,
                    "last_name": str,
                },
            )
            all_payments = pd.concat([all_payments, payments], ignore_index=True)

    return all_payments


def open_payments_directory() -> str:
    return os.path.join(os.path.expanduser('~'), 'open_payments_datasets')


def str_in_str(
    to_match: str,
    string: str,
    ignore_case: bool = True,
) -> bool:
    flags = [re.IGNORECASE] if ignore_case else []
    # sub() out parentheses in the search terms
    to_match = re.sub(r"\(|\)", "", to_match)
    # sub() out brackets in the search terms
    to_match = re.sub(r"\[|\]", "", to_match)

    for i, _ in enumerate(to_match):
        if (
            # Check for a substitution
            re.search(f"{to_match[:i]}.{to_match[i+1:]}", string, *flags)
            # Check for an addition
            or re.search(f"{to_match[:i]}.{to_match[i:]}", string, *flags)
            # Check for a deletion
            or re.search(f"{to_match[:i]}{to_match[i+1:]}", string, *flags)
        ):
            return True
    return False
