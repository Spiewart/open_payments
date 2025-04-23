import os
import pandas as pd
import re

from typing import Union, Literal


def get_conflicted_ids_from_file() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Method that returns a tuple of DataFrames containing the matched
    IDs of conflicteds and conflicteds who were unmatched after searching
    for an ID in OpenPayments."""

    path = open_payments_directory()

    with pd.ExcelFile(f"{path}/conflicted_ids.xlsx") as xls:
        # Read the first sheet into a DataFrame
        matched_df = pd.read_excel(xls, sheet_name="conflicted_ids")
        # Read the second sheet into a DataFrame
        unmatched_df = pd.read_excel(xls, sheet_name="unmatched")
    return matched_df, unmatched_df


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

            payments = pd.read_csv(f"{path}/{file_name}")

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
