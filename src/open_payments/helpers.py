import logging
import os
import re
from typing import Literal, Union

import pandas as pd

from .choices import PaymentFilters


# Configure logging
logging.basicConfig(level=logging.INFO)


def chunker(seq, size):
    # https://stackoverflow.com/questions/434287/how-to-iterate-over-a-list-in-chunks
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def get_conflicted_ids_from_file(data_directory: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Method that returns a tuple of DataFrames containing the matched
    IDs of conflicteds and conflicteds who were unmatched after searching
    for an ID in OpenPayments."""

    if data_directory is None:
        data_directory = get_data_directory()

    path = f"{data_directory}/conflicteds_ids.xlsx"

    with pd.ExcelFile(path) as xls:
        # Read the first sheet into a DataFrame
        matched_df = pd.read_excel(xls, sheet_name="conflicteds_ids")
        # Read the second sheet into a DataFrame
        unmatched_df = pd.read_excel(xls, sheet_name="unmatched")
        # Read every other sheet into a DataFrame

        unmatched_options = pd.concat(
            [
                pd.read_excel(xls, sheet_name=sheet)
                for sheet in xls.sheet_names
                if str_can_be_int(sheet)
            ],
            ignore_index=True,
        ) if len(xls.sheet_names) > 3 else pd.DataFrame()

    return matched_df, unmatched_df, unmatched_options


def get_data_directory() -> str:
    return f"{os.path.expanduser('~')}/open_payments_datasets"


def get_file_suffix(
    years: Union[
            list[Literal[2020, 2021, 2022, 2023, 2024]],
            Literal[2020, 2021, 2022, 2023, 2024],
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
                    year not in years for year in [2020, 2021, 2022, 2023, 2024]
                ) or isinstance(
                    years,
                    int,
                ) and years not in [2020, 2021, 2022, 2023, 2024]
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
                logging.warning(
                    f"File {path}/{file_name} does not exist. Please create the file first."
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


def str_can_be_int(
    s: str,
) -> bool:
    """Method that checks if a string can be converted to an int."""
    try:
        int(s)
        return True
    except ValueError:
        return False


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


def update_or_create_conflicteds_ids(
    unique_ids: pd.DataFrame,
    unmatcheds: pd.DataFrame,
    unmatched_options: pd.DataFrame,
    data_directory: str | None = None,
) -> None:

    if data_directory is None:
        data_directory = get_data_directory()

    # Check if the conflicteds_ids.xlsx file exists
    if os.path.exists(f"{data_directory}/conflicteds_ids.xlsx"):
        # If it exists, read the existing data
        (
            matched_df, unmatched_df, existing_options
            ) = get_conflicted_ids_from_file(data_directory=data_directory)
        # Update the options DataFrame with the new unmatched options
        unmatched_options = update_unmatched_options(
            unmatched_options=unmatched_options,
            existing_options=existing_options,
            matched_df=matched_df,
        )

        for matched in unique_ids.iterrows():
            matched_df = update_or_insert_provider(
                provider_row=matched[1],
                df=matched_df,
            ) if not matched[1].empty else matched_df
        for unmatched in unmatcheds.iterrows():
            unmatched_df = update_or_insert_provider(
                provider_row=unmatched[1],
                df=unmatched_df,
            ) if not unmatched[1].empty else unmatched_df

        # Write the updated DataFrames back to the Excel file
        with pd.ExcelWriter(f"{data_directory}/conflicteds_ids.xlsx") as writer:
            # Write the updated DataFrames to the same sheets
            matched_df.to_excel(writer, sheet_name="conflicteds_ids", index=False)
            unmatched_df.to_excel(writer, sheet_name="unmatched", index=False)
            matched_df[matched_df["filters"].apply(
                lambda x: (
                    PaymentFilters.FIRSTNAME not in x
                    and PaymentFilters.NPI not in x
                )
            )].to_excel(writer, sheet_name="without_firstname", index=False)
            write_unmatched_options_to_excel(
                unmatched_options=unmatched_options,
                writer=writer,
            )
    else:
        # Make the file if it doesn't exist
        with pd.ExcelWriter(f"{data_directory}/conflicteds_ids.xlsx") as writer:
            # Create a new sheet with the DataFrame
            unique_ids.to_excel(writer, sheet_name="conflicteds_ids", index=False)
            unmatcheds.to_excel(writer, sheet_name="unmatched", index=False)
            write_unmatched_options_to_excel(
                unmatched_options=unmatched_options,
                writer=writer,
            )


def update_unmatched_options(
    unmatched_options: pd.DataFrame,
    existing_options: pd.DataFrame,
    matched_df: pd.DataFrame,
) -> pd.DataFrame:
    """Updates the unmatched options DataFrame with the new unmatched options
    and removes any options that are already in the matched DataFrame."""

    existing_options = existing_options[
        ~existing_options["provider_pk"].isin(matched_df["provider_pk"])
    ] if (
        not existing_options.empty and not matched_df.empty
    ) else pd.DataFrame() if existing_options.empty else existing_options

    for _, unmatched_option in unmatched_options.iterrows():
        # Compare only on columns present in BOTH the persisted existing
        # options and the in-memory unmatched_option. Without this column
        # intersection, pandas raises "Operands are not aligned" whenever
        # successive chunks (or the persisted xlsx and the new in-memory
        # data) have different column sets — which is the common case
        # since match-filter shapes vary by dean.
        if existing_options.empty:
            already_present = False
        else:
            common = [
                c for c in existing_options.columns
                if c in unmatched_option.index
            ]
            if not common:
                already_present = False
            else:
                already_present = (
                    existing_options[common]
                    .eq(unmatched_option[common])
                    .all(axis=1)
                    .any()
                )
        if not already_present:
            existing_options = pd.concat(
                [
                    pd.DataFrame([unmatched_option]),
                    existing_options,
                ],
                ignore_index=True,
            )
    return existing_options


def update_or_insert_provider(
    provider_row: pd.Series,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Updates or inserts a provider into the DataFrame based
    on his or her provider_pk."""

    # Check if the provider already exists in the DataFrame
    existing_index = (
        df.index[df["provider_pk"] == provider_row["provider_pk"]]
    ) if not df.empty else pd.Series()

    if not existing_index.empty:
        # Update the existing row
        df.loc[existing_index[0]] = provider_row
    else:
        # Append the new row to the DataFrame
        df = pd.concat(
            [df, pd.DataFrame([provider_row])],
            ignore_index=True,
        )

    return df


def write_unmatched_options_to_excel(
    unmatched_options: pd.DataFrame,
    writer: pd.ExcelWriter,
) -> None:
    """Writes the unmatched options DataFrames to an Excel file."""

    if not unmatched_options.empty:
        for unmatched_provider in unmatched_options["provider_pk"].unique():
            unmatched = unmatched_options[unmatched_options["provider_pk"] == unmatched_provider]

            unmatched.to_excel(
                writer,
                sheet_name=f"{unmatched.iloc[0]['provider_pk']}",
                index=False,
            )