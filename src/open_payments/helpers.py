import logging
import os
from typing import Union

import pandas as pd

from .choices import PaymentFilters
from .config import Settings

# Configure logging
logging.basicConfig(level=logging.INFO)


def chunker(seq, size):
    # https://stackoverflow.com/questions/434287/how-to-iterate-over-a-list-in-chunks
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def get_conflicted_ids_from_file(
    data_directory: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Method that returns a tuple of DataFrames containing the matched
    IDs of conflicteds and conflicteds who were unmatched after searching
    for an ID in OpenPayments."""

    if data_directory is None:
        data_directory = str(Settings().data_dir)

    path = f"{data_directory}/conflicteds_ids.xlsx"

    with pd.ExcelFile(path) as xls:
        # Read the first sheet into a DataFrame
        matched_df = pd.read_excel(xls, sheet_name="conflicteds_ids")
        # Read the second sheet into a DataFrame
        unmatched_df = pd.read_excel(xls, sheet_name="unmatched")
        # Read every other sheet into a DataFrame

        unmatched_options = (
            pd.concat(
                [
                    pd.read_excel(xls, sheet_name=sheet)
                    for sheet in xls.sheet_names
                    if str_can_be_int(sheet)
                ],
                ignore_index=True,
            )
            if len(xls.sheet_names) > 3
            else pd.DataFrame()
        )

    return matched_df, unmatched_df, unmatched_options


def get_file_suffix(
    years: Union[list[int], int, None],
    payment_classes: Union[list[str], str, None],
    settings: Union[Settings, None] = None,
) -> str:
    """Returns a filename suffix describing which years/classes a derived file
    covers, OR the empty string if the inputs are the complete default set.

    The "complete set" is read from `settings.years` and `settings.payment_classes`
    (defaulting to `Settings()` if not provided). Previously the complete set
    was hardcoded to `[2020-2024]` x `[general, ownership, research]` which
    drifted out of sync with callers — now it tracks Settings.
    """
    if settings is None:
        settings = Settings()

    if isinstance(years, int):
        years_list: list[int] = [years]
    elif years is None:
        years_list = []
    else:
        years_list = list(years)

    if isinstance(payment_classes, str):
        classes_list: list[str] = [payment_classes]
    elif payment_classes is None:
        classes_list = []
    else:
        classes_list = list(payment_classes)

    years_complete = set(years_list) == set(settings.years)
    classes_complete = set(classes_list) == set(settings.payment_classes)

    if years_complete and classes_complete:
        return ""

    return f"_{'_'.join(classes_list)}_{'_'.join(str(y) for y in years_list)}"


def load_all_MD_DO_payments_csvs(
    settings: Union[Settings, None] = None,
) -> pd.DataFrame:
    """Imports cached per-(year, class) `MD_DO_payments*.csv` files from
    `settings.data_dir` and concatenates them into one DataFrame.

    Iterates the cartesian product of `settings.years` x `settings.payment_classes`,
    so the previously-hardcoded 2020-2023 range is now driven by config.
    """
    if settings is None:
        settings = Settings()
    path = str(settings.data_dir)

    all_payments = pd.DataFrame()

    for payment_class in settings.payment_classes:
        for year in settings.years:
            file_suffix = get_file_suffix(
                years=year, payment_classes=payment_class, settings=settings
            )
            file_name = f"MD_DO_payments{file_suffix}.csv"

            if not os.path.isdir(path) or file_name not in os.listdir(path):
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
    """Deprecated shim — kept for any external callers. Prefer
    `open_payments.names.within_one_edit_substring`. Will be removed once
    Section 7 lands the public API.
    """
    from .names import within_one_edit_substring

    return within_one_edit_substring(to_match, string, ignore_case=ignore_case)


def update_or_create_conflicteds_ids(
    unique_ids: pd.DataFrame,
    unmatcheds: pd.DataFrame,
    unmatched_options: pd.DataFrame,
    data_directory: str | None = None,
) -> None:

    if data_directory is None:
        data_directory = str(Settings().data_dir)

    # Check if the conflicteds_ids.xlsx file exists
    if os.path.exists(f"{data_directory}/conflicteds_ids.xlsx"):
        # If it exists, read the existing data
        (matched_df, unmatched_df, existing_options) = get_conflicted_ids_from_file(
            data_directory=data_directory
        )
        # Update the options DataFrame with the new unmatched options
        unmatched_options = update_unmatched_options(
            unmatched_options=unmatched_options,
            existing_options=existing_options,
            matched_df=matched_df,
        )

        for matched in unique_ids.iterrows():
            matched_df = (
                update_or_insert_provider(
                    provider_row=matched[1],
                    df=matched_df,
                )
                if not matched[1].empty
                else matched_df
            )
        for unmatched in unmatcheds.iterrows():
            unmatched_df = (
                update_or_insert_provider(
                    provider_row=unmatched[1],
                    df=unmatched_df,
                )
                if not unmatched[1].empty
                else unmatched_df
            )

        # Write the updated DataFrames back to the Excel file
        with pd.ExcelWriter(f"{data_directory}/conflicteds_ids.xlsx") as writer:
            # Write the updated DataFrames to the same sheets
            matched_df.to_excel(writer, sheet_name="conflicteds_ids", index=False)
            unmatched_df.to_excel(writer, sheet_name="unmatched", index=False)
            matched_df[
                matched_df["filters"].apply(
                    lambda x: PaymentFilters.FIRSTNAME not in x and PaymentFilters.NPI not in x
                )
            ].to_excel(writer, sheet_name="without_firstname", index=False)
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

    existing_options = (
        existing_options[~existing_options["provider_pk"].isin(matched_df["provider_pk"])]
        if (not existing_options.empty and not matched_df.empty)
        else pd.DataFrame()
        if existing_options.empty
        else existing_options
    )

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
            common = [c for c in existing_options.columns if c in unmatched_option.index]
            if not common:
                already_present = False
            else:
                already_present = (
                    existing_options[common].eq(unmatched_option[common]).all(axis=1).any()
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
        (df.index[df["provider_pk"] == provider_row["provider_pk"]])
        if not df.empty
        else pd.Series()
    )

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
