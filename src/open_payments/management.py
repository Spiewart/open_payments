import os
import pandas as pd
from typing import Union, Literal, Type

from .helpers import get_file_suffix, open_payments_directory
from .ids import PaymentIDs
from .payment_types import PaymentTypes
from .payments import PaymentsSearch


def create_MD_DO_payments_csv(
    method: Union[Type[PaymentIDs], Type[PaymentsSearch]],
    payment_class: Literal["general", "ownership", "research"],
    year: Literal[2020, 2021, 2022, 2023]
) -> None:

    """Creates an csv file containing the year's OpenPayments payments for the
    payment type for MDs and DOs."""

    print(
        f"Creating csv file for {payment_class} payments for {year}..."
    )
    path = open_payments_directory()

    directory = os.path.join(
        path,
        f"{method.__name__}_csvs",
    )

    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Directory {directory} created.")

    file_suffix = get_file_suffix([year], [payment_class])

    file_name = f"MD_DO_payments{file_suffix}.csv"

    # Check if the file exists
    if file_name in os.listdir(
        directory
    ):
        print(
            f"File {directory}/{file_name} already exists. "
            "Please delete the file if you want to overwrite it."
        )
        return

    id_maker = method(
        nrows=None,
        payment_classes=[payment_class],
        years=year,
    )

    payments = id_maker.all_payments()

    print(
        payments.shape[0],
        f"physician {payment_class} payments found for {year}."
    )

    payments.to_csv(
        f"{directory}/{file_name}",
        index=False,
    )


def create_id_MD_DO_payments_csvs() -> None:
    """Creates csv files for all OpenPayments payments for MDs and DOs
    for the years 2020-2023 for all payment types (general, ownership,
    and research)."""

    for payment_class in ["general", "ownership", "research"]:
        for year in [2020, 2021, 2022, 2023]:
            create_MD_DO_payments_csv(PaymentIDs, payment_class, year)


def create_search_general_MD_DO_payments_csvs() -> None:
    """Creates csv files for all OpenPayments payments for MDs and DOs
    for the years 2020-2023 for all payment types (general, ownership,
    and research)."""

    for payment_class in ["general"]:
        for year in [2020, 2021, 2022, 2023]:
            create_MD_DO_payments_csv(PaymentsSearch, payment_class, year)


def create_payment_types_excel() -> None:
    PaymentTypes(
        payment_classes=["general", "ownership", "research"]
    ).create_payment_types_excel()


def load_MD_DO_id_search_payments() -> pd.DataFrame:
    """Method that loads all OpenPayments payments for MDs and DOs
    for the years 2020-2023 for all payment types (general, ownership,
    and research). The method will return a dataframe with all the
    payments."""

    path = open_payments_directory()

    path = f"{path}/PaymentIDs_csvs"

    # Get the list of files in the directory
    files = os.listdir(path)

    # Filter the files to only include the ones that start with "MD_DO_payments"
    files = [f for f in files if f.startswith("MD_DO_payments")]

    # Load the files into a dataframe
    payments = pd.concat(
        [pd.read_csv(
            os.path.join(path, f),
            dtype={
                "first_name": str,
                "middle_name": str,
                "last_name": str,
            }) for f in files],
        ignore_index=True,
    )

    return payments


def MD_DO_general_search_df() -> pd.DataFrame:
    """Loads MD/DO general payments from saved csvs
    and returns a dataframe with the payments."""

    path = open_payments_directory()

    directory = os.path.join(
        path,
        f"{PaymentsSearch.__name__}_csvs",
    )

    # Get the list of files in the directory
    files = os.listdir(directory)

    # Filter the files to only include the ones that start with "MD_DO_payments"
    files = [f for f in files if f.startswith("MD_DO_payments")]

    # Load the files into a dataframe
    payments = pd.concat(
        [pd.read_csv(os.path.join(directory, f)) for f in files],
        ignore_index=True,
    )

    return payments
