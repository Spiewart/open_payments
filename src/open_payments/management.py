import logging
import os
from typing import Literal, Union

import pandas as pd

from .config import Settings
from .helpers import get_file_suffix
from .ids import PaymentIDs
from .payment_types import PaymentTypes
from .payments import PaymentsSearch

# Configure logging
logging.basicConfig(level=logging.INFO)


def create_MD_DO_payments_csv(
    method: Union[type[PaymentIDs], type[PaymentsSearch]],
    payment_class: Literal["general", "ownership", "research"],
    year: int,
    settings: Union[Settings, None] = None,
) -> None:
    """Creates an csv file containing the year's OpenPayments payments for the
    payment type for MDs and DOs."""

    settings = settings if settings is not None else Settings()

    logging.info(f"Creating csv file for {payment_class} payments for {year}...")
    path = str(settings.data_dir)

    directory = os.path.join(
        path,
        f"{method.__name__}_csvs",
    )

    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Directory {directory} created.")

    file_suffix = get_file_suffix([year], [payment_class], settings=settings)

    file_name = f"MD_DO_payments{file_suffix}.csv"

    # Check if the file exists
    if file_name in os.listdir(directory):
        logging.warning(
            f"File {directory}/{file_name} already exists. Please delete the file if you want to overwrite it."
        )
        return

    id_maker = method(
        nrows=None,
        payment_classes=[payment_class],
        years=year,
        settings=settings,
        # Default MD_DO_only is True
    )

    payments = id_maker.all_payments()

    logging.info(f"{payments.shape[0]} physician {payment_class} payments found for {year}.")

    payments.to_csv(
        f"{directory}/{file_name}",
        index=False,
    )


def create_id_MD_DO_payments_csvs(settings: Union[Settings, None] = None) -> None:
    """Creates csv files from OpenPayments payments that facilitate
    cross-referencing project specific provider information for OpenPayments IDs
    (Covered_Recipient_Profile_ID).

    Year + class range come from `settings` (defaults to env-based Settings()).
    """

    settings = settings if settings is not None else Settings()

    logging.info("Creating ID csv files for MDs and DOs...")
    for payment_class in settings.payment_classes:
        for year in settings.years:
            logging.info(f"Creating csv file for {payment_class} payments for {year}...")
            create_MD_DO_payments_csv(PaymentIDs, payment_class, year, settings=settings)


def create_search_general_MD_DO_payments_csvs(settings: Union[Settings, None] = None) -> None:
    """Creates csv files for general OpenPayments payments for MDs and DOs
    for the years in `settings.years`."""

    settings = settings if settings is not None else Settings()
    for year in settings.years:
        create_MD_DO_payments_csv(PaymentsSearch, "general", year, settings=settings)


def create_payment_types_excel(settings: Union[Settings, None] = None) -> None:
    settings = settings if settings is not None else Settings()
    PaymentTypes(
        payment_classes=list(settings.payment_classes), settings=settings
    ).create_payment_types_excel()


def load_MD_DO_payments_csvs(settings: Union[Settings, None] = None) -> pd.DataFrame:
    """Loads all cached MD/DO payment CSVs from `settings.data_dir`."""

    settings = settings if settings is not None else Settings()
    path = str(settings.data_dir)

    # Get the list of files in the directory
    files = os.listdir(path)

    # Filter the files to only include the ones that start with "MD_DO_payments"
    files = [f for f in files if f.startswith("MD_DO_payments")]

    # Load the files into a dataframe
    payments = pd.concat(
        [pd.read_csv(os.path.join(path, f)) for f in files],
        ignore_index=True,
    )

    return payments


def load_MD_DO_id_search_payments(settings: Union[Settings, None] = None) -> pd.DataFrame:
    """Loads all MD/DO payment CSVs cached under `{settings.data_dir}/PaymentIDs_csvs`
    for use as the pre-computed payments DataFrame in the ID-search pipeline."""

    settings = settings if settings is not None else Settings()
    logging.info("Loading MD/DO payments for ID search...")
    path = f"{settings.data_dir}/PaymentIDs_csvs"

    # Get the list of files in the directory
    files = os.listdir(path)

    # Filter the files to only include the ones that start with "MD_DO_payments"
    files = [f for f in files if f.startswith("MD_DO_payments")]
    logging.info(
        f"Found {len(files)} files for MD/DO payments: "
        f"{', '.join([file.split('MD_DO_payments_')[-1] for file in files])}"
    )
    # Load the files into a dataframe
    payments = pd.concat(
        [
            pd.read_csv(
                os.path.join(path, f),
                dtype={
                    "first_name": str,
                    "middle_name": str,
                    "last_name": str,
                },
            )
            for f in files
        ],
        ignore_index=True,
    )
    logging.info(f"Loaded {payments.shape[0]} MD/DO payments for ID search.")
    return payments


def MD_DO_general_search_df(settings: Union[Settings, None] = None) -> pd.DataFrame:
    """Loads MD/DO general payments from saved csvs
    and returns a dataframe with the payments."""

    settings = settings if settings is not None else Settings()
    directory = os.path.join(
        str(settings.data_dir),
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
