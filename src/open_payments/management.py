import os
import pandas as pd
from typing import Union, Literal

from .helpers import get_file_suffix, open_payments_directory
from .ids import PaymentIDs


def create_MD_DO_payments_csv(
    payment_class: Union[
        Literal["general", "ownership", "research"],
    ],
    year: Union[Literal[2020, 2021, 2022, 2023]]
) -> None:

    """Creates an csv file containing the year's OpenPayments payments for the
    payment type for MDs and DOs."""

    print(
        f"Creating csv file for {payment_class} payments for {year}..."
    )
    path = open_payments_directory()

    file_suffix = get_file_suffix([year], [payment_class])

    file_name = f"MD_DO_payments{file_suffix}.csv"

    # Check if the file exists
    if file_name in os.listdir(
        os.path.join(
            os.path.expanduser('~'),
            'open_payments_datasets',
        )
    ):
        print(
            f"File {path}/{file_name} already exists. "
            "Please delete the file if you want to overwrite it."
        )
        return

    id_maker = PaymentIDs(
        nrows=None,
        payment_classes=[payment_class],
        years=year,
    )

    payments = id_maker.all_payments(physicians_only=True)

    print(
        payments.shape[0],
        f"physician {payment_class} payments found for {year}."
    )

    payments.to_csv(
        f"{path}/{file_name}.csv",
        index=False,
    )


def create_all_MD_DO_payments_csvs() -> None:
    """Creates csv files for all OpenPayments payments for MDs and DOs
    for the years 2020-2023 for all payment types (general, ownership,
    and research)."""

    for payment_class in ["general", "ownership", "research"]:
        for year in [2020, 2021, 2022, 2023]:
            create_MD_DO_payments_csv(payment_class, year)
