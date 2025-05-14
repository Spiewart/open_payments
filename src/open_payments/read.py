from typing import Literal, Union

import pandas as pd

from .helpers import open_payments_directory


class ReadPayments:
    """Class method whose only job is to read the OpenPayments csv files.
    It can be subclassed to add additional functionality and efficiency
    when reading the csv files."""

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
        self.years = [years] if isinstance(years, int) else years if years is not None else [2020, 2021, 2022, 2023]
        self.payment_classes = (
            [payment_classes] if isinstance(payment_classes, str)
            else payment_classes if payment_classes is not None
            else ["general", "ownership", "research"]
        )
        self.payments_folder = payments_folder if payments_folder is not None else open_payments_directory()
        self.nrows = nrows
        self.general_payments = pd.DataFrame() if general_payments is None else general_payments
        self.ownership_payments = pd.DataFrame() if ownership_payments is None else ownership_payments
        self.research_payments = pd.DataFrame() if research_payments is None else research_payments

    def all_payments(self, physicians_only: bool = True) -> pd.DataFrame:
        """Returns a DataFrame of all payments with merged column names
        from the OpenPayments datasets."""

        print((
            "Reading and updating payment classes: "
            f"{(', ').join(self.payment_classes)}...")
        )

        for payment_class in self.payment_classes:

            setattr(
                self,
                f"{payment_class}_payments", self.read_payments_csvs(
                    payment_class=payment_class,
                    physicians_only=physicians_only
                )
            )

            if hasattr(self, f"update_{payment_class}_payments"):
                setattr(
                    self,
                    f"{payment_class}_payments",
                    getattr(self, f"update_{payment_class}_payments")()
                )
            else:
                setattr(
                    self,
                    f"{payment_class}_payments",
                    getattr(self, "update_payments")(payment_class)
                )

        all_payments = pd.concat([
            self.general_payments, self.ownership_payments, self.research_payments
        ])

        return all_payments

    def read_payments_csvs(
        self,
        payment_class: Literal["general", "ownership", "research"],
        physicians_only: bool = True
    ) -> pd.DataFrame:
        """Reads the OpenPayments csv files for the specified payment class
        and returns a DataFrame of the payments. The csv files are read in
        chunks to avoid memory issues. The DataFrame is filtered for physicians
        only if specified."""

        print(f"Reading {payment_class} payments...")

        csv_kwargs = {"nrows": self.nrows} if self.nrows is not None else {"chunksize": 50000}

        csv_kwargs = self.update_csv_kwargs(csv_kwargs, payment_class)

        for year in self.years:

            csv_path = (
                f"{self.payments_folder}/{self.get_payment_csv_path(
                    payment_class=payment_class, year=year
                )}"
            )

            payments = pd.concat(
                (
                    self.filter_payment_chunk(
                        x,
                        physicians_only=physicians_only,
                    )
                ) for x in pd.read_csv(
                    csv_path,
                    header=0,
                    engine="c",
                    low_memory=False,
                    **csv_kwargs,
                )
            ) if self.nrows is None else (
                self.filter_payment_chunk(
                    pd.read_csv(
                        csv_path,
                        header=0,
                        engine="c",
                        low_memory=False,
                        **csv_kwargs,
                    ), physicians_only=physicians_only
                )
            )

            setattr(
                self,
                f"{payment_class}_payments",
                pd.concat([
                    getattr(self, f"{payment_class}_payments"),
                    payments,
                ])
            )

        return getattr(self, f"{payment_class}_payments")

    def filter_payment_chunk(
        self,
        payment_chunk: pd.DataFrame,
        physicians_only: bool = True,
    ) -> pd.DataFrame:
        """Filters the payment chunk for physicians only if specified."""

        return payment_chunk if physicians_only else payment_chunk

    @staticmethod
    def get_payment_csv_path(
        payment_class: Literal["general", "ownership", "research"],
        year: Union[Literal[2020, 2021, 2022, 2023], int]
    ) -> str:
        """Returns the csv path for the specified payment class and year."""

        prefixes = {
            "general": "GNRL",
            "ownership": "OWNRSHP",
            "research": "RSRCH",
        }

        postfix = "P06282024_06122024.csv"

        return f"{str(year)}/OP_DTL_{prefixes[payment_class]}_PGYR{str(year)}_{postfix}"

    def update_csv_kwargs(
        self,
        csv_kwargs: dict[str, Union[list[str], dict[str, Union[str, int]]]],
        payment_class: Union[Literal["general", "ownership", "research"]],
    ) -> dict[str, Union[list[str], dict[str, Union[str, int]]]]:

        columns: dict[str, tuple[str, Union[str, int]]] = getattr(
            self,
            f"{payment_class}_columns",
        )

        csv_kwargs["usecols"] = columns.keys()
        csv_kwargs["dtype"] = {key: value[1] for key, value in columns.items()}

        return csv_kwargs

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        return {
                "Covered_Recipient_Profile_ID": ("profile_id", "Int32"),
                "Covered_Recipient_Last_Name": ("last_name", str),
                "Covered_Recipient_First_Name": ("first_name", str),
                "Covered_Recipient_Middle_Name": ("middle_name", str),
        }

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        return {
                "Physician_Profile_ID": ("profile_id", "Int32"),
                "Physician_First_Name": ("first_name", str),
                "Physician_Last_Name": ("last_name", str),
                "Physician_Middle_Name": ("middle_name", str),
        }

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        return {
            **self.general_columns
        }

    def update_payments(
        self,
        payment_class: Literal["general", "ownership", "research"],
    ) -> pd.DataFrame:
        """Renames columns based on payment_class properties, for consistency
        and brevity. Also adds a payment_class column to the DataFrame to keep
        track of which payments came from which dataset after they are combined."""

        payments: pd.DataFrame = getattr(self, f"{payment_class}_payments")

        payments.insert(1, "payment_class", payment_class)

        payments.rename(
            columns={key: val[0] for key, val in getattr(self, f"{payment_class}_columns").items()},
            inplace=True,
        )

        return payments

    def update_ownership_payments(self) -> pd.DataFrame:
        """The ownership OpenPayments dataset has different column
        names than the general and research datasets, so it requires
        a separate method. Calls the update_payments method, but can be
        overwritten before or afterwards for additional processing."""

        self.ownership_payments = self.update_payments("ownership")

        return self.ownership_payments
