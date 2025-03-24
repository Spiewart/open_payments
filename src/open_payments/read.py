from typing import Literal, Union

import pandas as pd
from pydantic import BaseModel

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

    def read_general_payments_csvs(
        self,
        usecols: Union[list[str], None] = None,
        dtype: Union[dict[str, Union[str, int]], None] = None,
    ) -> pd.DataFrame:

        csv_kwargs = {"nrows": self.nrows} if self.nrows is not None else {}

        self.usecols_dtype_error(usecols, dtype)

        csv_kwargs = self.update_csv_kwargs(csv_kwargs, "general", usecols, dtype)

        for year in self.years:
            general_payments = pd.read_csv(
                f"{self.payments_folder}/{str(year)}/OP_DTL_GNRL_PGYR{str(year)}_P06282024_06122024.csv",
                **csv_kwargs,
            )

            self.general_payments = pd.concat([self.general_payments, general_payments])

        return self.general_payments

    @staticmethod
    def usecols_dtype_error(
        usecols: Union[list[str], None],
        dtype: Union[dict[str, Union[str, int]], None]
    ) -> None:
        if usecols and not dtype or dtype and not usecols:
            raise ValueError("Both usecols and dtype must be provided.")
        elif usecols is not None:
            for col in usecols:
                if col not in dtype:
                    raise ValueError(f"{col} not found in dtype.")

    def update_csv_kwargs(
        self,
        csv_kwargs: dict[str, Union[list[str], dict[str, Union[str, int]]]],
        payment_class: Union[Literal["general", "ownership", "research"]],
        usecols: Union[list[str], None],
        dtype: Union[dict[str, Union[str, int]], None]
    ) -> dict[str, Union[list[str], dict[str, Union[str, int]]]]:

        columns: dict[str, tuple[str, Union[str, int]]] = getattr(self, f"{payment_class}_columns")

        if usecols is not None or dtype is not None:
            csv_kwargs["usecols"] = usecols
            csv_kwargs["dtype"] = dtype
        elif columns:
            csv_kwargs["usecols"] = columns.keys()
            csv_kwargs["dtype"] = {key: value[1] for key, value in columns.items()}

        return csv_kwargs

    def read_ownership_payments_csvs(
        self,
        usecols: Union[list[str], None] = None,
        dtype: Union[dict[str, Union[str, int]], None] = None
    ) -> pd.DataFrame:

        csv_kwargs = {"nrows": self.nrows} if self.nrows is not None else {}

        self.usecols_dtype_error(usecols, dtype)

        csv_kwargs = self.update_csv_kwargs(csv_kwargs, "ownership", usecols, dtype)

        self.ownership_payments = pd.DataFrame()

        for year in self.years:
            ownership_payments = pd.read_csv(
                f"{self.payments_folder}/{str(year)}/OP_DTL_OWNRSHP_PGYR{str(year)}_P06282024_06122024.csv",
                **csv_kwargs,
            )

            self.ownership_payments = pd.concat([self.ownership_payments, ownership_payments])

        return self.ownership_payments

    def read_research_payments_csvs(
        self,
        usecols: Union[list[str], None] = None,
        dtype: Union[dict[str, Union[str, int]], None] = None
    ) -> pd.DataFrame:

        csv_kwargs = {"nrows": self.nrows} if self.nrows is not None else {}

        self.usecols_dtype_error(usecols, dtype)

        csv_kwargs = self.update_csv_kwargs(csv_kwargs, "research", usecols, dtype)

        self.research_payments = pd.DataFrame()

        for year in self.years:
            research_payments = pd.read_csv(
                f"{self.payments_folder}/{str(year)}/OP_DTL_RSRCH_PGYR{str(year)}_P06282024_06122024.csv",
                **csv_kwargs,
            )

            self.research_payments = pd.concat([self.research_payments, research_payments])

        return self.research_payments

    @property
    def general_columns(self) -> Union[dict[str, tuple[str, Union[str, int]]]]:
        """Returns columns of interest and a tuple of the column's rename
        and dtype for reading general payments."""

        return {}

    @property
    def ownership_columns(self) -> Union[dict[str, tuple[str, Union[str, int]]]]:
        """Returns columns of interest and a tuple of the column's rename
        and dtype for reading ownership payments."""

        return {}

    @property
    def research_columns(self) -> Union[dict[str, tuple[str, Union[str, int]]]]:
        """Returns columns of interest and a tuple of the column's rename
        and dtype for reading research payments."""

        return {}

    def update_payments(
        self,
        payment_class: Literal["general", "ownership", "research"],
    ) -> pd.DataFrame:
        """Renames columns for the payment class DataFrame."""
        payments: pd.DataFrame = getattr(self, f"{payment_class}_payments")

        payments.rename(
            columns={key: val[0] for key, val in getattr(self, f"{payment_class}_columns").items()},
            inplace=True,
        )
        return payments