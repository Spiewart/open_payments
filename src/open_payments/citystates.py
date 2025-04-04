from typing import Union

import pandas as pd
from pydantic import BaseModel, model_validator
from typing_extensions import Self

from .helpers import get_file_suffix, open_payments_directory
from .read import ReadPayments


class CityState(BaseModel):
    """Class that contains the city and state of a payment."""

    city: str | None = None
    state: str | None = None

    def __str__(self) -> str:
        return f"{self.city}|{self.state}"

    @model_validator(mode="after")
    def validate_city_state(self) -> Self:
        if self.city is None and self.state is None:
            raise ValueError("Both city and state cannot be None.")
        return self


class PaymentCityStates(ReadPayments):

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().general_columns
        cols.update({
            "Recipient_City": ("city", str),
            "Recipient_State": ("state_primary", str),
            "Covered_Recipient_License_State_code1": ("state_license_1", str),
            "Covered_Recipient_License_State_code2": ("state_license_2", str),
            "Covered_Recipient_License_State_code3": ("state_license_3", str),
            "Covered_Recipient_License_State_code4": ("state_license_4", str),
            "Covered_Recipient_License_State_code5": ("state_license_5", str),
        })
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().ownership_columns
        cols.update({
                "Recipient_City": ("city", str),
                "Recipient_State": ("state_primary", str),
        })
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().research_columns

        cols.update(
            self.general_columns
        )
        return cols

    @classmethod
    def citystates(cls, payments: pd.DataFrame) -> pd.DataFrame:
        """Inserts a citystates column into the df, which is an array
        of combinations of city/states. Removes the individual city/state
        columns from the df."""

        payments.insert(
            1,
            "citystates",
            payments.apply(cls.create_citystates, axis=1),
        )

        payments = cls.drop_city_individual_states(payments)

        return payments

    @staticmethod
    def create_citystates(payment: pd.Series) -> list[CityState]:
        """Aggregates the different states into a Series."""

        states = payment[
            [
                "state_primary",
                "state_license_1",
                "state_license_2",
                "state_license_3",
                "state_license_4",
                "state_license_5",
            ]
        ].dropna().unique()

        city = payment["city"]

        return [
            CityState(city=city, state=state) for state in states
        ] if len(states) > 0 else [
            CityState(city=city, state=None)
        ] if pd.notna(city) else []

    @staticmethod
    def drop_city_individual_states(payments: pd.DataFrame) -> pd.DataFrame:
        """Drops the city and individual state columns from the DataFrame."""

        payments.drop(
            columns=[
                "city",
                "state_primary",
                "state_license_1",
                "state_license_2",
                "state_license_3",
                "state_license_4",
                "state_license_5",
            ],
            inplace=True,
        )

        return payments

    def update_ownership_payments(self) -> pd.DataFrame:
        """Overwritten to add state_license_1-5 to the DataFrame, as they
        won't be present after renaming pre-existing columns."""

        self.ownership_payments["state_license_1"] = None
        self.ownership_payments["state_license_2"] = None
        self.ownership_payments["state_license_3"] = None
        self.ownership_payments["state_license_4"] = None
        self.ownership_payments["state_license_5"] = None
        self.ownership_payments = super().update_ownership_payments()

        return self.ownership_payments
