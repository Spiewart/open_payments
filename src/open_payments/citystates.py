import re
from typing import ClassVar, Type, Union

import pandas as pd
from pydantic import BaseModel, model_validator
from typing_extensions import Self

from .choices import PaymentFilters, States
from .helpers import ColumnMixin
from .read import ReadPayments


class CityState(BaseModel):
    """Class encompassing a city and state combination.
    To reflect where a provider could potentially live
    or practice and thus be reflected in a payment
    to them."""

    city: Union[str, None] = None
    state: Union[str, None] = None

    States: ClassVar = States

    @classmethod
    def state_is_abbrev(cls, state: str) -> bool:
        """Returns True if the state is an abbreviation, False otherwise."""
        # Remove any periods that may be in the state abbreviation
        state = re.sub(r"\.", "", state) if state else None
        return state in cls.States.__members__

    @property
    def state_abbrev(self) -> str:
        """Returns the state abbreviation for the state."""
        if self.state_is_abbrev(self.state):
            return self.state
        else:
            try:
                return next(
                    iter(
                        state.name
                        for state in self.States.__members__.values()
                        if state == self.state
                        ),
                    None,
                ) if self.state else None
            except KeyError as e:
                raise ValueError(
                        f"State {self.state} is not a valid state."
                    ) from e

    @classmethod
    def state_is_full_name(cls, state: str) -> bool:

        return (
            state in cls.States.__members__.values()
        )

    @property
    def state_full(self) -> str:
        """Returns the full name of the state."""
        if self.state_is_abbrev(self.state):
            return self.States[self.state].value
        else:
            return self.state

    def state_matches(self, state: str) -> bool:
        """Method that compares the CityState object's state attribute
        to the state passed in. Returns True if they are the same, False
        if not."""

        if self.state_is_abbrev(state=state):
            return self.state_abbrev == state
        elif self.state_is_full_name(state=state):
            return self.state_full == state
        else:
            return False

    def citystate_matches(self, citystate: "CityState") -> bool:
        """Method that compares the CityState object's city and state
        attribute to the CityState passed in. Returns True if they are the
        same, False if not."""

        return (
            self.city == citystate.city
            and self.state_matches(state=citystate.state)
        )

    def __str__(self) -> str:
        return f"{self.city}|{self.state}"

    @model_validator(mode="after")
    def validate_city_state(self) -> Self:
        if self.city is None and self.state is None:
            raise ValueError("Both city and state cannot be None.")
        return self


class CityStatesMixin(ColumnMixin):
    @property
    def general_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

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
    def ownership_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols = super().ownership_columns
        cols.update({
                "Recipient_City": ("city", str),
                "Recipient_State": ("state_primary", str),
        })
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols = super().research_columns

        cols.update(
            self.general_columns
        )
        return cols


class PaymentCityStates(ReadPayments, CityStatesMixin):

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

        if pd.isna(city):
            city = None

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


def convert_citystates(citystates: str) -> list[CityState]:
    """Convert a string representation of a list of CityState objects
    to a list of CityState objects."""

    converted = []

    citystates = re.findall(r"CityState\(city='(.*?)', state='(.*?)'\)", citystates)

    for citystate in citystates:

        citystate = CityState(
            city=citystate[0] if (citystate[0] != 'None' and citystate[0] != 'Nan') else None,
            state=citystate[1] if (citystate[1] != 'None' and citystate[1] != "Nan") else None,
        )
        converted.append(citystate)

    return converted


class PaymentIDsCityStates(CityStatesMixin):
    """Filters OpenPayments payments by city and state."""

    @property
    def filters(self) -> list[PaymentFilters]:
        """Adds city and state to the filters list."""
        filters: list[PaymentFilters] = super().filters
        filters.append(PaymentFilters.CITYSTATE)
        filters.append(PaymentFilters.CITY)
        filters.append(PaymentFilters.STATE)
        return filters

    @classmethod
    def filter_by_city(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by city."""

        value = (
                cls.payment_conflict_city_match(
                    payment_citystates=payments_x_conflicted["citystates"],
                    conflict_citystates=payments_x_conflicted[
                        "conflict_citystates"
                    ],
                )
        )

        if value and PaymentFilters.CITYSTATE not in payments_x_conflicted["filters"]:
            return value
        return False

    @classmethod
    def payment_conflict_city_match(
        cls,
        payment_citystates: Union[list[CityState], None],
        conflict_citystates: Union[list[CityState], None],
    ) -> bool:

        return any(
            citystate.city in [
                citystate.city for citystate in payment_citystates
                if (
                    pd.notna(citystate)
                    and pd.notna(
                        citystate.city
                    )
                )
            ] for citystate in [
                citystate for citystate in conflict_citystates
                if (
                    pd.notna(citystate)
                    and pd.notna(
                        citystate.city
                    )
                )
            ]
        )

    @classmethod
    def filter_by_state(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by state."""

        value = (
                cls.payment_conflict_state_match(
                    payment_citystates=payments_x_conflicted["citystates"],
                    conflict_citystates=payments_x_conflicted[
                        "conflict_citystates"
                    ],
                )
        )

        if value and PaymentFilters.CITYSTATE not in payments_x_conflicted["filters"]:
            return value
        return False

    @classmethod
    def payment_conflict_state_match(
        cls,
        payment_citystates: Union[list[CityState], None],
        conflict_citystates: Union[list[CityState], None],
    ) -> bool:

        return any(
            payment_citystate.state_matches(conflict_citystate.state)
            for payment_citystate in payment_citystates
            for conflict_citystate in conflict_citystates
            if (
                pd.notna(payment_citystate)
                and pd.notna(conflict_citystate)
                and pd.notna(payment_citystate.state)
                and pd.notna(conflict_citystate.state)
            )
        )

    @classmethod
    def filter_by_citystate(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by city and state."""

        value = (
            cls.payment_conflict_citystate_match(
                payment_citystates=payments_x_conflicted["citystates"],
                conflict_citystates=payments_x_conflicted[
                    "conflict_citystates"
                ],
            )
        )

        if value:
            if PaymentFilters.CITY in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(
                    PaymentFilters.CITY
                )
            if PaymentFilters.STATE in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(
                    PaymentFilters.STATE
                )

        return value

    @classmethod
    def payment_conflict_citystate_match(
        cls,
        payment_citystates: Union[list[CityState], None],
        conflict_citystates: Union[list[CityState], None],
    ) -> bool:

        return any(
            payment_citystate.citystate_matches(
                citystate=conflict_citystate
            )
            for payment_citystate in payment_citystates
            for conflict_citystate in conflict_citystates
            if (
                pd.notna(payment_citystate)
                and pd.notna(conflict_citystate)
            )
        )

    @staticmethod
    def get_full_citystate_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        return payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(
                lambda x: PaymentFilters.CITYSTATE in x
            )
        ]

    @staticmethod
    def get_citystate_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a DataFrame for city/state matches in order of priority:
        1. City/State match
        2. State match
        3. City match
    """
        refined_matches = payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(
                lambda x: PaymentFilters.CITYSTATE in x
            )
        ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.STATE in x
                )
            ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.CITY in x
                )
            ]

        return refined_matches

    def convert_merged_dtypes(
        self,
        merged: pd.DataFrame,
    ) -> pd.DataFrame:
        """Updates  payments and conflicteds columns into lists after
        they are loaded as strs in CSVs and Excel files."""

        merged = super().convert_merged_dtypes(merged)
        merged["citystates"] = merged["citystates"].apply(
            lambda x: convert_citystates(x) if isinstance(x, str) else x
        )

        merged["conflict_citystates"] = merged["conflict_citystates"].apply(
            lambda x: convert_citystates(x) if isinstance(x, str) else x
        )
        return merged
