"""City + state handling for both CMS Open Payments data and conflicted-provider input.

Three concerns, separated by stability:

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  1. `CityState` model + low-level state helpers  —  used by both sides  │
  │     Pydantic model with abbreviation/full-name canonicalization,        │
  │     `state_matches`, `citystate_matches`.                               │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  2. CMS / OpenPayments side  —  STABLE, NOT INTENDED TO BE OVERRIDDEN   │
  │     `CityStatesMixin`, `PaymentCityStates`, `PaymentIDsCityStatesMixin` │
  │     CMS publishes `Recipient_City` + `Recipient_State` (plus            │
  │     `Covered_Recipient_License_State_code1..5` for general/research).   │
  │     The library encodes that layout once.                               │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  3. Conflicted side  —  GENERIC DEFAULT, EXPECTED TO BE OVERRIDDEN      │
  │     `parse_citystate_string`, `parse_citystates_string`,                │
  │     `ConflictCityStates`                                                │
  │     The default handles a single ``citystates`` column with one or more │
  │     "City, State" strings (semicolon- or pipe-separated). Child apps    │
  │     with structured columns (deans: separate `City` + `State` columns), │
  │     institution-only data (uptodate: needs lookup or LLM), or HTML      │
  │     scrapes (abim: walks `<p>` siblings against `Major_US_Cities`)      │
  │     subclass `ConflictCityStates` to plug in project-specific parsing.  │
  └─────────────────────────────────────────────────────────────────────────┘
"""

import re
from typing import ClassVar, Union

import pandas as pd
from pydantic import BaseModel, model_validator
from typing_extensions import Self

from .choices import FilterOutcome, PaymentFilters, States
from .conflicts import Conflicts
from .names import is_blank
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
                return (
                    next(
                        iter(
                            state.name
                            for state in self.States.__members__.values()
                            if state == self.state
                        ),
                        None,
                    )
                    if self.state
                    else None
                )
            except KeyError as e:
                raise ValueError(f"State {self.state} is not a valid state.") from e

    @classmethod
    def state_is_full_name(cls, state: str) -> bool:

        return state in cls.States.__members__.values()

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

        return self.city == citystate.city and self.state_matches(state=citystate.state)

    def __str__(self) -> str:
        return f"{self.city}|{self.state}"

    @model_validator(mode="after")
    def validate_city_state(self) -> Self:
        if self.city is None and self.state is None:
            raise ValueError("Both city and state cannot be None.")
        return self


# ---------------------------------------------------------------------------
# CMS / OpenPayments side  —  STABLE, GENERIC, NOT INTENDED TO BE OVERRIDDEN.
# CMS publishes city + state already split into columns (`Recipient_City`,
# `Recipient_State`, plus license-state codes for general/research). The
# mixins below register those columns and apply CityState canonicalization.
# ---------------------------------------------------------------------------


class CityStatesMixin:
    @property
    def general_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols = super().general_columns
        cols.update(
            {
                "Recipient_City": ("city", str),
                "Recipient_State": ("state_primary", str),
                "Covered_Recipient_License_State_code1": ("state_license_1", str),
                "Covered_Recipient_License_State_code2": ("state_license_2", str),
                "Covered_Recipient_License_State_code3": ("state_license_3", str),
                "Covered_Recipient_License_State_code4": ("state_license_4", str),
                "Covered_Recipient_License_State_code5": ("state_license_5", str),
            }
        )
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols = super().ownership_columns
        cols.update(
            {
                "Recipient_City": ("city", str),
                "Recipient_State": ("state_primary", str),
            }
        )
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols = super().research_columns

        cols.update(self.general_columns)
        return cols


class PaymentCityStates(CityStatesMixin, ReadPayments):
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

        states = (
            payment[
                [
                    "state_primary",
                    "state_license_1",
                    "state_license_2",
                    "state_license_3",
                    "state_license_4",
                    "state_license_5",
                ]
            ]
            .dropna()
            .unique()
        )

        city = payment["city"]

        if pd.isna(city):
            city = None

        return (
            [CityState(city=city, state=state) for state in states]
            if len(states) > 0
            else [CityState(city=city, state=None)]
            if pd.notna(city)
            else []
        )

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
            city=citystate[0] if (citystate[0] != "None" and citystate[0] != "Nan") else None,
            state=citystate[1] if (citystate[1] != "None" and citystate[1] != "Nan") else None,
        )
        converted.append(citystate)

    return converted


class PaymentIDsCityStatesMixin(CityStatesMixin):
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
    ) -> FilterOutcome:
        """City-list intersection. Returns:
        - NO_DATA when CITYSTATE already matched (superseded — city is
          redundant when the full city+state pair agreed) OR when either
          side lacks any city values.
        - MATCH when at least one city value overlaps.
        - DISAGREE when both sides have cities AND none overlap.
        """
        if PaymentFilters.CITYSTATE in payments_x_conflicted["filters"]:
            return FilterOutcome.NO_DATA  # superseded
        payment_cities = [
            cs.city
            for cs in (payments_x_conflicted["citystates"] or [])
            if pd.notna(cs) and pd.notna(cs.city)
        ]
        conflict_cities = [
            cs.city
            for cs in (payments_x_conflicted["conflict_citystates"] or [])
            if pd.notna(cs) and pd.notna(cs.city)
        ]
        if not payment_cities or not conflict_cities:
            return FilterOutcome.NO_DATA
        return (
            FilterOutcome.MATCH
            if any(c in payment_cities for c in conflict_cities)
            else FilterOutcome.DISAGREE
        )

    @classmethod
    def payment_conflict_city_match(
        cls,
        payment_citystates: Union[list[CityState], None],
        conflict_citystates: Union[list[CityState], None],
    ) -> bool:

        return any(
            citystate.city
            in [
                citystate.city
                for citystate in payment_citystates
                if (pd.notna(citystate) and pd.notna(citystate.city))
            ]
            for citystate in [
                citystate
                for citystate in conflict_citystates
                if (pd.notna(citystate) and pd.notna(citystate.city))
            ]
        )

    @classmethod
    def filter_by_state(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> FilterOutcome:
        """State-list intersection. Returns:
        - NO_DATA when CITYSTATE already matched OR when either side has
          no states.
        - MATCH when at least one state matches via
          :meth:`CityState.state_matches` (handles abbreviation ↔ full-name
          canonicalization).
        - DISAGREE when both sides have states AND none match.
        """
        if PaymentFilters.CITYSTATE in payments_x_conflicted["filters"]:
            return FilterOutcome.NO_DATA  # superseded
        payment_citystates = [
            cs
            for cs in (payments_x_conflicted["citystates"] or [])
            if pd.notna(cs) and pd.notna(cs.state)
        ]
        conflict_citystates = [
            cs
            for cs in (payments_x_conflicted["conflict_citystates"] or [])
            if pd.notna(cs) and pd.notna(cs.state)
        ]
        if not payment_citystates or not conflict_citystates:
            return FilterOutcome.NO_DATA
        matched = any(
            payment_cs.state_matches(conflict_cs.state)
            for payment_cs in payment_citystates
            for conflict_cs in conflict_citystates
        )
        return FilterOutcome.MATCH if matched else FilterOutcome.DISAGREE

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
    ) -> FilterOutcome:
        """Full city+state pair match. Returns:
        - MATCH when at least one CityState pair agrees on both city AND
          state. Supersedes weaker CITY / STATE labels (removes them from
          the row's filters list).
        - DISAGREE when both sides have CityState entries AND none fully
          agree on both dimensions.
        - NO_DATA when either side has no CityState entries.
        """
        payment_citystates = payments_x_conflicted["citystates"] or []
        conflict_citystates = payments_x_conflicted["conflict_citystates"] or []
        if not payment_citystates or not conflict_citystates:
            return FilterOutcome.NO_DATA

        matched = cls.payment_conflict_citystate_match(
            payment_citystates=payment_citystates,
            conflict_citystates=conflict_citystates,
        )
        if matched:
            if PaymentFilters.CITY in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(PaymentFilters.CITY)
            if PaymentFilters.STATE in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(PaymentFilters.STATE)
            return FilterOutcome.MATCH
        return FilterOutcome.DISAGREE

    @classmethod
    def payment_conflict_citystate_match(
        cls,
        payment_citystates: Union[list[CityState], None],
        conflict_citystates: Union[list[CityState], None],
    ) -> bool:

        return any(
            payment_citystate.citystate_matches(citystate=conflict_citystate)
            for payment_citystate in payment_citystates
            for conflict_citystate in conflict_citystates
            if (pd.notna(payment_citystate) and pd.notna(conflict_citystate))
        )

    @staticmethod
    def get_full_citystate_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        return payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(lambda x: PaymentFilters.CITYSTATE in x)
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
            payments_x_conflicteds["filters"].apply(lambda x: PaymentFilters.CITYSTATE in x)
        ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(lambda x: PaymentFilters.STATE in x)
            ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(lambda x: PaymentFilters.CITY in x)
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


# ---------------------------------------------------------------------------
# Conflicted side  —  GENERIC DEFAULT, EXPECTED TO BE OVERRIDDEN.
#
# This is the most provenance-divergent of the four domains. Real-world child
# apps observed so far:
#
#   - deans_conflicts:    separate ``City`` + ``State`` columns -> direct.
#   - uptodate_conflicts: single ``institution`` column -> needs LLM lookup.
#   - abim_conflicts:     HTML ``<p>`` siblings -> scans against a known cities
#                         enum.
#
# None of these can be the parent-repo default. The default below handles the
# simplest ad-hoc shape — a free-text ``citystates`` column containing one or
# more "City, State" strings separated by ``;`` or ``|``. Override for richer
# inputs.
# ---------------------------------------------------------------------------


# Delimiters for multi-location strings, e.g. "Boston, MA | New York, NY".
_LOCATION_DELIMS_RE = re.compile(r"[;|]+")


def parse_citystate_string(s: Union[str, None]) -> Union[CityState, None]:
    """**Conflicted-side helper.** Parse a single ``"City, State"`` (or
    ``"City"`` or ``"State"``) string into a :class:`CityState`.

    Behavior:
      - ``"Boston, MA"``       -> ``CityState(city="Boston", state="MA")``
      - ``"Boston, Massachusetts"`` -> ``CityState(city="Boston", state="Massachusetts")``
      - ``"Boston"``            -> ``CityState(city="Boston")`` (single token,
        not a known state)
      - ``"MA"``                -> ``CityState(state="MA")`` (known state abbrev)
      - ``"Massachusetts"``     -> ``CityState(state="Massachusetts")`` (known full)
      - Blank / None / empty parts -> ``None``

    For multi-location strings, use :func:`parse_citystates_string`.
    """
    if is_blank(s):
        return None
    parts = [p.strip() for p in s.split(",")]
    if len(parts) >= 2:
        city = parts[0] or None
        state = parts[1] or None
        if not city and not state:
            return None
        return CityState(city=city, state=state)

    single = parts[0]
    if not single:
        return None
    if CityState.state_is_abbrev(single) or CityState.state_is_full_name(single):
        return CityState(state=single)
    return CityState(city=single)


def parse_citystates_string(s: Union[str, None]) -> list[CityState]:
    """**Conflicted-side helper.** Parse a multi-location string into a list
    of :class:`CityState`. Splits on ``;`` or ``|`` before parsing each
    segment with :func:`parse_citystate_string`. Blank or unparseable
    segments are skipped (not raised) so a partial input still yields what
    info is parseable.
    """
    if is_blank(s):
        return []
    segments = _LOCATION_DELIMS_RE.split(s)
    result: list[CityState] = []
    for seg in segments:
        cs = parse_citystate_string(seg)
        if cs is not None:
            result.append(cs)
    return result


class ConflictCityStates(Conflicts):
    """Default citystates mixin for raw conflicted-provider input.

    DESIGN NOTE: **Conflicted-side** parser. The default assumes a single
    ``citystates`` column with free-text like ``"Boston, MA"`` or
    ``"Boston, MA | New York, NY"`` (one or more "City, State" entries,
    separated by ``;`` or ``|``). Override when the input shape differs:

      - **Separate columns** (deans: ``City`` + ``State``): override
        ``get_citystates`` to read both columns.
      - **Institution-only** (uptodate: ``"Brigham and Women's Hospital"``):
        override ``get_citystates`` with project-specific lookup (gazetteer,
        LLM, etc.) — too project-specific to be the parent default.
      - **HTML scrape** (abim: ``<p>`` siblings): override ``conflict_citystates``
        to walk the structured source before falling back to parsing.

    Override surface:
      - ``CITYSTATES_COLUMN`` class var — name of the free-text input column.
      - ``get_citystates(row)`` — per-row override.
      - ``conflict_citystates()`` — full-pipeline override.

    Output: a ``citystates`` column of ``list[CityState]``. The source column
    is dropped after parsing.
    """

    CITYSTATES_COLUMN: ClassVar[str] = "citystates"

    def conflict_citystates(self) -> pd.DataFrame:
        """Apply ``get_citystates`` to every row, replace the ``citystates``
        column with the parsed list (or drop a differently-named source
        column afterward)."""
        self.conflicts = self.conflicts.copy()
        parsed = self.conflicts.apply(self.get_citystates, axis=1)

        if (
            self.CITYSTATES_COLUMN != "citystates"
            and self.CITYSTATES_COLUMN in self.conflicts.columns
        ):
            self.conflicts = self.conflicts.drop(columns=[self.CITYSTATES_COLUMN])
        self.conflicts["citystates"] = parsed.values
        return self.conflicts

    @classmethod
    def get_citystates(cls, row: pd.Series) -> list[CityState]:
        """Parse the row's source column into ``list[CityState]``. Returns
        ``[]`` for blank inputs so a row with no location info still produces
        a valid (empty) list rather than raising."""
        if cls.CITYSTATES_COLUMN not in row.index:
            return []
        return parse_citystates_string(row[cls.CITYSTATES_COLUMN])
