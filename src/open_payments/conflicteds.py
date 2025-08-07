import pandas as pd

from .citystates import ConflictCityStates
from .credentials import ConflictCredentials
from .names import ConflictNames
from .specialtys import ConflictSpecialtys


class Conflicteds(
    ConflictCredentials,
    ConflictSpecialtys,
    ConflictCityStates,
    ConflictNames,
):

    def us_conflicteds_id_search_df(self) -> pd.DataFrame:
        self.conflicts = self.remove_non_us()

        self.conflicts = self.conflicteds_id_search_df()

        return self.conflicts

    def conflicteds_id_search_df(self) -> pd.DataFrame:
        """Method that takes the conflicteds DataFrame and modifies it to be
        used for searching the OpenPayments payment data for a unique ID."""

        self.conflicts = self.conflict_credentials()

        self.conflicts = self.remove_non_md_do()

        # conflict_names should be called AFTER conflict_credentials
        # because the latter uses the "name" column and the former
        # drops it

        # conflict_names should also be called as soon as possible
        # because it will allow dropping duplicates without
        # losing information
        self.conflicts = self.conflict_names()

        middle_name_components = [
            "middle_initial_1",
            "middle_initial_2",
            "middle_name_1",
            "middle_name_2",
        ]

        for component in middle_name_components:
            self.conflicts.drop_duplicates(
                subset=[
                    "first_name",
                    "last_name",
                    component,
                ],
                inplace=True,
            )

        self.conflicts = self.conflict_citystates()

        self.conflicts = self.conflict_specialtys()

        self.conflicts = self.conflicts.drop(
            columns=["article", "rank", "entity", ]
        )

        self.conflicts.insert(0, "provider_pk", value=range(len(self.conflicts)))

        self.conflicts.set_index("provider_pk")

        return self.conflicts

    def remove_non_us(self) -> pd.DataFrame:
        """Method that removes non-US institutions from the conflicteds DataFrame."""

        self.conflicts = self.conflicts[self.conflicts["non_us"].isna()]

        self.conflicts = self.conflicts.drop(columns=["non_us"])

        return self.conflicts

    def remove_non_md_do(self) -> pd.DataFrame:
        """Removes conflicteds who don't have credentials."""

        self.conflicts = self.conflicts[
            self.conflicts["credentials"].apply(
                lambda x: x is not None
            )
        ]

        return self.conflicts
