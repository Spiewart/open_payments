import pandas as pd

from .citystates import ConflictCityStates
from .credentials import ConflictCredentials
from .names import ConflictNames
from .npi import ConflictNPI
from .specialtys import ConflictSpecialtys


class Conflicteds(
    ConflictNPI,
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

        # NPI has no dependencies on other columns and is the canonical
        # provider identifier, so parse it first. Tolerates child apps
        # without an NPI column (adds an all-NA `npi` column for them).
        self.conflicts = self.conflict_npi()

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

        # Drop deans-style provenance columns when present. Child apps
        # without these columns simply skip the drop — the canonical output
        # only needs the conflict_* columns plus provider_pk.
        self.conflicts = self.conflicts.drop(
            columns=[
                "article",
                "rank",
                "entity",
            ],
            errors="ignore",
        )

        self.conflicts.insert(0, "provider_pk", value=range(len(self.conflicts)))

        # Bug 1 fix: previously called set_index("provider_pk") and discarded
        # the result. Downstream code (`search_for_conflicteds_ids` rename
        # loop, filter_by_* methods, add_unique_id) consumes `provider_pk` as
        # a column, not an index, so the set_index call was both wrong AND
        # silently a no-op. Removed.

        return self.conflicts

    def remove_non_us(self) -> pd.DataFrame:
        """Drop rows flagged as non-US institutions, then drop the
        ``non_us`` column.

        The ``non_us`` column is a deans-style flag — child apps without
        this column simply skip the filter. The ``non_us`` semantic is:
        non-null value = "this row is at a non-US institution, drop it";
        null value = "this row is at a US institution, keep it".
        """
        if "non_us" not in self.conflicts.columns:
            return self.conflicts
        self.conflicts = self.conflicts[self.conflicts["non_us"].isna()]
        self.conflicts = self.conflicts.drop(columns=["non_us"])
        return self.conflicts

    def remove_non_md_do(self) -> pd.DataFrame:
        """Removes conflicteds who don't have credentials."""

        self.conflicts = self.conflicts[
            self.conflicts["credentials"].apply(lambda x: x is not None)
        ]

        return self.conflicts
