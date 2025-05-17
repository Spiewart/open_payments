import re
from typing import Type, Union

import pandas as pd

from .choices import PaymentFilters
from .helpers import ColumnMixin, str_in_str


class NamesMixin(ColumnMixin):

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols = super().general_columns
        cols.update(
            {
                "Covered_Recipient_Last_Name": ("last_name", str),
                "Covered_Recipient_First_Name": ("first_name", str),
                "Covered_Recipient_Middle_Name": ("middle_name", str),
            }
        )
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols = super().ownership_columns
        cols.update(
            {
                "Physician_First_Name": ("first_name", str),
                "Physician_Last_Name": ("last_name", str),
                "Physician_Middle_Name": ("middle_name", str),
            }
        )
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols = super().research_columns
        cols.update(
            self.general_columns
        )
        return cols


class PaymentIDsNames(NamesMixin):
    """Filters OpenPayments data by first, middle, and last names."""

    @property
    def filters(self) -> list[PaymentFilters]:
        """Adds first, middle, and last name filters to the list of filters."""
        filters: list[PaymentFilters] = super().filters
        filters.append(PaymentFilters.FIRSTNAME)
        filters.append(PaymentFilters.FIRSTNAME_PARTIAL)
        filters.append(PaymentFilters.FIRST_MIDDLE_NAME)
        filters.append(PaymentFilters.MIDDLE_INITIAL)
        filters.append(PaymentFilters.MIDDLENAME)
        return filters

    @classmethod
    def merge_by_last_name(
        cls,
        payments: pd.DataFrame,
        conflicted: pd.Series,
    ) -> pd.DataFrame:
        """Merges the payments DataFrame with the conflicted provider
        Series by last name. Returns a DataFrame of payments
        that match the conflicted provider's last name."""

        print(f"Merging Payments df with Conflicted df for {conflicted['last_name']}...")

        merged_payments = payments[
            payments["last_name"].str.lower()
            == conflicted["last_name"].lower()
        ]

        # If no last name matches are found, some last names contain
        # multiple last names, so we can check if the conflicted last
        # name is in the payments last name
        if merged_payments.empty:
            # Split the last name by hyphen and whitespace
            conflicted_last_names = re.split(
                r"-|\s+",
                conflicted["last_name"].lower()
            )

            # Check if any of potentially multiple last names
            # are in the payments last name
            merged_payments = payments[
                payments["last_name"].str.contains(
                    '|'.join(conflicted_last_names),
                    na=False,
                    case=False,
                )
            ]
            if not merged_payments.empty:
                # If there are multiple last names, select payments
                # that match all the last names to avoid false positives
                double_matches = merged_payments[
                    merged_payments["last_name"].str.contains(
                        '&'.join(conflicted_last_names),
                        na=False,
                        case=False,
                    )
                ]
                if not double_matches.empty:
                    merged_payments = double_matches

        if merged_payments.empty:
            return merged_payments

        conflicted_df = pd.concat(
            [conflicted.drop("last_name")] * len(merged_payments),
            axis=1,
        ).T.set_index(merged_payments.index)

        merged = pd.concat(
            [
                merged_payments,
                conflicted_df,
            ],
            axis=1,
        )

        merged.insert(0, "filters", [[PaymentFilters.LASTNAME]] * len(merged))

        return merged

    @staticmethod
    def get_firstname_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a DataFrame for first name matches in order of priority:
        1. First name match
        2. First name partial match
        3. First name and middle name match
    """
        refined_matches = payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(
                lambda x: PaymentFilters.FIRSTNAME in x
            )
        ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.FIRSTNAME_PARTIAL in x
                )
            ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.FIRST_MIDDLE_NAME in x
                )
            ]

        return refined_matches

    @staticmethod
    def get_middlename_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a DataFrame for middle name matches in order of priority:
        1. Middle name match
        2. Middle initial match
    """
        refined_matches = payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(
                lambda x: PaymentFilters.MIDDLENAME in x
            )
        ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.MIDDLE_INITIAL in x
                )
            ]

        return refined_matches

    @classmethod
    def filter_by_firstname(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Checks if a payment_x_conflicted series has a match
        in its first_name and conflict_first_name columns and adds a
        filter to the filters column to indicate as such if so."""

        value = (
            pd.notna(payments_x_conflicted["first_name"])
            and pd.notna(payments_x_conflicted["conflict_first_name"])
            and (
                payments_x_conflicted["first_name"].lower()
                == payments_x_conflicted["conflict_first_name"].lower()
            )
        )
        # Full match should supercede a partial match or first/middle name match
        if value:
            if PaymentFilters.FIRSTNAME_PARTIAL in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(
                    PaymentFilters.FIRSTNAME_PARTIAL
                )
            if PaymentFilters.FIRST_MIDDLE_NAME in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(
                    PaymentFilters.FIRST_MIDDLE_NAME
                )

        return value

    @classmethod
    def filter_by_firstname_partial(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Checks if a payment_x_conflicted series has a match
        in its first_name and conflict_first_name columns and adds a
        filter to the filters column to indicate as such if so."""

        value = (
            pd.notna(payments_x_conflicted["first_name"])
            and pd.notna(payments_x_conflicted["conflict_first_name"])
            and PaymentFilters.FIRSTNAME not in payments_x_conflicted[
                "filters"
            ]
            and (
                str_in_str(
                        payments_x_conflicted["first_name"],
                        payments_x_conflicted["conflict_first_name"]
                    )
                or str_in_str(
                    payments_x_conflicted["conflict_first_name"],
                    payments_x_conflicted["first_name"]
                )
            )
        )
        # Partial match should supercede first / middle name match
        if value and PaymentFilters.FIRST_MIDDLE_NAME in payments_x_conflicted["filters"]:
            payments_x_conflicted["filters"].remove(
                PaymentFilters.FIRST_MIDDLE_NAME
            )

        return value

    @classmethod
    def filter_by_first_middle_name(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Checks if a payment_x_conflicted series has a match
        in its OpenPayments first_name and any of its conflicts
        middle name columns, or vice versa (conflict_first_name
        and OpenPayments middle_name)."""

        return (
            PaymentFilters.FIRSTNAME not in payments_x_conflicted[
                "filters"
            ] and
            PaymentFilters.FIRSTNAME_PARTIAL not in payments_x_conflicted[
                "filters"
            ]
        ) and (
            # Conflict first_name and OpenPayments middle_name
            (
                pd.notna(payments_x_conflicted["middle_name"])
                and pd.notna(payments_x_conflicted["conflict_first_name"])
                and (
                    payments_x_conflicted["middle_name"].lower()
                    == payments_x_conflicted["conflict_first_name"].lower()
                )
            )
            # Conflict middle_names and OpenPayments first_name
            or (
                (
                    pd.notna(payments_x_conflicted["conflict_middle_name_1"])
                    and pd.notna(payments_x_conflicted["first_name"])
                    and (
                        payments_x_conflicted["first_name"].lower()
                        == payments_x_conflicted["conflict_middle_name_1"].lower()
                    )
                ) or (
                    pd.notna(payments_x_conflicted["conflict_middle_name_2"])
                    and pd.notna(payments_x_conflicted["first_name"])
                    and (
                        payments_x_conflicted["first_name"].lower()
                        == payments_x_conflicted["conflict_middle_name_2"].lower()
                    )
                )
            )
        )

    @classmethod
    def filter_by_middle_initial(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by middle initial."""

        return (
            cls.middle_initial_match(
                conflicted_middle_initial_1=payments_x_conflicted[
                    "conflict_middle_initial_1"
                    ],
                conflicted_middle_initial_2=payments_x_conflicted[
                    "conflict_middle_initial_2"
                ],
                conflicted_middle_name_1=payments_x_conflicted[
                    "conflict_middle_name_1"
                ],
                conflicted_middle_name_2=payments_x_conflicted[
                    "conflict_middle_name_2"
                ],
                payment_middle_name=payments_x_conflicted["middle_name"],
            )
        )

    @staticmethod
    def middle_initial_match(
        conflicted_middle_initial_1: Union[str, None],
        conflicted_middle_initial_2: Union[str, None],
        conflicted_middle_name_1: Union[str, None],
        conflicted_middle_name_2: Union[str, None],
        payment_middle_name: Union[str, None],
    ) -> bool:
        """Checks if the middle initial matches."""

        return pd.notna(payment_middle_name) and (
            (
                pd.notna(conflicted_middle_initial_1)
                and (
                    payment_middle_name[0].lower()
                    == conflicted_middle_initial_1.lower()
                )
            )
            or (
                pd.notna(conflicted_middle_initial_2)
                and (
                    payment_middle_name[0].lower()
                    == conflicted_middle_initial_2.lower()
                )
            )
            or (
                pd.notna(conflicted_middle_name_1)
                and (
                    payment_middle_name.lower()
                    == conflicted_middle_name_1[0].lower()
                )
            )
            or (
                pd.notna(conflicted_middle_name_2)
                and (
                    payment_middle_name.lower()
                    == conflicted_middle_name_2[0].lower()
                )
            )
        )

    @classmethod
    def filter_by_middlename(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by middle name."""

        return (
            cls.middlename_match(
                conflicted_middle_name_1=payments_x_conflicted[
                    "conflict_middle_name_1"
                    ],
                conflicted_middle_name_2=payments_x_conflicted[
                    "conflict_middle_name_2"
                ],
                payment_middle_name=payments_x_conflicted["middle_name"],
            )
        )

    @staticmethod
    def middlename_match(
        conflicted_middle_name_1: Union[str, None],
        conflicted_middle_name_2: Union[str, None],
        payment_middle_name: Union[str, None],
    ) -> bool:
        """Checks if the middle name matches."""

        return pd.notna(payment_middle_name) and (
            (
                pd.notna(conflicted_middle_name_1)
                and (
                    payment_middle_name.lower()
                    == conflicted_middle_name_1.lower()
                )
            )
            or (
                pd.notna(conflicted_middle_name_2)
                and (
                    payment_middle_name.lower()
                    == conflicted_middle_name_2.lower()
                )
            )
        )
