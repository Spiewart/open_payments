import re
from typing import Type, Union

import pandas as pd
from pydantic import BaseModel, model_validator
from typing_extensions import Self

from .choices import PaymentFilters
from .helpers import ColumnMixin, get_file_suffix, open_payments_directory
from .read import ReadPayments


class Specialtys(BaseModel):
    """Class that contains the specialtys of a payment."""

    specialty: str | None = None
    subspecialty: str | None = None

    def __str__(self) -> str:
        return f"{self.specialty}|{self.subspecialty}"

    @model_validator(mode="after")
    def validate_specialty_subspecialty(self) -> Self:
        if self.specialty is None and self.subspecialty is None:
            raise ValueError("Both specialty and subspecialty cannot be None.")
        return self


class SpecialtysMixin(ColumnMixin):

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols: dict[str, tuple[str, Union[Type[str], str]]] = super().general_columns
        cols.update({
                "Covered_Recipient_Specialty_1": ("specialty_1", str),
                "Covered_Recipient_Specialty_2": ("specialty_2", str),
                "Covered_Recipient_Specialty_3": ("specialty_3", str),
                "Covered_Recipient_Specialty_4": ("specialty_4", str),
                "Covered_Recipient_Specialty_5": ("specialty_5", str),
                "Covered_Recipient_Specialty_6": ("specialty_6", str),
        })
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols: dict[str, tuple[str, Union[Type[str], str]]] = super().ownership_columns
        cols.update({
                "Physician_Specialty": ("specialty_1", str),
        })
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[Type[str], str]]]:

        cols: dict[str, tuple[str, Union[Type[str], str]]] = super().research_columns

        cols.update(
            self.general_columns
        )
        return cols


class PaymentSpecialtys(ReadPayments, SpecialtysMixin):

    def create_unique_specialtys_excel(self, path: Union[str, None] = None) -> None:
        path = open_payments_directory() if path is None else path

        unique_specialtys = self.unique_specialtys()

        MD_DO = unique_specialtys[unique_specialtys["provider_type"].str.contains(
            "Allopathic & Osteopathic Physicians",
            case=False,
            na=False
        )]

        MD_DO.drop("provider_type", axis=1, inplace=True)

        file_suffix = get_file_suffix(self.years, self.payment_classes)

        with pd.ExcelWriter(
            f"{path}/unique_specialtys{file_suffix}.xlsx",
            engine="openpyxl",
        ) as writer:
            unique_specialtys.to_excel(writer, sheet_name="unique_specialtys", index=False)
            MD_DO.to_excel(writer, sheet_name="MD_DO", index=False)

    def unique_specialtys(self) -> pd.Series:
        """Returns a Series of unique specialties from OpeyPayments payment datasets."""

        self.general_payments = self.read_general_payments_csvs(
            usecols=self.general_columns.keys(),
            dtype={key: value[1] for key, value in self.general_columns.items()},
        )
        self.general_payments = self.update_payments("general")

        self.ownership_payments = self.read_ownership_payments_csvs(
            usecols=self.ownership_columns.keys(),
            dtype={key: value[1] for key, value in self.ownership_columns.items()},
        )
        self.ownership_payments = self.update_payments("ownership")

        self.research_payments = self.read_research_payments_csvs(
            usecols=self.research_columns.keys(),
            dtype={key: value[1] for key, value in self.research_columns.items()},
        )
        self.research_payments = self.update_payments("research")

        all_payments = pd.concat([
            self.general_payments, self.ownership_payments, self.research_payments
        ])

        unique_specialty_1_6 = all_payments.drop_duplicates(
            subset=[
                "specialty_1",
                "specialty_2",
                "specialty_3",
                "specialty_4",
                "specialty_5",
                "specialty_6",
            ]
        )

        all_specialtys = self.get_all_specialtys(unique_specialty_1_6)

        all_specialtys = all_specialtys.dropna()

        return all_specialtys.reset_index(drop=True)

    @classmethod
    def get_all_specialtys(cls, df: pd.DataFrame) -> pd.Series:
        """Returns a Series of all Specialtys from the DataFrame."""

        df.insert(
            1,
            "specialty",
            df.apply(cls.specialtys_strs, axis=1),
        )
        df = cls.drop_individual_specialtys(df)

        return df["specialty"]

    @classmethod
    def specialtys(cls, payments: pd.DataFrame) -> pd.DataFrame:
        """Method that combines the specialtys into a Series"""

        payments.insert(
            1,
            "specialtys",
            payments.apply(cls.create_specialtys, axis=1),
        )

        payments = cls.drop_individual_specialtys(payments)

        return payments

    @classmethod
    def specialtys_strs(cls, payment: pd.Series) -> pd.DataFrame:
        """Aggregates the different specialties into a series of
        specialty/subspecialty pairs."""

        specialtys = payment[
            [
                "specialty_1",
                "specialty_2",
                "specialty_3",
                "specialty_4",
                "specialty_5",
                "specialty_6",
            ]
        ]
        specialtys = specialtys.dropna()

        specialtys = specialtys.apply(
            cls.parse_specialty,
        )

        return specialtys

    @classmethod
    def create_specialtys(cls, payment: pd.Series) -> list[Specialtys]:
        """Returns a list of Specialtys from a single payment's
        specialty_1-6 columns."""

        specialtys = payment[
            [
                "specialty_1",
                "specialty_2",
                "specialty_3",
                "specialty_4",
                "specialty_5",
                "specialty_6",
            ]
        ]

        specialtys = cls.parse_specialtys(specialtys)

        specialtys = specialtys.drop("provider_type", axis=1)
        specialtys = specialtys.dropna(how="all", subset=["specialty", "subspecialty"])
        specialtys = specialtys.drop_duplicates()
        specialtys.reset_index(drop=True, inplace=True)

        return [
                Specialtys(
                    specialty=x["specialty"],
                    subspecialty=x["subspecialty"],
                )
                for _, x in specialtys.iterrows()
        ]

    @staticmethod
    def parse_specialtys(
        specialtys: pd.Series,
    ) -> pd.DataFrame:
        """Parses a series of a single payment's
        specialties strings into a DataFrame. Returns an
        empty DataFrame with columns
        ["provider_type", "specialty", "subspecialty"]
        retained to avoid downstream errors."""

        specialtys = specialtys.str.split("|")
        specialtys = specialtys.dropna()

        specialtys = pd.DataFrame(
            {
                "provider_type": specialty[0],
                "specialty": specialty[1] if len(specialty) > 1 else None,
                "subspecialty": specialty[2] if len(specialty) > 2 else None,
            }
            for specialty in specialtys if specialty is not None
        ) if specialtys.size > 0 else pd.DataFrame(
            columns=["provider_type", "specialty", "subspecialty"]
        )

        return specialtys

    @staticmethod
    def drop_individual_specialtys(payments: pd.DataFrame) -> pd.DataFrame:
        """Removes specialty_1-6 columns from the DataFrame."""

        payments = payments.drop(
            columns=[
                "specialty_1",
                "specialty_2",
                "specialty_3",
                "specialty_4",
                "specialty_5",
                "specialty_6",
            ],
        )

        return payments

    def update_ownership_payments(self) -> pd.DataFrame:
        """Overwritten to add specialty_2-6 to the DataFrame, as they
        won't be present after renaming pre-existing columns."""

        self.ownership_payments["specialty_2"] = None
        self.ownership_payments["specialty_3"] = None
        self.ownership_payments["specialty_4"] = None
        self.ownership_payments["specialty_5"] = None
        self.ownership_payments["specialty_6"] = None

        self.ownership_payments = super().update_ownership_payments()

        return self.ownership_payments


def convert_specialtys(specialtys: str) -> list[Specialtys]:
    """Convert a string representation of a list of Specialtys objects
    to a list of Specialtys objects."""

    converted = []

    specialtys_list = re.findall(r"Specialtys\(specialty='(.*?)', subspecialty=('.*?'|None)\)", specialtys)

    for specialty in specialtys_list:
        specialty = Specialtys(
            specialty=specialty[0],
            subspecialty=specialty[1].strip("'") if specialty[1] != 'None' else None,
        )
        converted.append(specialty)

    return converted


def unique_specialties() -> None:
    """Creates an Excel file containing unique specialties."""

    PaymentSpecialtys(nrows=None, years=2023).create_unique_specialtys_excel()


class PaymentIDsSpecialtys(SpecialtysMixin):
    """Filters OpenPayments payments by specialty."""

    @property
    def filters(self) -> list[PaymentFilters]:
        filters: list[PaymentFilters] = super().filters
        filters.append(PaymentFilters.SPECIALTY)
        filters.append(PaymentFilters.SUBSPECIALTY)
        filters.append(PaymentFilters.FULLSPECIALTY)
        return filters

    @classmethod
    def filter_by_specialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Checks for specialty match between the payment and conflicted
        provider. If there is a match, it appends the filter to the
        filters list."""

        value = (
            cls.payment_conflict_specialty_match(
                payment_specialtys=payments_x_conflicted["specialtys"],
                conflict_specialtys=payments_x_conflicted[
                    "conflict_specialtys"
                ],
            )
        )

        if value and PaymentFilters.FULLSPECIALTY not in payments_x_conflicted[
            "filters"
        ]:
            return value
        return False

    @classmethod
    def payment_conflict_specialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:
        """Checks if the specialtys exist and match."""

        return any(
            cls.specialty_str_matcher(
                payment_specialty=payment_specialty.specialty,
                conflict_specialty=conflict_specialty.specialty,
            ) for payment_specialty in payment_specialtys
            for conflict_specialty in conflict_specialtys
        )

    @staticmethod
    def specialty_str_matcher(
        payment_specialty: Union[str, None],
        conflict_specialty: Union[str, None],
    ) -> bool:
        """Checks if the specialtys exist and match."""

        payment_specialty = payment_specialty.lower() if pd.notna(
            payment_specialty
        ) else None
        conflict_specialty = conflict_specialty.lower() if pd.notna(
            conflict_specialty
        ) else None

        # If either specialty is None, return False
        if payment_specialty is None or conflict_specialty is None:
            return False
        # If both specialties are the same, return True
        elif payment_specialty == conflict_specialty:
            return True

        payment_specialty_strs = payment_specialty.split(" ")

        conflict_specialty_strs = conflict_specialty.split(" ")

        # Remove "medicine" from the specialty strings, as it is non-specific
        if "medicine" in payment_specialty_strs:
            payment_specialty_strs.remove("medicine")
        if "medicine" in conflict_specialty_strs:
            conflict_specialty_strs.remove("medicine")

        return any(
            payment_str in conflict_specialty_strs
            for payment_str in payment_specialty_strs
        ) or any(
            conflict_str in payment_specialty_strs
            for conflict_str in conflict_specialty_strs
        )

    @classmethod
    def filter_by_subspecialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by subspecialty."""

        value = (
                cls.payment_conflict_subspecialty_match(
                    payment_specialtys=payments_x_conflicted["specialtys"],
                    conflict_specialtys=payments_x_conflicted[
                        "conflict_specialtys"
                    ],
                )
        )

        if value and PaymentFilters.FULLSPECIALTY not in payments_x_conflicted[
            "filters"
        ]:
            return value
        return False

    @classmethod
    def payment_conflict_subspecialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            cls.specialty_str_matcher(
                payment_specialty=payment_specialty.subspecialty,
                conflict_specialty=conflict_specialty.subspecialty,
            ) for payment_specialty in payment_specialtys
            for conflict_specialty in conflict_specialtys
        )

    @classmethod
    def filter_by_fullspecialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> bool:
        """Filters by full specialty."""

        value = (
            cls.payment_conflict_full_specialty_match(
                payment_specialtys=payments_x_conflicted["specialtys"],
                conflict_specialtys=payments_x_conflicted[
                    "conflict_specialtys"
                ],
            )
        )
        if value:
            if PaymentFilters.SPECIALTY in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(
                    PaymentFilters.SPECIALTY,
                )
            if PaymentFilters.SUBSPECIALTY in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(
                    PaymentFilters.SUBSPECIALTY,
                )
        return value

    @classmethod
    def payment_conflict_full_specialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            (
                cls.specialty_str_matcher(
                    payment_specialty=payment_specialty.specialty,
                    conflict_specialty=conflict_specialty.specialty,
                ) and cls.specialty_str_matcher(
                    payment_specialty=payment_specialty.subspecialty,
                    conflict_specialty=conflict_specialty.subspecialty,
                )
                for payment_specialty in payment_specialtys
                for conflict_specialty in conflict_specialtys
            )
        )

    def convert_merged_dtypes(
        self,
        merged: pd.DataFrame,
    ) -> pd.DataFrame:
        """Updates  payments and conflicteds columns into lists after
        they are loaded as strs in CSVs and Excel files."""

        merged = super().convert_merged_dtypes(merged)

        merged["specialtys"] = merged["specialtys"].apply(
            lambda x: convert_specialtys(x) if isinstance(x, str) else x
        )

        merged["conflict_specialtys"] = merged["conflict_specialtys"].apply(
            lambda x: convert_specialtys(x) if isinstance(x, str) else x
        )

        return merged

    @staticmethod
    def get_specialty_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a payments_x_conflicteds DataFrame by specialty
        in order of priority:
        1. Full specialty
        2. Specialty
        3. Subspecialty
        """
        refined_matches = payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(
                lambda x: PaymentFilters.FULLSPECIALTY in x
                )
            ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.SPECIALTY in x
                )
            ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(
                    lambda x: PaymentFilters.SUBSPECIALTY in x
                )
            ]

        return refined_matches
