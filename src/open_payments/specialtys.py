from pydantic import BaseModel
from enum import StrEnum
from typing import Union

import pandas as pd
import re

from .helpers import get_file_suffix, open_payments_directory
from .read import ReadPayments


class Specialtys(BaseModel):
    """Class that contains the specialtys of a payment."""

    specialty: str
    subspecialty: str | None = None

    def __str__(self) -> str:
        return f"{self.specialty}|{self.subspecialty}"


class PaymentSpecialtys(ReadPayments):

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

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().general_columns
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
    def ownership_columns(self) -> dict[str, tuple[str, Union[str, int]]]:

        cols = super().ownership_columns
        cols.update({
                "Physician_Specialty": ("specialty_1", str),
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

        payments.drop(
            columns=[
                "specialty_1",
                "specialty_2",
                "specialty_3",
                "specialty_4",
                "specialty_5",
                "specialty_6",
            ],
            inplace=True,
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