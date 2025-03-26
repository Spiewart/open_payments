from typing import Literal, Union

import pandas as pd

from .helpers import get_file_suffix, open_payments_directory
from .read import ReadPayments


class PaymentSpecialtys(ReadPayments):

    def create_unique_specialtys_excel(self, path: Union[str, None] = None) -> None:
        path = open_payments_directory() if path is None else path

        unique_specialtys = self.unique_specialtys()

        unique_specialtys = self.get_subspecialtys(unique_specialtys)

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

        all_specialtys = self.get_all_specialtys(all_payments)

        all_specialtys = all_specialtys.drop_duplicates()

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

    @staticmethod
    def get_all_specialtys(df: pd.DataFrame) -> pd.Series:
        """Returns a Series of all specialties from the DataFrame."""

        all_specialtys = pd.concat([
            df["specialty_1"],
            df["specialty_2"],
            df["specialty_3"],
            df["specialty_4"],
            df["specialty_5"],
            df["specialty_6"],
        ])

        all_specialtys.rename("specialty", inplace=True)

        return all_specialtys

    @staticmethod
    def get_subspecialtys(specialtys: pd.Series) -> pd.DataFrame:
        """Splits MD/DO specialties into specialty and subspecialty columns."""

        specialtys = specialtys.str.split("|", expand=True)

        # TODO: fix, not elegant, should be done with a better regex pattern
        if len(specialtys.columns) == 1:
            specialtys.columns = ["provider_type"]
            specialtys.insert(1, "specialty", None)
            specialtys.insert(2, "subspecialty", None)
        elif len(specialtys.columns) == 2:
            specialtys.columns = ["provider_type", "specialty"]
            specialtys.insert(2, "subspecialty", None)
        else:
            specialtys.columns = ["provider_type", "specialty", "subspecialty"]

        return specialtys

    @classmethod
    def specialtys(cls, payment: pd.Series) -> pd.Series:
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

        specialtys = cls.get_subspecialtys(specialtys)

        specialtys = specialtys.drop("provider_type", axis=1)
        specialtys = specialtys.dropna(how="all", subset=["specialty", "subspecialty"])
        specialtys = specialtys.drop_duplicates()
        specialtys.reset_index(drop=True, inplace=True)

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