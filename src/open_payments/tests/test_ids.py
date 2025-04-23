import unittest

import numpy as np
import pandas as pd

from ..citystates import CityState
from ..credentials import Credentials
from ..ids import ConflictedPaymentIDs, PaymentFilters, PaymentIDs, Unmatcheds
from ..specialtys import Specialtys


class TestPaymentIDs(unittest.TestCase):

    def test__unique_payments(self):
        unique_payments = PaymentIDs(payment_classes="general").unique_payments()

        self.assertIsInstance(unique_payments, pd.DataFrame)
        self.assertIn("profile_id", unique_payments.columns)
        self.assertIn("npi", unique_payments.columns)
        self.assertIn("first_name", unique_payments.columns)
        self.assertIn("middle_name", unique_payments.columns)
        self.assertIn("last_name", unique_payments.columns)
        self.assertIn("specialtys", unique_payments.columns)
        self.assertIn("credentials", unique_payments.columns)
        self.assertIn("citystates", unique_payments.columns)
        self.assertEqual(len(unique_payments.columns), 8)


def add_conflicted_to_conflicteds_df(
    conflicteds: pd.DataFrame,
    provider_pk: int,
    first_name: str,
    last_name: str,
    middle_initial_1: str,
    middle_initial_2: str,
    middle_name_1: str,
    middle_name_2: str,
    credentials: list,
    specialtys: list,
    citystates: list,
) -> pd.DataFrame:
    """
    Add conflicted information to the conflicteds DataFrame.
    """

    conflicteds = pd.concat(
        [
            conflicteds,
            pd.DataFrame({
                "provider_pk": provider_pk,
                "first_name": first_name,
                "last_name": last_name,
                "middle_initial_1": middle_initial_1,
                "middle_initial_2": middle_initial_2,
                "middle_name_1": middle_name_1,
                "middle_name_2": middle_name_2,
                "credentials": credentials,
                "specialtys": specialtys,
                "citystates": citystates,
            })
        ],
        ignore_index=True
    )

    return conflicteds


def add_conflict_prefix(conflicteds: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a conflict prefix to the columns of the conflicteds DataFrame
    except for the last_name column.
    """

    return conflicteds.rename(
        columns={
            col: f"conflict_{col}" for col in conflicteds.columns if col != "last_name"
        }
    )


def add_payment_id_to_payments_df(
    payments: pd.DataFrame,
    profile_id: int,
    first_name: str,
    middle_name: str,
    last_name: str,
    specialtys: list,
    credentials: list,
    citystates: list,
) -> pd.DataFrame:
    """
    Add a new payment ID to the payments DataFrame.
    """

    payments = pd.concat(
        [
            payments,
            pd.DataFrame({
                "profile_id": profile_id,
                "first_name": first_name,
                "middle_name": middle_name,
                "last_name": last_name,
                "specialtys": specialtys,
                "credentials": credentials,
                "citystates": citystates,
            }),
        ],
        ignore_index=True
    )

    return payments


class TestConflictedPaymentIDs(unittest.TestCase):
    def setUp(self):
        self.fake_conflicteds = pd.DataFrame({
                "provider_pk": [1, 2, 3, 4],
                "first_name": ["John", "Judd", "Joey", "Dave"],
                "last_name": ["Doe", "Smith", "Johnson", "Ebalt"],
                "middle_initial_1": ["A", "E", "C", "Z"],
                "middle_initial_2": [None, None, None, None],
                "middle_name_1": ["Alpha", None, None, "Clark"],
                "middle_name_2": [None, "Echo", None, None],
                "credentials": [
                    [Credentials.MEDICAL_DOCTOR],
                    [Credentials.DOCTOR_OF_OSTEOPATHY],
                    [Credentials.MEDICAL_DOCTOR],
                    [Credentials.MEDICAL_DOCTOR],
                ],
                "specialtys": [
                    [Specialtys(specialty="Pediatrics", subspecialty="Gastroenterology")],
                    [Specialtys(specialty="Family Medicine")],
                    [Specialtys(specialty="Internal Medicine")],
                    [
                        Specialtys(specialty="Internal Medicine", subspecialty="Rheumatology"),
                        Specialtys(specialty="Internal Medicine", subspecialty="Chief Resident")
                    ]
                ],
                "citystates": [
                    [CityState(city="New York", state="NY")],
                    [CityState(city="Los Angeles", state="NV")],
                    [CityState(city="Chicago", state="IL")],
                    [CityState(city="Saint Paul", state="MN")]
                ],
            })

        self.fake_payments = pd.DataFrame({
            "profile_id": [1, 2, 3],
            "first_name": ["John", "Jane", "Joe"],
            "middle_name": ["Alpha", "Edward", None],
            "last_name": ["Doe", "Smith", "Johnson"],
            "specialtys": [
                [Specialtys(specialty="Pediatrics", subspecialty="Neonatology")],
                [Specialtys(specialty="Surgery")],
                [Specialtys(specialty="Internal Medicine")]
            ],
            "credentials": [
                [Credentials.MEDICAL_DOCTOR],
                [Credentials.DOCTOR_OF_OSTEOPATHY],
                [Credentials.PHYSICIAN_ASSISTANT]
            ],
            "citystates": [
                [CityState(city="New York", state="NY")],
                [CityState(city="Los Angeles", state="CA")],
                [CityState(city="Rochester", state="IL")]
            ],
        })
        self.reader = ConflictedPaymentIDs(
            conflicteds=self.fake_conflicteds,
            payments=self.fake_payments
        )

    def test__search_for_conflicteds_ids(self):
        # Test with fake conflicteds
        self.reader = ConflictedPaymentIDs(conflicteds=self.fake_conflicteds, payments=self.fake_payments)
        self.reader.search_for_conflicteds_ids()
        self.assertFalse(self.reader.unique_ids.empty)
        self.assertFalse(self.reader.unmatched.empty)

        self.assertIn("profile_id", self.reader.unique_ids.columns)
        self.assertIn("citystates", self.reader.unique_ids.columns)
        self.assertIn("credentials", self.reader.unique_ids.columns)
        self.assertIn("specialtys", self.reader.unique_ids.columns)
        self.assertIn("first_name", self.reader.unique_ids.columns)
        self.assertIn("middle_name", self.reader.unique_ids.columns)
        self.assertIn("last_name", self.reader.unique_ids.columns)
        self.assertIn("conflict_provider_pk", self.reader.unique_ids.columns)
        self.assertIn("conflict_first_name", self.reader.unique_ids.columns)
        self.assertIn("conflict_middle_initial_1", self.reader.unique_ids.columns)
        self.assertIn("conflict_middle_initial_2", self.reader.unique_ids.columns)
        self.assertIn("conflict_middle_name_1", self.reader.unique_ids.columns)
        self.assertIn("conflict_middle_name_2", self.reader.unique_ids.columns)
        self.assertIn("conflict_credentials", self.reader.unique_ids.columns)
        self.assertIn("conflict_specialtys", self.reader.unique_ids.columns)
        self.assertIn("conflict_citystates", self.reader.unique_ids.columns)
        self.assertIn("filters", self.reader.unique_ids.columns)

        self.assertIn("unmatched", self.reader.unmatched.columns)

        # Test that each row has a filter applied
        for i in self.reader.unique_ids.index:
            row = self.reader.unique_ids.iloc[i]
            self.assertIn(PaymentFilters.LASTNAME, row["filters"])

        last_names = self.reader.unique_ids["last_name"].values.tolist()
        first_names = self.reader.unique_ids["first_name"].values.tolist()

        self.assertIn("Doe", last_names)
        self.assertIn("Smith", last_names)
        self.assertIn("Johnson", last_names)
        self.assertIn("Jane", first_names)
        self.assertIn("John", first_names)
        self.assertIn("Joe", first_names)

        self.assertNotIn("Ebalt", last_names)
        self.assertIn("Ebalt", self.reader.unmatched["last_name"].values.tolist())
        self.assertEqual(
            self.reader.unmatched.iloc[0]["unmatched"],
            Unmatcheds.NOLASTNAME
        )

    def test__merge_by_lastname(self):
        # Add more mock data to payments
        extra_mock_data = [
            [4, "Nathan", "EG", "Doe", [Specialtys(specialty="Pediatrics")], [Credentials.MEDICAL_DOCTOR], [CityState(city="New York", state="NY")]],
            [5, "Handsy", None, "Doe", [Specialtys(specialty="Pediatrics")], [Credentials.MEDICAL_DOCTOR], [CityState(city="New York", state="NY")]],
            [6, "Johnson", "C", "Doe", [Specialtys(specialty="Pediatrics")], [Credentials.MEDICAL_DOCTOR], [CityState(city="New York", state="NY")]],
        ]

        for data in extra_mock_data:
            self.reader.payments = add_payment_id_to_payments_df(
                self.reader.payments,
                *data
            )

        self.reader.conflicteds = add_conflict_prefix(self.reader.conflicteds)

        merged = self.reader.merge_by_last_name(
            conflicted=self.reader.conflicteds.iloc[0]
        )

        self.assertIsInstance(merged, pd.DataFrame)
        self.assertIn("profile_id", merged.columns)
        self.assertIn("first_name", merged.columns)
        self.assertIn("middle_name", merged.columns)
        self.assertIn("last_name", merged.columns)
        self.assertIn("specialtys", merged.columns)
        self.assertIn("credentials", merged.columns)
        self.assertIn("citystates", merged.columns)
        self.assertIn("conflict_provider_pk", merged.columns)
        self.assertIn("conflict_first_name", merged.columns)
        self.assertIn("conflict_middle_initial_1", merged.columns)
        self.assertIn("conflict_middle_initial_2", merged.columns)
        self.assertIn("conflict_middle_name_1", merged.columns)
        self.assertIn("conflict_middle_name_2", merged.columns)
        self.assertIn("conflict_credentials", merged.columns)
        self.assertIn("conflict_specialtys", merged.columns)
        self.assertIn("conflict_citystates", merged.columns)

        self.assertEqual(len(merged), 4)

    def test__filter_by_credential(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "credentials": [Credentials.MEDICAL_DOCTOR],
            "conflict_credentials": [
                Credentials.NURSE_PRACTITIONER,
                Credentials.PHYSICIAN_ASSISTANT
            ],
            "filters": [PaymentFilters.LASTNAME],
        })

        filtered_df = ConflictedPaymentIDs.filter_by_credential(
            fake_row,
        )

        self.assertIsInstance(filtered_df, pd.Series)
        self.assertNotIn(
            PaymentFilters.CREDENTIAL,
            filtered_df["filters"],
        )
        fake_row.update({"conflict_credentials": [Credentials.MEDICAL_DOCTOR]})
        filtered_df = ConflictedPaymentIDs.filter_by_credential(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertIn(
            PaymentFilters.CREDENTIAL,
            filtered_df["filters"],
        )

    def test__filter_by_first_name(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "first_name": "John",
            "conflict_first_name": "Judd",
            "filters": [PaymentFilters.LASTNAME],
        })
        filtered_df = ConflictedPaymentIDs.filter_by_firstname(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertNotIn(
            PaymentFilters.FIRSTNAME,
            filtered_df["filters"],
        )

        fake_row.update({"conflict_first_name": "John"})
        filtered_df = ConflictedPaymentIDs.filter_by_firstname(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertIn(
            PaymentFilters.FIRSTNAME,
            filtered_df["filters"],
        )

        fake_row["conflict_first_name"] = None
        fake_row["filters"] = [PaymentFilters.LASTNAME]

        filtered_df = ConflictedPaymentIDs.filter_by_firstname(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertNotIn(
            PaymentFilters.FIRSTNAME,
            filtered_df["filters"],
        )

    def test__filter_by_specialty(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "specialtys": [
                Specialtys(specialty="Pediatrics", subspecialty="Gastroenterology")
            ],
            "conflict_specialtys": [
                Specialtys(specialty="Family Medicine"),
                Specialtys(specialty="Internal Medicine")
            ],
            "filters": [PaymentFilters.LASTNAME],
        })
        filtered_df = ConflictedPaymentIDs.filter_by_specialty(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertNotIn(
            PaymentFilters.SPECIALTY,
            filtered_df["filters"],
        )
        fake_row.update({"conflict_specialtys": [Specialtys(specialty="Pediatrics")]})
        filtered_df = ConflictedPaymentIDs.filter_by_specialty(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertIn(
            PaymentFilters.SPECIALTY,
            filtered_df["filters"],
        )

    def test__filter_by_subspecialty(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "specialtys": [
                Specialtys(specialty="Pediatrics", subspecialty="Gastroenterology")
            ],
            "conflict_specialtys": [
                Specialtys(specialty="Family Medicine"),
                Specialtys(specialty="Internal Medicine")
            ],
            "filters": [PaymentFilters.LASTNAME],
        })
        filtered_df = ConflictedPaymentIDs.filter_by_subspecialty(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertNotIn(
            PaymentFilters.SUBSPECIALTY,
            filtered_df["filters"],
        )
        fake_row.update({"conflict_specialtys": [Specialtys(specialty="Pediatrics", subspecialty="Gastroenterology")]})
        filtered_df = ConflictedPaymentIDs.filter_by_subspecialty(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertIn(
            PaymentFilters.SUBSPECIALTY,
            filtered_df["filters"],
        )

    def test__filter_by_fullspecialty(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "specialtys": [
                Specialtys(specialty="Pediatrics", subspecialty="Gastroenterology")
            ],
            "conflict_specialtys": [
                Specialtys(specialty="Family Medicine"),
                Specialtys(specialty="Internal Medicine")
            ],
            "filters": [PaymentFilters.LASTNAME],
        })
        filtered_df = ConflictedPaymentIDs.filter_by_fullspecialty(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertNotIn(
            PaymentFilters.FULLSPECIALTY,
            filtered_df["filters"],
        )
        fake_row.update({"conflict_specialtys": [Specialtys(specialty="Pediatrics", subspecialty="Gastroenterology")]})
        filtered_df = ConflictedPaymentIDs.filter_by_fullspecialty(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertIn(
            PaymentFilters.FULLSPECIALTY,
            filtered_df["filters"],
        )

    def test__filter_by_city(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "citystates": [CityState(city="New York", state="NY")],
            "conflict_citystates": [CityState(city="Los Angeles", state="CA")],
            "filters": [PaymentFilters.LASTNAME],
        })

        filtered_df = ConflictedPaymentIDs.filter_by_city(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertNotIn(
            PaymentFilters.CITY,
            filtered_df["filters"],
        )

        fake_row.update({"conflict_citystates": [CityState(city="New York", state="NY")]})
        filtered_df = ConflictedPaymentIDs.filter_by_city(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertIn(
            PaymentFilters.CITY,
            filtered_df["filters"],
        )

    def test__filter_by_state(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "citystates": [CityState(city="New York", state="NY")],
            "conflict_citystates": [CityState(city="Los Angeles", state="CA")],
            "filters": [PaymentFilters.LASTNAME],
        })

        filtered_df = ConflictedPaymentIDs.filter_by_state(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertNotIn(
            PaymentFilters.STATE,
            filtered_df["filters"],
        )

        fake_row.update({"conflict_citystates": [CityState(city="New York", state="NY")]})
        filtered_df = ConflictedPaymentIDs.filter_by_state(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertIn(
            PaymentFilters.STATE,
            filtered_df["filters"],
        )

    def test__filter_by_citystate(self):

        fake_row = pd.Series({
            "provider_pk": 1,
            "citystates": [CityState(city="New York", state="NY")],
            "conflict_citystates": [CityState(city="Los Angeles", state="CA")],
            "filters": [PaymentFilters.LASTNAME],
        })

        filtered_df = ConflictedPaymentIDs.filter_by_citystate(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertNotIn(
            PaymentFilters.CITYSTATE,
            filtered_df["filters"],
        )

        fake_row.update({"conflict_citystates": [CityState(city="New York", state="NY")]})
        filtered_df = ConflictedPaymentIDs.filter_by_citystate(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertIn(
            PaymentFilters.CITYSTATE,
            filtered_df["filters"],
        )

    def test__filter_by_middle_initial(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "middle_name": "Alpha",
            "conflict_middle_initial_1": "B",
            "conflict_middle_initial_2": None,
            "conflict_middle_name_1": None,
            "conflict_middle_name_2": None,
            "filters": [PaymentFilters.LASTNAME],
        })

        filtered_df = ConflictedPaymentIDs.filter_by_middle_initial(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertNotIn(
            PaymentFilters.MIDDLE_INITIAL,
            filtered_df["filters"],
        )

        fake_row.update({"conflict_middle_initial_1": "A"})
        filtered_df = ConflictedPaymentIDs.filter_by_middle_initial(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertIn(
            PaymentFilters.MIDDLE_INITIAL,
            filtered_df["filters"],
        )

    def test__middle_initial_match(self):
        self.assertFalse(
            ConflictedPaymentIDs.middle_initial_match(
                conflicted_middle_initial_1=None,
                conflicted_middle_initial_2=None,
                conflicted_middle_name_1=None,
                conflicted_middle_name_2=None,
                payment_middle_name="Potter",
            )
        )
        self.assertTrue(
            ConflictedPaymentIDs.middle_initial_match(
                conflicted_middle_initial_1="P",
                conflicted_middle_initial_2=None,
                conflicted_middle_name_1=None,
                conflicted_middle_name_2=None,
                payment_middle_name="Potter",
            )
        )

    def test__middlename_match(self):
        self.assertFalse(
            ConflictedPaymentIDs.middlename_match(
                conflicted_middle_name_1=None,
                conflicted_middle_name_2=None,
                payment_middle_name="Potter",
            )
        )
        self.assertTrue(
            ConflictedPaymentIDs.middlename_match(
                conflicted_middle_name_1="Potter",
                conflicted_middle_name_2=None,
                payment_middle_name="Potter",
            )
        )
        self.assertTrue(
            ConflictedPaymentIDs.middlename_match(
                conflicted_middle_name_1=None,
                conflicted_middle_name_2="Potter",
                payment_middle_name="Potter",
            )
        )
        self.assertFalse(
            ConflictedPaymentIDs.middlename_match(
                conflicted_middle_name_1="Weasley",
                conflicted_middle_name_2=None,
                payment_middle_name="Potter",
            )
        )

    def test__filter_by_middlename(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "middle_name": "Alpha",
            "conflict_middle_name_1": "Beta",
            "conflict_middle_name_2": None,
            "conflict_middle_initial_1": None,
            "conflict_middle_initial_2": None,
            "filters": [PaymentFilters.LASTNAME],
        })

        filtered_df = ConflictedPaymentIDs.filter_by_middlename(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertNotIn(
            PaymentFilters.MIDDLENAME,
            filtered_df["filters"],
        )

        fake_row.update({"conflict_middle_name_1": "Alpha"})
        filtered_df = ConflictedPaymentIDs.filter_by_middlename(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertIn(
            PaymentFilters.MIDDLENAME,
            filtered_df["filters"],
        )

        fake_row.update({"conflict_middle_name_2": "Alpha"})
        filtered_df = ConflictedPaymentIDs.filter_by_middlename(
            fake_row,
        )
        self.assertIsInstance(filtered_df, pd.Series)
        self.assertIn(
            PaymentFilters.MIDDLENAME,
            filtered_df["filters"],
        )