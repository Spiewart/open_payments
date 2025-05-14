import unittest
from typing import Union

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
            col: f"conflict_{col}" for col in conflicteds.columns if (
                col != "last_name" and
                col != "provider_pk"
            )
        }
    )


def add_payment_id_to_payments_df(
    payments: pd.DataFrame,
    profile_id: Union[int, None],
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
    if profile_id is None:
        # Get the next unique profile_id
        profile_id = payments["profile_id"].max() + 1

    payments = pd.concat(
        [
            payments,
            pd.DataFrame({
                "profile_id": profile_id,
                "first_name": first_name,
                "middle_name": middle_name,
                "last_name": last_name,
                "specialtys": [specialtys],
                "credentials": [credentials],
                "citystates": [citystates],
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
                    [
                        Specialtys(
                            specialty="Pediatrics",
                            subspecialty="Gastroenterology"
                        ),
                    ],
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
                [Credentials.PHYSICIAN_ASSISTANT],
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
        # Add more mock data to payments
        self.extra_mock_data = [
            [4, "Nathan", "EG", "Doe", [Specialtys(specialty="Pediatrics")], [Credentials.MEDICAL_DOCTOR], [CityState(city="New York", state="NY")]],
            [5, "Handsy", None, "Doe", [Specialtys(specialty="Pediatrics")], [Credentials.MEDICAL_DOCTOR], [CityState(city="New York", state="NY")]],
            [6, "Johnson", "C", "Doe", [Specialtys(specialty="Pediatrics")], [Credentials.MEDICAL_DOCTOR], [CityState(city="New York", state="NY")]],
        ]

    def test__search_for_conflicteds_ids(self):
        # Test with fake conflicteds
        self.reader = ConflictedPaymentIDs(
            conflicteds=self.fake_conflicteds,
            payments=self.fake_payments,
        )
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
        self.assertIn("provider_pk", self.reader.unique_ids.columns)
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
        for data in self.extra_mock_data:
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
        self.assertIn("provider_pk", merged.columns)
        self.assertIn("conflict_first_name", merged.columns)
        self.assertIn("conflict_middle_initial_1", merged.columns)
        self.assertIn("conflict_middle_initial_2", merged.columns)
        self.assertIn("conflict_middle_name_1", merged.columns)
        self.assertIn("conflict_middle_name_2", merged.columns)
        self.assertIn("conflict_credentials", merged.columns)
        self.assertIn("conflict_specialtys", merged.columns)
        self.assertIn("conflict_citystates", merged.columns)

        self.assertEqual(len(merged), 4)

    def test__merge_by_lastname_two_lastnames(self):
        conflicteds = pd.DataFrame({
            "provider_pk": [11],
            "first_name": ["John"],
            "last_name": ["Doe Smith"],
        })

        merged = self.reader.merge_by_last_name(
            conflicted=conflicteds.iloc[0]
        )
        self.assertIsInstance(merged, pd.DataFrame)
        self.assertEqual(len(merged), 2)

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

        match = ConflictedPaymentIDs.filter_by_credential(
            fake_row,
        )

        self.assertIsInstance(match, bool)
        self.assertFalse(match)
        fake_row.update({"conflict_credentials": [Credentials.MEDICAL_DOCTOR]})
        match = ConflictedPaymentIDs.filter_by_credential(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

    def test__filter_by_first_name(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "first_name": "John",
            "conflict_first_name": "Judd",
            "filters": [PaymentFilters.LASTNAME],
        })
        match = ConflictedPaymentIDs.filter_by_firstname(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertFalse(match)

        fake_row.update({"conflict_first_name": "John"})
        match = ConflictedPaymentIDs.filter_by_firstname(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

        fake_row["conflict_first_name"] = None
        fake_row["filters"] = [PaymentFilters.LASTNAME]

        match = ConflictedPaymentIDs.filter_by_firstname(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertFalse(match)

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
        match = ConflictedPaymentIDs.filter_by_specialty(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertFalse(match)
        fake_row.update(
            {"conflict_specialtys": [Specialtys(specialty="Pediatrics")]}
        )
        match = ConflictedPaymentIDs.filter_by_specialty(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

    def test__filter_by_specialty_partial(self):
        fake_row = pd.Series({
            "specialtys": [
                Specialtys(specialty="Family Medicine", subspecialty=None)
            ],
            "conflict_specialtys": [
                Specialtys(specialty="Family", subspecialty=None)
            ]
        })

        self.assertTrue(
            ConflictedPaymentIDs.filter_by_specialty(
                fake_row,
            )
        )

        fake_row_reverse = pd.Series({
            "specialtys": [
                Specialtys(specialty="Family", subspecialty=None)
            ],
            "conflict_specialtys": [
                Specialtys(specialty="Family Medicine", subspecialty=None)
            ]
        })
        self.assertTrue(
            ConflictedPaymentIDs.filter_by_specialty(
                fake_row_reverse,
            )
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
        match = ConflictedPaymentIDs.filter_by_subspecialty(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertFalse(match)
        fake_row.update({"conflict_specialtys": [Specialtys(specialty="Pediatrics", subspecialty="Gastroenterology")]})
        match = ConflictedPaymentIDs.filter_by_subspecialty(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

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
        match = ConflictedPaymentIDs.filter_by_fullspecialty(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertFalse(match)
        fake_row.update({"conflict_specialtys": [Specialtys(specialty="Pediatrics", subspecialty="Gastroenterology")]})
        match = ConflictedPaymentIDs.filter_by_fullspecialty(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

    def test__filter_by_city(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "citystates": [CityState(city="New York", state="NY")],
            "conflict_citystates": [CityState(city="Los Angeles", state="CA")],
            "filters": [PaymentFilters.LASTNAME],
        })

        match = ConflictedPaymentIDs.filter_by_city(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertFalse(match)

        fake_row.update({"conflict_citystates": [CityState(city="New York", state="NY")]})
        match = ConflictedPaymentIDs.filter_by_city(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

    def test__filter_by_state(self):
        fake_row = pd.Series({
            "provider_pk": 1,
            "citystates": [CityState(city="New York", state="NY")],
            "conflict_citystates": [CityState(city="Los Angeles", state="CA")],
            "filters": [PaymentFilters.LASTNAME],
        })

        match = ConflictedPaymentIDs.filter_by_state(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertFalse(match)

        fake_row.update({"conflict_citystates": [CityState(city="New York", state="NY")]})
        match = ConflictedPaymentIDs.filter_by_state(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

        fake_row.update({"conflict_citystates": [CityState(city="New York", state="New York"), CityState(city="Los Angeles", state="CA")]})

        match = ConflictedPaymentIDs.filter_by_state(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

        fake_row.update({
            "conflict_citystates": [CityState(city="New York", state="NY"), CityState(city="Los Angeles", state="CA")],
            "citystates": [CityState(city="New York", state="New York")],
        })

        match = ConflictedPaymentIDs.filter_by_state(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

    def test__filter_by_citystate(self):

        fake_row = pd.Series({
            "provider_pk": 1,
            "citystates": [CityState(city="New York", state="NY")],
            "conflict_citystates": [CityState(city="Los Angeles", state="CA")],
            "filters": [PaymentFilters.LASTNAME],
        })

        match = ConflictedPaymentIDs.filter_by_citystate(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertFalse(match)

        fake_row.update({"conflict_citystates": [CityState(city="New York", state="NY")]})
        match = ConflictedPaymentIDs.filter_by_citystate(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

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

        match = ConflictedPaymentIDs.filter_by_middle_initial(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertFalse(match)

        fake_row.update({"conflict_middle_initial_1": "A"})
        match = ConflictedPaymentIDs.filter_by_middle_initial(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

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

        match = ConflictedPaymentIDs.filter_by_middlename(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertFalse(match)

        fake_row.update({"conflict_middle_name_1": "Alpha"})
        match = ConflictedPaymentIDs.filter_by_middlename(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

        fake_row.update({"conflict_middle_name_2": "Alpha"})
        match = ConflictedPaymentIDs.filter_by_middlename(
            fake_row,
        )
        self.assertIsInstance(match, bool)
        self.assertTrue(match)

    def test__filter_payments_for_conflicted(self):
        for data in self.extra_mock_data:
            self.reader.payments = add_payment_id_to_payments_df(
                self.reader.payments,
                *data
            )
        self.reader.conflicteds = add_conflict_prefix(self.reader.conflicteds)
        doe_conflicted = self.reader.conflicteds.iloc[0]

        self.assertTrue(
            self.reader.unique_ids.empty
        )

        self.reader.filter_payments_for_conflicted(
            conflicted=doe_conflicted,
        )

        self.assertFalse(
            self.reader.unique_ids.empty
        )

        doe_id = self.reader.unique_ids.iloc[0]

        # Test that all the filters are applied
        self.assertIn(PaymentFilters.LASTNAME, doe_id["filters"])
        self.assertIn(PaymentFilters.FIRSTNAME, doe_id["filters"])
        self.assertIn(PaymentFilters.CREDENTIAL, doe_id["filters"])
        self.assertIn(PaymentFilters.SPECIALTY, doe_id["filters"])
        self.assertIn(PaymentFilters.CITY, doe_id["filters"])
        self.assertIn(PaymentFilters.STATE, doe_id["filters"])
        self.assertIn(PaymentFilters.CITYSTATE, doe_id["filters"])
        self.assertIn(PaymentFilters.MIDDLENAME, doe_id["filters"])
        self.assertIn(PaymentFilters.MIDDLE_INITIAL, doe_id["filters"])

        # Test that the "ebalt" row is in the unmatched DataFrame

        self.reader.filter_payments_for_conflicted(
            conflicted=self.reader.conflicteds.iloc[3],
        )
        self.assertFalse(
            self.reader.unique_ids.empty
        )
        self.assertIn(
            "Ebalt",
            self.reader.unmatched["last_name"].values.tolist()
        )
        self.assertEqual(
            self.reader.unmatched.iloc[0]["unmatched"],
            Unmatcheds.NOLASTNAME
        )

        # Test that the algorithm still works when there are duplicate,
        # equally likely payments
        self.reader.payments = add_payment_id_to_payments_df(
            self.reader.payments,
            *self.fake_payments.iloc[0].values.tolist()
        )

        self.reader.unique_ids = pd.DataFrame()

        self.reader.filter_payments_for_conflicted(
            conflicted=doe_conflicted,
        )

        self.assertFalse(
            self.reader.unique_ids.empty
        )

        self.assertIn(
            "Doe",
            self.reader.unique_ids["last_name"].values.tolist()
        )

        # Test that the algorithm will filter by credential

        # Get the "Smith" row
        smith_payment = self.reader.payments.iloc[1]

        smith_copy = smith_payment.copy()

        # Change the profile_id and cerdentials
        smith_copy["profile_id"] = None
        smith_copy["credentials"] = [Credentials.NURSE_PRACTITIONER]

        # Add a second "Smith" row with a non-MD/DO credential
        self.reader.payments = add_payment_id_to_payments_df(
            self.reader.payments,
            *smith_copy.values.tolist()
        )

        self.reader.unique_ids = pd.DataFrame()

        self.reader.filter_payments_for_conflicted(
            conflicted=self.reader.conflicteds.iloc[1],
        )
        self.assertFalse(
            self.reader.unique_ids.empty
        )
        self.assertIn(
            "Smith",
            self.reader.unique_ids["last_name"].values.tolist()
        )
        self.assertIn(
            PaymentFilters.CREDENTIAL,
            self.reader.unique_ids.iloc[0]["filters"]
        )
        self.assertEqual(
            self.reader.unique_ids.iloc[0]["profile_id"],
            2,
        )
        self.assertNotEqual(
            self.reader.unique_ids.iloc[0]["profile_id"],
            7
        )
