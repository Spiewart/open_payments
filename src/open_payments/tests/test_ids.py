import unittest

import pandas as pd

from ..credentials import Credentials
from ..ids import (ConflictedPaymentIDs, PaymentIDFilters, PaymentIDs,
                   Unmatcheds)
from ..specialtys import Specialtys


class TestPaymentIDs(unittest.TestCase):

    def test__unique_payment_ids(self):
        unique_payment_ids = PaymentIDs(payment_classes="general").unique_payment_ids()

        self.assertIsInstance(unique_payment_ids, pd.DataFrame)
        self.assertIn("profile_id", unique_payment_ids.columns)
        self.assertIn("npi", unique_payment_ids.columns)
        self.assertIn("first_name", unique_payment_ids.columns)
        self.assertIn("middle_name", unique_payment_ids.columns)
        self.assertIn("last_name", unique_payment_ids.columns)
        self.assertIn("specialtys", unique_payment_ids.columns)
        self.assertIn("credentials", unique_payment_ids.columns)
        self.assertIn("citystates", unique_payment_ids.columns)
        self.assertEqual(len(unique_payment_ids.columns), 8)

    def test__unique_MD_DO_payment_ids(self):
        unique_md_do_payment_ids = PaymentIDs(payment_classes="general").unique_MD_DO_payment_ids()

        self.assertIsInstance(unique_md_do_payment_ids, pd.DataFrame)
        self.assertIn("profile_id", unique_md_do_payment_ids.columns)
        self.assertIn("npi", unique_md_do_payment_ids.columns)
        self.assertIn("first_name", unique_md_do_payment_ids.columns)
        self.assertIn("middle_name", unique_md_do_payment_ids.columns)
        self.assertIn("last_name", unique_md_do_payment_ids.columns)
        self.assertIn("specialtys", unique_md_do_payment_ids.columns)
        self.assertIn("credentials", unique_md_do_payment_ids.columns)
        self.assertIn("citystates", unique_md_do_payment_ids.columns)
        self.assertEqual(len(unique_md_do_payment_ids.columns), 8)


class TestConflictedPaymentIDs(unittest.TestCase):
    def setUp(self):
        self.fake_conflicteds = pd.DataFrame({
                "provider_pk": [1, 2, 3, 4],
                "first_name": ["John", "Judd", "Joey", "Dave"],
                "last_name": ["Doe", "Smith", "Johnson", "Ebalt"],
                "middle_initial_1": ["A", "B", "C", "C"],
                "middle_initial_2": ["D", "E", "F", None],
                "middle_name_1": ["Alpha", "Beta", "Charlie", "Clark"],
                "middle_name_2": ["Delta", "Echo", "Foxtrot", None],
                "credentials": [[Credentials.MEDICAL_DOCTOR], [Credentials.DOCTOR_OF_OSTEOPATHY], [Credentials.MEDICAL_DOCTOR], [Credentials.MEDICAL_DOCTOR]],
                "specialtys": [
                    [Specialtys(specialty="Pediatrics", subspecialty="Gastroenterology")],
                    [Specialtys(specialty="Family Medicine")],
                    [Specialtys(specialty="Internal Medicine")],
                    [
                        Specialtys(specialty="Internal Medicine", subspecialty="Rheumatology"),
                        Specialtys(specialty="Internal Medicine", subspecialty="Chief Resident")
                    ]
                ],
                "citystates": [["New York, NY"], ["Los Angeles, CA"], ["Chicago, IL"], ["Saint Paul, MN"]]
            })

        self.fake_payment_ids = pd.DataFrame({
            "profile_id": [1, 2, 3],
            "npi": [123, 456, 789],
            "first_name": ["John", "Jane", "Joe"],
            "middle_name": ["A", "B", "C"],
            "last_name": ["Doe", "Smith", "Johnson"],
            "specialtys": [
                [Specialtys(specialty="Pediatrics", subspecialty="Neonatology")],
                [Specialtys(specialty="Surgery")],
                [Specialtys(specialty="Internal Medicine")]
            ],
            "credentials": [[Credentials.MEDICAL_DOCTOR], [Credentials.DOCTOR_OF_OSTEOPATHY], [Credentials.PHYSICIAN_ASSISTANT]],
            "city": ["New York", "Los Angeles", "Chicago"],
            "states": [["NY"], ["CA"], ["IL"]]
        })
        self.reader_fake = ConflictedPaymentIDs(self.fake_conflicteds, self.fake_payment_ids)

    def test__unique_payment_ids(self):
        unique_payemnt_ids = ConflictedPaymentIDs(
            conflicteds=self.fake_conflicteds,
            payment_ids=None,
            nrows=1000,
            years=2023,
        ).unique_payment_ids()

        self.assertIsInstance(unique_payemnt_ids, pd.DataFrame)
        self.assertIn("specialtys", unique_payemnt_ids.columns)
        self.assertIn("citystates", unique_payemnt_ids.columns)
        self.assertIn("citystates", unique_payemnt_ids.columns)

    def test__add_conflict_prefix(self):
        updated_conflicteds = self.reader_fake.add_conflict_prefix(self.fake_conflicteds)

        self.assertIn("conflict_first_name", updated_conflicteds.columns)
        self.assertIn("conflict_middle_initial_1", updated_conflicteds.columns)
        self.assertIn("conflict_middle_initial_2", updated_conflicteds.columns)
        self.assertIn("conflict_middle_name_1", updated_conflicteds.columns)
        self.assertIn("conflict_middle_name_2", updated_conflicteds.columns)
        self.assertIn("conflict_credentials", updated_conflicteds.columns)
        self.assertIn("conflict_specialtys", updated_conflicteds.columns)
        self.assertIn("conflict_citystates", updated_conflicteds.columns)

    def test__conflicteds_payments_ids(self):
        # Test with fake conflicteds
        reader_fake = ConflictedPaymentIDs(conflicteds=self.fake_conflicteds, payment_ids=self.fake_payment_ids)
        conflicteds = reader_fake.conflicteds_payments_ids()
        self.assertIsInstance(conflicteds, pd.DataFrame)

        # Test with real conflicteds
        reader_real = ConflictedPaymentIDs(conflicteds=self.fake_conflicteds, payment_ids=None, nrows=1000, years=2023)
        conflicteds = reader_real.conflicteds_payments_ids()
        self.assertIsInstance(conflicteds, pd.DataFrame)

    def test__merge_by_lastname(self):
        merged = self.reader_fake.merge_by_last_name(
            payment_ids=self.reader_fake.payment_ids,
            conflicteds=self.reader_fake.conflicteds,
        )

        self.assertIsInstance(merged, pd.DataFrame)
        self.assertIn("profile_id", merged.columns)
        self.assertIn("npi", merged.columns)
        self.assertIn("first_name", merged.columns)
        self.assertIn("middle_name", merged.columns)
        self.assertIn("last_name", merged.columns)
        self.assertIn("specialtys", merged.columns)
        self.assertIn("credentials", merged.columns)
        self.assertIn("city", merged.columns)
        self.assertIn("states", merged.columns)
        self.assertIn("conflict_provider_pk", merged.columns)
        self.assertIn("conflict_last_name", merged.columns)
        self.assertIn("conflict_first_name", merged.columns)
        self.assertIn("conflict_middle_initial_1", merged.columns)
        self.assertIn("conflict_middle_initial_2", merged.columns)
        self.assertIn("conflict_middle_name_1", merged.columns)
        self.assertIn("conflict_middle_name_2", merged.columns)
        self.assertIn("conflict_credentials", merged.columns)
        self.assertIn("conflict_specialtys", merged.columns)
        self.assertIn("conflict_citystates", merged.columns)
        self.assertEqual(len(merged.columns), 19)

        self.assertEqual(len(merged), 4)

    def test__update_ids(self):
        self.reader_fake.payment_ids = self.reader_fake.merge_by_last_name(
            self.reader_fake.payment_ids,
            self.reader_fake.conflicteds,
        )
        self.reader_fake.update_ids(
            new_unique_ids=self.reader_fake.extract_unique_ids(
                self.reader_fake.payment_ids,
            ),
            filters=[PaymentIDFilters.LASTNAME],
        )
        self.assertIsInstance(self.reader_fake.unique_ids, pd.DataFrame)
        self.assertIn("profile_id", self.reader_fake.unique_ids.columns)
        self.assertIn("npi", self.reader_fake.unique_ids.columns)
        self.assertIn("first_name", self.reader_fake.unique_ids.columns)
        self.assertIn("middle_name", self.reader_fake.unique_ids.columns)
        self.assertIn("last_name", self.reader_fake.unique_ids.columns)
        self.assertIn("specialtys", self.reader_fake.unique_ids.columns)
        self.assertIn("credentials", self.reader_fake.unique_ids.columns)
        self.assertIn("city", self.reader_fake.unique_ids.columns)
        self.assertIn("states", self.reader_fake.unique_ids.columns)

        self.assertFalse(self.reader_fake.unique_ids.empty)
        self.assertEqual(len(self.reader_fake.unique_ids), 3)
        self.assertEqual(self.reader_fake.unique_ids["filters"].values.tolist(), [PaymentIDFilters.LASTNAME] * 3)
        self.assertIn("Doe", self.reader_fake.unique_ids["last_name"].values.tolist())
        self.assertIn("Smith", self.reader_fake.unique_ids["last_name"].values.tolist())
        self.assertIn("Johnson", self.reader_fake.unique_ids["last_name"].values.tolist())
        self.assertIn("Jane", self.reader_fake.unique_ids["first_name"].values.tolist())
        self.assertIn("John", self.reader_fake.unique_ids["first_name"].values.tolist())
        self.assertIn("Joe", self.reader_fake.unique_ids["first_name"].values.tolist())

    def test__update_unmatched(self):
        self.reader_fake.payment_ids = self.reader_fake.merge_by_last_name(
            self.reader_fake.payment_ids,
            self.reader_fake.conflicteds,
        )
        self.reader_fake.update_ids(self.reader_fake.extract_unique_ids(
                self.reader_fake.payment_ids,
            ), filters=[PaymentIDFilters.LASTNAME])

        unmatched = self.reader_fake.payment_ids[self.reader_fake.payment_ids["profile_id"].isna()]

        self.reader_fake.update_unmatched(unmatched, Unmatcheds.NOLASTNAME)

        self.assertFalse(self.reader_fake.unmatched.empty)

        self.assertTrue((self.reader_fake.unmatched["reason"] == Unmatcheds.NOLASTNAME).all())

        self.assertEqual(len(self.reader_fake.unmatched), 1)

        self.assertIn("Ebalt", self.reader_fake.unmatched["last_name"].values.tolist())

        self.assertIn("Dave", self.reader_fake.unmatched["first_name"].values.tolist())

    def test__remove_conflict_prefix(self):
        un_prefixed = self.reader_fake.remove_conflict_prefix(self.fake_conflicteds)
        self.assertIn("first_name", un_prefixed.columns)
        self.assertIn("middle_initial_1", un_prefixed.columns)
        self.assertIn("middle_initial_2", un_prefixed.columns)
        self.assertIn("middle_name_1", un_prefixed.columns)
        self.assertIn("middle_name_2", un_prefixed.columns)
        self.assertIn("credentials", un_prefixed.columns)
        self.assertIn("specialtys", un_prefixed.columns)
        self.assertIn("citystates", un_prefixed.columns)
        self.assertNotIn("conflict_first_name", un_prefixed.columns)
        self.assertNotIn("conflict_middle_initial_1", un_prefixed.columns)
        self.assertNotIn("conflict_middle_initial_2", un_prefixed.columns)
        self.assertNotIn("conflict_middle_name_1", un_prefixed.columns)
        self.assertNotIn("conflict_middle_name_2", un_prefixed.columns)

    def test__check_sanity(self):
        self.reader_fake.payment_ids = self.reader_fake.merge_by_last_name(
            self.reader_fake.payment_ids,
            self.reader_fake.conflicteds,
        )
        self.reader_fake.update_unmatched(
            self.reader_fake.payment_ids[self.reader_fake.payment_ids["profile_id"].isna()],
            Unmatcheds.NOLASTNAME,
        )
        self.reader_fake.filter_and_update_unique_ids(
            payment_ids_x_conflicteds=self.reader_fake.payment_ids,
            id_filters=[PaymentIDFilters.LASTNAME],
        )

        self.assertIsNone(self.reader_fake.check_sanity())

        # Remove a row from the payment_ids DataFrame
        self.reader_fake.num_conflicteds = self.reader_fake.num_conflicteds - 1
        self.assertRaises(
            AssertionError,
            self.reader_fake.check_sanity
        )

    def test__filter_and_update_unique_ids(self):
        self.reader_fake.payment_ids = self.reader_fake.merge_by_last_name(
            self.reader_fake.payment_ids,
            self.reader_fake.conflicteds,
        )

        pre_filter_len = len(self.reader_fake.unique_ids)

        self.reader_fake.filter_and_update_unique_ids(
            payment_ids_x_conflicteds=self.reader_fake.payment_ids,
            id_filters=[PaymentIDFilters.CREDENTIAL],
        )

        self.assertFalse(self.reader_fake.unique_ids.empty)
        self.assertEqual(len(self.reader_fake.unique_ids), pre_filter_len+2)
        self.assertIn("Doe", self.reader_fake.unique_ids["last_name"].values.tolist())
        self.assertIn("Smith", self.reader_fake.unique_ids["last_name"].values.tolist())

    def test__filter_by_credential(self):
        self.reader_fake.payment_ids = self.reader_fake.merge_by_last_name(
            self.reader_fake.payment_ids,
            self.reader_fake.conflicteds,
        )

        filtered_df = self.reader_fake.filter_by_credential(
            self.reader_fake.payment_ids,
        )

        pre_filter_len = len(self.reader_fake.payment_ids)
        self.assertIsInstance(filtered_df, pd.DataFrame)
        self.assertEqual(len(filtered_df), pre_filter_len-2)

    def test__filter_by_first_name(self):
        self.reader_fake.payment_ids = self.reader_fake.merge_by_last_name(
            self.reader_fake.payment_ids,
            self.reader_fake.conflicteds,
        )

        filtered_df = self.reader_fake.filter_by_firstname(
            self.reader_fake.payment_ids,
        )

        pre_filter_len = len(self.reader_fake.payment_ids)
        self.assertIsInstance(filtered_df, pd.DataFrame)
        self.assertEqual(len(filtered_df), pre_filter_len-2)

    def test__filter_by_specialty(self):
        self.reader_fake.payment_ids = self.reader_fake.merge_by_last_name(
            self.reader_fake.payment_ids,
            self.reader_fake.conflicteds,
        )

        filtered_df = self.reader_fake.filter_by_specialty(
            self.reader_fake.payment_ids,
        )

        pre_filter_len = len(self.reader_fake.payment_ids)
        self.assertIsInstance(filtered_df, pd.DataFrame)
        self.assertEqual(len(filtered_df), pre_filter_len-2)

    def test__filter_by_subspecialty(self):
        self.reader_fake.payment_ids = self.reader_fake.merge_by_last_name(
            self.reader_fake.payment_ids,
            self.reader_fake.conflicteds,
        )

        filtered_df = self.reader_fake.filter_by_subspecialty(
            self.reader_fake.payment_ids,
        )

        pre_filter_len = len(self.reader_fake.payment_ids)
        self.assertIsInstance(filtered_df, pd.DataFrame)
        self.assertEqual(len(filtered_df), pre_filter_len-2)

    def test__filter_by_fullspecialty(self):
        self.reader_fake.payment_ids = self.reader_fake.merge_by_last_name(
            self.reader_fake.payment_ids,
            self.reader_fake.conflicteds,
        )

        filtered_df = self.reader_fake.filter_by_fullspecialty(
            self.reader_fake.payment_ids,
        )

        pre_filter_len = len(self.reader_fake.payment_ids)
        self.assertIsInstance(filtered_df, pd.DataFrame)
        self.assertEqual(len(filtered_df), pre_filter_len-3)
        self.assertEqual(len(filtered_df.iloc[0]["specialtys"]), 1)
        self.assertIn(
            Specialtys(specialty="Internal Medicine", subspecialty=None),
            filtered_df.iloc[0]["specialtys"],
        )