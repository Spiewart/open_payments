import unittest

import pandas as pd

from ..credentials import Credentials, CredentialsMixin
from ..physicians_only import PhysicianFilter
from ..read import ReadPayments
from ..specialtys import SpecialtysMixin


class SpecialtyCredentialReader(SpecialtysMixin, CredentialsMixin, ReadPayments):
    """Test class for reading payments and selecting specialty
    and credential columns."""

    pass


class TestPhysiciansFilter(unittest.TestCase):
    def setUp(self):
        self.reader = SpecialtyCredentialReader(
            nrows=1000,
            payment_classes=[
                "general",
                "ownership",
            ],
        )
        # Bug 5 fix: chunked CSV read preserves per-chunk indices, which
        # collide across chunks. reset_index here so the test can use scalar
        # `.loc[idx]` instead of the legacy `==idx -> .any()` workaround.
        self.general_payments = self.reader.read_payments_csvs(payment_class="general").reset_index(
            drop=True
        )
        self.ownership_payments = self.reader.read_payments_csvs(
            payment_class="ownership"
        ).reset_index(drop=True)
        self.general_filter = PhysicianFilter(self.general_payments)
        self.ownership_filter = PhysicianFilter(self.ownership_payments)
        self.fake_general_data = pd.DataFrame(
            {
                "Covered_Recipient_Primary_Type_1": [
                    Credentials.MEDICAL_DOCTOR,
                    Credentials.DOCTOR_OF_PODIATRIC_MEDICINE,
                    Credentials.DOCTOR_OF_OSTEOPATHY,
                    Credentials.PHYSICIAN_ASSISTANT,
                    Credentials.NURSE_PRACTITIONER,
                ],
                "Covered_Recipient_Specialty_1": [
                    "Allopathic & Osteopathic Physicians",
                    "Podiatric Medicine & Surgery",
                    "Allopathic & Osteopathic Physicians",
                    "Physician Assistants & Advanced Practice Nursing Providers|Physician Assistant",
                    "Nurse Practitioners",
                ],
            }
        )

    def test__physician_filter(self):
        physician_filter = PhysicianFilter(self.general_payments)
        filtered_payments = physician_filter.filter()

        self.assertIsInstance(filtered_payments, pd.DataFrame)

        # Bug 5 fix: previously this loop did
        #   `specialty_results.index == idx` -> mask, then `.any()` on the masked
        # Series. If the test data ever had repeated indices, the mask
        # could select multiple rows and `.any()` would mask data-quality
        # bugs. Replace with direct index lookup (KeyError surfaces problems
        # rather than silently succeeding via .any()).
        specialty_results = physician_filter.physician_specialty()
        credential_results = physician_filter.physician_credential()

        for idx in filtered_payments.index:
            specialty_passed = bool(specialty_results.loc[idx])
            credential_passed = bool(credential_results.loc[idx])

            self.assertTrue(
                specialty_passed or credential_passed,
                f"Row {idx} should pass either specialty or credential filter",
            )

        # Verify that filtered payments contain expected credentials
        if len(filtered_payments) > 0:
            all_credential_cols = physician_filter.get_credential_filter_columns()
            credential_values = set()
            for col in all_credential_cols:
                if col in filtered_payments.columns:
                    credential_values.update(filtered_payments[col].dropna().unique())

            # Should contain physician credentials if any credential
            # filtering occurred
            physician_credentials = {Credentials.MEDICAL_DOCTOR, Credentials.DOCTOR_OF_OSTEOPATHY}
            if credential_values:
                self.assertTrue(
                    len(physician_credentials.intersection(credential_values)) > 0,
                    "Filtered payments should contain physician credentials",
                )

        fake_data_filter = PhysicianFilter(self.fake_general_data)
        fake_filtered = fake_data_filter.filter()
        self.assertEqual(
            len(fake_filtered),
            2,
            "Fake data should have two rows with 'Allopathic & Osteopathic Physicians' specialty",
        )

    def test__get_credential_filter_columns(self):
        cred_filter_columns = PhysicianFilter(self.general_payments).get_credential_filter_columns()

        general_credential_columns = [
            "Covered_Recipient_Primary_Type_1",
            "Covered_Recipient_Primary_Type_2",
            "Covered_Recipient_Primary_Type_3",
            "Covered_Recipient_Primary_Type_4",
            "Covered_Recipient_Primary_Type_5",
            "Covered_Recipient_Primary_Type_6",
        ]

        for column in general_credential_columns:
            self.assertIn(column, cred_filter_columns)

        cred_filter_columns = PhysicianFilter(
            self.ownership_payments
        ).get_credential_filter_columns()

        research_credential_columns = [
            "Physician_Primary_Type",
        ]

        for column in research_credential_columns:
            self.assertIn(column, cred_filter_columns)

    def test__get_specialty_filter_columns(self):
        spec_filter_columns = PhysicianFilter(self.general_payments).get_specialty_filter_columns()

        general_specialty_columns = [
            "Covered_Recipient_Specialty_1",
            "Covered_Recipient_Specialty_2",
            "Covered_Recipient_Specialty_3",
            "Covered_Recipient_Specialty_4",
            "Covered_Recipient_Specialty_5",
            "Covered_Recipient_Specialty_6",
        ]

        for column in general_specialty_columns:
            self.assertIn(column, spec_filter_columns)

        spec_filter_columns = PhysicianFilter(
            self.ownership_payments
        ).get_specialty_filter_columns()

        research_specialty_columns = [
            "Physician_Specialty",
        ]

        for column in research_specialty_columns:
            self.assertIn(column, spec_filter_columns)

    def test__physician_specialty(self):

        physician_specialty_series = self.general_filter.physician_specialty()

        self.assertIsInstance(physician_specialty_series, pd.Series)

        self.assertEqual(len(physician_specialty_series.unique()), 2)

        self.assertIn(True, physician_specialty_series.values)
        self.assertIn(False, physician_specialty_series.values)

        fake_data_filter = PhysicianFilter(self.fake_general_data)
        fake_specialty_series = fake_data_filter.physician_specialty()
        self.assertEqual(
            fake_specialty_series.sum(),
            2,
            "Fake data should have two rows with 'Allopathic & Osteopathic Physicians' specialty",
        )

    def test__physician_credential(self):

        physician_credential_series = self.general_filter.physician_credential()

        self.assertIsInstance(physician_credential_series, pd.Series)

        self.assertEqual(len(physician_credential_series.unique()), 2)

        self.assertIn(True, physician_credential_series.values)
        self.assertIn(False, physician_credential_series.values)

        fake_data_filter = PhysicianFilter(self.fake_general_data)
        fake_credential_series = fake_data_filter.physician_credential()
        self.assertEqual(
            fake_credential_series.sum(),
            2,
            "Fake data should have two rows with 'Medical Doctor' or 'Doctor of Osteopathy' credentials",
        )
