import unittest

import numpy as np
import pandas as pd

from ..credentials import Credentials
from ..physicians_only import ReadPaymentsPhysicians
from ..read import ReadPayments


class TestPhysiciansFilter(unittest.TestCase):
    def setUp(self):
        self.reader = ReadPayments(nrows=1000, payment_classes=[
            "general",
            "ownership",
        ])
        self.general_payments = self.reader.read_general_payments_csvs()
        self.ownership_payments = self.reader.read_ownership_payments_csvs()
        self.general_filter = ReadPaymentsPhysicians(self.general_payments)
        self.ownership_filter = ReadPaymentsPhysicians(self.ownership_payments)

    def test__physician_filter(self):

        filtered_payments = ReadPaymentsPhysicians(self.general_payments).filter()
        self.assertIsInstance(filtered_payments, pd.DataFrame)

        self.assertTrue(
            filtered_payments[
                "Covered_Recipient_Specialty_1"
            ].apply(
                lambda x: (pd.isna(x) or "Allopathic & Osteopathic Physicians" in x)
            ).all()
        )

        self.assertTrue(
            filtered_payments[
                "Covered_Recipient_Specialty_2"
            ].apply(
                lambda x: (pd.isna(x) or "Allopathic & Osteopathic Physicians" in x)
            ).all()
        )


        unique_credentials = np.unique(filtered_payments["Covered_Recipient_Primary_Type_1"].values.tolist())
        self.assertIn(Credentials.MEDICAL_DOCTOR, unique_credentials)
        self.assertIn(Credentials.DOCTOR_OF_OSTEOPATHY, unique_credentials)
        self.assertIn("nan", unique_credentials)
        self.assertNotIn("", unique_credentials)

    def test__get_credential_filter_columns(self):
        cred_filter_columns = ReadPaymentsPhysicians(self.general_payments).get_credential_filter_columns()

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

        cred_filter_columns = ReadPaymentsPhysicians(self.ownership_payments).get_credential_filter_columns()

        research_credential_columns = [
            "Physician_Primary_Type",
        ]

        for column in research_credential_columns:
            self.assertIn(column, cred_filter_columns)

    def test__get_specialty_filter_columns(self):
        spec_filter_columns = ReadPaymentsPhysicians(self.general_payments).get_specialty_filter_columns()

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

        spec_filter_columns = ReadPaymentsPhysicians(self.ownership_payments).get_specialty_filter_columns()

        research_specialty_columns = [
            "Physician_Specialty",
        ]

        for column in research_specialty_columns:
            self.assertIn(column, spec_filter_columns)

    def test__physician_specialty(self):

        physician_specialty_series = self.general_filter.physician_specialty()

        self.assertIsInstance(physician_specialty_series, pd.Series)

        self.assertEqual(len(np.unique(physician_specialty_series.values)), 2)

        self.assertIn(True, physician_specialty_series.values)
        self.assertIn(False, physician_specialty_series.values)

    def test__physician_credential(self):

        physician_credential_series = self.general_filter.physician_credential()

        self.assertIsInstance(physician_credential_series, pd.Series)

        self.assertEqual(len(np.unique(physician_credential_series.values)), 2)

        self.assertIn(True, physician_credential_series.values)
        self.assertIn(False, physician_credential_series.values)

    def test__specialty_null(self):
        specialty_null_series = self.general_filter.specialty_null()

        self.assertIsInstance(specialty_null_series, pd.Series)

        self.assertEqual(len(np.unique(specialty_null_series.values)), 2)

        self.assertIn(True, specialty_null_series.values)
        self.assertIn(False, specialty_null_series.values)

    def test__credential_null(self):
        credential_null_series = self.general_filter.credential_null()

        self.assertIsInstance(credential_null_series, pd.Series)

        self.assertEqual(len(np.unique(credential_null_series.values)), 2)

        self.assertIn(True, credential_null_series.values)
        self.assertIn(False, credential_null_series.values)