import unittest
import pandas as pd

from ..citys import PaymentCity


class TestPaymentCity(unittest.TestCase):
    def setUp(self):
        self.reader = PaymentCity()
        self.general_payments = self.reader.read_general_payments_csvs()
        self.ownership_payments = self.reader.read_ownership_payments_csvs()

    def test__general_columns(self):
        self.assertIn("Recipient_City", self.reader.general_columns)
        self.assertEqual(self.reader.general_columns["Recipient_City"], ("city", str))

        self.assertFalse("city" in self.general_payments.columns)
        self.general_payments = self.reader.update_payments("general")
        self.assertTrue("city" in self.general_payments.columns)

    def test__ownership_columns(self):
        self.assertIn("Recipient_City", self.reader.ownership_columns)
        self.assertEqual(self.reader.ownership_columns["Recipient_City"], ("city", str))

        self.assertFalse("city" in self.ownership_payments.columns)
        self.ownership_payments = self.reader.update_payments("ownership")
        self.assertTrue("city" in self.ownership_payments.columns)