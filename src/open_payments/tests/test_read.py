import unittest

from ..read import ReadPayments


class TestReadPayments(unittest.TestCase):
    def test__read_ownership_payment_csvs(self):
        reader = ReadPayments(
            years=2023,
            payment_classes=["ownership"],
            nrows=100,
        )

        ownership_payments = reader.read_ownership_payments_csvs()
        print(ownership_payments.columns)
        self.assertIn("Physician_Primary_Type", ownership_payments.columns)

    def test__read_general_payment_csvs(self):
        reader = ReadPayments(
            years=2023,
            payment_classes=["general"],
            nrows=100,
        )

        general_payments = reader.read_general_payments_csvs()
        print(general_payments.columns)
        self.assertIn("Covered_Recipient_Primary_Type_1", general_payments.columns)