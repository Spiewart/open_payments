import unittest
import pandas as pd

from ..ids import PaymentIDs


class TestPaymentIDs(unittest.TestCase):

    def test__unique_payment_ids(self):
        unique_payment_ids = PaymentIDs(payment_classes="general").unique_payment_ids()
        print(unique_payment_ids.columns)
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
