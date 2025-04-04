import unittest
import pandas as pd

from ..credentials import PaymentCredentials
from ..read import ReadPayments


class TestPaymentCredentials(unittest.TestCase):
    def setUp(self):
        self.fake_payments = pd.DataFrame(
            {
                "credential_1": ["Medical Doctor", None, "Medical Doctor", "Medical Doctor", "Doctor of Osteopathy", "Chiropractor"],
                "credential_2": [None, "Doctor of Osteopathy", "Doctor of Osteopathy", "CRNA", "Doctor of Osteopathy", None],
                "credential_3": ["Medical Doctor", "Nurse Practitioner", "Medical Doctor", None, "Doctor of Osteopathy", None],
                "credential_4": ["Medical Doctor", "Doctor of Osteopathy", None, "Medical Doctor", "Physician Assistant", None],
                "credential_5": ["Medical Doctor", "Doctor of Osteopathy", "Medical Doctor", "Nurse Practitioner", None, None],
                "credential_6": [None, None, None, None, None, None],
            }
        )

    def test__unique_credentials(self):
        unique_credentials = PaymentCredentials(nrows=10000).unique_credentials().values.tolist()

        self.assertEqual(len(unique_credentials), 12)
        self.assertIn("Medical Doctor", unique_credentials)
        self.assertIn("Doctor of Dentistry", unique_credentials)
        self.assertIn("Doctor of Osteopathy", unique_credentials)
        self.assertIn("Doctor of Optometry", unique_credentials)
        self.assertIn("Chiropractor", unique_credentials)
        self.assertIn("Doctor of Podiatric Medicine", unique_credentials)
        self.assertIn("Nurse Practitioner", unique_credentials)
        self.assertIn("Physician Assistant", unique_credentials)
        self.assertIn("Certified Registered Nurse Anesthetist", unique_credentials)
        self.assertIn("Clinical Nurse Specialist", unique_credentials)
        self.assertIn("Certified Nurse-Midwife", unique_credentials)
        self.assertIn("Anesthesiologist Assistant", unique_credentials)

    def test__credentials(self):

        credentials_1 = PaymentCredentials.credentials(self.fake_payments.iloc[0])
        self.assertEqual(len(credentials_1), 1)
        self.assertIn("Medical Doctor", credentials_1)

        credentials_2 = PaymentCredentials.credentials(self.fake_payments.iloc[1])
        self.assertEqual(len(credentials_2), 2)
        self.assertIn("Doctor of Osteopathy", credentials_2)
        self.assertIn("Nurse Practitioner", credentials_2)

        credentials_3 = PaymentCredentials.credentials(self.fake_payments.iloc[2])
        self.assertEqual(len(credentials_3), 2)
        self.assertIn("Medical Doctor", credentials_3)
        self.assertIn("Doctor of Osteopathy", credentials_3)

        credentials_4 = PaymentCredentials.credentials(self.fake_payments.iloc[3])
        self.assertEqual(len(credentials_4), 3)
        self.assertIn("Medical Doctor", credentials_4)
        self.assertIn("CRNA", credentials_4)
        self.assertIn("Nurse Practitioner", credentials_4)

        credentials_5 = PaymentCredentials.credentials(self.fake_payments.iloc[4])
        self.assertEqual(len(credentials_5), 2)
        self.assertIn("Doctor of Osteopathy", credentials_5)
        self.assertIn("Physician Assistant", credentials_5)

        credentials_6 = PaymentCredentials.credentials(self.fake_payments.iloc[5])
        self.assertEqual(len(credentials_6), 1)
        self.assertIn("Chiropractor", credentials_6)

    def test__filter_MD_DO(self):
        payments_with_credentials = PaymentCredentials.combine_credentials(self.fake_payments)

        self.assertEqual(len(self.fake_payments), 6)
        filtered_payments = PaymentCredentials.filter_MD_DO(payments_with_credentials)
        self.assertEqual(len(filtered_payments), 5)
