import unittest
import pandas as pd

from ..credentials import PaymentCredentials


class TestPaymentCredentials(unittest.TestCase):
    def setUp(self):
        self.fake_payments = pd.DataFrame(
            {
                "credential_1": ["Medical Doctor", None, "Medical Doctor", "Medical Doctor", "Doctor of Osteopathy", "Chiropracter"],
                "credential_2": [None, "Doctor of Osteopathy", "Doctor of Osteopathy", "CRNA", "Doctor of Osteopathy", None],
                "credential_3": ["Medical Doctor", "Nurse Practitioner", "Medical Doctor", None, "Doctor of Osteopathy", None],
                "credential_4": ["Medical Doctor", "Doctor of Osteopathy", None, "Medical Doctor", "Physician Assistant", None],
                "credential_5": ["Medical Doctor", "Doctor of Osteopathy", "Medical Doctor", "Nurse Practitioner", None, None],
                "credential_6": [None, None, None, None, None, None],
            }
        )

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
        self.assertIn("Chiropracter", credentials_6)

    def test__filter_MD_DO(self):
        self.assertEqual(len(self.fake_payments), 6)
        filtered_payments = PaymentCredentials.filter_MD_DO(self.fake_payments)
        self.assertEqual(len(filtered_payments), 5)
