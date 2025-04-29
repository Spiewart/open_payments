import unittest
import pandas as pd

from ..credentials import convert_credentials, Credentials, PaymentCredentials


class TestPaymentCredentials(unittest.TestCase):
    def setUp(self):
        self.fake_payments = pd.DataFrame(
            {
                "credential_1": ["Medical Doctor", None, "Medical Doctor", "Medical Doctor", "Doctor of Osteopathy", "Chiropractor"],
                "credential_2": [None, "Doctor of Osteopathy", "Doctor of Osteopathy", "Certified Registered Nurse Anesthetist", "Doctor of Osteopathy", None],
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

        self.fake_payments = PaymentCredentials.credentials(self.fake_payments)

        payment_1 = self.fake_payments.iloc[0]
        credentials_1 = payment_1["credentials"]
        self.assertEqual(len(credentials_1), 1)
        self.assertIn(Credentials.MEDICAL_DOCTOR, credentials_1)

        payment_2 = self.fake_payments.iloc[1]
        credentials_2 = payment_2["credentials"]
        self.assertEqual(len(credentials_2), 2)
        self.assertIn(Credentials.DOCTOR_OF_OSTEOPATHY, credentials_2)
        self.assertIn(Credentials.NURSE_PRACTITIONER, credentials_2)

        payment_3 = self.fake_payments.iloc[2]
        credentials_3 = payment_3["credentials"]
        self.assertEqual(len(credentials_3), 2)
        self.assertIn(Credentials.MEDICAL_DOCTOR, credentials_3)
        self.assertIn(Credentials.DOCTOR_OF_OSTEOPATHY, credentials_3)

        payment_4 = self.fake_payments.iloc[3]
        credentials_4 = payment_4["credentials"]
        self.assertEqual(len(credentials_4), 3)
        self.assertIn(Credentials.MEDICAL_DOCTOR, credentials_4)
        self.assertIn(Credentials.CERTIFIED_REGISTERED_NURSE_ANAESTHETIST, credentials_4)
        self.assertIn(Credentials.NURSE_PRACTITIONER, credentials_4)

        payment_5 = self.fake_payments.iloc[4]
        credentials_5 = payment_5["credentials"]
        self.assertEqual(len(credentials_5), 2)
        self.assertIn(Credentials.DOCTOR_OF_OSTEOPATHY, credentials_5)
        self.assertIn(Credentials.PHYSICIAN_ASSISTANT, credentials_5)

        payment_6 = self.fake_payments.iloc[5]
        credentials_6 = payment_6["credentials"]
        self.assertEqual(len(credentials_6), 1)
        self.assertIn(Credentials.CHIROPRACTOR, credentials_6)
        self.assertIn(Credentials.DOCTOR_OF_OSTEOPATHY, credentials_2)
        self.assertIn(Credentials.NURSE_PRACTITIONER, credentials_2)


class TestConvertCredentials(unittest.TestCase):
    def test__with_str(self):
        converted = convert_credentials(
            "[<Credentials.MEDICAL_DOCTOR: 'Medical Doctor'>]"
        )
        self.assertIsInstance(converted, list)
        self.assertTrue(converted)
        self.assertEqual(len(converted), 1)
        self.assertIn(Credentials.MEDICAL_DOCTOR, converted)

    def test__with_multiple_strs(self):
        converted = convert_credentials(
            (
                "[<Credentials.MEDICAL_DOCTOR: 'Medical Doctor'>, "
                "<Credentials.DOCTOR_OF_OSTEOPATHY: 'Doctor of Osteopathy'>]"
            )
        )
        self.assertIsInstance(converted, list)
        self.assertTrue(converted)
        self.assertEqual(len(converted), 2)
        self.assertIn(Credentials.MEDICAL_DOCTOR, converted)
        self.assertIn(Credentials.DOCTOR_OF_OSTEOPATHY, converted)

    def test__applied_to_df(self):
        df = pd.DataFrame(
            {"credentials": [
                "[<Credentials.MEDICAL_DOCTOR: 'Medical Doctor'>]",
                "[<Credentials.MEDICAL_DOCTOR: 'Medical Doctor'>, <Credentials.DOCTOR_OF_OSTEOPATHY: 'Doctor of Osteopathy'>]",
            ]}
        )

        df["credentials"] = df["credentials"].apply(convert_credentials)
        self.assertIsInstance(df["credentials"].iloc[0], list)
        self.assertEqual(len(df["credentials"].iloc[0]), 1)
        self.assertIn(Credentials.MEDICAL_DOCTOR, df["credentials"].iloc[0])
        self.assertIsInstance(df["credentials"].iloc[1], list)
        self.assertEqual(len(df["credentials"].iloc[1]), 2)
        self.assertIn(Credentials.MEDICAL_DOCTOR, df["credentials"].iloc[1])
        self.assertIn(Credentials.DOCTOR_OF_OSTEOPATHY, df["credentials"].iloc[1])