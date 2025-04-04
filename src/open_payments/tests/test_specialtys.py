import unittest
import pandas as pd

from ..ids import PaymentIDs
from ..specialtys import PaymentSpecialtys, Specialtys


class TestPaymentSpecialtys(unittest.TestCase):
    def setUp(self):
        self.fake_payments = pd.DataFrame({
            "specialty_1": [
                "MD|Nephrology|Transplant",
                "MD|Nephrology",
                "MD|Infectious Diseases",
                "MD|Hematology & Oncology|Bone Marrow Transplant",
                "DO|Infectious Diseases",
            ],
            "specialty_2": [
                None, None, None, None, None
            ],
            "specialty_3": [
                "NP|Family Practice",
                "MD|Family Practice",
                "MD|Family Practice",
                "MD|Orthopedic Surgery",
                None,
            ],
            "specialty_4": [
                None, None, "MD|Ear Nose & Throat", None, None
            ],
            "specialty_5": [
                None, None, None, None, None
            ],
            "specialty_6": [
                None, None, None, None, None
            ],
        })

    def test__specialtys(self):

        # Test that the method works when applied to a fake DataFrame
        updated_payments = PaymentSpecialtys.specialtys(self.fake_payments)
        self.assertTrue(isinstance(updated_payments, pd.DataFrame))

        updated_payment_1 = updated_payments.iloc[0]
        specialtys_1 = updated_payment_1["specialtys"]
        self.assertTrue(isinstance(specialtys_1, list))
        self.assertEqual(len(specialtys_1), 2)
        specialty_1 = specialtys_1[0]
        specialty_2 = specialtys_1[1]
        self.assertEqual(specialty_1.specialty, "Nephrology")
        self.assertEqual(specialty_1.subspecialty, "Transplant")
        self.assertEqual(specialty_2.specialty, "Family Practice")
        self.assertIsNone(specialty_2.subspecialty)

        updated_payment_2 = updated_payments.iloc[1]
        specialtys_2 = updated_payment_2["specialtys"]
        self.assertTrue(isinstance(specialtys_2, list))
        self.assertEqual(len(specialtys_2), 2)
        specialty_1 = specialtys_2[0]
        specialty_2 = specialtys_2[1]
        self.assertEqual(specialty_1.specialty, "Nephrology")
        self.assertIsNone(specialty_1.subspecialty)
        self.assertEqual(specialty_2.specialty, "Family Practice")
        self.assertIsNone(specialty_2.subspecialty)

        updated_payment_3 = updated_payments.iloc[2]
        specialtys_3 = updated_payment_3["specialtys"]
        self.assertTrue(isinstance(specialtys_3, list))
        self.assertEqual(len(specialtys_3), 3)
        specialty_1 = specialtys_3[0]
        specialty_2 = specialtys_3[1]
        specialty_3 = specialtys_3[2]
        self.assertEqual(specialty_1.specialty, "Infectious Diseases")
        self.assertIsNone(specialty_1.subspecialty)
        self.assertEqual(specialty_2.specialty, "Family Practice")
        self.assertIsNone(specialty_2.subspecialty)
        self.assertEqual(specialty_3.specialty, "Ear Nose & Throat")
        self.assertIsNone(specialty_3.subspecialty)

        updated_payment_4 = updated_payments.iloc[3]
        specialtys_4 = updated_payment_4["specialtys"]
        self.assertTrue(isinstance(specialtys_4, list))
        self.assertEqual(len(specialtys_4), 2)
        specialty_1 = specialtys_4[0]
        specialty_2 = specialtys_4[1]
        self.assertEqual(specialty_1.specialty, "Hematology & Oncology")
        self.assertEqual(specialty_1.subspecialty, "Bone Marrow Transplant")
        self.assertEqual(specialty_2.specialty, "Orthopedic Surgery")
        self.assertIsNone(specialty_2.subspecialty)

        updated_payment_5 = updated_payments.iloc[4]
        specialtys_5 = updated_payment_5["specialtys"]
        self.assertTrue(isinstance(specialtys_5, list))
        self.assertEqual(len(specialtys_5), 1)
        specialty_1 = specialtys_5[0]
        self.assertEqual(specialty_1.specialty, "Infectious Diseases")
        self.assertIsNone(specialty_1.subspecialty)

    def test__parse_specialtys(self):
        """Tests the PaymentSpecialtys parse_specialtys method."""
        payment_1 = self.fake_payments.iloc[0]

        parsed_specialtys = PaymentSpecialtys.parse_specialtys(payment_1)

        self.assertTrue(isinstance(parsed_specialtys, pd.DataFrame))
        self.assertEqual(len(parsed_specialtys), 2)
        self.assertIn("provider_type", parsed_specialtys.columns)
        self.assertIn("specialty", parsed_specialtys)
        self.assertIn("subspecialty", parsed_specialtys)

        self.assertIn("MD", parsed_specialtys["provider_type"].values)
        self.assertIn("NP", parsed_specialtys["provider_type"].values)
        self.assertIn("Family Practice", parsed_specialtys["specialty"].values)
        self.assertIn("Nephrology", parsed_specialtys["specialty"].values)
        self.assertIn("Transplant", parsed_specialtys["subspecialty"].values)

    def test__create_specialtys(self):
        """Tests the PaymentSpecialtys specialties_list method."""
        payment_1 = self.fake_payments.iloc[0]

        create_specialtys = PaymentSpecialtys.create_specialtys(payment=payment_1)

        self.assertTrue(isinstance(create_specialtys, list))
        self.assertEqual(len(create_specialtys), 2)
        for specialty in create_specialtys:
            self.assertTrue(isinstance(specialty, Specialtys))
        self.assertEqual(create_specialtys[0].specialty, "Nephrology")
        self.assertEqual(create_specialtys[0].subspecialty, "Transplant")
        self.assertEqual(create_specialtys[1].specialty, "Family Practice")
        self.assertIsNone(create_specialtys[1].subspecialty)