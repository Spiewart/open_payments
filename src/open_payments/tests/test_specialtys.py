import unittest
import pandas as pd

from ..ids import PaymentIDs
from ..specialtys import PaymentSpecialtys


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
        self.real_ID_payments = PaymentIDs(payment_classes="general").unique_MD_DO_payment_ids()

    def test__specialtys(self):

        # Test that the method works when applied to a fake DataFrame
        specialtys_1 = PaymentSpecialtys.specialtys(self.fake_payments.iloc[0])
        self.assertEqual(len(specialtys_1), 2)
        self.assertEqual(specialtys_1["specialty"].iloc[0], "Nephrology")
        self.assertEqual(specialtys_1["subspecialty"].iloc[0], "Transplant")
        self.assertNotIn("MD", specialtys_1.columns)

        # Test that the method works when applied to a real DataFrame
        self.real_ID_payments["specialtys"] = self.real_ID_payments.apply(PaymentSpecialtys.specialtys, axis=1)
        self.assertIn("specialtys", self.real_ID_payments.columns)
        self.assertEqual(len(self.real_ID_payments["specialtys"].iloc[0]), 1)
        self.assertNotIn("provider_type", self.real_ID_payments["specialtys"].iloc[0].columns)
        self.assertIn("specialty", self.real_ID_payments["specialtys"].iloc[0].columns)
        self.assertIn("subspecialty", self.real_ID_payments["specialtys"].iloc[0].columns)