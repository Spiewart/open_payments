import unittest
import pandas as pd

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

    def test__specialtys(self):

        specialtys_1 = PaymentSpecialtys.specialtys(self.fake_payments.iloc[0])
        print(specialtys_1)
        self.assertEqual(len(specialtys_1), 2)
        self.assertEqual(specialtys_1["specialty"].iloc[0], "Nephrology")
        self.assertEqual(specialtys_1["subspecialty"].iloc[0], "Transplant")