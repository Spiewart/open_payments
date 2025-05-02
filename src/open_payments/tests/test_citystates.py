import unittest

import pandas as pd

from ..citystates import CityState, PaymentCityStates


class TestPaymentCityStates(unittest.TestCase):
    def setUp(self):
        self.reader = PaymentCityStates()
        self.general_payments = self.reader.read_general_payments_csvs()
        self.ownership_payments = self.reader.read_ownership_payments_csvs()
        self.fake_states_general = pd.DataFrame({
            "Recipient_City": [
                "New York",
                "Minneapolis",
                "Cheyenne",
                None,
                "Billings",
            ],
            "Recipient_State": [
                "NY",
                "MN",
                "WY",
                "WY",
                None,
            ],
            "Covered_Recipient_License_State_code1": [
                "NY",
                None,
                "NY",
                "AL",
                "MT",
            ],
            "Covered_Recipient_License_State_code2": [
                "NY",
                None,
                "NY",
                "CA",
                "ND",
            ],
            "Covered_Recipient_License_State_code3": [
                "NY",
                "FL",
                "NY",
                "NV",
                "SD",
            ],
            "Covered_Recipient_License_State_code4": [
                "NY",
                None,
                "NY",
                "AZ",
                None,
            ],
            "Covered_Recipient_License_State_code5": [
                "NY",
                None,
                "NY",
                "WI",
                None,
            ],
        })
        self.fake_states_ownership = pd.DataFrame({
            "Recipient_City": [
                "New York",
                "Minneapolis",
                "Cheyenne",
                None,
                "Billings",
            ],
            "Recipient_State": [
                "NY",
                "MN",
                "WY",
                "WY",
                None,
            ],
        })
        self.fake_reader = PaymentCityStates(
            general_payments=self.fake_states_general,
            ownership_payments=self.fake_states_ownership,
        )

    def test__general_columns(self):
        self.assertIn("Recipient_City", self.reader.general_columns)
        self.assertEqual(self.reader.general_columns["Recipient_City"], ("city", str))
        self.assertIn("Recipient_State", self.general_payments.columns)
        self.assertIn("Covered_Recipient_License_State_code1", self.general_payments.columns)
        self.assertIn("Covered_Recipient_License_State_code2", self.general_payments.columns)
        self.assertIn("Covered_Recipient_License_State_code3", self.general_payments.columns)
        self.assertIn("Covered_Recipient_License_State_code4", self.general_payments.columns)
        self.assertIn("Covered_Recipient_License_State_code5", self.general_payments.columns)

        self.assertFalse("city" in self.general_payments.columns)
        self.general_payments = self.reader.update_payments("general")
        self.assertTrue("city" in self.general_payments.columns)

    def test__ownership_columns(self):
        self.assertIn("Recipient_City", self.reader.ownership_columns)
        self.assertEqual(self.reader.ownership_columns["Recipient_City"], ("city", str))
        self.assertIn("Recipient_State", self.general_payments.columns)

        self.assertFalse("city" in self.ownership_payments.columns)
        self.ownership_payments = self.reader.update_payments("ownership")
        self.assertTrue("city" in self.ownership_payments.columns)

    def test__research_columns(self):
        self.assertIn("Recipient_State", self.ownership_payments.columns)
        self.assertIn("Covered_Recipient_License_State_code1", self.general_payments.columns)
        self.assertIn("Covered_Recipient_License_State_code2", self.general_payments.columns)
        self.assertIn("Covered_Recipient_License_State_code3", self.general_payments.columns)
        self.assertIn("Covered_Recipient_License_State_code4", self.general_payments.columns)
        self.assertIn("Covered_Recipient_License_State_code5", self.general_payments.columns)

        self.assertFalse("city" in self.general_payments.columns)
        self.general_payments = self.reader.update_payments("general")
        self.assertTrue("city" in self.general_payments.columns)

    def test__citystates(self):
        self.fake_reader.update_payments("general")
        self.fake_reader = self.fake_reader.citystates(self.fake_states_general)
        self.assertIn("citystates", self.fake_reader.columns)
        self.assertNotIn("city", self.fake_reader.columns)
        self.assertNotIn("state_primary", self.fake_reader.columns)
        self.assertNotIn("state_license_1", self.fake_reader.columns)
        self.assertNotIn("state_license_2", self.fake_reader.columns)
        self.assertNotIn("state_license_3", self.fake_reader.columns)
        self.assertNotIn("state_license_4", self.fake_reader.columns)
        self.assertNotIn("state_license_5", self.fake_reader.columns)

        citystates_1 = self.fake_reader["citystates"].iloc[0]
        self.assertEqual(1, len(citystates_1))
        self.assertEqual(citystates_1[0].state, "NY")
        self.assertEqual(citystates_1[0].city, "New York")

        citystates_2 = self.fake_reader["citystates"].iloc[1]
        self.assertEqual(2, len(citystates_2))
        self.assertEqual(citystates_2[0].state, "MN")
        self.assertEqual(citystates_2[0].city, "Minneapolis")
        self.assertEqual(citystates_2[1].state, "FL")
        self.assertEqual(citystates_2[1].city, "Minneapolis")

        citystates_3 = self.fake_reader["citystates"].iloc[2]
        self.assertEqual(2, len(citystates_3))
        self.assertEqual(citystates_3[0].state, "WY")
        self.assertEqual(citystates_3[0].city, "Cheyenne")
        self.assertEqual(citystates_3[1].state, "NY")
        self.assertEqual(citystates_3[1].city, "Cheyenne")

        citystates_4 = self.fake_reader["citystates"].iloc[3]
        self.assertEqual(6, len(citystates_4))
        self.assertEqual(citystates_4[0].state, "WY")
        self.assertEqual(citystates_4[0].city, None)
        self.assertEqual(citystates_4[1].state, "AL")
        self.assertEqual(citystates_4[1].city, None)
        self.assertEqual(citystates_4[2].state, "CA")
        self.assertEqual(citystates_4[2].city, None)
        self.assertEqual(citystates_4[3].state, "NV")
        self.assertEqual(citystates_4[3].city, None)
        self.assertEqual(citystates_4[4].state, "AZ")
        self.assertEqual(citystates_4[4].city, None)
        self.assertEqual(citystates_4[5].state, "WI")
        self.assertEqual(citystates_4[5].city, None)

        citystates_5 = self.fake_reader["citystates"].iloc[4]
        self.assertEqual(3, len(citystates_5))
        self.assertEqual(citystates_5[0].state, "MT")
        self.assertEqual(citystates_5[0].city, "Billings")
        self.assertEqual(citystates_5[1].state, "ND")
        self.assertEqual(citystates_5[1].city, "Billings")
        self.assertEqual(citystates_5[2].state, "SD")
        self.assertEqual(citystates_5[2].city, "Billings")

    def test__create_citystates(self):

        self.fake_reader.update_payments("general")
        payment_1 = self.fake_reader.general_payments.iloc[0]
        citystates_1 = self.reader.create_citystates(payment_1)
        self.assertTrue(isinstance(citystates_1, list))
        self.assertEqual(1, len(citystates_1))
        for citystate in citystates_1:
            self.assertTrue(isinstance(citystate, CityState))
            self.assertEqual("NY", citystate.state)
            self.assertEqual("New York", citystate.city)


class TestCityState(unittest.TestCase):

    def test__state_is_abbrev(self):
        citystate = CityState(city="New York", state="NY")
        self.assertTrue(citystate.state_is_abbrev(citystate.state))
        citystate = CityState(city="New York", state="New York")
        self.assertFalse(citystate.state_is_abbrev(citystate.state))
        citystate = CityState(city="New York", state=None)
        self.assertFalse(citystate.state_is_abbrev(citystate.state))

    def test__state_abbrev(self):
        citystate = CityState(city="New York", state="NY")
        self.assertEqual(citystate.state_abbrev, "NY")
        citystate = CityState(city="New York", state="New York")
        self.assertEqual(citystate.state_abbrev, "NY")
        citystate = CityState(city="New York", state=None)
        self.assertEqual(citystate.state_abbrev, None)

    def test__state_is_full_name(self):
        citystate = CityState(city="New York", state="NY")
        self.assertFalse(citystate.state_is_full_name(state=citystate.state))
        citystate = CityState(city="New York", state="New York")
        self.assertTrue(citystate.state_is_full_name(state=citystate.state))
        citystate = CityState(city="New York", state=None)
        self.assertFalse(citystate.state_is_full_name(state=citystate.state))

    def test__state_full(self):
        citystate = CityState(city="New York", state="NY")
        self.assertEqual(citystate.state_full, "New York")
        citystate = CityState(city="New York", state="New York")
        self.assertEqual(citystate.state_full, "New York")
        citystate = CityState(city="New York", state=None)
        self.assertEqual(citystate.state_full, None)

    def test__state_matches(self):
        citystate = CityState(city="New York", state="NY")
        self.assertTrue(citystate.state_matches("NY"))
        self.assertTrue(citystate.state_matches("New York"))
        self.assertFalse(citystate.state_matches("New Jersey"))
        self.assertFalse(citystate.state_matches(None))

    def test__citystate_matches(self):
        citystate = CityState(city="New York", state="NY")
        self.assertTrue(citystate.citystate_matches(CityState(city="New York", state="NY")))
        self.assertTrue(citystate.citystate_matches(CityState(city="New York", state="New York")))
        self.assertFalse(citystate.citystate_matches(CityState(city="New Jersey", state="NY")))
