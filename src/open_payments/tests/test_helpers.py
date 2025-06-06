import os
import pandas as pd
import unittest

from ..helpers import get_conflicted_ids_from_file, get_file_suffix, open_payments_directory


class TestGetConflictedIdsFromFile(unittest.TestCase):
    def test__get_conflicted_ids_from_file(self):

        ids = get_conflicted_ids_from_file()

        self.assertTrue(isinstance(ids, tuple))
        self.assertEqual(len(ids), 2)
        self.assertTrue(isinstance(ids[0], pd.DataFrame))
        self.assertTrue(isinstance(ids[1], pd.DataFrame))


class TestFileSuffix(unittest.TestCase):
    def test__suffix_required(self):
        suffix = get_file_suffix(
            years=2020,
            payment_classes=["research", "ownership"]
        )
        self.assertEqual(suffix, "_research_ownership_2020")

    def test__no_suffix_required(self):
        suffix = get_file_suffix(
            years=[2020, 2021, 2022, 2023],
            payment_classes=["general", "research", "ownership"]
        )
        self.assertEqual(suffix, "")


class TestOpenPaymentsDirectory(unittest.TestCase):
    def test__open_payments_directory(self):
        path = open_payments_directory()
        self.assertEqual(path, os.path.join(os.path.expanduser('~'), 'open_payments_datasets'))