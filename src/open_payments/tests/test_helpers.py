import os
import unittest

import pandas as pd
import pytest

from ..config import Settings
from ..helpers import get_conflicted_ids_from_file, get_file_suffix


class TestGetConflictedIdsFromFile(unittest.TestCase):
    @pytest.mark.integration
    def test__get_conflicted_ids_from_file(self):
        # Requires ~/open_payments_datasets/conflicteds_ids.xlsx — a local-only
        # xlsx that the production pipeline writes. Section 7 lifts persistence
        # into SearchResult.to_excel() which makes this testable in isolation.
        ids = get_conflicted_ids_from_file()

        self.assertTrue(isinstance(ids, tuple))
        self.assertEqual(len(ids), 2)
        self.assertTrue(isinstance(ids[0], pd.DataFrame))
        self.assertTrue(isinstance(ids[1], pd.DataFrame))


class TestFileSuffix(unittest.TestCase):
    def test__suffix_required(self):
        # Partial subset of years/classes -> suffix is emitted.
        suffix = get_file_suffix(years=2020, payment_classes=["research", "ownership"])
        self.assertEqual(suffix, "_research_ownership_2020")

    def test__no_suffix_when_complete_set(self):
        # Pulling the canonical "complete set" from Settings instead of
        # hardcoding it locally — this is exactly the bug the previous test
        # exposed (2020-2023 vs 2020-2024 drift).
        settings = Settings()
        suffix = get_file_suffix(
            years=list(settings.years),
            payment_classes=list(settings.payment_classes),
            settings=settings,
        )
        self.assertEqual(suffix, "")

    def test__settings_override_changes_completeness_threshold(self):
        # Child app with only 2023 cares about; "all years" for that app is [2023].
        custom = Settings(years=[2023])
        suffix = get_file_suffix(
            years=[2023], payment_classes=list(custom.payment_classes), settings=custom
        )
        self.assertEqual(suffix, "")


class TestSettingsDataDir(unittest.TestCase):
    def test__default_data_dir_matches_legacy_open_payments_directory(self):
        # The deleted `open_payments_directory()` helper returned this exact
        # path; the Settings default keeps the contract.
        s = Settings()
        self.assertEqual(
            str(s.data_dir), os.path.join(os.path.expanduser("~"), "open_payments_datasets")
        )
