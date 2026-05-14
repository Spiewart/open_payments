import os
import tempfile
import unittest

import pytest

from ..read import ReadPayments


class TestGetPaymentCsvPath(unittest.TestCase):
    """Unit tests for ReadPayments.get_payment_csv_path (glob-based resolver).

    These do not touch the real OpenPayments dataset; they build a temp
    directory tree with fabricated filenames matching the CMS naming
    convention.
    """

    def test__finds_postfix_unknown_in_advance(self):
        with tempfile.TemporaryDirectory() as tmp:
            year_dir = os.path.join(tmp, "2018")
            os.makedirs(year_dir)
            fname = "OP_DTL_GNRL_PGYR2018_P01232026_01102026.csv"
            open(os.path.join(year_dir, fname), "w").close()

            reader = ReadPayments(
                years=[2018],
                payment_classes=["general"],
                payments_folder=tmp,
            )
            self.assertEqual(
                reader.get_payment_csv_path("general", 2018),
                f"2018/{fname}",
            )

    def test__prefers_newer_mtime_when_multiple_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            year_dir = os.path.join(tmp, "2020")
            os.makedirs(year_dir)
            older = "OP_DTL_GNRL_PGYR2020_P06282024_06122024.csv"
            newer = "OP_DTL_GNRL_PGYR2020_P01232026_01102026.csv"
            older_path = os.path.join(year_dir, older)
            newer_path = os.path.join(year_dir, newer)
            open(older_path, "w").close()
            open(newer_path, "w").close()
            # Force older to actually be older on disk (some filesystems may
            # not give us distinct mtimes on rapid sequential creates).
            old_ts = 1_700_000_000  # ~Nov 2023
            new_ts = 1_730_000_000  # ~Oct 2024
            os.utime(older_path, (old_ts, old_ts))
            os.utime(newer_path, (new_ts, new_ts))

            reader = ReadPayments(
                years=[2020],
                payment_classes=["general"],
                payments_folder=tmp,
            )
            self.assertEqual(
                reader.get_payment_csv_path("general", 2020),
                f"2020/{newer}",
            )

    def test__raises_when_no_matching_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            year_dir = os.path.join(tmp, "2030")
            os.makedirs(year_dir)
            reader = ReadPayments(
                years=[2030],
                payment_classes=["general"],
                payments_folder=tmp,
            )
            with self.assertRaises(FileNotFoundError):
                reader.get_payment_csv_path("general", 2030)


class TestReadPayments(unittest.TestCase):
    @pytest.mark.integration
    def test__read_ownership_payment_csvs(self):
        # read_*_payments_csvs returns RAW CMS column names; the rename to
        # canonical names (profile_id, etc.) happens later in update_payments.
        reader = ReadPayments(
            years=2023,
            payment_classes=["ownership"],
            nrows=100,
        )

        ownership_payments = reader.read_ownership_payments_csvs()

        self.assertIn("Physician_Profile_ID", ownership_payments.columns)

    @pytest.mark.integration
    def test__read_general_payment_csvs(self):
        reader = ReadPayments(
            years=2023,
            payment_classes=["general"],
            nrows=100,
        )

        general_payments = reader.read_general_payments_csvs()

        self.assertIn("Covered_Recipient_Profile_ID", general_payments.columns)
