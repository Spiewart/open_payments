import glob
import logging
import os
from typing import Literal, Union

import pandas as pd

from .config import Settings
from .physicians_only import PhysicianFilter

# Configure logging
logging.basicConfig(level=logging.INFO)


class ReadPayments:
    """Class method whose only job is to read the OpenPayments csv files.
    It can be subclassed to add additional functionality and efficiency
    when reading the csv files.

    All path / year / payment-class defaults come from a `Settings` object,
    which itself defaults to reading from `OPEN_PAYMENTS_*` env vars. Pass an
    explicit `settings=` to override per-call, or pass `years=`/`payment_classes=`/
    `payments_folder=` to override individual fields without constructing a
    Settings.
    """

    def __init__(
        self,
        years: Union[list[int], int, None] = None,
        payment_classes: Union[
            list[Literal["general", "ownership", "research"]],
            Literal["general", "ownership", "research"],
            None,
        ] = None,
        payments_folder: Union[str, None] = None,
        nrows: Union[int, None] = 100,
        general_payments: pd.DataFrame = None,
        ownership_payments: pd.DataFrame = None,
        research_payments: pd.DataFrame = None,
        MD_DO_only: bool = True,
        settings: Union[Settings, None] = None,
    ):
        self.settings = settings if settings is not None else Settings()

        self.years = (
            [years]
            if isinstance(years, int)
            else years
            if years is not None
            else self.settings.years
        )
        self.payment_classes = (
            [payment_classes]
            if isinstance(payment_classes, str)
            else payment_classes
            if payment_classes is not None
            else list(self.settings.payment_classes)
        )
        self.payments_folder = (
            payments_folder if payments_folder is not None else str(self.settings.data_dir)
        )
        self.nrows = nrows
        self.general_payments = pd.DataFrame() if general_payments is None else general_payments
        self.ownership_payments = (
            pd.DataFrame() if ownership_payments is None else ownership_payments
        )
        self.research_payments = pd.DataFrame() if research_payments is None else research_payments
        self.MD_DO_only = MD_DO_only

    def all_payments(self) -> pd.DataFrame:
        """Returns a DataFrame of all payments with merged column names
        from the OpenPayments datasets."""

        logging.info(
            f"Reading and updating payment classes: {(', ').join(self.payment_classes)}..."
        )

        for payment_class in self.payment_classes:
            setattr(
                self,
                f"{payment_class}_payments",
                self.read_payments_csvs(
                    payment_class=payment_class,
                ),
            )

            if hasattr(self, f"update_{payment_class}_payments"):
                setattr(
                    self,
                    f"{payment_class}_payments",
                    getattr(self, f"update_{payment_class}_payments")(),
                )
            else:
                setattr(self, f"{payment_class}_payments", self.update_payments(payment_class))

        all_payments = pd.concat(
            [self.general_payments, self.ownership_payments, self.research_payments]
        )

        return all_payments

    def read_payments_csvs(
        self,
        payment_class: Literal["general", "ownership", "research"],
    ) -> pd.DataFrame:
        """Reads the OpenPayments csv files for the specified payment class
        and returns a DataFrame of the payments. The csv files are read in
        chunks to avoid memory issues. The DataFrame is filtered for physicians
        only if specified."""

        logging.info(f"Reading {payment_class} payments...")

        csv_kwargs = {"nrows": self.nrows} if self.nrows is not None else {"chunksize": 50000}

        csv_kwargs = self.update_csv_kwargs(csv_kwargs, payment_class)

        for year in self.years:
            payment_csv = self.get_payment_csv_path(payment_class=payment_class, year=year)
            csv_path = f"{self.payments_folder}/{payment_csv}"

            payments = (
                pd.concat(
                    (
                        self.filter_payment_chunk(
                            payment_chunk=x,
                            payment_class=payment_class,
                        )
                    )
                    for x in pd.read_csv(
                        csv_path,
                        header=0,
                        engine="c",
                        low_memory=False,
                        **csv_kwargs,
                    )
                )
                if self.nrows is None
                else (
                    self.filter_payment_chunk(
                        payment_chunk=pd.read_csv(
                            csv_path,
                            header=0,
                            engine="c",
                            low_memory=False,
                            **csv_kwargs,
                        ),
                        payment_class=payment_class,
                    )
                )
            )

            setattr(
                self,
                f"{payment_class}_payments",
                pd.concat(
                    [
                        getattr(self, f"{payment_class}_payments"),
                        payments,
                    ]
                ),
            )

        return getattr(self, f"{payment_class}_payments")

    def read_general_payments_csvs(self, **_ignored) -> pd.DataFrame:
        """Convenience wrapper for read_payments_csvs("general"). Existing
        callers in credentials.py / specialtys.py / tests pass redundant
        `usecols=` / `dtype=` kwargs — those are silently ignored because
        the same info is derived from `self.general_columns` in
        `update_csv_kwargs`. Section 5 bug 0b regression fix.
        """
        return self.read_payments_csvs(payment_class="general")

    def read_ownership_payments_csvs(self, **_ignored) -> pd.DataFrame:
        """Convenience wrapper for read_payments_csvs("ownership"). See
        read_general_payments_csvs."""
        return self.read_payments_csvs(payment_class="ownership")

    def read_research_payments_csvs(self, **_ignored) -> pd.DataFrame:
        """Convenience wrapper for read_payments_csvs("research"). See
        read_general_payments_csvs."""
        return self.read_payments_csvs(payment_class="research")

    def filter_payment_chunk(
        self,
        payment_chunk: pd.DataFrame,
        payment_class: Literal["general", "ownership", "research"],
    ) -> pd.DataFrame:
        """Filters the payment chunk for physicians only if specified.

        Bug 6 fix: previously dropped rows with missing profile IDs
        silently. Some CMS publications (2024 general) have a small fraction
        of such rows. Now warns with the dropped-row count so a sudden spike
        is visible.
        """
        profile_id_col = (
            "Physician_Profile_ID"
            if payment_class == "ownership"
            else "Covered_Recipient_Profile_ID"
        )
        before = len(payment_chunk)
        payment_chunk = payment_chunk.dropna(subset=[profile_id_col])
        dropped = before - len(payment_chunk)
        if dropped:
            logging.warning(
                "filter_payment_chunk: dropped %d %s row(s) with missing %s (of %d)",
                dropped,
                payment_class,
                profile_id_col,
                before,
            )

        if self.MD_DO_only:
            payment_chunk = PhysicianFilter(payment_chunk).filter()

        return payment_chunk

    def get_payment_csv_path(
        self,
        payment_class: Literal["general", "ownership", "research"],
        year: int,
    ) -> str:
        """Returns the csv path (relative to payments_folder) for the
        specified payment class and year.

        Resolves the CMS publication-date postfix by globbing the year
        subdirectory. If multiple matches exist (e.g. CMS reissued the
        year's data after a refresh), returns the file with the most recent
        modification time. CMS P-date postfixes are MMDDYYYY-formatted and
        do NOT sort by lexical order to reflect recency, so mtime is the
        reliable signal here.

        The glob template and per-class prefixes come from `self.settings`
        so child apps can adapt to CMS naming changes without patching code.
        """

        prefix = self.settings.cms_class_prefixes[payment_class]
        filename_glob = self.settings.cms_filename_template.format(prefix=prefix, year=year)
        pattern = f"{self.payments_folder}/{year}/{filename_glob}"
        matches = sorted(glob.glob(pattern), key=os.path.getmtime)
        if not matches:
            raise FileNotFoundError(f"No OpenPayments CSV matching pattern: {pattern}")
        return os.path.relpath(matches[-1], self.payments_folder)

    def update_csv_kwargs(
        self,
        csv_kwargs: dict[str, Union[list[str], dict[str, Union[str, int]]]],
        payment_class: Union[Literal["general", "ownership", "research"]],
    ) -> dict[str, Union[list[str], dict[str, Union[str, int]]]]:

        columns: dict[str, tuple[str, Union[type[str], str]]] = getattr(
            self,
            f"{payment_class}_columns",
        )

        csv_kwargs["usecols"] = columns.keys()
        csv_kwargs["dtype"] = {key: value[1] for key, value in columns.items()}

        return csv_kwargs

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        return {
            "Covered_Recipient_Profile_ID": ("profile_id", "Int32"),
            "Covered_Recipient_Last_Name": ("last_name", str),
            "Covered_Recipient_First_Name": ("first_name", str),
            "Covered_Recipient_Middle_Name": ("middle_name", str),
        }

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        return {
            "Physician_Profile_ID": ("profile_id", "Int32"),
            "Physician_First_Name": ("first_name", str),
            "Physician_Last_Name": ("last_name", str),
            "Physician_Middle_Name": ("middle_name", str),
        }

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        return {**self.general_columns}

    def update_payments(
        self,
        payment_class: Literal["general", "ownership", "research"],
    ) -> pd.DataFrame:
        """Renames columns based on payment_class properties, for consistency
        and brevity. Also adds a payment_class column to the DataFrame to keep
        track of which payments came from which dataset after they are combined."""

        payments: pd.DataFrame = getattr(self, f"{payment_class}_payments")

        payments.insert(1, "payment_class", payment_class)

        payments.rename(
            columns={key: val[0] for key, val in getattr(self, f"{payment_class}_columns").items()},
            inplace=True,
        )

        return payments

    def update_ownership_payments(self) -> pd.DataFrame:
        """The ownership OpenPayments dataset has different column
        names than the general and research datasets, so it requires
        a separate method. Calls the update_payments method, but can be
        overwritten before or afterwards for additional processing."""

        self.ownership_payments = self.update_payments("ownership")

        return self.ownership_payments
