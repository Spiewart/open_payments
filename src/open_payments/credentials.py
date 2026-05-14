"""Credential handling for both CMS Open Payments data and conflicted-provider input.

This module is organized into two parsing layers, which are intentionally
separate because they have different stability properties:

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  1. CMS / OpenPayments side  —  TRULY GENERIC, STABLE                   │
  │     `CredentialsMixin`, `PaymentCredentials`, `PaymentIDsCredentialsMixin`│
  │     CMS emits credentials as one of 12 canonical strings in fixed       │
  │     columns (`Covered_Recipient_Primary_Type_1..6`,                     │
  │     `Physician_Primary_Type`). The library knows the shape exhaustively │
  │     — child apps should never need to override these classes.           │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  2. Conflicted side  —  GENERIC DEFAULT, EXPECTED TO BE OVERRIDDEN      │
  │     `ConflictCredentials`, `get_credentials`,                           │
  │     `parse_credentials_from_name`                                       │
  │     Conflicted-provider input shape is project-specific (scraped from   │
  │     articles, dean's lists, web data, etc.). The defaults handle the    │
  │     most common shapes (name column + optional credential column), but  │
  │     child apps will routinely subclass `ConflictCredentials` to swap    │
  │     in project-specific parsing.                                        │
  └─────────────────────────────────────────────────────────────────────────┘

The low-level helpers `parse_credential_token` and `CREDENTIAL_ALIASES` sit
between the two layers and are usable by either — they map any string the
library knows about (CMS canonical strings + common abbreviations) to the
`Credentials` enum.
"""

import re
from typing import ClassVar, Union

import pandas as pd

from .choices import Credentials, FilterOutcome, PaymentFilters
from .config import Settings
from .conflicts import Conflicts
from .helpers import get_file_suffix
from .names import is_blank
from .read import ReadPayments

# ---------------------------------------------------------------------------
# Low-level helpers — usable by both the CMS and conflicted sides.
# ---------------------------------------------------------------------------

CREDENTIAL_ALIASES: dict[Credentials, tuple[str, ...]] = {
    Credentials.MEDICAL_DOCTOR: ("MD", "MBBS"),
    Credentials.DOCTOR_OF_DENTISTRY: ("DDS", "DMD"),
    Credentials.DOCTOR_OF_OSTEOPATHY: ("DO",),
    Credentials.DOCTOR_OF_OPTOMETRY: ("OD",),
    Credentials.CHIROPRACTOR: ("DC",),
    Credentials.DOCTOR_OF_PODIATRIC_MEDICINE: ("DPM",),
    Credentials.NURSE_PRACTITIONER: ("NP",),
    Credentials.PHYSICIAN_ASSISTANT: ("PA", "PA-C"),
    Credentials.CERTIFIED_REGISTERED_NURSE_ANAESTHETIST: ("CRNA",),
    Credentials.CLINICAL_NURSE_SPECIALIST: ("CNS",),
    Credentials.CERTIFIED_NURSE_MIDWIFE: ("CNM",),
    Credentials.ANESTHESIOLOGIST_ASSISTANT: ("AA",),
    Credentials.REGISTERED_NURSE: ("RN",),
}


def _normalize_alias(s: str) -> str:
    """Lowercase, strip periods + surrounding whitespace. Hyphens preserved
    so 'PA-C' stays distinct from 'PAC' (which isn't an alias)."""
    return s.strip().lower().replace(".", "")


# Reverse lookup built once: normalized alias -> Credentials.
_ALIAS_TO_CREDENTIAL: dict[str, Credentials] = {
    _normalize_alias(alias): cred
    for cred, aliases in CREDENTIAL_ALIASES.items()
    for alias in aliases
}


def parse_credential_token(s: Union[str, None]) -> list[Credentials]:
    """Map a free-text credential string to a list of `Credentials` enum values.

    Recognizes (case-insensitive, period-tolerant):
      - Every abbreviation in `CREDENTIAL_ALIASES` (e.g. "MD", "PA-C", "CRNA")
      - Every enum `.value` (e.g. "Medical Doctor", "Nurse Practitioner")
      - The composite "Physician (MD or DO)" -> [MEDICAL_DOCTOR, DOCTOR_OF_OSTEOPATHY]

    Returns `[]` for blank input or any unrecognized token. Returns a single-
    element list for the typical "one token, one credential" case.
    """
    if is_blank(s):
        return []

    raw = s.strip()
    # Composite — common in semi-structured datasets like uptodate's `credential` column.
    if re.search(r"\bphysician\b.*\bmd\b.*\bdo\b", raw, re.IGNORECASE) or re.search(
        r"\bphysician\b.*\bdo\b.*\bmd\b", raw, re.IGNORECASE
    ):
        return [Credentials.MEDICAL_DOCTOR, Credentials.DOCTOR_OF_OSTEOPATHY]

    # Abbreviation match.
    norm = _normalize_alias(raw)
    if norm in _ALIAS_TO_CREDENTIAL:
        return [_ALIAS_TO_CREDENTIAL[norm]]

    # Full enum-value match (case-insensitive).
    lowered = raw.lower()
    for cred in Credentials:
        if lowered == cred.value.lower():
            return [cred]

    return []


def parse_credentials_from_name(name: Union[str, None]) -> list[Credentials]:
    """**Conflicted-side helper.** Extract credential tokens that appear as
    suffixes inside a name string. CMS data does not embed credentials in
    name fields — this is for the common conflicted-input pattern where a
    scraped name looks like ``"John Q. Smith, MD, FACP"``.

    Tokenizes `name` on whitespace + commas, normalizes each token (lowercase,
    strip periods), and reports each credential whose alias appears as a whole
    token. Preserves order of first appearance and deduplicates.

    Examples:
      "Smith MD"                -> [MEDICAL_DOCTOR]
      "Jones, M.D., FACP"       -> [MEDICAL_DOCTOR]  (FACP not in the alias table)
      "Doe, MD, PhD"            -> [MEDICAL_DOCTOR]  (PhD ignored)
      "Carter PA-C"             -> [PHYSICIAN_ASSISTANT]
      "Anna Apple"              -> []

    The `parse_credential_token` function is the right call for a *whole field*
    that contains one credential string (e.g. uptodate's `credential` column);
    `parse_credentials_from_name` is for *name strings* that may carry
    trailing degree suffixes after a comma or space.
    """
    if is_blank(name):
        return []

    found: list[Credentials] = []
    for token in re.split(r"[\s,]+", name.strip()):
        norm = _normalize_alias(token)
        if norm in _ALIAS_TO_CREDENTIAL:
            cred = _ALIAS_TO_CREDENTIAL[norm]
            if cred not in found:
                found.append(cred)
    return found


# ---------------------------------------------------------------------------
# CMS / OpenPayments side  —  STABLE, GENERIC, NOT INTENDED TO BE OVERRIDDEN.
# CMS publishes exactly 12 canonical credential strings via a fixed column
# layout. The classes below encode that layout and should rarely need changes
# from child apps.
# ---------------------------------------------------------------------------


class CredentialsMixin:
    """Mixin class for CMS-side credentials columns."""

    @property
    def general_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols: dict[str, tuple[str, Union[type[str], str]]] = super().general_columns
        cols.update(
            {
                "Covered_Recipient_Primary_Type_1": ("credential_1", str),
                "Covered_Recipient_Primary_Type_2": ("credential_2", str),
                "Covered_Recipient_Primary_Type_3": ("credential_3", str),
                "Covered_Recipient_Primary_Type_4": ("credential_4", str),
                "Covered_Recipient_Primary_Type_5": ("credential_5", str),
                "Covered_Recipient_Primary_Type_6": ("credential_6", str),
            }
        )
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols: dict[str, tuple[str, Union[type[str], str]]] = super().ownership_columns
        cols.update(
            {
                "Physician_Primary_Type": ("credential_1", str),
            }
        )
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols: dict[str, tuple[str, Union[type[str], str]]] = super().research_columns

        cols.update(self.general_columns)
        return cols


class PaymentCredentials(CredentialsMixin, ReadPayments):
    def create_unique_credentials_excel(self, path: Union[str, None] = None) -> None:
        path = str(self.settings.data_dir) if path is None else path

        unique_credentials = self.unique_credentials()

        file_suffix = get_file_suffix(self.years, self.payment_classes)

        with pd.ExcelWriter(
            f"{path}/unique_credentials{file_suffix}.xlsx",
            engine="openpyxl",
        ) as writer:
            unique_credentials.to_excel(writer, sheet_name="unique_credentials", index=False)

    def unique_credentials(self) -> pd.Series:
        """Returns a Series of unique credentials from OpeyPayments payment datasets."""

        self.general_payments = self.read_general_payments_csvs(
            usecols=self.general_columns.keys(),
            dtype={key: value[1] for key, value in self.general_columns.items()},
        )
        self.general_payments = self.update_payments("general")

        self.ownership_payments = self.read_ownership_payments_csvs(
            usecols=self.ownership_columns.keys(),
            dtype={key: value[1] for key, value in self.ownership_columns.items()},
        )
        self.ownership_payments = self.update_payments("ownership")

        self.research_payments = self.read_research_payments_csvs(
            usecols=self.research_columns.keys(),
            dtype={key: value[1] for key, value in self.research_columns.items()},
        )
        self.research_payments = self.update_payments("research")

        all_payments = pd.concat(
            [self.general_payments, self.ownership_payments, self.research_payments]
        )

        all_credentials = self.get_all_credentials(all_payments)

        all_credentials = all_credentials.drop_duplicates()

        all_credentials = all_credentials.dropna()

        return all_credentials.reset_index(drop=True)

    @staticmethod
    def get_all_credentials(df: pd.DataFrame) -> pd.Series:
        """Method that returns all unique credentials from the DataFrame."""

        credentials = pd.concat(
            [
                df["credential_1"],
                df["credential_2"],
                df["credential_3"],
                df["credential_4"],
                df["credential_5"],
                df["credential_6"],
            ]
        )

        credentials.rename("credential", inplace=True)

        return credentials

    @classmethod
    def credentials(cls, payments: pd.DataFrame) -> pd.DataFrame:
        """Method that combines the credentials into a Series."""

        payments.insert(1, "credentials", payments.apply(cls.create_credentials, axis=1))

        payments = cls.drop_individual_credentials(payments)

        return payments

    @staticmethod
    def create_credentials(payment: pd.Series) -> pd.Series:
        """Aggregates the credentials into a Series."""

        credentials = payment[
            [
                "credential_1",
                "credential_2",
                "credential_3",
                "credential_4",
                "credential_5",
                "credential_6",
            ]
        ].dropna()

        credentials = credentials.unique()

        credentials = [Credentials(cred) for cred in credentials]

        return credentials

    @staticmethod
    def drop_individual_credentials(payments: pd.DataFrame) -> pd.DataFrame:

        payments = payments.drop(
            [
                "credential_1",
                "credential_2",
                "credential_3",
                "credential_4",
                "credential_5",
                "credential_6",
            ],
            axis=1,
        )

        return payments

    def update_ownership_payments(self) -> pd.DataFrame:
        """Overwritten to add credential_2-6 to the DataFrame, as they
        won't be present after renaming pre-existing columns.

        TODO(payment-class differential audit, see TODO.md): this padding
        pattern (ownership has 1 credential col, pad to 6 with None so the
        general/research aggregator can iterate uniformly) is fragile. Audit
        whether the aggregator should be column-count-aware instead, and
        cover the same pattern for specialty / license_state on
        SpecialtysMixin / CityStatesMixin.
        """

        self.ownership_payments["credential_2"] = None
        self.ownership_payments["credential_3"] = None
        self.ownership_payments["credential_4"] = None
        self.ownership_payments["credential_5"] = None
        self.ownership_payments["credential_6"] = None
        self.ownership_payments = super().update_ownership_payments()

        self.ownership_payments["credential_7"] = None
        return self.ownership_payments


def convert_credentials(credentials: str) -> Union[list[Credentials], None]:
    """Convert a string representation of a list of Credentials objects
    to a list of Credentials objects."""

    if not credentials:
        return None

    converted = []

    credentials = re.findall(r": '(.*?)'", credentials)

    for credential in credentials:
        credential = Credentials(credential)
        converted.append(credential)

    return converted


def unique_credentials(settings: Union[Settings, None] = None) -> None:
    """Creates an Excel file containing unique credentials.

    Years and data directory come from `settings` (defaults to env-based
    Settings()). The legacy hardcoded `years=2023` was a leak from CLI
    invocation into library logic; child apps should set
    `OPEN_PAYMENTS_YEARS=2023` instead.
    """

    settings = settings if settings is not None else Settings()
    PaymentCredentials(nrows=None, settings=settings).create_unique_credentials_excel()


# ---------------------------------------------------------------------------
# Conflicted side  —  GENERIC DEFAULT, EXPECTED TO BE OVERRIDDEN.
# Conflicted-provider input shape is project-specific. The default below
# handles the most common pattern (name column + optional credential column),
# but child apps should subclass `ConflictCredentials` to plug in their own
# parsing whenever their input shape differs.
# ---------------------------------------------------------------------------


class ConflictCredentials(Conflicts):
    """Default credentials mixin for raw conflicted-provider input.

    DESIGN NOTE: This is the **conflicted-side** parser, which is generic only
    by virtue of covering the most common input shape. Unlike the CMS-side
    classes (``CredentialsMixin``, ``PaymentCredentials``) which encode a
    fixed and known CMS column layout, the conflicted side varies per project.
    **Child apps are expected to subclass this class** to plug in
    project-specific parsing whenever their raw input doesn't match the
    default shape below.

    Default shape (uptodate_conflicts template):
      1. A ``name`` column containing a full name with optional trailing degree
         suffix (e.g. ``"John Q. Smith, MD"``).
      2. An optional ``credential`` column with a structured credential string
         (e.g. ``"Physician (MD or DO)"``, ``"MD"``, ``"PA-C"``).

    The two signals are combined: if the structured column is the wildcard
    ``"Physician (MD or DO)"``, the trailing-degree match (if any) narrows it;
    if the structured column has a specific value, it wins; otherwise the
    trailing-degree match alone is used. Returns ``None`` when no credentials
    can be derived from either signal — the orchestrator's
    `remove_non_md_do` filter drops those rows.

    Override surface (in increasing order of intrusiveness):
      - Set ``NAME_COLUMN`` / ``CREDENTIAL_COLUMN`` class vars to point at
        different column names without touching logic.
      - Override ``get_credentials(row)`` to change per-row parsing logic
        (e.g. deans's boolean ``MD`` column — see test for example).
      - Override ``conflict_credentials()`` to change the whole pipeline
        (e.g. abim's ``NameCredentialParser`` that parses name + credentials
        in one pass).
    """

    NAME_COLUMN: ClassVar[str] = "name"
    CREDENTIAL_COLUMN: ClassVar[str] = "credential"

    def conflict_credentials(self) -> pd.DataFrame:
        """Adds a `credentials` column (list[Credentials] or None) derived from
        the configured name + credential columns. Drops the credential column
        (the name column survives — `ConflictNames` consumes it next)."""

        self.conflicts = self.conflicts.copy()
        self.conflicts["credentials"] = self.conflicts.apply(self.get_credentials, axis=1)

        if self.CREDENTIAL_COLUMN in self.conflicts.columns:
            self.conflicts = self.conflicts.drop(columns=[self.CREDENTIAL_COLUMN])

        return self.conflicts

    @classmethod
    def get_credentials(cls, conflict: pd.Series) -> Union[list[Credentials], None]:
        """Returns the canonical list[Credentials] for a single conflicted row,
        or None if no credentials can be derived.

        Strategy: parse the credential column (if present) and the name's
        trailing-degree (always), then combine.
        """
        name = conflict[cls.NAME_COLUMN] if cls.NAME_COLUMN in conflict.index else None
        credential_field = (
            conflict[cls.CREDENTIAL_COLUMN] if cls.CREDENTIAL_COLUMN in conflict.index else None
        )

        from_field = parse_credential_token(credential_field)
        from_name = parse_credentials_from_name(name)

        # "Physician (MD or DO)" wildcard case — narrow with trailing degree.
        if set(from_field) == {Credentials.MEDICAL_DOCTOR, Credentials.DOCTOR_OF_OSTEOPATHY}:
            if (
                Credentials.MEDICAL_DOCTOR in from_name
                and Credentials.DOCTOR_OF_OSTEOPATHY not in from_name
            ):
                return [Credentials.MEDICAL_DOCTOR]
            if (
                Credentials.DOCTOR_OF_OSTEOPATHY in from_name
                and Credentials.MEDICAL_DOCTOR not in from_name
            ):
                return [Credentials.DOCTOR_OF_OSTEOPATHY]
            return from_field

        if from_field:
            # Structured column had a specific (non-wildcard) value — it wins,
            # augmented by anything additional the name suffix exposes.
            combined: list[Credentials] = list(from_field)
            for cred in from_name:
                if cred not in combined:
                    combined.append(cred)
            return combined

        if from_name:
            return list(from_name)

        return None


class PaymentIDsCredentialsMixin(CredentialsMixin):
    """Filters OpenPayments payments by credentials."""

    @property
    def filters(self) -> list["PaymentFilters"]:
        """Overwritten to add CREDENTIAL PaymentFilter to
        the filters property."""

        filters: list[PaymentFilters] = super().filters
        filters.append(PaymentFilters.CREDENTIAL)
        return filters

    @classmethod
    def filter_by_credential(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> FilterOutcome:
        """Credentials-list intersection. Returns:
        - MATCH when conflicted and payment credential lists share at
          least one credential.
        - DISAGREE when both lists are non-empty AND share none — strong
          evidence this is a different practitioner type (e.g. MD vs
          Chiropractor).
        - NO_DATA when either list is None / empty.
        """
        payment_creds = payments_x_conflicted["credentials"]
        conflict_creds = payments_x_conflicted["conflict_credentials"]
        if not payment_creds or not conflict_creds:
            return FilterOutcome.NO_DATA
        return (
            FilterOutcome.MATCH
            if any(c in payment_creds for c in conflict_creds)
            else FilterOutcome.DISAGREE
        )

    def convert_merged_dtypes(
        self,
        merged: pd.DataFrame,
    ) -> pd.DataFrame:
        """Updates  payments and conflicteds columns into lists after
        they are loaded as strs in CSVs and Excel files."""

        merged: pd.DataFrame = super().convert_merged_dtypes(merged)

        merged["credentials"] = merged["credentials"].apply(
            lambda x: convert_credentials(x) if isinstance(x, str) else x
        )

        merged["conflict_credentials"] = merged["conflict_credentials"].apply(
            lambda x: convert_credentials(x) if isinstance(x, str) else x
        )

        return merged
