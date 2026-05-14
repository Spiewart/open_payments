"""Specialty handling for both CMS Open Payments data and conflicted-provider input.

Three concerns, separated by stability (same pattern as ``credentials.py``,
``names.py``, ``citystates.py``):

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  1. ``Specialtys`` model + low-level parsers  —  used by both sides     │
  │     Pydantic model with ``specialty`` + ``subspecialty`` (both free     │
  │     text). CMS publishes 230+ distinct ``(specialty, subspecialty)``    │
  │     pairs for MD/DO alone — too granular to enumerate, so the model    │
  │     stays free-text and matching is string-equality.                    │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  2. CMS / OpenPayments side  —  STABLE, NOT INTENDED TO BE OVERRIDDEN   │
  │     ``SpecialtysMixin``, ``PaymentSpecialtys``,                         │
  │     ``PaymentIDsSpecialtysMixin``                                       │
  │     CMS publishes specialty as pipe-delimited                           │
  │     ``provider_type|specialty[|subspecialty]`` in up to 6 columns       │
  │     (general/research) or 1 column (ownership). The mixin family        │
  │     encodes both layouts.                                               │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  3. Conflicted side  —  GENERIC DEFAULT, EXPECTED TO BE OVERRIDDEN      │
  │     ``parse_specialty_freetext``, ``parse_specialties_freetext``,       │
  │     ``ConflictSpecialtys``                                              │
  │     The default handles a ``specialtys`` column with free-text          │
  │     ``"Specialty | Subspecialty"`` strings (multi-specialty values      │
  │     separated by ``;``). Child apps with project-specific source        │
  │     taxonomies (uptodate's UpToDate sections, abim's ABIM boards,       │
  │     deans's structured Specialty + Subspecialty columns) plug in        │
  │     either a ``SPECIALTY_MAP`` class-var dict (lightest override) or    │
  │     override ``get_specialtys`` / ``conflict_specialtys`` directly.     │
  └─────────────────────────────────────────────────────────────────────────┘

Empirical scope from a 14.6M-row 2023 general-payments survey:
  - 385 distinct CMS specialty strings (pipe-delimited)
  - 7 provider types (Allopathic & Osteopathic Physicians is the bulk)
  - 230 distinct (specialty, subspecialty) pairs for MD/DO alone
  - Internal Medicine alone has 28 subspecialties

Because the universe is large and growing, the design avoids enumerating
specialty values. Free-text + string equality (with subspecialty as an
independent filter dimension) is the matching strategy.
"""

import re
from typing import ClassVar, Union

import pandas as pd
from pydantic import BaseModel, model_validator
from typing_extensions import Self

from .choices import FilterOutcome, PaymentFilters
from .config import Settings
from .conflicts import Conflicts
from .helpers import get_file_suffix
from .names import is_blank
from .read import ReadPayments


class Specialtys(BaseModel):
    """Class that contains the specialtys of a payment."""

    specialty: str | None = None
    subspecialty: str | None = None

    def __str__(self) -> str:
        return f"{self.specialty}|{self.subspecialty}"

    @model_validator(mode="after")
    def validate_specialty_subspecialty(self) -> Self:
        if self.specialty is None and self.subspecialty is None:
            raise ValueError("Both specialty and subspecialty cannot be None.")
        return self


# ---------------------------------------------------------------------------
# CMS / OpenPayments side  —  STABLE, GENERIC, NOT INTENDED TO BE OVERRIDDEN.
# CMS publishes specialty as pipe-delimited `provider_type|specialty[|subspecialty]`
# in 6 columns for general/research, 1 column for ownership.
# ---------------------------------------------------------------------------


def parse_cms_specialty_string(s: Union[str, None]) -> Union["Specialtys", None]:
    """**CMS-side helper.** Parse the pipe-delimited
    ``"provider_type|specialty[|subspecialty]"`` string CMS publishes in
    ``Covered_Recipient_Specialty_*`` columns into a :class:`Specialtys`.
    Drops ``provider_type`` because physician-only filtering already handles
    that dimension. Returns ``None`` for blank input.
    """
    if is_blank(s):
        return None
    parts = s.split("|")
    if len(parts) < 2:
        return None
    specialty = parts[1].strip() or None
    subspecialty = parts[2].strip() if len(parts) > 2 and parts[2].strip() else None
    if specialty is None and subspecialty is None:
        return None
    return Specialtys(specialty=specialty, subspecialty=subspecialty)


class SpecialtysMixin:
    @property
    def general_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols: dict[str, tuple[str, Union[type[str], str]]] = super().general_columns
        cols.update(
            {
                "Covered_Recipient_Specialty_1": ("specialty_1", str),
                "Covered_Recipient_Specialty_2": ("specialty_2", str),
                "Covered_Recipient_Specialty_3": ("specialty_3", str),
                "Covered_Recipient_Specialty_4": ("specialty_4", str),
                "Covered_Recipient_Specialty_5": ("specialty_5", str),
                "Covered_Recipient_Specialty_6": ("specialty_6", str),
            }
        )
        return cols

    @property
    def ownership_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols: dict[str, tuple[str, Union[type[str], str]]] = super().ownership_columns
        cols.update(
            {
                "Physician_Specialty": ("specialty_1", str),
            }
        )
        return cols

    @property
    def research_columns(self) -> dict[str, tuple[str, Union[type[str], str]]]:

        cols: dict[str, tuple[str, Union[type[str], str]]] = super().research_columns

        cols.update(self.general_columns)
        return cols


class PaymentSpecialtys(SpecialtysMixin, ReadPayments):
    def create_unique_specialtys_excel(self, path: Union[str, None] = None) -> None:
        path = str(self.settings.data_dir) if path is None else path

        unique_specialtys = self.unique_specialtys()

        MD_DO = unique_specialtys[
            unique_specialtys["provider_type"].str.contains(
                "Allopathic & Osteopathic Physicians", case=False, na=False
            )
        ]

        MD_DO.drop("provider_type", axis=1, inplace=True)

        file_suffix = get_file_suffix(self.years, self.payment_classes)

        with pd.ExcelWriter(
            f"{path}/unique_specialtys{file_suffix}.xlsx",
            engine="openpyxl",
        ) as writer:
            unique_specialtys.to_excel(writer, sheet_name="unique_specialtys", index=False)
            MD_DO.to_excel(writer, sheet_name="MD_DO", index=False)

    def unique_specialtys(self) -> pd.Series:
        """Returns a Series of unique specialties from OpeyPayments payment datasets."""

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

        unique_specialty_1_6 = all_payments.drop_duplicates(
            subset=[
                "specialty_1",
                "specialty_2",
                "specialty_3",
                "specialty_4",
                "specialty_5",
                "specialty_6",
            ]
        )

        all_specialtys = self.get_all_specialtys(unique_specialty_1_6)

        all_specialtys = all_specialtys.dropna()

        return all_specialtys.reset_index(drop=True)

    @classmethod
    def get_all_specialtys(cls, df: pd.DataFrame) -> pd.Series:
        """Returns a Series of all Specialtys from the DataFrame."""

        df.insert(
            1,
            "specialty",
            df.apply(cls.specialtys_strs, axis=1),
        )
        df = cls.drop_individual_specialtys(df)

        return df["specialty"]

    @classmethod
    def specialtys(cls, payments: pd.DataFrame) -> pd.DataFrame:
        """Method that combines the specialtys into a Series"""

        payments.insert(
            1,
            "specialtys",
            payments.apply(cls.create_specialtys, axis=1),
        )

        payments = cls.drop_individual_specialtys(payments)

        return payments

    @classmethod
    def specialtys_strs(cls, payment: pd.Series) -> pd.DataFrame:
        """Aggregates the different specialties into a series of
        specialty/subspecialty pairs."""

        specialtys = payment[
            [
                "specialty_1",
                "specialty_2",
                "specialty_3",
                "specialty_4",
                "specialty_5",
                "specialty_6",
            ]
        ]
        specialtys = specialtys.dropna()

        specialtys = specialtys.apply(
            cls.parse_specialty,
        )

        return specialtys

    @classmethod
    def create_specialtys(cls, payment: pd.Series) -> list[Specialtys]:
        """Returns a list of Specialtys from a single payment's
        specialty_1-6 columns."""

        specialtys = payment[
            [
                "specialty_1",
                "specialty_2",
                "specialty_3",
                "specialty_4",
                "specialty_5",
                "specialty_6",
            ]
        ]

        specialtys = cls.parse_specialtys(specialtys)

        specialtys = specialtys.drop("provider_type", axis=1)
        specialtys = specialtys.dropna(how="all", subset=["specialty", "subspecialty"])
        specialtys = specialtys.drop_duplicates()
        specialtys.reset_index(drop=True, inplace=True)

        return [
            Specialtys(
                specialty=x["specialty"],
                subspecialty=x["subspecialty"],
            )
            for _, x in specialtys.iterrows()
        ]

    @staticmethod
    def parse_specialtys(
        specialtys: pd.Series,
    ) -> pd.DataFrame:
        """Parses a series of a single payment's
        specialties strings into a DataFrame. Returns an
        empty DataFrame with columns
        ["provider_type", "specialty", "subspecialty"]
        retained to avoid downstream errors."""

        specialtys = specialtys.str.split("|")
        specialtys = specialtys.dropna()

        specialtys = (
            pd.DataFrame(
                {
                    "provider_type": specialty[0],
                    "specialty": specialty[1] if len(specialty) > 1 else None,
                    "subspecialty": specialty[2] if len(specialty) > 2 else None,
                }
                for specialty in specialtys
                if specialty is not None
            )
            if specialtys.size > 0
            else pd.DataFrame(columns=["provider_type", "specialty", "subspecialty"])
        )

        return specialtys

    @staticmethod
    def drop_individual_specialtys(payments: pd.DataFrame) -> pd.DataFrame:
        """Removes specialty_1-6 columns from the DataFrame."""

        payments = payments.drop(
            columns=[
                "specialty_1",
                "specialty_2",
                "specialty_3",
                "specialty_4",
                "specialty_5",
                "specialty_6",
            ],
        )

        return payments

    def update_ownership_payments(self) -> pd.DataFrame:
        """Overwritten to add specialty_2-6 to the DataFrame, as they
        won't be present after renaming pre-existing columns."""

        self.ownership_payments["specialty_2"] = None
        self.ownership_payments["specialty_3"] = None
        self.ownership_payments["specialty_4"] = None
        self.ownership_payments["specialty_5"] = None
        self.ownership_payments["specialty_6"] = None

        self.ownership_payments = super().update_ownership_payments()

        return self.ownership_payments


def convert_specialtys(specialtys: str) -> list[Specialtys]:
    """Convert a string representation of a list of Specialtys objects
    to a list of Specialtys objects."""

    converted = []

    specialtys_list = re.findall(
        r"Specialtys\(specialty='(.*?)', subspecialty=('.*?'|None)\)", specialtys
    )

    for specialty in specialtys_list:
        specialty = Specialtys(
            specialty=specialty[0],
            subspecialty=specialty[1].strip("'") if specialty[1] != "None" else None,
        )
        converted.append(specialty)

    return converted


def unique_specialties(settings: Union[Settings, None] = None) -> None:
    """Creates an Excel file containing unique specialties.

    Years and data directory come from `settings` (defaults to env-based
    Settings()). The legacy hardcoded `years=2023` was a leak from CLI
    invocation into library logic; child apps should set
    `OPEN_PAYMENTS_YEARS=2023` instead.
    """

    settings = settings if settings is not None else Settings()
    PaymentSpecialtys(nrows=None, settings=settings).create_unique_specialtys_excel()


class PaymentIDsSpecialtysMixin(SpecialtysMixin):
    """Filters OpenPayments payments by specialty."""

    @property
    def filters(self) -> list[PaymentFilters]:
        filters: list[PaymentFilters] = super().filters
        filters.append(PaymentFilters.SPECIALTY)
        filters.append(PaymentFilters.SUBSPECIALTY)
        filters.append(PaymentFilters.FULLSPECIALTY)
        return filters

    @classmethod
    def filter_by_specialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> FilterOutcome:
        """Specialty match (top-level, ignoring subspecialty). Returns:
        - NO_DATA when FULLSPECIALTY already matched (superseded) OR when
          either specialty list is empty.
        - MATCH when any cross-product of (payment_specialty,
          conflict_specialty) agrees via :meth:`specialty_str_matcher`.
        - DISAGREE when both lists have specialties AND none agree.
        """
        if PaymentFilters.FULLSPECIALTY in payments_x_conflicted["filters"]:
            return FilterOutcome.NO_DATA  # superseded
        payment_specialtys = payments_x_conflicted["specialtys"] or []
        conflict_specialtys = payments_x_conflicted["conflict_specialtys"] or []
        if not payment_specialtys or not conflict_specialtys:
            return FilterOutcome.NO_DATA
        matched = cls.payment_conflict_specialty_match(
            payment_specialtys=payment_specialtys,
            conflict_specialtys=conflict_specialtys,
        )
        return FilterOutcome.MATCH if matched else FilterOutcome.DISAGREE

    @classmethod
    def payment_conflict_specialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:
        """Checks if the specialtys exist and match."""

        return any(
            cls.specialty_str_matcher(
                payment_specialty=payment_specialty.specialty,
                conflict_specialty=conflict_specialty.specialty,
            )
            for payment_specialty in payment_specialtys
            for conflict_specialty in conflict_specialtys
        )

    @staticmethod
    def specialty_str_matcher(
        payment_specialty: Union[str, None],
        conflict_specialty: Union[str, None],
    ) -> bool:
        """Checks if the specialtys exist and match."""

        payment_specialty = payment_specialty.lower() if pd.notna(payment_specialty) else None
        conflict_specialty = conflict_specialty.lower() if pd.notna(conflict_specialty) else None

        # If either specialty is None, return False
        if payment_specialty is None or conflict_specialty is None:
            return False
        # If both specialties are the same, return True
        elif payment_specialty == conflict_specialty:
            return True

        payment_specialty_strs = payment_specialty.split(" ")

        conflict_specialty_strs = conflict_specialty.split(" ")

        # Remove "medicine" from the specialty strings, as it is non-specific
        if "medicine" in payment_specialty_strs:
            payment_specialty_strs.remove("medicine")
        if "medicine" in conflict_specialty_strs:
            conflict_specialty_strs.remove("medicine")

        return any(
            payment_str in conflict_specialty_strs for payment_str in payment_specialty_strs
        ) or any(conflict_str in payment_specialty_strs for conflict_str in conflict_specialty_strs)

    @classmethod
    def filter_by_subspecialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> FilterOutcome:
        """Subspecialty match. Returns:
        - NO_DATA when FULLSPECIALTY already matched, or when neither side
          has any subspecialty values at all.
        - MATCH when at least one (payment, conflict) subspecialty pair agrees.
        - DISAGREE when both sides have subspecialties AND none agree.
        """
        if PaymentFilters.FULLSPECIALTY in payments_x_conflicted["filters"]:
            return FilterOutcome.NO_DATA  # superseded
        payment_specialtys = payments_x_conflicted["specialtys"] or []
        conflict_specialtys = payments_x_conflicted["conflict_specialtys"] or []
        payment_has_sub = any(pd.notna(s.subspecialty) for s in payment_specialtys)
        conflict_has_sub = any(pd.notna(s.subspecialty) for s in conflict_specialtys)
        if not payment_has_sub or not conflict_has_sub:
            return FilterOutcome.NO_DATA
        matched = cls.payment_conflict_subspecialty_match(
            payment_specialtys=payment_specialtys,
            conflict_specialtys=conflict_specialtys,
        )
        return FilterOutcome.MATCH if matched else FilterOutcome.DISAGREE

    @classmethod
    def payment_conflict_subspecialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            cls.specialty_str_matcher(
                payment_specialty=payment_specialty.subspecialty,
                conflict_specialty=conflict_specialty.subspecialty,
            )
            for payment_specialty in payment_specialtys
            for conflict_specialty in conflict_specialtys
        )

    @classmethod
    def filter_by_fullspecialty(
        cls,
        payments_x_conflicted: pd.Series,
    ) -> FilterOutcome:
        """Full specialty + subspecialty pair match. Returns:
        - MATCH when at least one (payment, conflict) pair agrees on both
          specialty AND subspecialty. Removes superseded SPECIALTY /
          SUBSPECIALTY labels from filters.
        - DISAGREE when both sides have full (specialty + subspecialty)
          pairs AND none fully agree.
        - NO_DATA when either side has no pairs with both fields populated.
        """
        payment_specialtys = payments_x_conflicted["specialtys"] or []
        conflict_specialtys = payments_x_conflicted["conflict_specialtys"] or []
        payment_full = [
            s for s in payment_specialtys if pd.notna(s.specialty) and pd.notna(s.subspecialty)
        ]
        conflict_full = [
            s for s in conflict_specialtys if pd.notna(s.specialty) and pd.notna(s.subspecialty)
        ]
        if not payment_full or not conflict_full:
            return FilterOutcome.NO_DATA

        matched = cls.payment_conflict_full_specialty_match(
            payment_specialtys=payment_specialtys,
            conflict_specialtys=conflict_specialtys,
        )
        if matched:
            if PaymentFilters.SPECIALTY in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(PaymentFilters.SPECIALTY)
            if PaymentFilters.SUBSPECIALTY in payments_x_conflicted["filters"]:
                payments_x_conflicted["filters"].remove(PaymentFilters.SUBSPECIALTY)
            return FilterOutcome.MATCH
        return FilterOutcome.DISAGREE

    @classmethod
    def payment_conflict_full_specialty_match(
        cls,
        payment_specialtys: Union[list[PaymentSpecialtys], None],
        conflict_specialtys: Union[list[PaymentSpecialtys], None],
    ) -> bool:

        return any(
            cls.specialty_str_matcher(
                payment_specialty=payment_specialty.specialty,
                conflict_specialty=conflict_specialty.specialty,
            )
            and cls.specialty_str_matcher(
                payment_specialty=payment_specialty.subspecialty,
                conflict_specialty=conflict_specialty.subspecialty,
            )
            for payment_specialty in payment_specialtys
            for conflict_specialty in conflict_specialtys
        )

    def convert_merged_dtypes(
        self,
        merged: pd.DataFrame,
    ) -> pd.DataFrame:
        """Updates  payments and conflicteds columns into lists after
        they are loaded as strs in CSVs and Excel files."""

        merged = super().convert_merged_dtypes(merged)

        merged["specialtys"] = merged["specialtys"].apply(
            lambda x: convert_specialtys(x) if isinstance(x, str) else x
        )

        merged["conflict_specialtys"] = merged["conflict_specialtys"].apply(
            lambda x: convert_specialtys(x) if isinstance(x, str) else x
        )

        return merged

    @staticmethod
    def get_specialty_matches(
        payments_x_conflicteds: pd.DataFrame,
    ) -> pd.DataFrame:
        """Filters a payments_x_conflicteds DataFrame by specialty
        in order of priority:
        1. Full specialty
        2. Specialty
        3. Subspecialty
        """
        refined_matches = payments_x_conflicteds[
            payments_x_conflicteds["filters"].apply(lambda x: PaymentFilters.FULLSPECIALTY in x)
        ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(lambda x: PaymentFilters.SPECIALTY in x)
            ]

        if refined_matches.empty:
            refined_matches = payments_x_conflicteds[
                payments_x_conflicteds["filters"].apply(lambda x: PaymentFilters.SUBSPECIALTY in x)
            ]

        return refined_matches


# ---------------------------------------------------------------------------
# Conflicted side  —  GENERIC DEFAULT, EXPECTED TO BE OVERRIDDEN.
#
# The most provenance-divergent of the four domains because specialty
# taxonomy is large (230+ MD/DO pairs in CMS) and source-project taxonomies
# rarely line up 1:1 with it. Real-world child apps observed so far:
#
#   - deans_conflicts:    structured ``Specialty`` + ``Subspecialty`` columns
#                         -> direct two-column wrap.
#   - uptodate_conflicts: ``section`` column with UpToDate's hand-rolled
#                         specialty enum -> ~190-line if/elif map to Specialtys
#                         (collapses to a ~20-line SPECIALTY_MAP dict here).
#   - abim_conflicts:     specialty derived from ABIM board URL hierarchy
#                         -> custom per-row lookup.
#
# None of these can be the parent default. The default below handles a single
# ``specialtys`` column with free-text ``"Specialty | Subspecialty"`` entries
# (multi-specialty values separated by ``;``). Override via the
# ``SPECIALTY_MAP`` class var for one-to-many enum-style mappings, or override
# ``get_specialtys`` / ``conflict_specialtys`` for richer cases.
# ---------------------------------------------------------------------------


# Multi-specialty separator (semicolon). The inner ``|`` separates
# ``specialty`` from ``subspecialty`` within a single specialty entry.
_SPECIALTY_LIST_DELIM_RE = re.compile(r";+")


def parse_specialty_freetext(s: Union[str, None]) -> Union["Specialtys", None]:
    """**Conflicted-side helper.** Parse a single free-text specialty string
    into a :class:`Specialtys`. ``"Specialty"`` or ``"Specialty | Subspecialty"``
    (with or without spaces around the pipe).

    Single-token inputs default to the ``specialty`` field (top-level).
    Child apps whose source taxonomy emits granular terms ("Cardiology",
    "Hepatology") that should land in ``subspecialty`` override
    ``get_specialtys`` or use ``SPECIALTY_MAP``.

    Returns ``None`` for blank input.
    """
    if is_blank(s):
        return None
    parts = [p.strip() for p in s.split("|")]
    specialty = parts[0] or None
    subspecialty = parts[1] if len(parts) > 1 and parts[1] else None
    if specialty is None and subspecialty is None:
        return None
    return Specialtys(specialty=specialty, subspecialty=subspecialty)


def parse_specialties_freetext(s: Union[str, None]) -> list["Specialtys"]:
    """**Conflicted-side helper.** Parse a multi-specialty free-text string
    into ``list[Specialtys]``. Splits on ``;`` between entries, then delegates
    each entry to :func:`parse_specialty_freetext`. Blank / unparseable
    segments are skipped (not raised) — partial inputs still yield what
    info is parseable.
    """
    if is_blank(s):
        return []
    result: list[Specialtys] = []
    for seg in _SPECIALTY_LIST_DELIM_RE.split(s):
        spec = parse_specialty_freetext(seg)
        if spec is not None:
            result.append(spec)
    return result


class ConflictSpecialtys(Conflicts):
    """Default specialties mixin for raw conflicted-provider input.

    DESIGN NOTE: **Conflicted-side** parser. Specialty data is the most
    provenance-divergent of the four domains because CMS taxonomy is large
    (230+ MD/DO pairs) and source-project taxonomies rarely match 1:1.
    The default below handles a single ``specialtys`` column with free-text
    ``"Specialty | Subspecialty"`` entries (multi-specialty values separated
    by ``;``). **Override expectations are higher here than for the other
    three mixins** — most child apps will at least supply a SPECIALTY_MAP.

    Override surface (lightest to heaviest):
      - **``SPECIALTY_MAP: dict[str, list[Specialtys]]``** — easy-mode override
        for child apps with a closed source taxonomy. Declare the map once
        in the subclass; the default ``get_specialtys`` looks up each source
        value in the map before falling back to free-text parsing. Replaces
        uptodate's ~190-line if/elif chain with ~20 lines.
      - **``SPECIALTYS_COLUMN`` class var** — name of the input column (default
        ``"specialtys"``).
      - **``get_specialtys(row)``** — per-row override for cases that need
        more context than a string-keyed map (abim's URL-based lookup).
      - **``conflict_specialtys()``** — full-pipeline override (deans's
        separate Specialty + Subspecialty columns).

    Output column: ``specialtys`` of ``list[Specialtys]``. The source column
    is dropped after parsing (unless it has the same name, in which case it
    is replaced in place).
    """

    SPECIALTYS_COLUMN: ClassVar[str] = "specialtys"

    # Child apps override this with their source-taxonomy -> Specialtys list.
    SPECIALTY_MAP: ClassVar[dict[str, list["Specialtys"]]] = {}

    def conflict_specialtys(self) -> pd.DataFrame:
        """Apply ``get_specialtys`` to every row; replace ``specialtys`` column
        with the parsed list (or drop a differently-named source column)."""
        self.conflicts = self.conflicts.copy()
        parsed = self.conflicts.apply(self.get_specialtys, axis=1)

        if (
            self.SPECIALTYS_COLUMN != "specialtys"
            and self.SPECIALTYS_COLUMN in self.conflicts.columns
        ):
            self.conflicts = self.conflicts.drop(columns=[self.SPECIALTYS_COLUMN])
        self.conflicts["specialtys"] = parsed.values
        return self.conflicts

    @classmethod
    def get_specialtys(cls, row: pd.Series) -> list["Specialtys"]:
        """Per-row override hook. Default:
        1. If the source value matches a ``SPECIALTY_MAP`` key, return the
           mapped ``list[Specialtys]``.
        2. Else parse via :func:`parse_specialties_freetext`.
        3. Blank / unrecognized → ``[]``.
        """
        if cls.SPECIALTYS_COLUMN not in row.index:
            return []
        value = row[cls.SPECIALTYS_COLUMN]
        if is_blank(value):
            return []
        # SPECIALTY_MAP lookup: child-app source taxonomy -> canonical list.
        if isinstance(value, str) and value in cls.SPECIALTY_MAP:
            return list(cls.SPECIALTY_MAP[value])
        return parse_specialties_freetext(value)
