"""Runtime configuration for the open_payments pipeline.

Child applications wrap this package by either:

1. Reading from environment variables (`OPEN_PAYMENTS_DATA_DIR=/path/to/csvs`,
   `OPEN_PAYMENTS_YEARS=2022,2023,2024`, etc.) and calling the public
   entrypoints without a Settings argument — every entrypoint constructs a
   `Settings()` if none is passed, and `BaseSettings` resolves env vars
   automatically.

2. Constructing an explicit `Settings(data_dir=..., years=[...])` and passing
   it in.

The CMS filename template and per-class prefixes live here too, so a child
app can adapt to a CMS naming change (or to private re-published CSVs) without
patching code.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

PaymentClass = Literal["general", "ownership", "research"]


class Settings(BaseSettings):
    """Open Payments runtime configuration.

    All fields are env-overridable via `OPEN_PAYMENTS_*` (case-insensitive).
    List fields accept comma-separated values from the environment, e.g.
    `OPEN_PAYMENTS_YEARS=2022,2023,2024`.
    """

    model_config = SettingsConfigDict(
        env_prefix="OPEN_PAYMENTS_",
        env_file=None,
        case_sensitive=False,
        extra="ignore",
    )

    data_dir: Path = Field(
        default_factory=lambda: Path(os.path.expanduser("~")) / "open_payments_datasets",
        description="Directory containing CMS Open Payments CSVs (organized by year subdir).",
    )

    # NoDecode disables pydantic-settings's default JSON parsing of list env vars,
    # letting the field_validator below interpret the raw comma-separated string.
    years: Annotated[list[int], NoDecode] = Field(
        default_factory=lambda: [2020, 2021, 2022, 2023, 2024],
        description="CMS publication years to load. Loosened from the legacy "
        "Literal[2020-2024] so child apps can supply 2018, 2019, or future years.",
    )

    payment_classes: Annotated[list[PaymentClass], NoDecode] = Field(
        default_factory=lambda: ["general", "ownership", "research"],
        description="Which payment classes to load. CMS publishes three classes per year.",
    )

    cms_filename_template: str = Field(
        default="OP_DTL_{prefix}_PGYR{year}_*.csv",
        description="Glob template for CMS CSV filenames within `data_dir/{year}/`. "
        "The {prefix} placeholder is filled from `cms_class_prefixes` and {year} from `years`. "
        "Multiple matches resolve by mtime — see ReadPayments.get_payment_csv_path.",
    )

    cms_class_prefixes: dict[str, str] = Field(
        default_factory=lambda: {
            "general": "GNRL",
            "ownership": "OWNRSHP",
            "research": "RSRCH",
        },
        description="CMS filename-component prefixes per payment class.",
    )

    @field_validator("years", mode="before")
    @classmethod
    def _split_years_string(cls, v: object) -> object:
        # pydantic-settings parses comma-separated env-var lists, but if a caller
        # passes a single string in Python (e.g. "2022,2023"), be lenient.
        if isinstance(v, str):
            return [int(x.strip()) for x in v.split(",") if x.strip()]
        return v

    @field_validator("payment_classes", mode="before")
    @classmethod
    def _split_classes_string(cls, v: object) -> object:
        if isinstance(v, str):
            return [x.strip() for x in v.split(",") if x.strip()]
        return v

    def csv_glob(self, payment_class: str, year: int) -> str:
        """Returns the absolute glob pattern for a given payment class + year.

        Example: settings.csv_glob("general", 2023)
            -> "/.../2023/OP_DTL_GNRL_PGYR2023_*.csv"
        """
        if payment_class not in self.cms_class_prefixes:
            raise KeyError(
                f"Unknown payment class {payment_class!r}; known: {sorted(self.cms_class_prefixes)}"
            )
        prefix = self.cms_class_prefixes[payment_class]
        filename = self.cms_filename_template.format(prefix=prefix, year=year)
        return str(self.data_dir / str(year) / filename)
