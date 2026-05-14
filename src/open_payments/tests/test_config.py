"""Tests for the Settings configuration object."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from ..config import Settings


def test__defaults_resolve_to_home_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    # Make sure no OPEN_PAYMENTS_* env var bleeds in from the caller environment.
    for key in list(os.environ):
        if key.startswith("OPEN_PAYMENTS_"):
            monkeypatch.delenv(key, raising=False)
    s = Settings()
    assert s.data_dir == Path(os.path.expanduser("~")) / "open_payments_datasets"
    assert s.years == [2020, 2021, 2022, 2023, 2024]
    assert s.payment_classes == ["general", "ownership", "research"]
    assert s.cms_class_prefixes == {
        "general": "GNRL",
        "ownership": "OWNRSHP",
        "research": "RSRCH",
    }


def test__data_dir_env_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPEN_PAYMENTS_DATA_DIR", str(tmp_path))
    s = Settings()
    assert s.data_dir == tmp_path


def test__years_env_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPEN_PAYMENTS_YEARS", "2018,2019,2023")
    s = Settings()
    assert s.years == [2018, 2019, 2023]


def test__years_accepts_pre_2020(monkeypatch: pytest.MonkeyPatch) -> None:
    # The legacy Literal[2020..2024] would have rejected 2018; Settings must not.
    s = Settings(years=[2018, 2019])
    assert s.years == [2018, 2019]


def test__explicit_args_win_over_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPEN_PAYMENTS_DATA_DIR", "/should/not/be/used")
    s = Settings(data_dir=tmp_path)
    assert s.data_dir == tmp_path


def test__csv_glob_includes_year_subdir_and_prefix(tmp_path: Path) -> None:
    s = Settings(data_dir=tmp_path)
    glob = s.csv_glob("general", 2023)
    assert glob == str(tmp_path / "2023" / "OP_DTL_GNRL_PGYR2023_*.csv")


def test__csv_glob_unknown_class_raises() -> None:
    s = Settings()
    with pytest.raises(KeyError, match="Unknown payment class 'bogus'"):
        s.csv_glob("bogus", 2023)
