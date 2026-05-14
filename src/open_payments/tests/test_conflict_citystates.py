"""Tests for the conflicted-side citystate parsers and ConflictCityStates mixin."""

from __future__ import annotations

import pandas as pd
import pytest

from ..citystates import (
    CityState,
    ConflictCityStates,
    parse_citystate_string,
    parse_citystates_string,
)

# ---------------------------------------------------------------------------
# parse_citystate_string
# ---------------------------------------------------------------------------


def test__parse_basic_city_state():
    cs = parse_citystate_string("Boston, MA")
    assert cs.city == "Boston"
    assert cs.state == "MA"


def test__parse_full_state_name():
    cs = parse_citystate_string("Boston, Massachusetts")
    assert cs.city == "Boston"
    assert cs.state == "Massachusetts"


def test__parse_strips_whitespace():
    cs = parse_citystate_string("  Boston ,  MA  ")
    assert cs.city == "Boston"
    assert cs.state == "MA"


def test__parse_single_token_is_state_abbrev():
    cs = parse_citystate_string("MA")
    assert cs.state == "MA"
    assert cs.city is None


def test__parse_single_token_is_state_full_name():
    cs = parse_citystate_string("Massachusetts")
    assert cs.state == "Massachusetts"
    assert cs.city is None


@pytest.mark.parametrize("territory", ["PR", "GU", "VI", "MP", "FM", "PW", "AE", "AP"])
def test__parse_recognizes_us_territory_codes(territory):
    # Empirical: a 500K-row sample of CMS 2023 general payments emits all 8 of
    # these in Recipient_State. Before the States enum was expanded these would
    # have been (mis)classified as cities.
    cs = parse_citystate_string(territory)
    assert cs.state == territory
    assert cs.city is None


def test__citystate_matches_across_territory_pair():
    cms = CityState(city="SAN JUAN", state="PR")
    conflict = parse_citystate_string("SAN JUAN, Puerto Rico")
    assert cms.citystate_matches(conflict)


def test__parse_single_token_is_city():
    # Boston isn't in the States enum -> treated as city.
    cs = parse_citystate_string("Boston")
    assert cs.city == "Boston"
    assert cs.state is None


@pytest.mark.parametrize(
    "value",
    ["", "   ", None, ",", " , "],
)
def test__parse_blank_returns_none(value):
    assert parse_citystate_string(value) is None


def test__parse_city_only_with_trailing_comma():
    # "Boston," -> city only.
    cs = parse_citystate_string("Boston,")
    assert cs.city == "Boston"
    assert cs.state is None


def test__parse_extra_commas_takes_first_two_parts():
    # "Boston, MA, USA" -> we take the first two; USA discarded.
    cs = parse_citystate_string("Boston, MA, USA")
    assert cs.city == "Boston"
    assert cs.state == "MA"


# ---------------------------------------------------------------------------
# parse_citystates_string (multi-location)
# ---------------------------------------------------------------------------


def test__multi_semicolon_delimited():
    result = parse_citystates_string("Boston, MA; New York, NY")
    assert len(result) == 2
    assert result[0].city == "Boston"
    assert result[1].city == "New York"


def test__multi_pipe_delimited():
    result = parse_citystates_string("Boston, MA | New York, NY")
    assert len(result) == 2


def test__multi_mixed_delimiters():
    result = parse_citystates_string("Boston, MA; New York, NY | Seattle, WA")
    assert len(result) == 3


def test__multi_skips_blank_segments():
    result = parse_citystates_string("Boston, MA;; |;New York, NY")
    assert len(result) == 2
    assert result[0].city == "Boston"
    assert result[1].city == "New York"


def test__multi_blank_returns_empty_list():
    assert parse_citystates_string("") == []
    assert parse_citystates_string(None) == []
    assert parse_citystates_string("   ") == []


def test__single_location_still_works():
    result = parse_citystates_string("Boston, MA")
    assert len(result) == 1
    assert result[0].state == "MA"


# ---------------------------------------------------------------------------
# ConflictCityStates default mixin
# ---------------------------------------------------------------------------


def test__conflict_citystates_default_single_location():
    df = pd.DataFrame({"citystates": ["Manhattan, NY"], "other": [1]})
    out = ConflictCityStates(df).conflict_citystates()
    assert "citystates" in out.columns
    assert "other" in out.columns
    assert out.iloc[0]["citystates"] == [CityState(city="Manhattan", state="NY")]


def test__conflict_citystates_default_multi_location():
    df = pd.DataFrame({"citystates": ["Boston, MA | Cambridge, MA"]})
    out = ConflictCityStates(df).conflict_citystates()
    assert len(out.iloc[0]["citystates"]) == 2


def test__conflict_citystates_handles_blank_rows():
    df = pd.DataFrame({"citystates": ["Manhattan, NY", None, "", "  "]})
    out = ConflictCityStates(df).conflict_citystates()
    assert len(out.iloc[0]["citystates"]) == 1
    assert out.iloc[1]["citystates"] == []
    assert out.iloc[2]["citystates"] == []
    assert out.iloc[3]["citystates"] == []


def test__conflict_citystates_replaces_source_column_in_place():
    # Default CITYSTATES_COLUMN is also "citystates" - so it gets replaced rather
    # than appended-then-dropped.
    df = pd.DataFrame({"citystates": ["Manhattan, NY"], "other": [1]})
    out = ConflictCityStates(df).conflict_citystates()
    # citystates column is now list[CityState], not str
    assert isinstance(out.iloc[0]["citystates"], list)


def test__custom_source_column_via_classvar():
    class CustomConflictCityStates(ConflictCityStates):
        CITYSTATES_COLUMN = "location"

    df = pd.DataFrame({"location": ["Boston, MA"], "other": [1]})
    out = CustomConflictCityStates(df).conflict_citystates()
    assert "location" not in out.columns  # source dropped because name differs
    assert "citystates" in out.columns
    assert out.iloc[0]["citystates"] == [CityState(city="Boston", state="MA")]


# ---------------------------------------------------------------------------
# Subclass override examples — deans (separate columns) and uptodate
# (institution-only, would normally need LLM)
# ---------------------------------------------------------------------------


class _DeansLikeConflictCityStates(ConflictCityStates):
    """deans dataset has ``City`` + ``State`` columns - bypass string parsing."""

    @classmethod
    def get_citystates(cls, row):
        city = row.get("City") if "City" in row.index else None
        state = row.get("State") if "State" in row.index else None
        if pd.isna(city) and pd.isna(state):
            return []
        return [
            CityState(
                city=city.strip() if pd.notna(city) else None,
                state=state.strip() if pd.notna(state) else None,
            )
        ]

    def conflict_citystates(self):
        self.conflicts = self.conflicts.copy()
        self.conflicts["citystates"] = self.conflicts.apply(self.get_citystates, axis=1).values
        self.conflicts = self.conflicts.drop(columns=["City", "State"])
        return self.conflicts


def test__deans_style_subclass():
    df = pd.DataFrame(
        {
            "City": ["Boston", "New York", None],
            "State": ["MA", "NY", "CA"],
        }
    )
    out = _DeansLikeConflictCityStates(df).conflict_citystates()
    assert out.iloc[0]["citystates"] == [CityState(city="Boston", state="MA")]
    assert out.iloc[1]["citystates"] == [CityState(city="New York", state="NY")]
    assert out.iloc[2]["citystates"] == [CityState(state="CA")]
    assert "City" not in out.columns
    assert "State" not in out.columns


class _UptodateLikeConflictCityStates(ConflictCityStates):
    """Stand-in for uptodate's institution-name LLM lookup. Real subclass would
    call an LLM; here we use a hard-coded mini-gazetteer to demonstrate the
    override shape."""

    _MINI_GAZETTEER = {
        "Brigham and Women's Hospital": [CityState(city="Boston", state="MA")],
        "Mayo Clinic": [
            CityState(city="Rochester", state="MN"),
            CityState(city="Jacksonville", state="FL"),
        ],
    }

    CITYSTATES_COLUMN = "institution"

    @classmethod
    def get_citystates(cls, row):
        institution = row.get("institution") if "institution" in row.index else None
        if pd.isna(institution) or not institution:
            return []
        return cls._MINI_GAZETTEER.get(institution, [])


def test__uptodate_style_subclass():
    df = pd.DataFrame({"institution": ["Mayo Clinic", "Brigham and Women's Hospital", "Unknown"]})
    out = _UptodateLikeConflictCityStates(df).conflict_citystates()
    assert len(out.iloc[0]["citystates"]) == 2  # Mayo has two
    assert out.iloc[1]["citystates"] == [CityState(city="Boston", state="MA")]
    assert out.iloc[2]["citystates"] == []  # unknown -> empty
    assert "institution" not in out.columns
