"""Parametrized tri-state coverage for every ``filter_by_*`` method.

Section 5.8 changed filter return type from ``bool`` to
:class:`FilterOutcome` (``MATCH`` / ``DISAGREE`` / ``NO_DATA``).  Existing
tests in ``test_ids.py`` cover MATCH vs. not-MATCH but don't distinguish
DISAGREE from NO_DATA — this file pins that distinction so future regressions
(e.g. a filter accidentally returning DISAGREE for a missing-data case, or
NO_DATA when both sides genuinely disagree) are caught.

Each filter gets three parametrized scenarios:

  - **MATCH**: both sides have data and agree
  - **DISAGREE**: both sides have data and conflict (negative evidence)
  - **NO_DATA**: at least one side is blank / absent

For filters with supersession rules (e.g. ``filter_by_city`` returns NO_DATA
when CITYSTATE already fired), a dedicated supersession case is added on
top of the base three.
"""

from __future__ import annotations

import pandas as pd
import pytest

from ..choices import FilterOutcome, PaymentFilters
from ..citystates import CityState, PaymentIDsCityStatesMixin
from ..credentials import Credentials, PaymentIDsCredentialsMixin
from ..ids import ConflictedPaymentIDs
from ..names import PaymentIDsNamesMixin
from ..npi import PaymentIDsNPIMixin
from ..specialtys import PaymentIDsSpecialtysMixin, Specialtys

# ---------------------------------------------------------------------------
# NPI
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payment_npi,conflict_npi,expected",
    [
        (1234567890, 1234567890, FilterOutcome.MATCH),
        (1234567890, 9876543210, FilterOutcome.DISAGREE),
        (pd.NA, 1234567890, FilterOutcome.NO_DATA),
        (1234567890, pd.NA, FilterOutcome.NO_DATA),
        (pd.NA, pd.NA, FilterOutcome.NO_DATA),
        # Excel-stored-as-float case still produces MATCH via int() coercion.
        (1234567890.0, 1234567890, FilterOutcome.MATCH),
    ],
)
def test__filter_by_npi_tristate(payment_npi, conflict_npi, expected):
    row = pd.Series({"npi": payment_npi, "conflict_npi": conflict_npi, "filters": []})
    assert PaymentIDsNPIMixin.filter_by_npi(row) is expected


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payment_creds,conflict_creds,expected",
    [
        ([Credentials.MEDICAL_DOCTOR], [Credentials.MEDICAL_DOCTOR], FilterOutcome.MATCH),
        # Shared member → MATCH even with non-overlapping extras.
        (
            [Credentials.MEDICAL_DOCTOR, Credentials.PHYSICIAN_ASSISTANT],
            [Credentials.MEDICAL_DOCTOR],
            FilterOutcome.MATCH,
        ),
        (
            [Credentials.MEDICAL_DOCTOR],
            [Credentials.NURSE_PRACTITIONER, Credentials.PHYSICIAN_ASSISTANT],
            FilterOutcome.DISAGREE,
        ),
        ([], [Credentials.MEDICAL_DOCTOR], FilterOutcome.NO_DATA),
        ([Credentials.MEDICAL_DOCTOR], [], FilterOutcome.NO_DATA),
        ([], [], FilterOutcome.NO_DATA),
    ],
)
def test__filter_by_credential_tristate(payment_creds, conflict_creds, expected):
    row = pd.Series(
        {
            "credentials": payment_creds,
            "conflict_credentials": conflict_creds,
            "filters": [PaymentFilters.LASTNAME],
        }
    )
    assert PaymentIDsCredentialsMixin.filter_by_credential(row) is expected


# ---------------------------------------------------------------------------
# First name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "first,conflict_first,expected",
    [
        ("John", "John", FilterOutcome.MATCH),
        ("John", "john", FilterOutcome.MATCH),  # case-insensitive
        ("John", "Judd", FilterOutcome.DISAGREE),
        (None, "John", FilterOutcome.NO_DATA),
        ("John", None, FilterOutcome.NO_DATA),
        (None, None, FilterOutcome.NO_DATA),
        ("", "John", FilterOutcome.NO_DATA),
    ],
)
def test__filter_by_firstname_tristate(first, conflict_first, expected):
    row = pd.Series(
        {
            "first_name": first,
            "conflict_first_name": conflict_first,
            "filters": [PaymentFilters.LASTNAME],
        }
    )
    assert PaymentIDsNamesMixin.filter_by_firstname(row) is expected


# ---------------------------------------------------------------------------
# Middle initial
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payment_middle,conflict_mi_1,conflict_mi_2,conflict_mn_1,conflict_mn_2,expected",
    [
        # MATCH: payment "Alpha" initial 'A' matches conflict mi_1 'A'.
        ("Alpha", "A", None, None, None, FilterOutcome.MATCH),
        # MATCH via conflict middle_name_1 first initial.
        ("Alpha", None, None, "Alfred", None, FilterOutcome.MATCH),
        # DISAGREE: payment 'A' vs. conflict 'B'.
        ("Alpha", "B", None, None, None, FilterOutcome.DISAGREE),
        # DISAGREE: payment 'A' vs. multiple conflict initials neither match.
        ("Alpha", "B", "C", None, None, FilterOutcome.DISAGREE),
        # NO_DATA: payment has no middle name.
        (None, "A", None, None, None, FilterOutcome.NO_DATA),
        # NO_DATA: conflict has no middle initials anywhere.
        ("Alpha", None, None, None, None, FilterOutcome.NO_DATA),
    ],
)
def test__filter_by_middle_initial_tristate(
    payment_middle, conflict_mi_1, conflict_mi_2, conflict_mn_1, conflict_mn_2, expected
):
    row = pd.Series(
        {
            "middle_name": payment_middle,
            "conflict_middle_initial_1": conflict_mi_1,
            "conflict_middle_initial_2": conflict_mi_2,
            "conflict_middle_name_1": conflict_mn_1,
            "conflict_middle_name_2": conflict_mn_2,
            "filters": [PaymentFilters.LASTNAME],
        }
    )
    assert PaymentIDsNamesMixin.filter_by_middle_initial(row) is expected


# ---------------------------------------------------------------------------
# Middle name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payment_middle,conflict_mn_1,conflict_mn_2,expected",
    [
        ("Marie", "Marie", None, FilterOutcome.MATCH),
        ("Marie", None, "Marie", FilterOutcome.MATCH),
        ("MARIE", "marie", None, FilterOutcome.MATCH),  # normalize lowers
        ("Marie", "Anne", None, FilterOutcome.DISAGREE),
        ("Marie", "Anne", "Beth", FilterOutcome.DISAGREE),
        (None, "Marie", None, FilterOutcome.NO_DATA),
        ("Marie", None, None, FilterOutcome.NO_DATA),
        ("", "Marie", None, FilterOutcome.NO_DATA),
    ],
)
def test__filter_by_middlename_tristate(payment_middle, conflict_mn_1, conflict_mn_2, expected):
    row = pd.Series(
        {
            "middle_name": payment_middle,
            "conflict_middle_name_1": conflict_mn_1,
            "conflict_middle_name_2": conflict_mn_2,
            "filters": [PaymentFilters.LASTNAME],
        }
    )
    assert PaymentIDsNamesMixin.filter_by_middlename(row) is expected


# ---------------------------------------------------------------------------
# Name suffix (hit-only filter — whitelist enforced)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payment_suffix,conflict_suffix,expected",
    [
        # MATCH: both whitelisted + equal post-normalize.
        ("JR", "JR", FilterOutcome.MATCH),
        ("Jr.", "JR", FilterOutcome.MATCH),  # case + punctuation normalized
        ("III", "III", FilterOutcome.MATCH),
        # DISAGREE: both whitelisted but differ.
        ("JR", "SR", FilterOutcome.DISAGREE),
        ("II", "III", FilterOutcome.DISAGREE),
        # NO_DATA: non-whitelisted on either side (e.g. credential leak).
        ("MD", "JR", FilterOutcome.NO_DATA),
        ("JR", "MD", FilterOutcome.NO_DATA),
        # NO_DATA: missing on either side.
        (None, "JR", FilterOutcome.NO_DATA),
        ("JR", None, FilterOutcome.NO_DATA),
        (None, None, FilterOutcome.NO_DATA),
        # NO_DATA: Roman-I rejected by whitelist (avoid suffix/initial confusion).
        ("I", "I", FilterOutcome.NO_DATA),
    ],
)
def test__filter_by_name_suffix_tristate(payment_suffix, conflict_suffix, expected):
    row = pd.Series(
        {
            "name_suffix": payment_suffix,
            "conflict_name_suffix": conflict_suffix,
            "filters": [PaymentFilters.LASTNAME],
        }
    )
    assert PaymentIDsNamesMixin.filter_by_name_suffix(row) is expected


# ---------------------------------------------------------------------------
# City / State / CityState — with supersession cases
# ---------------------------------------------------------------------------


NY = CityState(city="New York", state="NY")
NY_FULL = CityState(city="New York", state="New York")
LA = CityState(city="Los Angeles", state="CA")
SF = CityState(city="San Francisco", state="CA")


@pytest.mark.parametrize(
    "payment_cs,conflict_cs,filters,expected",
    [
        ([NY], [NY], [PaymentFilters.LASTNAME], FilterOutcome.MATCH),
        # DISAGREE: same dimension (city) but values differ.
        ([NY], [LA], [PaymentFilters.LASTNAME], FilterOutcome.DISAGREE),
        # NO_DATA: empty on one side.
        ([], [NY], [PaymentFilters.LASTNAME], FilterOutcome.NO_DATA),
        ([NY], [], [PaymentFilters.LASTNAME], FilterOutcome.NO_DATA),
        # NO_DATA via supersession: CITYSTATE already fired.
        (
            [NY],
            [LA],
            [PaymentFilters.LASTNAME, PaymentFilters.CITYSTATE],
            FilterOutcome.NO_DATA,
        ),
    ],
)
def test__filter_by_city_tristate(payment_cs, conflict_cs, filters, expected):
    row = pd.Series(
        {
            "citystates": payment_cs,
            "conflict_citystates": conflict_cs,
            "filters": filters,
        }
    )
    assert PaymentIDsCityStatesMixin.filter_by_city(row) is expected


@pytest.mark.parametrize(
    "payment_cs,conflict_cs,filters,expected",
    [
        ([NY], [NY], [PaymentFilters.LASTNAME], FilterOutcome.MATCH),
        # MATCH: abbreviation ↔ full-name canonicalization.
        ([NY], [NY_FULL], [PaymentFilters.LASTNAME], FilterOutcome.MATCH),
        # DISAGREE: states differ.
        ([NY], [LA], [PaymentFilters.LASTNAME], FilterOutcome.DISAGREE),
        # NO_DATA on empty side.
        ([], [NY], [PaymentFilters.LASTNAME], FilterOutcome.NO_DATA),
        # NO_DATA via supersession.
        (
            [NY],
            [LA],
            [PaymentFilters.LASTNAME, PaymentFilters.CITYSTATE],
            FilterOutcome.NO_DATA,
        ),
    ],
)
def test__filter_by_state_tristate(payment_cs, conflict_cs, filters, expected):
    row = pd.Series(
        {
            "citystates": payment_cs,
            "conflict_citystates": conflict_cs,
            "filters": filters,
        }
    )
    assert PaymentIDsCityStatesMixin.filter_by_state(row) is expected


@pytest.mark.parametrize(
    "payment_cs,conflict_cs,expected",
    [
        ([NY], [NY], FilterOutcome.MATCH),
        # Multi-candidate: at least one full pair agrees.
        ([NY], [LA, NY], FilterOutcome.MATCH),
        # DISAGREE: both have pairs, none fully agree.
        ([NY], [LA], FilterOutcome.DISAGREE),
        ([NY], [SF, LA], FilterOutcome.DISAGREE),
        # NO_DATA: empty on either side.
        ([], [NY], FilterOutcome.NO_DATA),
        ([NY], [], FilterOutcome.NO_DATA),
    ],
)
def test__filter_by_citystate_tristate(payment_cs, conflict_cs, expected):
    row = pd.Series(
        {
            "citystates": payment_cs,
            "conflict_citystates": conflict_cs,
            "filters": [PaymentFilters.LASTNAME],
        }
    )
    assert PaymentIDsCityStatesMixin.filter_by_citystate(row) is expected


# ---------------------------------------------------------------------------
# Specialty / Subspecialty / Fullspecialty — with supersession
# ---------------------------------------------------------------------------


PEDS_GI = Specialtys(specialty="Pediatrics", subspecialty="Gastroenterology")
PEDS_ONLY = Specialtys(specialty="Pediatrics")
PEDS_CARDIO = Specialtys(specialty="Pediatrics", subspecialty="Cardiology")
FM = Specialtys(specialty="Family Medicine")
FM_GERIATRIC = Specialtys(specialty="Family Medicine", subspecialty="Geriatric Medicine")
IM = Specialtys(specialty="Internal Medicine")


@pytest.mark.parametrize(
    "payment_specs,conflict_specs,filters,expected",
    [
        ([PEDS_GI], [PEDS_ONLY], [PaymentFilters.LASTNAME], FilterOutcome.MATCH),
        ([PEDS_GI], [FM, IM], [PaymentFilters.LASTNAME], FilterOutcome.DISAGREE),
        ([], [PEDS_ONLY], [PaymentFilters.LASTNAME], FilterOutcome.NO_DATA),
        ([PEDS_GI], [], [PaymentFilters.LASTNAME], FilterOutcome.NO_DATA),
        # Supersession: FULLSPECIALTY already fired.
        (
            [PEDS_GI],
            [FM],
            [PaymentFilters.LASTNAME, PaymentFilters.FULLSPECIALTY],
            FilterOutcome.NO_DATA,
        ),
    ],
)
def test__filter_by_specialty_tristate(payment_specs, conflict_specs, filters, expected):
    row = pd.Series(
        {
            "specialtys": payment_specs,
            "conflict_specialtys": conflict_specs,
            "filters": filters,
        }
    )
    assert PaymentIDsSpecialtysMixin.filter_by_specialty(row) is expected


@pytest.mark.parametrize(
    "payment_specs,conflict_specs,filters,expected",
    [
        ([PEDS_GI], [PEDS_GI], [PaymentFilters.LASTNAME], FilterOutcome.MATCH),
        # DISAGREE: both sides have subspecialties, none agree.
        ([PEDS_GI], [PEDS_CARDIO], [PaymentFilters.LASTNAME], FilterOutcome.DISAGREE),
        # NO_DATA: conflict has no subspecialty values at all.
        ([PEDS_GI], [PEDS_ONLY], [PaymentFilters.LASTNAME], FilterOutcome.NO_DATA),
        # NO_DATA: conflict has specialty but no subspecialty.
        ([PEDS_GI], [FM], [PaymentFilters.LASTNAME], FilterOutcome.NO_DATA),
        ([], [PEDS_GI], [PaymentFilters.LASTNAME], FilterOutcome.NO_DATA),
        # Supersession.
        (
            [PEDS_GI],
            [PEDS_GI],
            [PaymentFilters.LASTNAME, PaymentFilters.FULLSPECIALTY],
            FilterOutcome.NO_DATA,
        ),
    ],
)
def test__filter_by_subspecialty_tristate(payment_specs, conflict_specs, filters, expected):
    row = pd.Series(
        {
            "specialtys": payment_specs,
            "conflict_specialtys": conflict_specs,
            "filters": filters,
        }
    )
    assert PaymentIDsSpecialtysMixin.filter_by_subspecialty(row) is expected


@pytest.mark.parametrize(
    "payment_specs,conflict_specs,expected",
    [
        ([PEDS_GI], [PEDS_GI], FilterOutcome.MATCH),
        # DISAGREE: both sides have full (specialty+subspecialty) pairs, none agree.
        ([PEDS_GI], [FM_GERIATRIC], FilterOutcome.DISAGREE),
        # NO_DATA when either side empty.
        ([], [PEDS_GI], FilterOutcome.NO_DATA),
        ([PEDS_GI], [], FilterOutcome.NO_DATA),
        # NO_DATA: conflict has specialty but no subspecialty (not a "full" pair).
        ([PEDS_GI], [FM], FilterOutcome.NO_DATA),
    ],
)
def test__filter_by_fullspecialty_tristate(payment_specs, conflict_specs, expected):
    row = pd.Series(
        {
            "specialtys": payment_specs,
            "conflict_specialtys": conflict_specs,
            "filters": [PaymentFilters.LASTNAME],
        }
    )
    assert PaymentIDsSpecialtysMixin.filter_by_fullspecialty(row) is expected


# ---------------------------------------------------------------------------
# Conflicted_x_PaymentIDs.filter_payment: outcome routing
#
# filter_payment receives a Series (one row, via DataFrame.apply(axis=1)),
# inspects the outcome of the underlying filter_by_* method, and appends the
# PaymentFilters to either `filters` (MATCH) or `negative_filters` (DISAGREE),
# or neither (NO_DATA).  We stub filter_by_credential per-test to pin each
# branch independently of credential-parsing logic.
# ---------------------------------------------------------------------------


class _StubMatch(ConflictedPaymentIDs):
    def filter_by_credential(self, payments_x_conflicted):
        return FilterOutcome.MATCH


class _StubDisagree(ConflictedPaymentIDs):
    def filter_by_credential(self, payments_x_conflicted):
        return FilterOutcome.DISAGREE


class _StubNoData(ConflictedPaymentIDs):
    def filter_by_credential(self, payments_x_conflicted):
        return FilterOutcome.NO_DATA


def _empty_matcher(cls):
    """Construct a stub matcher without running its full __init__ — we don't
    need real conflicteds/payments to test the routing logic."""
    return cls.__new__(cls)


def _routing_row() -> pd.Series:
    return pd.Series({"filters": [], "negative_filters": []})


def test__filter_payment_routes_match_to_filters():
    matcher = _empty_matcher(_StubMatch)
    out = matcher.filter_payment(
        payments_x_conflicted=_routing_row(),
        payment_filter=PaymentFilters.CREDENTIAL,
    )
    assert PaymentFilters.CREDENTIAL in out["filters"]
    assert PaymentFilters.CREDENTIAL not in out["negative_filters"]


def test__filter_payment_routes_disagree_to_negative_filters():
    matcher = _empty_matcher(_StubDisagree)
    out = matcher.filter_payment(
        payments_x_conflicted=_routing_row(),
        payment_filter=PaymentFilters.CREDENTIAL,
    )
    assert PaymentFilters.CREDENTIAL not in out["filters"]
    assert PaymentFilters.CREDENTIAL in out["negative_filters"]


def test__filter_payment_routes_no_data_to_neither():
    matcher = _empty_matcher(_StubNoData)
    out = matcher.filter_payment(
        payments_x_conflicted=_routing_row(),
        payment_filter=PaymentFilters.CREDENTIAL,
    )
    assert PaymentFilters.CREDENTIAL not in out["filters"]
    assert PaymentFilters.CREDENTIAL not in out["negative_filters"]


# ---------------------------------------------------------------------------
# apply_all_filters_to_row — Section 6 single-pass optimization
# ---------------------------------------------------------------------------


def test__apply_all_filters_to_row_routes_each_filter_through_filter_payment():
    """Section 6: replace 14 separate apply(axis=1) calls with one pass that
    runs every filter against a row. Behavior must be identical to
    sequential per-filter routing."""

    class _MultiStub(ConflictedPaymentIDs):
        # Stub two filter_by_* methods that return different outcomes.
        @property
        def filters(self):
            return [PaymentFilters.CREDENTIAL, PaymentFilters.NPI, PaymentFilters.FIRSTNAME]

        def filter_by_credential(self, payments_x_conflicted):
            return FilterOutcome.MATCH

        def filter_by_npi(self, payments_x_conflicted):
            return FilterOutcome.DISAGREE

        def filter_by_firstname(self, payments_x_conflicted):
            return FilterOutcome.NO_DATA

    matcher = _MultiStub.__new__(_MultiStub)
    row = pd.Series({"filters": [], "negative_filters": []})
    out = matcher.apply_all_filters_to_row(row)
    assert PaymentFilters.CREDENTIAL in out["filters"]
    assert PaymentFilters.NPI in out["negative_filters"]
    assert PaymentFilters.FIRSTNAME not in out["filters"]
    assert PaymentFilters.FIRSTNAME not in out["negative_filters"]


def test__apply_all_filters_to_row_preserves_filter_order():
    """Filter order matters because some filters supersede others (e.g.
    FIRSTNAME removing FIRSTNAME_PARTIAL from a row's filters list).
    apply_all_filters_to_row must iterate self.filters in order."""

    call_order: list[PaymentFilters] = []

    class _OrderStub(ConflictedPaymentIDs):
        @property
        def filters(self):
            return [PaymentFilters.LASTNAME, PaymentFilters.FIRSTNAME, PaymentFilters.NPI]

        def filter_by_lastname(self, payments_x_conflicted):
            call_order.append(PaymentFilters.LASTNAME)
            return FilterOutcome.MATCH

        def filter_by_firstname(self, payments_x_conflicted):
            call_order.append(PaymentFilters.FIRSTNAME)
            return FilterOutcome.MATCH

        def filter_by_npi(self, payments_x_conflicted):
            call_order.append(PaymentFilters.NPI)
            return FilterOutcome.MATCH

    matcher = _OrderStub.__new__(_OrderStub)
    row = pd.Series({"filters": [], "negative_filters": []})
    matcher.apply_all_filters_to_row(row)
    assert call_order == [
        PaymentFilters.LASTNAME,
        PaymentFilters.FIRSTNAME,
        PaymentFilters.NPI,
    ]


def test__apply_all_filters_to_row_handles_empty_row():
    matcher = _StubMatch.__new__(_StubMatch)
    out = matcher.apply_all_filters_to_row(pd.Series(dtype=object))
    assert out.empty
