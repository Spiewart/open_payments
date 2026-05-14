"""Tests for the Section 6 full vectorization (`_vectorized_search`).

What's pinned here:
  - **Behavior parity**: the vectorized cross-merge produces identical
    `unique_ids` / `unmatched` / `unmatched_options` to the per-provider
    path on the canonical 6-scenario fixture. This is the critical
    behavior-preservation gate.
  - **Multi-word fallback**: provider_pks NOT covered by the vectorized
    exact-key merge fall through to the per-provider path so
    `merge_by_last_name`'s str.contains fallback can fire.
  - **NOLASTNAME path**: providers with no last-name match anywhere still
    land in unmatched with the right reason.
  - **Vectorized handles all 6 fixture scenarios** (A through E + X)
    correctly without falling through to the per-provider path for the
    exact-match cases.
"""

from __future__ import annotations

import pandas as pd

from ..choices import Unmatcheds
from ..conflicteds import Conflicteds
from ..ids import ConflictedPaymentIDs, PaymentIDs
from .factories import make_raw_conflicted_row


def _raw_scenarios() -> pd.DataFrame:
    """The same 6 scenarios used by test_end_to_end.py — A/B/C/D unique
    matches, E ambiguous, X no last-name match."""
    return pd.DataFrame(
        [
            make_raw_conflicted_row(
                name="John M. Adams, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Manhattan, NY",
            ),
            make_raw_conflicted_row(
                name="Jane Marie Brown, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Boston, MA",
            ),
            make_raw_conflicted_row(
                name="David A. Smith, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Seattle, WA",
            ),
            make_raw_conflicted_row(
                name="Hannah Lee Smith-Jones, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="San Diego, CA",
            ),
            make_raw_conflicted_row(
                name="Emily White, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Chicago, IL",
            ),
            make_raw_conflicted_row(
                name="Nobody Nonexistent, MD",
                credential="Physician (MD or DO)",
                specialtys="Family Medicine",
                citystates="Nowheresville, NY",
            ),
        ]
    )


def _load_payments(cms_data_dir, fixture_years) -> pd.DataFrame:
    return PaymentIDs(
        years=fixture_years,
        payment_classes="general",
        payments_folder=str(cms_data_dir),
        nrows=None,
        MD_DO_only=True,
    ).all_payments()


class _PerProviderOnly(ConflictedPaymentIDs):
    """ConflictedPaymentIDs with the vectorized phase forced to no-op,
    so the test can compare against the legacy per-provider behavior."""

    def _vectorized_search(self, conflicteds):
        return set()


# ---------------------------------------------------------------------------
# Behavior parity: vectorized vs. per-provider
# ---------------------------------------------------------------------------


def test__vectorized_matches_per_provider_unique_ids(cms_data_dir, fixture_years):
    """The vectorized cross-merge must produce the same `(provider_pk,
    profile_id)` set in `unique_ids` as the per-provider path. This is the
    behavior-preservation gate."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    vectorized = ConflictedPaymentIDs(conflicteds=conflicteds.copy(), payments=payments)
    vectorized.search_for_conflicteds_ids()

    per_provider = _PerProviderOnly(conflicteds=conflicteds.copy(), payments=payments)
    per_provider.search_for_conflicteds_ids()

    vec_pairs = set(
        zip(vectorized.unique_ids["provider_pk"], vectorized.unique_ids["profile_id"], strict=True)
    )
    pp_pairs = set(
        zip(
            per_provider.unique_ids["provider_pk"],
            per_provider.unique_ids["profile_id"],
            strict=True,
        )
    )
    assert vec_pairs == pp_pairs


def test__vectorized_matches_per_provider_unmatched(cms_data_dir, fixture_years):
    """Same provider_pks should land in `unmatched` with the same reason."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    vectorized = ConflictedPaymentIDs(conflicteds=conflicteds.copy(), payments=payments)
    vectorized.search_for_conflicteds_ids()

    per_provider = _PerProviderOnly(conflicteds=conflicteds.copy(), payments=payments)
    per_provider.search_for_conflicteds_ids()

    def _pk_to_reason(df):
        if df.empty:
            return {}
        return dict(zip(df["provider_pk"], df["unmatched"], strict=True))

    assert _pk_to_reason(vectorized.unmatched) == _pk_to_reason(per_provider.unmatched)


def test__vectorized_matches_per_provider_unmatched_options(cms_data_dir, fixture_years):
    """Same `(provider_pk, profile_id)` candidates surfaced as
    unmatched_options."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    vectorized = ConflictedPaymentIDs(conflicteds=conflicteds.copy(), payments=payments)
    vectorized.search_for_conflicteds_ids()

    per_provider = _PerProviderOnly(conflicteds=conflicteds.copy(), payments=payments)
    per_provider.search_for_conflicteds_ids()

    def _pk_profile_set(df):
        if df.empty:
            return set()
        return set(zip(df["provider_pk"], df["profile_id"], strict=True))

    assert _pk_profile_set(vectorized.unmatched_options) == _pk_profile_set(
        per_provider.unmatched_options
    )


# ---------------------------------------------------------------------------
# Vectorized path explicitly handles the common cases
# ---------------------------------------------------------------------------


def test__vectorized_handles_clean_lastname_matches_without_fallback(cms_data_dir, fixture_years):
    """Provider_pks 0–4 (Adams / Brown / Smith / Smith-Jones / White) all have
    exact-match last names in the fixture — they should be handled by the
    vectorized phase. Provider_pk 5 (Nonexistent) has no last_name match
    anywhere → falls through to per-provider, which records NOLASTNAME."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    matcher = ConflictedPaymentIDs(conflicteds=conflicteds.copy(), payments=payments)

    # Capture which provider_pks the vectorized phase claims.
    vectorized_handled = matcher._vectorized_search(
        conflicteds=conflicteds.rename(
            columns={
                col: f"conflict_{col}"
                for col in conflicteds.columns
                if col not in ("last_name", "provider_pk")
            }
        )
    )
    # 5 of 6 conflicteds (provider_pks 0..4) should be handled by the
    # vectorized phase; provider_pk 5 (Nonexistent) misses the exact-key
    # merge so falls through.
    assert vectorized_handled == {0, 1, 2, 3, 4}


def test__vectorized_handles_hyphenated_lastname(cms_data_dir, fixture_years):
    """Smith-Jones (provider_pk 3) is hyphenated but lowercased equality
    still matches CMS's 'SMITH-JONES'. The vectorized exact-key merge must
    handle this without needing the per-provider multi-word fallback."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    matcher = ConflictedPaymentIDs(conflicteds=conflicteds.copy(), payments=payments)
    matcher.search_for_conflicteds_ids()

    smith_jones = matcher.unique_ids[matcher.unique_ids["provider_pk"] == 3]
    assert len(smith_jones) == 1
    assert smith_jones.iloc[0]["profile_id"] == 401


def test__vectorized_falls_through_for_no_lastname_match(cms_data_dir, fixture_years):
    """Provider_pk 5 (Nonexistent) has no last_name match anywhere — it falls
    through the vectorized phase and the per-provider phase records it as
    NOLASTNAME."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    matcher = ConflictedPaymentIDs(conflicteds=conflicteds.copy(), payments=payments)
    matcher.search_for_conflicteds_ids()

    nonexistent = matcher.unmatched[matcher.unmatched["provider_pk"] == 5]
    assert len(nonexistent) == 1
    assert nonexistent.iloc[0]["unmatched"] == Unmatcheds.NOLASTNAME


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test__vectorized_empty_payments_handled_gracefully(cms_data_dir, fixture_years):
    """If payments is empty, every conflicted should land in unmatched
    (NOLASTNAME) without the vectorized phase crashing."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    empty_payments = pd.DataFrame(columns=["last_name", "first_name", "profile_id"])

    matcher = ConflictedPaymentIDs(conflicteds=conflicteds.copy(), payments=empty_payments)
    matcher.search_for_conflicteds_ids()

    assert matcher.unique_ids.empty
    assert len(matcher.unmatched) == len(conflicteds)
    assert (matcher.unmatched["unmatched"] == Unmatcheds.NOLASTNAME).all()


def test__vectorized_empty_conflicteds_handled_gracefully(cms_data_dir, fixture_years):
    """Empty conflicteds → all output frames empty, no crash."""
    payments = _load_payments(cms_data_dir, fixture_years)
    matcher = ConflictedPaymentIDs(
        conflicteds=pd.DataFrame(columns=["provider_pk", "last_name"]),
        payments=payments,
    )
    matcher.search_for_conflicteds_ids()
    assert matcher.unique_ids.empty
    assert matcher.unmatched.empty
    assert matcher.unmatched_options.empty


def test__vectorized_search_can_be_disabled_by_subclass(cms_data_dir, fixture_years):
    """Subclasses can opt out of vectorization by overriding
    `_vectorized_search` — that's the escape hatch for studies whose
    merge_column isn't last_name or whose semantics need full per-provider
    handling. _PerProviderOnly subclass tests this in the parity tests
    above; this test pins the contract independently."""
    raw = _raw_scenarios()
    conflicteds = Conflicteds(raw).us_conflicteds_id_search_df()
    payments = _load_payments(cms_data_dir, fixture_years)

    matcher = _PerProviderOnly(conflicteds=conflicteds.copy(), payments=payments)
    matcher.search_for_conflicteds_ids()

    # The result still has 4 unique matches (A/B/C/D scenarios) — proving
    # the per-provider path produces a working result even with
    # vectorization disabled.
    assert len(matcher.unique_ids) == 4
