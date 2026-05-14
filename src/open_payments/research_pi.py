"""Research-CSV Principal Investigator (PI) block handling.

CMS Open Payments research CSVs publish up to 5 ``Principal_Investigator_N_*``
column blocks per row in addition to the ``Covered_Recipient_*`` block. Each
PI block represents a separate person who could be matched against a
conflicted-provider input (their own NPI, name, credentials, specialty,
city/state, license states).

The matcher historically scanned only ``Covered_Recipient_*``, silently
missing every conflicted who is a PI but not the principal recipient.
For ABIM (internal-medicine board certification) this is a critical gap:
most internal-medicine providers appear in research data as PIs on
manufacturer-sponsored trials, not as the trial's principal recipient.

Design — explode-on-read:

  1. ``read_payments_csvs("research")`` reads the wide CMS frame including
     Covered_Recipient + PI block columns.
  2. ``explode_research_pi_blocks(chunk)`` (called per chunk after
     filter_payment_chunk) transforms each input row into up to 6 output
     rows — one per populated person slot. Each output row uses the
     ``Covered_Recipient_*`` / ``Recipient_*`` CMS column names regardless
     of which slot it came from, so downstream code (the standard rename
     in ``update_payments``, the list-column builders in
     ``post_update_payments_mod``, the matcher itself) sees a uniform
     shape with no PI awareness needed.
  3. A ``person_slot`` provenance column is added: "covered_recipient"
     or "pi_1" through "pi_5". Useful for analysts to know which slot
     fired the match.

Sub-rows with a null profile_id for that slot are dropped (slot wasn't
populated for that payment).
"""

from __future__ import annotations

from typing import Union

import pandas as pd

#: Number of PI slots CMS publishes per research row.
RESEARCH_PI_SLOTS: tuple[int, ...] = (1, 2, 3, 4, 5)

#: Slot label used in the ``person_slot`` provenance column.
COVERED_RECIPIENT_SLOT: str = "covered_recipient"


#: For each canonical CMS column suffix on the Covered_Recipient side, the
#: corresponding suffix template used by PI blocks. Most are identical
#: ("Last_Name" on both); the exception is city/state, where Covered_Recipient
#: uses the bare ``Recipient_<X>`` prefix while PI uses
#: ``Principal_Investigator_N_<X>``.
#:
#: This dict is the source of truth for "which column on the PI side
#: corresponds to which column on the CR side". It's used both by
#: :func:`pi_block_cms_columns_for_dtype_dict` (to add PI columns to a
#: ``research_columns`` mapping) and by :func:`explode_research_pi_blocks`
#: (to rename PI columns to their CR equivalents during the explode).
#:
#: Format: ``{cr_cms_column_name: pi_suffix}``.  The PI N column is
#: ``f"Principal_Investigator_{N}_{pi_suffix}"``.
CR_TO_PI_SUFFIX: dict[str, str] = {
    # Profile + identifier
    "Covered_Recipient_Profile_ID": "Profile_ID",
    "Covered_Recipient_NPI": "NPI",
    # Names
    "Covered_Recipient_First_Name": "First_Name",
    "Covered_Recipient_Middle_Name": "Middle_Name",
    "Covered_Recipient_Last_Name": "Last_Name",
    "Covered_Recipient_Name_Suffix": "Name_Suffix",
    # Credentials (Primary_Type_1..6)
    "Covered_Recipient_Primary_Type_1": "Primary_Type_1",
    "Covered_Recipient_Primary_Type_2": "Primary_Type_2",
    "Covered_Recipient_Primary_Type_3": "Primary_Type_3",
    "Covered_Recipient_Primary_Type_4": "Primary_Type_4",
    "Covered_Recipient_Primary_Type_5": "Primary_Type_5",
    "Covered_Recipient_Primary_Type_6": "Primary_Type_6",
    # Specialty (Specialty_1..6)
    "Covered_Recipient_Specialty_1": "Specialty_1",
    "Covered_Recipient_Specialty_2": "Specialty_2",
    "Covered_Recipient_Specialty_3": "Specialty_3",
    "Covered_Recipient_Specialty_4": "Specialty_4",
    "Covered_Recipient_Specialty_5": "Specialty_5",
    "Covered_Recipient_Specialty_6": "Specialty_6",
    # City / State (Covered_Recipient uses bare Recipient_ prefix; PI has its own)
    "Recipient_City": "City",
    "Recipient_State": "State",
    # License state codes 1..5
    "Covered_Recipient_License_State_code1": "License_State_code1",
    "Covered_Recipient_License_State_code2": "License_State_code2",
    "Covered_Recipient_License_State_code3": "License_State_code3",
    "Covered_Recipient_License_State_code4": "License_State_code4",
    "Covered_Recipient_License_State_code5": "License_State_code5",
}


def pi_block_cms_columns_for_dtype_dict(
    cr_columns: dict[str, tuple[str, Union[type[str], str]]],
) -> dict[str, tuple[str, Union[type[str], str]]]:
    """Build the ``research_columns`` extensions for PI blocks 1-5.

    Given a mixin's existing ``research_columns`` mapping (which covers the
    Covered_Recipient side only), return the additional PI block entries to
    merge in. Entries map ``Principal_Investigator_N_<suffix>`` CMS column
    names to ``(slot_canonical_name, dtype)`` — the canonical name uses a
    ``pi_<n>_`` prefix to avoid colliding with the CR canonical name.

    The slot-prefixed canonical names are TEMPORARY — they exist only
    between the CSV read and the explode step. After
    :func:`explode_research_pi_blocks` runs, all rows use the standard
    canonical names (no slot prefix) and the explode adds a ``person_slot``
    provenance column.

    Mixins that don't want PI block coverage (e.g. ownership-only fields)
    don't need to call this. The default integration is to call this from
    each filter mixin's ``research_columns`` property and ``cols.update(...)``
    the result.
    """
    pi_cols: dict[str, tuple[str, Union[type[str], str]]] = {}
    for cms_cr_col, (canonical, dtype) in cr_columns.items():
        if cms_cr_col not in CR_TO_PI_SUFFIX:
            # Mixin's research_columns includes a CR column we don't have a
            # PI mapping for. That's intentional for fields with no PI
            # equivalent; skip it silently. (None of the current 5 filter
            # mixins should hit this path — every CR column they map has
            # a PI counterpart per CMS schema.)
            continue
        pi_suffix = CR_TO_PI_SUFFIX[cms_cr_col]
        for n in RESEARCH_PI_SLOTS:
            pi_cms_col = f"Principal_Investigator_{n}_{pi_suffix}"
            slot_canonical = f"pi_{n}_{canonical}"
            pi_cols[pi_cms_col] = (slot_canonical, dtype)
    return pi_cols


def explode_research_pi_blocks(chunk: pd.DataFrame) -> pd.DataFrame:
    """Transform a wide research-CSV chunk into a long per-person-slot frame.

    Each input row becomes up to 6 output rows (1 Covered_Recipient + 5 PI).
    PI block CMS columns are renamed to their Covered_Recipient equivalents
    so the post-explode frame has a uniform shape. A ``person_slot`` column
    records provenance.

    Sub-rows with a null profile_id for the slot are dropped (the slot
    wasn't populated for that payment).

    No-op when no PI block columns are present in ``chunk``. Safe to call
    on chunks that have already been exploded.
    """
    pi_block_cols = [c for c in chunk.columns if c.startswith("Principal_Investigator_")]
    if not pi_block_cols:
        # Either no PI block columns were loaded, or this chunk was already
        # exploded. Add the person_slot provenance column if missing.
        if "person_slot" not in chunk.columns:
            chunk = chunk.copy()
            chunk["person_slot"] = COVERED_RECIPIENT_SLOT
        return chunk

    sub_frames: list[pd.DataFrame] = []

    # Slot 0: Covered_Recipient. Drop ALL PI block columns; the resulting
    # frame matches the standard CR-only research shape.
    cr_frame = chunk.drop(columns=pi_block_cols).copy()
    cr_frame["person_slot"] = COVERED_RECIPIENT_SLOT
    sub_frames.append(cr_frame)

    # Slots 1-5: each PI block. Rename PI N columns to their CR equivalents,
    # drop other slots' columns, drop rows with a null PI N profile_id.
    for n in RESEARCH_PI_SLOTS:
        pi_n_prefix = f"Principal_Investigator_{n}_"
        pi_n_present = [c for c in pi_block_cols if c.startswith(pi_n_prefix)]
        if not pi_n_present:
            # CSV didn't include PI N columns at all — skip this slot.
            continue
        # Rename PI N columns to their CR equivalents.
        rename_map: dict[str, str] = {}
        for pi_col in pi_n_present:
            suffix = pi_col[len(pi_n_prefix) :]
            # Find the matching CR column for this suffix. The reverse
            # lookup over CR_TO_PI_SUFFIX is small (~26 entries) so a linear
            # scan is fine; cache if hot.
            cr_col = next(
                (cr for cr, pi_suf in CR_TO_PI_SUFFIX.items() if pi_suf == suffix),
                None,
            )
            if cr_col is None:
                # Loaded a PI N column we don't know how to map. Skip
                # silently — it'll be dropped below.
                continue
            rename_map[pi_col] = cr_col
        # Drop other slots' PI block columns AND the original CR columns
        # (we'll repopulate the CR columns from PI N via rename).
        cr_cols_to_drop = [c for c in chunk.columns if c in CR_TO_PI_SUFFIX]
        other_pi_cols = [c for c in pi_block_cols if not c.startswith(pi_n_prefix)]
        sub = chunk.drop(columns=cr_cols_to_drop + other_pi_cols).rename(columns=rename_map)
        # Drop unmapped PI N columns (those whose suffix wasn't in CR_TO_PI_SUFFIX).
        unmapped_pi_n = [c for c in sub.columns if c.startswith(pi_n_prefix)]
        if unmapped_pi_n:
            sub = sub.drop(columns=unmapped_pi_n)
        # Drop rows where this slot wasn't populated.
        if "Covered_Recipient_Profile_ID" in sub.columns:
            sub = sub.dropna(subset=["Covered_Recipient_Profile_ID"])
        if sub.empty:
            continue
        sub = sub.copy()
        sub["person_slot"] = f"pi_{n}"
        sub_frames.append(sub)

    if len(sub_frames) == 1:
        return sub_frames[0]
    return pd.concat(sub_frames, ignore_index=True)
