"""Normalize a corporate-entity string for fuzzy matching.

Child apps (``abim_conflicts``, ``deans_conflicts``) compare disclosed
conflict entities ("AbbVie, Inc.") against CMS payment entities
("AbbVie LLC"). Both need to reduce these strings to their core brand
before comparing, which is what this module does:

- Strip legal-form suffixes (``Inc.``, ``LLC``, ``GmbH``, ``Ltd``)
- Strip trademark / reserved-rights symbols
- Drop parenthesized clauses ("(formerly Acme Co.)")
- Apply a case-insensitive vocabulary of generic words ("pharmaceutical",
  "healthcare", "company", "international", "biosciences", ...) that
  vary across CMS reporting and aren't load-bearing for identity
- Collapse runs of whitespace and convert dash/slash separators to spaces

Example
-------
>>> EntityParser("AbbVie, Inc.").parse()[0]
'AbbVie'
>>> EntityParser("Pfizer Pharmaceuticals Corp.").parse()[0]
'Pfizer'

This is a post-match utility — it does not feed the matcher's
provider-resolution path. The matcher cares about provider name +
credentials + city/state + NPI; this module is about disclosed entity
text normalization on the *payor* side, used downstream when comparing
"what did the provider disclose?" vs "what did CMS report a sponsor
paid them for?".

Lifecycle
---------
Originally lived in ``abim_conflicts/entities.py`` (the entity-export
utility module that was deleted when ``open_payments`` took over the
CMS matcher). Briefly inlined into ``abim_conflicts/analysis.py``. Moved
here because both abim and deans share the same disclosed-entity
normalization needs — keeping a single canonical implementation
prevents the two repos from drifting on what counts as the same brand.

Override surface
----------------
The ``useless_words`` / ``useless_strings`` / ``useless_suffixes`` /
``useless_prefixes`` properties are designed to be overridden in a
subclass when a child app needs to tune the vocabulary without
rewriting the parser. Example:

.. code-block:: python

    class AbimEntityParser(EntityParser):
        @property
        def useless_words(self) -> list[str]:
            return super().useless_words + ["foundation"]
"""

from __future__ import annotations

import re

import pandas as pd


class EntityParser:
    """Normalize corporate-entity strings for fuzzy brand-level matching.

    Stateful: holds the current normalized form on ``self.entity`` and an
    accumulating dictionary of seen entities on ``self.entities`` so
    callers building a corpus can pass the running set in and out.

    Typical use::

        parsed, entities = EntityParser("Pfizer Pharmaceuticals Corp.", entities).parse()
    """

    def __init__(self, entity: str, entities: pd.DataFrame | None = None):
        self.entity = entity
        self.entities = pd.DataFrame() if entities is None else entities

    def parse(self) -> tuple[str, pd.DataFrame]:
        """Normalize ``self.entity`` and return ``(normalized, entities_df)``.

        Short-circuits to a matched entry when ``self.entities`` already
        contains a row whose entity contains ``self.entity`` as a substring
        (case-insensitive). Otherwise runs the full normalization pipeline
        and appends the result to ``entities`` so subsequent callers can
        hit the short-circuit path.
        """
        if not self.entities.empty:
            match = self.match_entity()
            if match is not None:
                return match, self.entities

        self.remove_parentheses_and_between()
        self.remove_a_an_and_the_of_for()
        self.remove_starting_The()
        self.remove_punctuation()
        self.space_for_dash_or_slash()
        self.remove_trademarks()
        self.swap_plus_for_space()
        self.remove_excess_spaces()
        self.remove_useless_words()
        self.remove_useless_strings()
        self.remove_useless_suffixes()
        self.remove_useless_prefixes()
        self.remove_excess_spaces()

        self.entities = pd.concat(
            [self.entities, pd.DataFrame({"entity": [self.entity]})], ignore_index=True
        )

        return self.entity, self.entities

    def match_entity(self) -> str | None:
        """Return the first entry in ``self.entities`` whose ``entity``
        column contains ``self.entity`` (case-insensitive substring), or None."""
        match = self.entities[self.entities["entity"].str.contains(self.entity, case=False)]
        if not match.empty:
            return match["entity"].iloc[0]
        return None

    # ------------------------------------------------------------------
    # Pipeline steps (each mutates ``self.entity``)
    # ------------------------------------------------------------------

    def remove_a_an_and_the_of_for(self):
        self.entity = re.sub(r"\s(and|an|a|the|of|for)(\s|,|.)", " ", self.entity)

    def remove_starting_The(self):
        self.entity = re.sub(r"^The\s", "", self.entity)

    def remove_punctuation(self):
        self.entity = re.sub(r"[.,&\[\]]", "", self.entity)

    def space_for_dash_or_slash(self):
        self.entity = re.sub(r"[-/]", " ", self.entity)

    def remove_trademarks(self):
        self.entity = re.sub(r"™®", "", self.entity)

    def remove_parentheses_and_between(self) -> None:
        self.entity = re.sub(pattern=r"\(.*\)", repl="", string=self.entity)

    def swap_plus_for_space(self) -> None:
        self.entity = re.sub(pattern=r"\+", repl=" ", string=self.entity)

    def remove_excess_spaces(self) -> None:
        self.entity = re.sub(pattern=r"\s+", repl=" ", string=self.entity)

    def remove_useless_words(self) -> None:
        """Strip vocabulary words anywhere they appear — including as part
        of a larger word. ``Pfizer Pharmaceuticals`` → ``Pfizer``; also
        ``PfizerPharmaceuticals`` → ``Pfizer``. Case-insensitive."""
        pattern = "|".join([rf"{w}(\s|$)" for w in self.useless_words])
        self.entity = re.sub(pattern=pattern, repl=" ", string=self.entity, flags=re.IGNORECASE)

    @property
    def useless_words(self) -> list[str]:
        """Generic descriptors that don't contribute to brand identity.
        Ordered with longer phrases first so a longer match wins against
        a shorter overlapping one (e.g. "biopharmaceuticals" before
        "biopharma")."""
        return [
            "america",
            "biopharmaceuticals",
            "biopharma",
            "biosciences",
            "biotechnology",
            "biotech",
            "business",
            "companies",
            "company",
            "corporation",
            "development & commercialization",
            "development commercialization",
            "distributors",
            "division of",
            "division",
            "health care",
            "healthcare",
            "holdings",
            "incorporated",
            "international",
            "laboratories",
            "life science",
            "life sciences",
            "lifescience",
            "lifesciences",
            "limited",
            "medical",
            "north america",
            "oncology",
            "pharmaceuticals",
            "pharmaceutical",
            "pharma",
            "pharm",
            "products",
            "research development",
            "sales service",
            "scientific affairs",
            "services",
            "service",
            "solutions",
            "technology",
            "technologies",
            "therapeutics",
        ]

    def remove_useless_strings(self) -> None:
        """Strip vocabulary words only when they appear as standalone tokens
        (surrounded by whitespace or string boundary). Case-insensitive."""
        pattern = "|".join([rf"\s{w}(\s|$)" for w in self.useless_strings])
        self.entity = re.sub(pattern=pattern, repl=" ", string=self.entity, flags=re.IGNORECASE)

    @property
    def useless_strings(self) -> list[str]:
        """Short tokens that only count as noise when standalone. ``Co.``
        in ``Coca-Cola`` should NOT match, but ``Acme Co`` should — this
        list is for the standalone-only path."""
        return [
            "Co", "corp", "F", "inc", "llc", "lp", "ltd",
            "nutrition", "sales", "sons", "UK", "us", "usa",
        ]

    def remove_useless_suffixes(self) -> None:
        """Strip vocabulary tokens anywhere in the string. Case-SENSITIVE
        because the entries are uppercase trademarks (Inc, LLC, etc.) that
        a case-insensitive pass would over-strip from real words."""
        pattern = "|".join([rf"{w}(\s|$)" for w in self.useless_suffixes])
        self.entity = re.sub(pattern=pattern, repl=" ", string=self.entity)

    @property
    def useless_suffixes(self) -> list[str]:
        """Case-sensitive legal-form / locale suffixes."""
        return [
            "AB", "AG", "An", "Corp", "DS", "ER", "GmbH",
            "Inc", "INC", "LLC", "LP", "Ltd", "plc", "TX", "US", "USA",
        ]

    def remove_useless_prefixes(self) -> None:
        """Strip vocabulary tokens only at the start of the string.
        Case-SENSITIVE."""
        pattern = "|".join([rf"^{w}\s" for w in self.useless_prefixes])
        self.entity = re.sub(pattern=pattern, repl="", string=self.entity)

    @property
    def useless_prefixes(self) -> list[str]:
        """Case-sensitive leading-noise tokens. ``ER`` is a CMS-emitted
        prefix on some research-payment entity strings."""
        return ["ER"]
