"""Tests for the corporate-entity normalizer.

These mirror the behavior tests that previously lived in
``abim_conflicts/tests/test_analysis.py`` — the parser moved here so
both abim_conflicts and deans_conflicts can share a single canonical
implementation.
"""

import pandas as pd

from open_payments import EntityParser


class TestEntityParserUnit:
    def setup_method(self):
        self.entities = pd.DataFrame({"entity": ["Janssen", "Pfizer", "Abbvie"]})

    def test_remove_useless_words_strips_corporation_suffix(self):
        # ``remove_useless_words`` strips known suffixes regardless of word
        # boundary — matches the docstring claim that ``PfizerCorporation``
        # → ``Pfizer``.
        parser = EntityParser("Pfizer Corporation")
        parser.remove_useless_words()
        assert "Corporation" not in parser.entity
        assert parser.entity.strip() == "Pfizer"

        parser = EntityParser("PfizerCorporation")
        parser.remove_useless_words()
        assert "Corporation" not in parser.entity

    def test_remove_punctuation_strips_commas_and_periods(self):
        parser = EntityParser("A hootin fun . company, in the middle of nowhere")
        parser.remove_punctuation()
        assert "," not in parser.entity
        assert "." not in parser.entity

    def test_match_entity_substring_hits_existing_corpus(self):
        match = EntityParser("Pfizer", self.entities).match_entity()
        assert match  # truthy: returns the matched entity string

    def test_match_entity_returns_none_when_no_substring_match(self):
        match = EntityParser("Bristol-Myers Squibb", self.entities).match_entity()
        assert match is None

    def test_full_parse_returns_normalized_entity_and_grows_corpus(self):
        seed = pd.DataFrame({"entity": []})
        normalized, grown = EntityParser("AbbVie, Inc.", seed).parse()
        # Normalization drops the comma and the legal-form suffix.
        assert "," not in normalized
        assert "Inc" not in normalized
        # The parsed entity gets appended to the corpus so a subsequent
        # caller can short-circuit via ``match_entity``.
        assert len(grown) == 1

    def test_short_circuit_when_entity_already_in_corpus(self):
        corpus = pd.DataFrame({"entity": ["Pfizer"]})
        # ``Pfizer`` is in the corpus → short-circuit returns it without
        # running the full normalization pipeline. The returned corpus is
        # unchanged.
        normalized, returned_corpus = EntityParser("Pfizer", corpus).parse()
        assert normalized == "Pfizer"
        assert len(returned_corpus) == 1
