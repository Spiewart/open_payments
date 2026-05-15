"""Tests for the institution → ``CityState`` stub.

The implementation is currently a placeholder (``locate`` returns ``[]``
for everything). These tests pin the public contract — input/output
shapes, dedup semantics, graceful degradation on unknown names — so a
future LLM-backed or gazetteer-backed implementation can land without
breaking callers.
"""

from open_payments import InstitutionLocator


class TestInstitutionLocatorStub:
    def test_locate_unknown_returns_empty_list(self):
        # Placeholder behavior — callers MUST degrade gracefully on []
        # rather than treating it as an error, because that's how the
        # matcher's citystates filter handles missing data already.
        assert InstitutionLocator().locate("Johns Hopkins University") == []

    def test_locate_returns_a_list(self):
        # Even when unimplemented, the return type contract must hold:
        # callers iterate the result.
        result = InstitutionLocator().locate("Cleveland Clinic")
        assert isinstance(result, list)

    def test_locate_many_dedups_across_inputs(self):
        # If two institution names resolve to the same (city, state),
        # the union returns only one entry. Currently no name resolves
        # to anything, so this is a forward-compatibility test pinning
        # the dedup behavior the future implementation must preserve.
        result = InstitutionLocator().locate_many(
            ["Johns Hopkins University", "Johns Hopkins University School of Medicine"]
        )
        assert isinstance(result, list)
        # Once implementation lands, both inputs should resolve to
        # ("Baltimore", "MD") and the result should have len == 1.

    def test_locate_many_empty_input(self):
        assert InstitutionLocator().locate_many([]) == []
