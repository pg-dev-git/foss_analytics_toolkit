"""Unit tests for FieldMatcher."""

import pytest

from asftool.core.models import MatchMode, MatchType
from asftool.core.services.field_matcher import FieldMatcher, extract_field_strings


class TestExactMatch:
    """Tests for exact_match."""

    def test_match_case_insensitive_default(self):
        m = FieldMatcher()
        assert m.exact_match("Amount", "amount") is True
        assert m.exact_match("AMOUNT", "amount") is True

    def test_match_case_sensitive(self):
        m = FieldMatcher()
        assert m.exact_match("Amount", "amount", case_sensitive=True) is False
        assert m.exact_match("Amount", "Amount", case_sensitive=True) is True

    def test_no_match(self):
        m = FieldMatcher()
        assert m.exact_match("Amount", "Quantity") is False
        assert m.exact_match("Amount", "Amount_2") is False

    def test_empty_strings(self):
        m = FieldMatcher()
        assert m.exact_match("", "amount") is False
        assert m.exact_match("amount", "") is False
        assert m.exact_match("", "") is False


class TestFuzzyMatch:
    """Tests for fuzzy_match."""

    def test_high_similarity_match(self):
        m = FieldMatcher(threshold=80)
        # partial_ratio handles substring matches well; default token_set_ratio
        # is more conservative.
        matched, score = m.fuzzy_match("Account", "AccountName", scorer="partial_ratio")
        assert matched is True
        assert score >= 80

    def test_below_threshold_no_match(self):
        m = FieldMatcher(threshold=95)
        matched, score = m.fuzzy_match("Account", "CompletelyDifferent")
        assert matched is False
        assert score < 95

    def test_threshold_override(self):
        m = FieldMatcher(threshold=95)
        # Lower the bar for this call.
        matched, _ = m.fuzzy_match("Amount", "Amnt", threshold=60)
        assert matched is True

    def test_empty_strings(self):
        m = FieldMatcher()
        assert m.fuzzy_match("", "amount") == (False, 0)
        assert m.fuzzy_match("amount", "") == (False, 0)

    def test_threshold_clamped(self):
        m = FieldMatcher(threshold=200)  # Should clamp to 100
        assert m.threshold == 100
        m2 = FieldMatcher(threshold=-5)  # Should clamp to 0
        assert m2.threshold == 0

    def test_different_scorers(self):
        m = FieldMatcher(threshold=50)
        for scorer in ("ratio", "partial_ratio", "token_sort_ratio", "token_set_ratio"):
            matched, score = m.fuzzy_match("Account", "AccountName", scorer=scorer)
            assert isinstance(matched, bool)
            assert 0 <= score <= 100


class TestMatchField:
    """Tests for match_field (the main entry point)."""

    def test_exact_api_name(self):
        m = FieldMatcher()
        mtype, score, side = m.match_field("Amount", "Amount", None, MatchMode.EXACT)
        assert mtype == MatchType.EXACT
        assert score == 100
        assert side == "api_name"

    def test_exact_label(self):
        m = FieldMatcher()
        mtype, score, side = m.match_field(
            "Total Amount", "amt", "Total Amount", MatchMode.EXACT
        )
        assert mtype == MatchType.EXACT
        assert score == 100
        assert side == "label"

    def test_fuzzy_api_name(self):
        m = FieldMatcher(threshold=80)
        # partial_ratio of "Account" in "AccountName" = 100.
        mtype, score, side = m.match_field("Account", "AccountName", None, MatchMode.FUZZY)
        assert mtype == MatchType.FUZZY
        assert score >= 80
        assert side in ("api_name", "label")

    def test_fuzzy_label(self):
        m = FieldMatcher(threshold=80)
        # partial_ratio of "Account" in "Account Name" = 100.
        mtype, score, side = m.match_field(
            "Account", "id", "Account Name", MatchMode.FUZZY
        )
        assert mtype == MatchType.FUZZY
        assert score >= 80

    def test_no_match(self):
        m = FieldMatcher(threshold=95)
        mtype, score, side = m.match_field(
            "Zzzzz", "AccountName", "Account Name", MatchMode.BOTH
        )
        assert mtype is None
        assert score == 0
        assert side is None

    def test_both_mode_picks_exact_first(self):
        m = FieldMatcher()
        mtype, _, side = m.match_field("Amount", "Amount", "Some Label", MatchMode.BOTH)
        assert mtype == MatchType.EXACT
        assert side == "api_name"

    def test_empty_search_term(self):
        m = FieldMatcher()
        mtype, score, side = m.match_field("", "Amount", "Amount", MatchMode.BOTH)
        assert mtype is None
        assert score == 0
        assert side is None

    def test_exact_only_mode_no_fuzzy(self):
        m = FieldMatcher(threshold=50)
        mtype, _, _ = m.match_field("AcctNm", "AccountName", None, MatchMode.EXACT)
        assert mtype is None

    def test_fuzzy_only_mode_no_exact(self):
        # Exact match on api name, but mode is FUZZY only - still matched because
        # the fuzzy score will be 100 (same string). But MatchType is FUZZY.
        m = FieldMatcher(threshold=80)
        mtype, score, _ = m.match_field("Amount", "Amount", None, MatchMode.FUZZY)
        assert mtype == MatchType.FUZZY
        assert score >= 80


class TestExtractFieldStrings:
    """Tests for extract_field_strings helper."""

    def test_simple_dict(self):
        obj = {"field": "Amount", "label": "Amount Label"}
        results = extract_field_strings(obj)
        assert ("field", "Amount") in results

    def test_nested_dict(self):
        obj = {
            "query": {
                "measures": [{"field": "Amount"}],
            }
        }
        results = extract_field_strings(obj)
        assert any(r[1] == "Amount" for r in results)

    def test_list_of_dicts(self):
        obj = {
            "transformations": [
                {"field": "Col1"},
                {"field": "Col2"},
            ]
        }
        results = extract_field_strings(obj)
        values = [r[1] for r in results]
        assert "Col1" in values
        assert "Col2" in values

    def test_non_field_keys_ignored(self):
        obj = {"description": "Should not match", "field": "Amount"}
        results = extract_field_strings(obj)
        # description isn't a field-like key.
        values = [r[1] for r in results]
        assert "Should not match" not in values
        assert "Amount" in values

    def test_empty_input(self):
        assert extract_field_strings({}) == []
        assert extract_field_strings([]) == []
        assert extract_field_strings(None) == []
        assert extract_field_strings("string") == []
