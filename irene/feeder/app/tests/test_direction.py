"""Unit tests for direction.py — keyword classifier."""

from __future__ import annotations

import pytest

from direction import DirectionClassifier, DirectionResult
from irene_shared.models import Direction


class TestDirectionClassifier:
    def setup_method(self):
        self.clf = DirectionClassifier()

    # ------------------------------------------------------------------
    # YES signals
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("text", [
        "Senate passes bipartisan infrastructure bill",
        "Fed approved rate increase of 25 basis points",
        "Bitcoin hits all-time high above $100,000",
        "Candidate wins election by a wide margin",
        "Treaty ratified by all member states",
        "Company reaches $1 trillion market cap",
    ])
    def test_yes_headlines(self, text):
        result = self.clf.classify(text)
        assert result.direction == Direction.YES, f"Expected YES for: {text!r}"
        assert result.confidence > 0.0

    # ------------------------------------------------------------------
    # NO signals
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("text", [
        "Senate rejects immigration reform bill",
        "Bitcoin falls below $50,000 support level",
        "Candidate loses election in landslide",
        "Bill vetoed by president",
        "Company cancels planned IPO",
        "Deal collapses after final negotiations fail",
        "Court dismissed all charges in the case",
    ])
    def test_no_headlines(self, text):
        result = self.clf.classify(text)
        assert result.direction == Direction.NO, f"Expected NO for: {text!r}"
        assert result.confidence > 0.0

    # ------------------------------------------------------------------
    # NEUTRAL signals
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("text", [
        "Markets open unchanged on Monday morning",
        "Analyst predicts mixed results for Q3",
        "Breaking: press conference scheduled for 3pm",
        "Weather forecast for London this weekend",
    ])
    def test_neutral_headlines(self, text):
        result = self.clf.classify(text)
        assert result.direction == Direction.NEUTRAL, f"Expected NEUTRAL for: {text!r}"

    # ------------------------------------------------------------------
    # Confidence threshold
    # ------------------------------------------------------------------

    def test_confidence_below_threshold_returns_neutral(self):
        # Text with equal YES and NO signals → low confidence → NEUTRAL
        text = "Bill passes but then rejected after appeal"
        result = self.clf.classify(text, min_confidence=0.99)
        assert result.direction == Direction.NEUTRAL

    def test_confidence_zero_threshold_classifies_anything_with_hits(self):
        text = "Senate passes the bill"
        result = self.clf.classify(text, min_confidence=0.0)
        assert result.direction == Direction.YES

    # ------------------------------------------------------------------
    # Hit counts
    # ------------------------------------------------------------------

    def test_hit_counts_returned(self):
        text = "Senate passes and approves the bill"
        result = self.clf.classify(text)
        assert result.yes_hits >= 2  # "passes" + "approved/approves"
        assert result.no_hits == 0

    def test_zero_hits_returns_neutral_with_zero_confidence(self):
        result = self.clf.classify("Unrelated generic text about nothing")
        assert result.direction == Direction.NEUTRAL
        assert result.confidence == 0.0
        assert result.yes_hits == 0
        assert result.no_hits == 0

    # ------------------------------------------------------------------
    # Extra keywords from config
    # ------------------------------------------------------------------

    def test_extra_yes_keywords(self):
        result = self.clf.classify(
            "The proposal was greenlit by the committee",
            extra_yes_keywords=["greenlit"],
        )
        assert result.direction == Direction.YES

    def test_extra_no_keywords(self):
        result = self.clf.classify(
            "The deal was quashed unexpectedly",
            extra_no_keywords=["quashed"],
        )
        assert result.direction == Direction.NO

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    def test_empty_text(self):
        result = self.clf.classify("")
        assert result.direction == Direction.NEUTRAL
        assert result.yes_hits == 0
        assert result.no_hits == 0

    def test_case_insensitive(self):
        result = self.clf.classify("SENATE PASSES THE BILL")
        assert result.direction == Direction.YES

    def test_word_boundary_prevents_partial_match(self):
        # "passes" should not match inside "trespasses"
        result = self.clf.classify("He trespasses on private land")
        # "passes" is a substring of "trespasses" — the word boundary
        # regex should prevent it matching
        assert result.yes_hits == 0

    def test_multi_word_phrase_match(self):
        # "falls below" is a two-word phrase — should match even with word boundaries
        result = self.clf.classify("Price falls below the key support level")
        assert result.no_hits >= 1
        assert result.direction == Direction.NO
