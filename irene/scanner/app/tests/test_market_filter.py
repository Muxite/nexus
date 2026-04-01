"""Unit tests for market_filter — parametrized filter rules."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from irene_shared.config import MarketsSection
from market_filter import MarketFilter


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _market(
    hours_to_close: float = 24.0,
    volume_usdc: float = 10000.0,
    question: str = "Will the Fed raise rates?",
    condition_id: str = "cond_001",
) -> dict:
    return {
        "condition_id": condition_id,
        "slug": "test-market",
        "question": question,
        "end_date": _now() + timedelta(hours=hours_to_close),
        "volume_usdc": volume_usdc,
        "token_yes": "tok_yes",
        "token_no": "tok_no",
    }


def _cfg(**overrides) -> MarketsSection:
    defaults = dict(
        min_volume_usdc=5000.0,
        max_hours_to_close=72.0,
        min_hours_to_close=2.0,
        exclude_keywords=["nfl", "nba"],
    )
    defaults.update(overrides)
    return MarketsSection(**defaults)


class TestMarketFilter:
    def setup_method(self):
        self.f = MarketFilter()

    # ------------------------------------------------------------------
    # Time window
    # ------------------------------------------------------------------

    def test_passes_within_window(self):
        result = self.f.filter([_market(hours_to_close=24)], _cfg())
        assert len(result) == 1

    def test_rejects_too_soon(self):
        result = self.f.filter([_market(hours_to_close=1)], _cfg())
        assert len(result) == 0

    def test_rejects_too_far_out(self):
        result = self.f.filter([_market(hours_to_close=100)], _cfg())
        assert len(result) == 0

    def test_accepts_at_exact_min(self):
        # Pass an explicit `now` so the end_date is exactly at the boundary.
        now = datetime.now(timezone.utc)
        m = {
            "condition_id": "c_exact_min",
            "slug": "test",
            "question": "Will the Fed raise rates?",
            "end_date": now + timedelta(hours=2.0),
            "volume_usdc": 10000.0,
            "token_yes": "t",
            "token_no": "t",
        }
        result = self.f.filter([m], _cfg(), now=now)
        assert len(result) == 1

    def test_accepts_at_exact_max(self):
        now = datetime.now(timezone.utc)
        m = {
            "condition_id": "c_exact_max",
            "slug": "test",
            "question": "Will the Fed raise rates?",
            "end_date": now + timedelta(hours=72.0),
            "volume_usdc": 10000.0,
            "token_yes": "t",
            "token_no": "t",
        }
        result = self.f.filter([m], _cfg(), now=now)
        assert len(result) == 1

    def test_rejects_past_end_date(self):
        m = _market(hours_to_close=-1.0)
        result = self.f.filter([m], _cfg())
        assert len(result) == 0

    def test_missing_end_date_rejected(self):
        m = _market()
        m.pop("end_date")
        result = self.f.filter([m], _cfg())
        assert len(result) == 0

    # ------------------------------------------------------------------
    # Volume
    # ------------------------------------------------------------------

    def test_rejects_low_volume(self):
        result = self.f.filter([_market(volume_usdc=100.0)], _cfg())
        assert len(result) == 0

    def test_passes_at_min_volume(self):
        result = self.f.filter([_market(volume_usdc=5000.0)], _cfg())
        assert len(result) == 1

    def test_passes_none_volume(self):
        # None volume is not filtered (we can't verify it)
        m = _market()
        m["volume_usdc"] = None
        result = self.f.filter([m], _cfg())
        assert len(result) == 1

    # ------------------------------------------------------------------
    # Keyword exclusion
    # ------------------------------------------------------------------

    def test_rejects_excluded_keyword(self):
        result = self.f.filter(
            [_market(question="Who wins the NFL Super Bowl?")], _cfg()
        )
        assert len(result) == 0

    def test_keyword_is_case_insensitive(self):
        result = self.f.filter(
            [_market(question="NBA Championship winner 2026")], _cfg()
        )
        assert len(result) == 0

    def test_keyword_match_partial_word(self):
        # "nfl" inside "nfl" word boundary — should still reject
        result = self.f.filter(
            [_market(question="Will the nfl season end early?")], _cfg()
        )
        assert len(result) == 0

    def test_no_excluded_keywords(self):
        result = self.f.filter(
            [_market(question="Will Bitcoin exceed $100k?")], _cfg(exclude_keywords=[])
        )
        assert len(result) == 1

    def test_custom_exclude_list(self):
        result = self.f.filter(
            [_market(question="Euro 2026 winner?")],
            _cfg(exclude_keywords=["euro 2026"]),
        )
        assert len(result) == 0

    # ------------------------------------------------------------------
    # Multiple markets
    # ------------------------------------------------------------------

    def test_filters_mixed_batch(self):
        markets = [
            _market(hours_to_close=24, question="Will the Fed raise rates?", condition_id="c1"),
            _market(hours_to_close=1, question="Expires soon", condition_id="c2"),     # too soon
            _market(hours_to_close=36, question="NBA finals winner", condition_id="c3"),  # excluded
            _market(hours_to_close=48, volume_usdc=100, condition_id="c4"),             # low volume
        ]
        result = self.f.filter(markets, _cfg())
        assert len(result) == 1
        assert result[0]["condition_id"] == "c1"

    def test_empty_input_returns_empty(self):
        assert self.f.filter([], _cfg()) == []

    # ------------------------------------------------------------------
    # Config hot-reload simulation
    # ------------------------------------------------------------------

    def test_config_change_applied_on_next_call(self):
        m = _market(hours_to_close=80)  # outside default 72h window

        cfg_tight = _cfg(max_hours_to_close=72)
        assert len(self.f.filter([m], cfg_tight)) == 0

        cfg_wide = _cfg(max_hours_to_close=100)
        assert len(self.f.filter([m], cfg_wide)) == 1
