"""LiveMarketCache unit tests — snapshot/get, staleness, sequence gating,
parser semantics, and the empty-cache no-mid fallback shape."""

from __future__ import annotations

import time

import pytest

from app.live_market_cache import LiveMarketCache, LiveTick


def _book(bid_price: float, ask_price: float, *, seq: int = 1) -> dict:
    return {
        "asset_id": "irrelevant",
        "bids": [{"price": str(bid_price), "size": "100"}],
        "asks": [{"price": str(ask_price), "size": "100"}],
        "seq": seq,
    }


def test_book_snapshot_roundtrip_populates_mid_bid_ask():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.apply_book_snapshot("tok-1", _book(0.40, 0.44, seq=1))
    tick = cache.get("tok-1")
    assert tick is not None
    assert tick.best_bid == pytest.approx(0.40)
    assert tick.best_ask == pytest.approx(0.44)
    assert tick.mid == pytest.approx(0.42)
    assert tick.sequence == 1


def test_is_fresh_boundary():
    cache = LiveMarketCache(stale_after_s=0.05)
    cache.apply_book_snapshot("tok-1", _book(0.50, 0.52))
    tick = cache.get("tok-1")
    assert cache.is_fresh(tick)
    time.sleep(0.08)
    # Same tick reference; staleness should have crossed the boundary.
    assert not cache.is_fresh(tick)


def test_out_of_order_delta_is_dropped():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.apply_book_snapshot("tok-1", _book(0.40, 0.44, seq=5))
    # An older snapshot must NOT overwrite the newer one.
    cache.apply_book_snapshot("tok-1", _book(0.10, 0.90, seq=2))
    tick = cache.get("tok-1")
    assert tick.best_bid == 0.40
    assert tick.best_ask == 0.44
    assert tick.sequence == 5


def test_equal_sequence_is_allowed_for_snapshot_replay():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.apply_book_snapshot("tok-1", _book(0.40, 0.44, seq=7))
    # Replay of the same seq (Polymarket retransmits snapshots on reconnect).
    cache.apply_book_snapshot("tok-1", _book(0.41, 0.45, seq=7))
    tick = cache.get("tok-1")
    assert tick.best_bid == 0.41
    assert tick.best_ask == 0.45


def test_price_change_with_precomputed_top_of_book_updates():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.apply_book_snapshot("tok-1", _book(0.40, 0.44, seq=1))
    cache.apply_price_change(
        "tok-1",
        {"best_bid": "0.42", "best_ask": "0.45", "seq": 2},
    )
    tick = cache.get("tok-1")
    assert tick.best_bid == 0.42
    assert tick.best_ask == 0.45
    assert tick.mid == (0.42 + 0.45) / 2.0


def test_price_change_without_top_of_book_is_dropped():
    """We don't maintain full depth — a single-level delta with no
    precomputed top must NOT silently corrupt the cached top."""
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.apply_book_snapshot("tok-1", _book(0.40, 0.44, seq=1))
    cache.apply_price_change("tok-1", {"price": "0.41", "size": "5", "seq": 2})
    tick = cache.get("tok-1")
    # Untouched.
    assert tick.best_bid == 0.40
    assert tick.best_ask == 0.44
    assert tick.sequence == 1


def test_last_trade_updates_only_last_trade_slot_when_book_known():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.apply_book_snapshot("tok-1", _book(0.40, 0.44, seq=1))
    cache.apply_last_trade("tok-1", {"price": "0.43", "seq": 2})
    tick = cache.get("tok-1")
    assert tick.last_trade == pytest.approx(0.43)
    # Book-derived mid is preserved.
    assert tick.mid == pytest.approx(0.42)
    assert tick.best_bid == pytest.approx(0.40)
    assert tick.best_ask == pytest.approx(0.44)


def test_last_trade_falls_back_for_mid_when_book_unknown():
    cache = LiveMarketCache(stale_after_s=5.0)
    # No book snapshot first — trade-only path.
    cache.apply_last_trade("tok-1", {"price": "0.30", "seq": 1})
    tick = cache.get("tok-1")
    assert tick.last_trade == 0.30
    # Without a book, mid falls back to last trade so reads aren't useless.
    assert tick.mid == 0.30


def test_crossed_book_returns_no_mid():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.apply_book_snapshot("tok-1", _book(0.60, 0.40, seq=1))  # crossed
    tick = cache.get("tok-1")
    assert tick.best_bid == 0.60
    assert tick.best_ask == 0.40
    assert tick.mid is None


def test_out_of_range_prices_dropped():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.apply_book_snapshot(
        "tok-1",
        {"bids": [{"price": "1.5"}], "asks": [{"price": "2.0"}], "seq": 1},
    )
    tick = cache.get("tok-1")
    assert tick.best_bid is None
    assert tick.best_ask is None
    assert tick.mid is None


def test_set_tick_direct_injection_respects_sequence_gate():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.set_tick(
        LiveTick("tok-1", 0.5, 0.49, 0.51, None, time.monotonic(), 10),
    )
    # Older tick is dropped.
    cache.set_tick(
        LiveTick("tok-1", 0.6, 0.59, 0.61, None, time.monotonic(), 5),
    )
    tick = cache.get("tok-1")
    assert tick.mid == 0.5
    assert tick.sequence == 10


def test_size_matches_observed_tokens():
    cache = LiveMarketCache(stale_after_s=5.0)
    assert cache.size == 0
    cache.apply_book_snapshot("tok-a", _book(0.4, 0.5))
    cache.apply_book_snapshot("tok-b", _book(0.6, 0.7))
    assert cache.size == 2


def test_get_unknown_token_returns_none():
    cache = LiveMarketCache(stale_after_s=5.0)
    assert cache.get("nope") is None


def test_malformed_messages_are_no_ops():
    cache = LiveMarketCache(stale_after_s=5.0)
    cache.apply_book_snapshot("tok-1", None)  # type: ignore[arg-type]
    cache.apply_book_snapshot("", _book(0.4, 0.5))
    cache.apply_price_change("tok-1", None)  # type: ignore[arg-type]
    cache.apply_last_trade("tok-1", None)  # type: ignore[arg-type]
    cache.apply_last_trade("tok-1", {"price": "not-a-number"})
    cache.apply_last_trade("tok-1", {"price": "1.5"})  # out of range
    assert cache.size == 0
