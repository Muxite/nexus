"""Cycle 25 — pre-execute re-check that the market is still tradable.

`_gate_market_vanished` re-compares the cached market dict captured at
lookup time against a fresh read after the LLM call. The trade path uses
it to refuse trades on markets that closed mid-flight, with a distinct
`market_vanished` reason that flags market-discovery races to the audit
layer.
"""

from __future__ import annotations

from app.stages._gates import _gate_market_vanished


def _cached_live() -> dict:
    return {
        "question": "Will X happen?",
        "active": 1,
        "closed_at": None,
        "token_yes": "ty",
        "token_no": "tn",
    }


def test_passes_when_fresh_unchanged():
    status, entry, reason = _gate_market_vanished(_cached_live(), _cached_live())
    assert status == "ok"
    assert entry is None
    assert reason is None


def test_rejects_when_row_deleted_between_lookup_and_decision():
    status, entry, reason = _gate_market_vanished(_cached_live(), None)
    assert status == "reject"
    assert reason == "market_vanished"
    assert entry is not None and entry["gate"] == "market_vanished"
    assert entry["value"] == "row_deleted"


def test_rejects_when_active_flipped_off_during_decision():
    fresh = _cached_live()
    fresh["active"] = 0
    status, entry, reason = _gate_market_vanished(_cached_live(), fresh)
    assert status == "reject"
    assert reason == "market_vanished"
    assert "active" in str(entry["value"])


def test_rejects_when_closed_at_appeared_during_decision():
    fresh = _cached_live()
    fresh["closed_at"] = "2026-05-26T20:00:00Z"
    status, entry, reason = _gate_market_vanished(_cached_live(), fresh)
    assert status == "reject"
    assert reason == "market_vanished"
    assert "closed_at" in str(entry["value"])


def test_does_not_re_reject_when_cached_already_indicated_closed():
    # If the cached dict already said closed_at, _gate_market_state should
    # have caught it before this gate fires. Defensive: if it still hits
    # this gate, we don't double-count — the closed_at hasn't *appeared*.
    cached = _cached_live()
    cached["closed_at"] = "2026-05-25T12:00:00Z"
    fresh = dict(cached)  # same closed_at
    status, entry, reason = _gate_market_vanished(cached, fresh)
    assert status == "ok"


def test_does_not_re_reject_when_cached_already_inactive():
    cached = _cached_live()
    cached["active"] = 0
    fresh = dict(cached)  # still inactive
    status, entry, reason = _gate_market_vanished(cached, fresh)
    assert status == "ok"
