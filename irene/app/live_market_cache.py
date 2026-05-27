"""
## Specification

**Purpose.** In-process cache for live CLOB orderbook + last-trade data.
Populated by the WebSocket pool (`app/clob_ws_pool.py`); read by
`CLOBClient.get_mid` / `get_top_of_book` so the hot path returns a sub-
second value without round-tripping REST.

**Public interface.**
- `LiveTick(token_id, mid, best_bid, best_ask, last_trade, updated_at,
  sequence)` — immutable snapshot of one token's most-recent state.
- `LiveMarketCache(stale_after_s)`
  - `get(token_id) -> LiveTick | None` — pure read; never blocks.
  - `is_fresh(tick) -> bool` — true iff `monotonic() - tick.updated_at`
    is below `stale_after_s`.
  - `apply_book_snapshot(token_id, snap)` / `apply_price_change(...)` /
    `apply_last_trade(...)` — Polymarket-CLOB-WS message parsers; each
    is a no-op when the payload is malformed.
  - `set_tick(tick)` — direct injection for tests + the probe script.
  - `size` — number of tokens with at least one observation.

**Invariants.**
- A read returning a `LiveTick` never raises. Staleness is the caller's
  to check via `is_fresh`.
- Out-of-order updates are dropped: if `incoming.sequence` is less than
  the cached sequence for that token, the cache is unchanged. Equal
  sequences are allowed (snapshot replays).
- The cache holds only what the read surface needs: mid, best bid/ask,
  last trade price, timestamp, sequence. Full book depth is dropped
  at parse time so the memory footprint stays small even at ~18k tokens.

**Side effects.** None. This module is pure.

**Failure modes.** Malformed messages logged at DEBUG and dropped;
the cache state is never partially mutated.

**Non-goals.**
- WebSocket transport — that's `app/clob_ws.py`.
- Subscription-set management — that's the pool's job.
- Authoritative price source — REST stays authoritative for the
  reconciliation path; the cache is a latency optimization.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, replace
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiveTick:
    """Immutable snapshot of one token's most-recent CLOB state.

    `mid` is the bid-ask midpoint when both sides are present; falls back
    to `last_trade` only when the orderbook hasn't been observed yet.
    `updated_at` is `time.monotonic()` at message-apply time — used by
    `is_fresh`. `sequence` is the CLOB-WS message sequence number; the
    cache uses it to drop out-of-order deltas.
    """

    token_id: str
    mid: Optional[float]
    best_bid: Optional[float]
    best_ask: Optional[float]
    last_trade: Optional[float]
    updated_at: float
    sequence: int


def _midpoint(best_bid: Optional[float], best_ask: Optional[float]) -> Optional[float]:
    if best_bid is None or best_ask is None:
        return None
    if not (0.0 <= best_bid <= 1.0 and 0.0 <= best_ask <= 1.0):
        return None
    if best_bid > best_ask:
        # Crossed book — refuse to compute a mid; caller falls back to REST.
        return None
    return (best_bid + best_ask) / 2.0


def _best_price(levels, side: str) -> Optional[float]:
    """Pick the best price from a `[{"price": "...", "size": "..."}, ...]`
    list. `side='bid'` → max; `side='ask'` → min. Returns `None` when
    the list is empty / malformed / all entries unparseable."""
    if not isinstance(levels, list) or not levels:
        return None
    candidates: list[float] = []
    for lvl in levels:
        if not isinstance(lvl, dict):
            continue
        raw = lvl.get("price")
        try:
            p = float(raw)
        except (TypeError, ValueError):
            continue
        if 0.0 <= p <= 1.0:
            candidates.append(p)
    if not candidates:
        return None
    return max(candidates) if side == "bid" else min(candidates)


class LiveMarketCache:
    """Process-singleton (lives on StageDeps once wired) holding the
    most-recent observation per token_id.

    Reads are lock-free atomic dict gets (CPython); writes use a per-
    token sequence-number gate so the read surface always sees a
    coherent `LiveTick` snapshot via the `@dataclass(frozen=True)`
    replace pattern.
    """

    def __init__(self, stale_after_s: float = 5.0) -> None:
        self._stale_after_s = float(stale_after_s)
        self._ticks: dict[str, LiveTick] = {}
        # Counter used when an incoming message lacks a sequence number
        # (e.g. last-trade prints in some CLOB shapes). Strictly forward;
        # never goes backwards even across reconnects.
        self._fallback_seq: int = 0

    @property
    def stale_after_s(self) -> float:
        return self._stale_after_s

    @property
    def size(self) -> int:
        return len(self._ticks)

    def get(self, token_id: str) -> Optional[LiveTick]:
        return self._ticks.get(token_id)

    def is_fresh(self, tick: LiveTick) -> bool:
        return (time.monotonic() - tick.updated_at) < self._stale_after_s

    # -- direct injection (tests + probe) ---------------------------------

    def set_tick(self, tick: LiveTick) -> None:
        """Skip the parser path and install a LiveTick directly. Used by
        unit tests and the probe script. Honors the sequence gate so the
        same out-of-order discipline applies."""
        prior = self._ticks.get(tick.token_id)
        if prior is not None and tick.sequence < prior.sequence:
            return
        self._ticks[tick.token_id] = tick

    # -- parsers (called by ClobWsPool on every inbound message) ---------

    def apply_book_snapshot(self, token_id: str, snap: dict) -> None:
        """Apply a full-book snapshot — emitted by Polymarket-CLOB-WS
        on subscribe and after a reconnect.

        Expected envelope (verified by probe; subject to confirmation):
        `{"asset_id": "...", "bids": [{"price","size"}, ...],
          "asks": [{"price","size"}, ...], "timestamp": ..., "hash": ...}`.
        The pool routes the message here based on its `event_type`; this
        method does not need to re-check.
        """
        if not isinstance(snap, dict) or not token_id:
            return
        best_bid = _best_price(snap.get("bids"), "bid")
        best_ask = _best_price(snap.get("asks"), "ask")
        mid = _midpoint(best_bid, best_ask)
        seq = self._extract_sequence(snap)
        prior = self._ticks.get(token_id)
        if prior is not None and seq < prior.sequence:
            return
        last_trade = prior.last_trade if prior is not None else None
        self._ticks[token_id] = LiveTick(
            token_id=token_id,
            mid=mid if mid is not None else last_trade,
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade=last_trade,
            updated_at=time.monotonic(),
            sequence=seq,
        )

    def apply_price_change(self, token_id: str, delta: dict) -> None:
        """Apply a `price_change` delta — Polymarket emits one of these
        whenever a single price level on a known token is added /
        modified / removed. We do NOT maintain full depth; instead we
        accept the delta-encoded `best_bid` / `best_ask` if present
        (some envelopes include them precomputed), and otherwise drop
        the message — the next book snapshot will resync."""
        if not isinstance(delta, dict) or not token_id:
            return
        seq = self._extract_sequence(delta)
        prior = self._ticks.get(token_id)
        if prior is not None and seq < prior.sequence:
            return
        # Some Polymarket WS shapes include precomputed top-of-book on
        # price_change messages. Read them defensively; if absent, we
        # cannot reconstruct the top-of-book from a single-level delta
        # without maintaining the book, so we skip the update.
        best_bid_raw = delta.get("best_bid")
        best_ask_raw = delta.get("best_ask")
        try:
            new_bid = float(best_bid_raw) if best_bid_raw is not None else None
            new_ask = float(best_ask_raw) if best_ask_raw is not None else None
        except (TypeError, ValueError):
            return
        if new_bid is None and new_ask is None:
            return
        base = prior if prior is not None else _empty_tick(token_id)
        bid = new_bid if new_bid is not None else base.best_bid
        ask = new_ask if new_ask is not None else base.best_ask
        mid = _midpoint(bid, ask) or base.last_trade
        self._ticks[token_id] = replace(
            base,
            mid=mid,
            best_bid=bid,
            best_ask=ask,
            updated_at=time.monotonic(),
            sequence=seq,
        )

    def apply_last_trade(self, token_id: str, trade: dict) -> None:
        """Apply a `last_trade_price` message. Only updates the
        `last_trade` slot; mid / bid / ask remain whatever the most
        recent book snapshot set them to."""
        if not isinstance(trade, dict) or not token_id:
            return
        seq = self._extract_sequence(trade)
        prior = self._ticks.get(token_id)
        if prior is not None and seq < prior.sequence:
            return
        raw = trade.get("price")
        try:
            price = float(raw)
        except (TypeError, ValueError):
            return
        if not (0.0 <= price <= 1.0):
            return
        base = prior if prior is not None else _empty_tick(token_id)
        # If we have no book yet, mid falls back to last trade so the
        # CLOBClient read still has something to return.
        new_mid = base.mid if base.mid is not None else price
        self._ticks[token_id] = replace(
            base,
            mid=new_mid,
            last_trade=price,
            updated_at=time.monotonic(),
            sequence=seq,
        )

    # -- internal --------------------------------------------------------

    def _extract_sequence(self, msg: dict) -> int:
        """Pull a forward-only sequence number out of a WS message.
        Polymarket has shipped a `timestamp` (ms) on book / price_change
        messages — that doubles as a monotonic sequence when no explicit
        `seq` field exists. Falls back to a per-cache counter so the
        gate always has SOMETHING to compare against."""
        raw = msg.get("seq") or msg.get("sequence") or msg.get("timestamp")
        if raw is None:
            self._fallback_seq += 1
            return self._fallback_seq
        try:
            return int(raw)
        except (TypeError, ValueError):
            self._fallback_seq += 1
            return self._fallback_seq


def _empty_tick(token_id: str) -> LiveTick:
    return LiveTick(
        token_id=token_id,
        mid=None,
        best_bid=None,
        best_ask=None,
        last_trade=None,
        updated_at=time.monotonic(),
        sequence=0,
    )
