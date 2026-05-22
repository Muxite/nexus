# Impact-tracker extension plan for Phase 2 A2.2 (continuous price tape)

**Date:** 2026-05-22  
**Agent:** A0.11  
**Scope:** `seulgi/app/impact_tracker.py`, `seulgi/app/market_context.py`, and the new `position_price_snaps` table promised in master-plan §A1.4 / §A2.2.

---

## 1. What today's `impact_tracker.py` actually does

Anchor confusion fixed up-front: the current module is **headline-anchored**, not trade-anchored. Every row in `price_impacts` is created by `PriceImpactTracker.record(...)`, called fire-and-forget from `engine.EngineRunner` (`engine.py:919`) for each survivor decision on the *first engine only* (de-dupe), gated by `MIN_CONFIDENCE=0.50` and `MIN_SIMILARITY=0.55`. Rows exist for matches that never executed a trade. There is no `trade_id` on `price_impacts` today.

### 1.1 Capture cadence (master-plan claim audited)

Master plan §A1.4 says `delta_t5m/15m/30m` exist on `price_impacts`. Confirmed and *bigger* — schema (db.py:122) has **five** buckets and deltas:

| Column | Offset | Source |
| --- | --- | --- |
| `price_t0` / `price_pre_60s` / `price_pre_300s` | `record()` time, plus a -60s and -300s sample from `/prices-history` | one-shot at headline detect |
| `price_t1m`, `price_t5m`, `price_t15m`, `price_t30m`, `price_t60m` | +60s / +300s / +900s / +1800s / +3600s after `detected_at` | filled by `_poll_loop` (`_POLL_INTERVAL = 30s`) |
| `delta_t{bucket}` | computed in `update_price_impact_snapshot` | DB write |
| `price_timeseries` table (`impact_id`, `t_unix`, `price`, `relative_s`) | every 1-minute sample in -1h..+1h around `detected_at` | `_timeseries_backfill_loop` once row is ≥ 65 min old |

Capture is a **hybrid**: five point-snapshots filled live (polled every 30s, written when the offset elapses), plus an offline batch backfill of the full minute-fidelity history once the window has closed.

### 1.2 Lazy or continuous?

**Continuously while the row is open**: `_poll_loop` sweeps every 30s, walks `get_pending_price_impacts()`, fills any bucket whose offset has elapsed AND is still NULL. Not "lazily on trade close" — no trade involvement.

### 1.3 Critical-path + 60s-monitor checks

`PriceImpactTracker.record(...)` is wrapped in `asyncio.create_task(...)` at `engine.py:919`. **Not on the critical path of `trader.evaluate`.** Confirmed.

A0.4's 60s SL-drift bug lives in **`main._position_monitor_loop(interval_secs=60.0)`** (`main.py:89-99`) → `runner.monitor_all_positions()` → `trader.monitor_positions(...)`. **NOT in `impact_tracker.py`** (whose own poll is 30s and only fills bucket columns; never closes a position). The two loops are unrelated; the impact tracker is innocent of the SL-drift bug.

---

## 2. Phase 2 A2.2 extension proposal

Goal per master-plan §A2.2: write `position_price_snaps` at **+5m / +15m / +30m / +1h / +4h** for every executed trade, and have `_prefetch_prices` (engine.py) capture `bid`/`ask` instead of only mid.

### 2.1 Where the per-trade monitor loop will live

No per-position loop in `impact_tracker.py` today. **Recommendation: add a sibling `_position_snap_loop`** alongside `_poll_loop` / `_dashboard_loop` / `_timeseries_backfill_loop`, started from `start()`. Reasons: (1) different anchor semantics (detect-time vs. open-time) and different retention (1h vs. 4h+); (2) trade rows must survive trade close — the snap loop cannot require the position to be open at +4h; (3) the existing `_poll_loop` is at the comfort edge of one coroutine — widening it risks cross-anchor corruption.

Loop sketch (NOT implemented in this phase):
```python
async def _position_snap_loop(self):
    while self._running:
        await asyncio.sleep(_SNAP_POLL_INTERVAL)  # 30-60s
        rows = await self._db.get_position_snaps_pending(horizon_s=4*3600+300)
        # rows include trade_id, condition_id, token_id_entered, opened_at,
        # and a bitmask / per-column NULL of which checkpoints are still empty.
        await self._fill_due_snaps(rows)
```

Where to add `+1h` / `+4h`: **do not** extend the module-level `_BUCKETS` (shared with headline rows — would add bogus +4h cols to `price_impacts`). Define a sibling `_POSITION_BUCKETS = [("t5m",300),("t15m",900),("t30m",1800),("t1h",3600),("t4h",14400)]` keyed off the new schema.

### 2.2 Where to plumb `bid` / `ask` / `book_depth_usdc`

Two write sites — keep them aligned:

1. **`_prefetch_prices` (engine.py:704)** — today calls `_fetch_mid_price`. Introduce `_fetch_bbo(session, token)` returning `(mid, bid, ask, book_depth_usdc)` via CLOB `/book`, and widen `insert_market_price_snaps` (Phase 2 migration on `market_price_snaps`).
2. **The new `_position_snap_loop`** — every snap also calls `_fetch_bbo` and writes `bid`/`ask`/`book_depth_usdc` into `position_price_snaps`. Book depth is captured but NOT used until Phase 4A.3.

### 2.3 Backpressure: 50 positions × 5 checkpoints × 30s polling

50 positions × 5 checkpoints × (3600/30) sweeps/hour = 30k row-evaluations/hour, but HTTP cost is bounded by *new* due checkpoints: each position contributes ≤ 5 calls over its lifetime, not per sweep. The real risk is co-fetching the same `token_id` for both an open position and an active headline impact row.

**Recommendations:**
- **Coalesce per token_id within a sweep.** Today `_poll_loop` calls `_fetch_price` once per pending row; two rows sharing a token fire two requests. Add a sweep-local `dict[token_id, asyncio.Task[float]]` (same pattern `market_context.py` uses with its 30s TTL).
- **Bound concurrency.** Replace serial `for row in pending_rows` with `asyncio.gather(...)` wrapped in `asyncio.Semaphore(10)`.
- **Skip not-yet-due rows.** Today the loop already skips filled cols but still iterates rows whose nearest due checkpoint is > 60s away. Small wasted-CPU win.

### 2.4 Crash recovery

Existing behavior (good): `start()` reloads `get_pending_price_impacts()` on boot and the loop catches up. **`position_price_snaps` must mirror this.** Pending = `trade.opened_at + 4h > now() AND any t{bucket} IS NULL`.

**Recommendation:** insert the empty `position_price_snaps` row inside the same transaction as `trader.open_position`. The snap loop then queries by NULL columns, not by in-memory membership — trades opened during an outage are never orphaned.

### 2.5 Performance smells

1. **DB read every 30s** for `get_pending_price_impacts()` regardless of pending-set stability. With the new sibling loop firing the same shape, writer-lock contention against `monitor_positions` (A0.4 §10.3) deserves a benchmark. Read every Nth sweep or invalidate on insert.
2. **`for row: for bucket:`** is serial. Use `gather` per row, semaphore-capped.
3. `_dashboard_loop` re-runs `get_price_impact_stats(limit=100)` + a second pending fetch every 60s. Cache the stats; only refresh on `_pending` change, or stretch interval to 300s.

---

## 3. DB migration shopping list (Phase 2)

1. **NEW table** `position_price_snaps(id, trade_id FK, condition_id, token_id, side, t_unix, offset_label TEXT, mid, bid, ask, book_depth_usdc, UNIQUE(trade_id, offset_label))`.
2. **ALTER** `market_price_snaps ADD COLUMN bid REAL`, `ADD COLUMN ask REAL`, `ADD COLUMN book_depth_usdc REAL`. (Today columns are mid + side only — confirmed db.py:427.)
3. `price_impacts` does **NOT** have `bid`/`ask`. Adding them retro is optional and out of scope for A2.2 — the new continuous-tape table covers the need; impact_tracker headline rows stay mid-only for backward compatibility with `delta_t{bucket}`.

---

## 4. Phase 2 readiness summary (3 lines)

- **Already there:** `price_impacts` with 5 bucket offsets (+1m/+5m/+15m/+30m/+60m), 30s poll loop with DB-backed crash recovery, full -1h..+1h minute-fidelity backfill, and `market_price_snaps` (mid-only) for skipped markets.
- **Phase 2 must add:** new `position_price_snaps` table (trade-anchored, includes +1h and +4h offsets), a sibling `_position_snap_loop` in `impact_tracker.py`, BBO (bid/ask/book_depth) capture in `_prefetch_prices` and the new snap loop, and per-token sweep-level fetch coalescing + concurrency cap to avoid CLOB-call blow-up.
- **Non-blocker noted:** the 60s SL-drift bug from A0.4 is in `main._position_monitor_loop` / `trader.monitor_positions`, not in `impact_tracker.py`; Phase 2 changes will not regress (or fix) that path.
