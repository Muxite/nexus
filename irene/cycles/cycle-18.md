# Cycle 18 — closedTime authoritative timestamp + cycle-12 active-flicker fix + impact_tracker schema audit

- **Commit:** (pending push)
- **Theme:** Small-scope cleanups driven by cycle-17 findings. Fix a latent cycle-12 bug along the way.

## What landed

### Prefer Gamma's `closedTime` over our first-observation `_now_iso()` (foreground)
- **Parser** (`_parse_gamma_row` in `markets.py`): now surfaces `closed: bool` and `closed_time: Optional[str]` from the Gamma payload (matches the cycle-17 verification: `closedTime` is present on essentially all recently-closed markets).
- **Upsert** (`_upsert_market`):
  - Computes `active_value = 0 if closed_flag else 1` (was unconditional `1`).
  - Computes `closed_at_value = closed_time (if present) else now (if closed) else NULL`.
  - **UPDATE SET `closed_at = COALESCE(markets.closed_at, excluded.closed_at)`** — preserves an earlier authoritative timestamp if Gamma transiently drops `closedTime` between polls. Mirrors the cycle-11 `last_outcome_prices` COALESCE pattern.
- **Latent bug fix**: cycle-12 noted that `_close_orphaned_trades_for` only fires when Gamma drops a market — but the single-market re-poll path (also cycle 12) was previously re-stamping `active=1, closed_at=NULL` on closed-market refetches, causing 1-cycle of active-flickering. Cycle 18 closes that — `_upsert_market` now correctly preserves `active=0` and `closed_at` when the inbound row has `closed=true`.
- +5 tests in `test_markets.py`:
  - parser surfaces `closed` + `closed_time`
  - default closed=False when missing
  - upsert with closed=true + closedTime sets active=0 + closed_at=platform ts
  - upsert with closed=true no closedTime falls back to now()
  - upsert preserves earlier closedTime via COALESCE on re-observation

### impact_tracker schema audit (foreground)
- Verified the cycles 11 + 14 cumulative schema additions against `BUCKETS_SECONDS`:
  - Original CREATE TABLE has 5 buckets (60s/300s/900s/1800s/3600s) × 3 column types (price_t / delta_t / edge_realised_t) = 15 base bucket columns.
  - Cycle 11 ALTER adds 2 columns (pre_news_drift_15m / 60m).
  - Cycle 14 ALTER adds 4 long-tail buckets × 3 types = 12 columns.
  - Total schema: 27 bucket columns + 2 pre_news columns = 29 schema-extension columns.
- `BUCKETS_SECONDS = (60, 300, 900, 1800, 3600, 7200, 14400, 28800, 86400)` exactly matches the existing columns. No drift, no orphans.
- `_fill_buckets` and `_fetch_unfilled_rows` both iterate `BUCKETS_SECONDS` (or query-include all bucket columns) — schema-driven, future-proof against further additions.

### Sub-$30k spread sampler — still running (cycle 17 background)
- Mid-run; will report findings in cycle 19.

## Verified
- `pytest irene/app/tests/` → **492 / 492 passed** (was 487 baseline; +5 from cycle 18).
- All existing markets tests still green — the active-flicker fix is backwards-compatible (the cycle-7 `_deactivate_missing` path still uses `now()` because it operates on markets that have dropped from the response and thus have no `closedTime` to read).

## Not yet (carried forward)
- **Sub-$30k spread soak analysis** — cycle 19.
- Other items as listed in cycle 17.
