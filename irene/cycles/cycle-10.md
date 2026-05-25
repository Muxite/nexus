# Cycle 10 — per-market fee_rate + explicit spread term + spread-sampling soak

- **Commit:** (pending push)
- **Theme:** Wire per-market `fee_rate` from CLOB; decompose the lumped `fee_buffer` in the edge gate into explicit executable-side + half-spread + taker-fee terms; start a 30-min background soak collecting real spread/volume data.

## What landed

### Per-market fee_rate fetcher (Agent A, `plans/markets-gamma.md` + A2 cross-cut)
- **Live probe:** `GET https://clob.polymarket.com/markets/<conditionId>` returns 200 with `maker_base_fee` + `taker_base_fee` in **basis points**. 1000-market survey: `taker_base_fee ∈ {0: 999, 200: 1}` — the dominant case is 0 fee; the BTC $150k market is an outlier at **1000 bps = 10%** taker.
- `app/clob.py`: new `get_market_fee_rate(condition_id) -> Optional[float]` returns fractional (bps / 10000).
- `app/stages/markets.py`: new `fee_rate REAL` column on `markets` (idempotent migration); `_maybe_fetch_fee_rate` runs after each upsert, **first-seen only** (refresh cadence deferred).
- `app/stages/closer.py`: `_resolve_fee_rate` now takes `market_fee_rate=` kwarg with priority `per-market → category → default`. Category table in `config.py` stays as fallback.
- `markets.py:_close_orphaned_trades_for` also threads `fee_rate` so auto-close uses the same priority.

**0.07 vs 0.072 question resolved with a third answer: both were wrong.** Per-market fees are far more variable than either category number suggested. Hardcoded 0.072 in `config.polymarket_fee_rates` is now a fallback for unfetched markets only.

### Explicit spread term in edge gate (Agent B, `plans/order-execution-spreads.md` §2)
- `app/stages/trade.py`: new `_resolve_executable_side(direction, market) -> (executable_price, spread) | None`. For YES the executable side is `best_ask`; for NO it's `1 - best_ask_no` (algebraically equals `best_bid_yes` — verified).
- Edge formula switched to `edge = executable_price - p_hat - taker_fee_buffer - half_spread` when book is available; falls back to legacy `p_hat - mid - fee_buffer` when book is unavailable.
- `cfg.trade.taker_fee_buffer = 0.01` (new). `cfg.trade.fee_buffer` kept (still feeds `target_mid` at trade.py:925 — separate concept).
- `edge_components` (mid, executable_price, spread, half_spread, taker_fee_buffer, fallback flag) plumbed into `features_snapshot` JSON so we can audit fallback-rate over time.

### Spread-sampling soak (running)
- `irene/scripts/soak_spread_sampler.py` — top-30 active markets by `volume24hr`, `/book` every 60s, 30-min total.
- 10 min in: 9 cycles complete, 29/30 markets sampled per cycle, 262 rows collected. CSV at `/tmp/spread_soak.csv`.
- Live distribution preview (so far): high-volume politics/geopolitics markets at **0.01 spread**; thin World Cup longshots at **0.001 spread** (price floor effect); mid-range markets at **0.013 spread**. Already enough to invalidate the RAG prior "$0.05+ wide" claim — the high-volume tail is much tighter.
- Doubles as a live regression test for `CLOBClient.get_top_of_book`.

## Verified
- `pytest irene/app/tests/` → **433 / 433 passed** (was 419 baseline, +14).
- 8 new clob tests for `get_market_fee_rate` (happy path + zero-fee + every failure mode).
- 3 new trade tests for executable-side edge math (book path, fallback path, executable-side-eats-budget reject).
- Live `/markets/<conditionId>` and `/book?token_id=` probes both returning expected shapes.

## Not yet (carried forward)
- Refresh cadence for per-market `fee_rate` (cycle 10 only does first-seen).
- Three-state close machine for UMA resolution (`plans/resolution-oracle.md` D1).
- `pre_news_drift` field on `price_impacts` (W6 weakspot — distinguishes "wrong wording" vs "already priced in").
- Impact-tracker bucket extension to +2h/+4h/+8h/+24h/+48h (`plans/latency-repricing.md`).
- LLM A/B probe lane (Haiku vs Gemini vs DeepSeek).
- Per-market `min_order_size` read at arm-time.
- Markets-gamma: Retry-After backoff on Cloudflare 429s.
- Replace C1 `📊` priors in a future `calibration_seeds.md` once the soak finishes.
