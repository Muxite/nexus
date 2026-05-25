# Cycle 13 — Gamma 429 backoff + per-market min_order_size gate + failed low-vol soak

- **Commit:** (pending push)
- **Theme:** Harden Gamma against Cloudflare 429s; add the per-market trade-size floor to the entry gate. Low-volume spread sampler experiment failed; carries to cycle 14 with a fixed approach.

## What landed

### Gamma Retry-After backoff (foreground, this engineer)
- `app/stages/markets.py`: new `_gamma_get_with_backoff(params, max_retries=3)`. On 429:
  - Honors `Retry-After` header (seconds form — what Polymarket sends).
  - Falls back to exponential backoff (`1.0 → 2.0 → 4.0 …` capped at 60s) with jitter (+0-0.5s) when header is missing.
  - After `max_retries` consecutive 429s, returns the 429 unchanged so the caller's `raise_for_status()` fires — never loops forever.
  - Non-429 status codes (including 4xx other and 5xx) pass through unchanged.
- Both `_fetch_all_gamma` (paginated discovery) and `_fetch_single_market` (cycle 12 single-market lookup) now route through the helper. The cycle-11 resolution poll path inherits the backoff.
- New constants: `_GAMMA_429_MAX_RETRIES = 3`, `_GAMMA_429_INITIAL_BACKOFF_S = 1.0`, `_GAMMA_429_MAX_BACKOFF_S = 60.0`.
- +5 tests in `app/tests/stages/test_markets.py` covering: 200 passthrough; one 429-then-200; missing-header fallback; max-retries exhausted; non-numeric HTTP-date header.

### Per-market `min_order_size` gate (Agent, `plans/order-execution-spreads.md` #4)
- `app/clob.py`: existing `get_market_fee_rate` is now a thin shim over a widened `get_market_info(condition_id) -> dict` that returns BOTH `fee_rate` and `min_order_size` from a single `GET /markets/<cid>` (the cycle-10 endpoint already returns both — extending preserves backwards-compat for every test).
- `markets` table: +1 idempotent column `min_order_size REAL`. NULL = not-fetched-or-failed.
- `markets.py:_maybe_fetch_market_info` populates both fields at first-seen (same one-shot per market; same `fee_rate IS NULL` marker for "have we tried").
- `trade.py` size gate:
  - **Unit conversion (verified):** platform `minimum_order_size` is in **shares**, not USDC. Per-share cost = `executable_price` (YES) or `1 - executable_price` (NO). USDC floor = `min_shares × per_share_cost`.
  - New rule: `effective_floor = max(cfg.trade.min_position_usdc, market_floor_usdc or 0)`. Rejection reason is `below_market_min` when the per-market floor binds; legacy `size_below_min` otherwise (preserves funnel monitoring).
  - Falls back to mid (side-aware) when book is unavailable — same pattern as the cycle-10 explicit-spread-term refactor.
- 9 new tests across clob / markets / trade.

### Low-volume spread sampler — FAILED experiment
- Wrote `irene/scripts/soak_spread_low_volume.py` to validate the RAG "10¢+ on thin" claim by sampling top-30 of the $10k–$50k volume band.
- Failed: Gamma's `order=volume24hr&ascending=true` returns the LOWEST-volume active markets first, mostly $0 volume (active but never-traded). 500 markets walked in 15 min returned zero matches inside the band.
- Process killed at cycle 16; CSV has only the header row.
- **Lesson:** for low-volume sampling we need either a (a) bigger window with `descending=true` + post-filter on `volume <= 50000` (probably faster), or (b) hit `/markets?max_volume24hr=50000` if that param exists. Either way the script needs a rewrite. Defer to cycle 14.

## Verified
- `pytest irene/app/tests/` → **469 / 469 passed** (was 455 baseline; +14: +5 backoff + +9 min_order_size).
- Backoff helper tested across 5 scenarios including hostile inputs (non-numeric Retry-After).

## Not yet (carried forward to cycle 14)
- **Rewrite low-volume spread sampler** with `descending=true + post-filter`.
- **Per-signal latency chain measurement** (mid + book + fee_rate + min_order_size at arm-time). Replans the philosophy.md budget.
- **LLM A/B probe lane** (Haiku vs Gemini vs DeepSeek). Forward-looking infrastructure.
- **Per-market `fee_rate` / `min_order_size` refresh cadence** (still first-seen only).
- **`cfg.trade.wide_spread_threshold` tightening** — defer until 2nd window of high-volume spread data + low-volume data both land.
- **CLOB hosting region experiment** (eu-west-2 VPS) to validate the cycle-12 latency-budget claim.
