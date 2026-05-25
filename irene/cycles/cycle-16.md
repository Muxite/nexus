# Cycle 16 — default_fee_rate bump + reject-path latency + low-vol spread analysis

- **Commit:** (pending push)
- **Theme:** Apply the cycle-15 audit recommendation (fee_rate default) + close the cycle-14 instrumentation story (rejects too) + analyze the cycle-14 low-volume soak.

## What landed

### `default_fee_rate` raised 0.05 → 0.10 (foreground)
- `app/config.py`: `TradeSection.default_fee_rate` default changed from `0.05` to `0.10` per the cycle-15 audit (top-500 markets by `volume24hr` are 82.2% at 10% taker).
- Comment block above `polymarket_fee_rates` now explicitly labels the category table as **FALLBACK ONLY** — per-market fetch via `app/clob.py:get_market_info` (cycle 10) is the source of truth, refreshed weekly since cycle 15.
- `app/stages/closer.py:_resolve_fee_rate` defensive fallback also raised from 0.05 → 0.10 (defensive against cfg missing the attribute).
- No tests broke (test fixtures use explicit overrides, not the default).

### Reject-path latency instrumentation (Agent, cycle-14 follow-up)
- `decisions` table: +2 idempotent columns mirroring the cycle-14 trades columns —`decision_latency_ms REAL`, `decision_latency_breakdown TEXT`.
- `_record_reject` signature extended with two optional kwargs. Plumbed into `_insert_decision`.
- All **14 `_record_reject` call sites** in `handle()` now pass the current latency snapshot — reusing the cycle-14 `t_start` + `breakdown` already in scope (no re-timing).
- Breakdown reflects only steps that ran BEFORE the reject (e.g., similarity_floor reject = empty dict; edge_below_min reject includes fetch_article + fetch_market + ai_direction + resolve_mid + resolve_book).
- `app/dashboard/pages/latency_breakdown.py` extended with a "Reject-path latencies (last 500)" section bucketed by `reason`. Empty-breakdown rejects (gate 1) explicitly surface as "no per-step samples — reject fired before any instrumented step".
- +5 tests covering schema, partial-breakdown for gate-1 reject, larger breakdown for edge reject, execute-path regression guard, dashboard reject section.

### Low-volume spread analysis (cycle-14 soak landed)
- Cycle-14 v2 sampler completed cleanly: **790 samples / 37 markets over 28 min**.
- Effective band sampled: **$30k-$50k volume24hr** (descending walk filled TOP_N=30 before reaching below $30k).
- Results: **p50 $0.001, p90 $0.01, p99 $0.03, max $0.045**. **0/790 samples > $0.05; 0/790 > $0.10.**
- Indistinguishable from the cycle-12 high-volume sample ($100k-$5M+) at median + p90; mildly heavier tail.
- **Combined verdict** (1602 samples across $30k-$5M+ volume): the RAG "$0.05+ wide on thin" claim is invalidated for everything above $30k. `wide_spread_threshold = $0.10` is very conservative and could likely tighten to $0.05 with high confidence — defer until we have a sub-$30k sample to confirm the `thin_market + wide_spread` pair holds at the bottom of the active universe.
- `calibration_seeds.md` C1 entry updated with the dual-window finding; full report at `irene/reports/2026-05-25-cycle16-low-vol-spread-analysis.md`.

## Verified
- `pytest irene/app/tests/` → **487 / 487 passed** (was 482; +5 reject-path tests).
- Soak completed all 28 cycles cleanly; no rate-limit or CLOB issues.

## Not yet (carried forward to cycle 17+)
- **Sub-$30k sample** — opportunistic low-volume sampler walking from the bottom of the active list upward; would close the "<$30k spread distribution" gap and unlock `wide_spread_threshold` tightening.
- **Tighten `wide_spread_threshold` to $0.05** — gated on the sub-$30k sample.
- **CLOB hosting region experiment** (eu-west-2 VPS) — would close the ~50ms latency gap to pro-floor; requires infra.
- **LLM probe lane enablement** — pick a provider list (Haiku 4.5 + DeepSeek likely first), confirm OpenRouter credits, extend budget tracker for probe spend.
- **Per-signal latency aggregation against real data** — needs a soak run with live trades.
- **Three-state machine production exercise** — needs a market actually transitioning through closed → proposed → resolved with one of our positions on it.
