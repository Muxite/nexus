# Cycle 12 — orchestration fix for the close machine + first measured calibration seeds

- **Commit:** (pending push)
- **Theme:** Close the cycle-11 orchestration gap (state machine never fires in production) and turn the cycle-10/11 background soaks into measured `📊` seeds replacing RAG priors.

## What landed

### Closed-but-not-finalized poll path (Agent A, closes cycle-11 orchestration gap)
- `app/stages/markets.py` — new `_fetch_single_market(condition_id)` hits `GET /markets?condition_ids=<cid>` (verified live; same base URL as `_fetch_all_gamma`, just a different query). Critically, this endpoint does NOT require the market to be in the active list — exactly what the state machine needs.
- New background task `_resolution_poll_loop` runs every `cfg.markets.resolution_poll_seconds` (default 600 = 10 min). Per pass:
  - Walks open trades joined to `markets` where `closed_at IS NOT NULL AND uma_finalized_at IS NULL AND closed_at >= now - resolution_timeout_hours`.
  - For each unique market: refetch via single-market call, upsert (re-uses the cycle-11 transition-stamping logic), then `_close_orphaned_trades_for([cid])` to let the existing state machine evaluate.
  - Per-market errors caught and logged; one bad market doesn't abort the pass.
- New `cfg.markets.resolution_poll_seconds: int = 600` config knob.
- `MarketsStage.start()` / `stop()` now manage 3 background tasks (regular poll, resolution poll, and any pre-existing).
- 5 new tests in `app/tests/stages/test_markets.py` (proposed→stamped no-close, resolved→close, past-timeout-skipped, no-open-trade-skipped, Gamma-failure-graceful).

### Data analysis from cycle-10 / cycle-11 soaks
- **Spread soak** (cycle 10, 30 min, top-30 markets by volume24hr, 812 samples / 29 conditionIds):
  - All-sample p50 = **$0.0010**, p99 = **$0.0200**, max = **$0.0200**.
  - **0/812 samples would have tripped the `wide_spread` gate** (threshold = $0.10). High-volume markets are 5–50× tighter than the RAG prior.
  - Sample bias: top-30 by volume only; thin-market RAG claim ("10¢+ on niche") is not in this sample's universe.
- **Latency probe** (cycle 11, 5 min, 300 sequential `/midpoint` calls from this host):
  - p50 = **149.74 ms**, p95 = 160.84 ms, p99 = 193.43 ms, max = 308.88 ms, 0 failures.
  - ~50 ms over the RAG-claimed sub-100 ms pro floor → confirms the AWS eu-west-2 hosting recommendation from `legal-ops-hosting.md`.
  - 13× margin under the `philosophy.md` 2-second budget on a single CLOB call. Multi-call chains will eat that margin — followup needed.
- **`irene/questions/calibration_seeds.md` (NEW)** — annex tracking 7 📊 seeds. C1 (spread tier) and C4/E2 (latency) are now **measured**; E1, F1, F2, F4, D3 still seeds awaiting more data.
- **`irene/reports/2026-05-25-cycle12-data-collection.md` (NEW)** — full numbers + interpretation + actions per dataset.

## Verified
- `pytest irene/app/tests/` → **455 / 455 passed** (was 450 baseline; +5 from new markets tests).
- Both soaks completed cleanly (no failures in 300 latency samples; 29/30 markets sampled per spread cycle for 30 cycles).
- Live verified: `GET /markets?condition_ids=<cid>` returns a single-item list (or `[]` on miss).

## Not yet (carried forward to cycle 13)
- **Markets-gamma Retry-After backoff** on Cloudflare 429s.
- **LLM A/B probe lane** (Haiku vs Gemini vs DeepSeek). Forward-looking.
- **Per-market `fee_rate` refresh cadence** (currently first-seen only).
- **Per-market `min_order_size` read at arm-time**.
- **Low-volume spread sampler** ($10k–$50k volume band) to validate the `thin_market + wide_spread` pair vs the RAG thin-market prior.
- **Per-signal latency chain measurement** (mid + book + fee_rate at arm-time) — replans the philosophy.md 200ms budget.
- **`cfg.trade.wide_spread_threshold` tightening** — defer until 2nd spread soak corroborates the cycle-12 finding.
