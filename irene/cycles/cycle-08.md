# Cycle 8 — research-distill artifacts + 3 cycle-7 bug fixes

- **Commit:** `583242c` (2026-05-25)
- **Theme:** Stand up the RAG-research → distilled-answers → region-plans pipeline; fix three bugs surfaced by the first 5 RAG batches.
- **Plans driving this cycle:** all 9 `irene/plans/*.md` (drafted this cycle by 9 parallel agents).

## What landed

### Research-distill scaffolding (`irene/questions/`, `irene/plans/`)
- `questions/research_questions.md` — 9-section bank (A–I + W) with `**unblocks:**` footers.
- `questions/research_answers.md` — distilled output of 5 RAG batches; sections A1–A4, B1–B5, C1–C4, D1–D3, W1–W7 with the `Primary info / Maps to / Plan stub` shape + trust tags ✅/⚠️/❌/📊.
- `questions/unstructured answers.md` — raw drop zone for incoming RAG dumps.
- `plans/closer-fees.md`, `plans/markets-gamma.md`, `plans/latency-repricing.md`, `plans/sizing-edge-similarity.md`, `plans/scraper-news-sources.md`, `plans/llm-prompt-cache.md`, `plans/legal-ops-hosting.md`, `plans/resolution-oracle.md`, `plans/order-execution-spreads.md` — region-targeted implementation plans (drafted by parallel agents in target-sharded fan-out).

### Bug 1 — `closer.py` over-charged fees on `market_resolved`
- `write_trade_closure` now zeros `exit_fee_usdc` when `close_reason='market_resolved'` (Polymarket: no fee at resolution; winning shares redeem at $1/$0).
- Entry-leg taker fee still stands.
- +2 unit tests; updated `test_disappearing_market_auto_closes_open_trades` to the corrected expected value.

### Bug 2 — `markets.py` read `volume` (lifetime) instead of `volume24hr`
- `_parse_gamma_row` now reads `volume24hr` (rolling 24h USDC notional).
- The `thin_market` gate was silently letting low-24h-volume markets through; now correctly filters.
- +2 regression tests guarding against fallback-to-lifetime.

### Bug 3 — wide-spread mid bug
- Live-verified: `GET /book?token_id=` works (200, full depth); `/book/<id>` path-param form 404s.
- New `CLOBClient.get_top_of_book(token_id) -> (bid, ask) | None`.
- New `cfg.trade.wide_spread_threshold = 0.10`.
- `trade.py` inserts a `wide_spread` reject between `mid_degenerate` and `market_news_imbalance`.
- Graceful degrade: book read failure → gate skipped; existing guards stand.
- +3 trade tests, +8 clob tests.

## Verified
- `pytest app/tests/` → **406 / 406 passed** (was 395 baseline).
- Live `/book` probe against high-volume conditionId returned 200 + populated bid/ask ladders.

## Not yet (carried forward)
- Per-market `fee_rate` fetcher via `getClobMarketInfo(conditionID)` (resolves the 0.07 vs 0.072 question).
- Closer-side wide-spread correction (exit at executable side, not stale mid).
- Three-state close machine for UMA resolution flow (`plans/resolution-oracle.md`).
- Explicit spread term in `trade.py` edge math (replaces `fee_buffer` lumping).
- Per-market `min_order_size` at arm-time.
