# Cycle 14 — per-signal latency chain + LLM A/B probe lane + low-vol sampler v2

- **Commit:** (pending push)
- **Theme:** Observability infrastructure (no behaviour changes) + a working low-volume spread sampler. Two parallel-agent landings + a background soak.

## What landed

### Per-signal latency chain instrumentation (Agent A, `plans/latency-repricing.md`)
- `trades` table: +2 idempotent columns — `decision_latency_ms REAL`, `decision_latency_breakdown TEXT` (JSON dict of `{step_name: ms}`). NULL on legacy + reject rows; populated on execute paths.
- `TradeStage.handle()` instrumented with `time.perf_counter()` around 7 steps: `fetch_article`, `fetch_market`, `ai_direction`, `resolve_mid`, `resolve_book`, `imbalance_check`, `db_insert`. Total `decision_latency_ms` measured from handle entry to just before the trade insert.
- `_insert_trade` extended to 22 columns / 22 values. Defaults on the two new kwargs preserve callability from non-handle paths.
- +2 tests (`test_trade_row_has_decision_latency_columns`, `test_executed_trade_records_latency_ms`).
- Replans `philosophy.md` 200ms budget: pending real data accumulation.

### LLM A/B probe lane (Agent B, `plans/llm-prompt-cache.md`)
- New `llm_probe_calls` table (12 columns) registered alongside `predictions`:
  - `signal_id`, `prediction_id`, `prompt_hash` (sha256), `provider`, `model`, `ttft_ms`, `total_ms`, `direction`, `confidence`, `raw_response`, `error`, `created_at`. 3 indexes.
- New `ProbeSection` in `config.py`: `probe_enabled: bool = False` (DEFAULT OFF), `probe_sample_rate: float = 0.10`, `probe_providers: list[str] = []`, `probe_timeout_seconds: float = 10.0`.
- `AIClient.direction()` wires the probe: after the production prediction is written, if all opt-in conditions hold (`probe_enabled` + non-empty providers + `random()<sample_rate`), spawns one `asyncio.create_task` per provider via `_maybe_fire_probe()`. Fire-and-forget; never blocks or alters the production return value.
- TTFT measured via OpenRouter SSE streaming (`stream=true`) on probe path; total measured around the full request. Failures (timeout / parse / HTTP) write rows with `error=…` populated.
- `AIClient.close()` drains outstanding probe tasks.
- +4 tests (default-off; one-row-per-provider; failure-records-error; doesn't-change-production).
- Hard constraint respected: production model stays `google/gemini-2.5-flash` (per saved-memory rule about the 2026-05-21 3.1-flash-lite-preview incident).
- **Required prep before enabling in prod**: (1) confirm OpenRouter credits at each challenger; (2) extend the budget tracker to monitor probe spend separately; (3) deliberately populate `probe_providers` (default `[]` is the safety net).

### Low-volume spread sampler v2 (background, running)
- v1 (cycle 13) used `ascending=true` and was drowned in $0-volume markets. v2 walks `descending=true` past the high-volume tail and stops collecting at TOP_N within the $10k–$50k band.
- 8 min in: 28-29/30 markets sampled per cycle, 229 rows. Will auto-stop at 30 min.
- Output `/tmp/spread_soak_low_vol.csv` — feeds the next pass at `calibration_seeds.md` C1 (low-volume tier).

## Verified
- `pytest irene/app/tests/` → **475 / 475 passed** (was 469 baseline; +6: +2 latency + +4 probe).
- Low-vol v2 returning real data; 30 markets per pass inside the target volume band.

## Not yet (carried forward to cycle 15)
- **Analyze low-vol v2 soak when it completes** (~22 min from now); update `calibration_seeds.md` C1 with the thin-market spread tier; decide whether to tighten `cfg.trade.wide_spread_threshold`.
- **Reject-path latency instrumentation** — cycle 14 only annotates execute paths; rejects need their own breakdown for funnel-side latency analysis.
- **Refresh cadence for per-market `fee_rate` / `min_order_size`** (still first-seen only).
- **CLOB hosting region experiment** (eu-west-2 VPS) — requires new infra, not codable in this session.
- **Enable LLM probe lane against a sandbox** — requires deciding on a challenger list (Haiku 4.5 is the obvious first pick) + budget guardrails.
- **Per-signal latency aggregation** — a new dashboard panel reading from `trades.decision_latency_breakdown` once we have a soak of data.
