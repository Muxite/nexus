# Cycle 19 — session retrospective (cycles 8–18) + config.yaml sanity-check

- **Commit:** (pending push)
- **Theme:** Pause point to summarise where we are. No code changes besides a small audit verifying `config.yaml` doesn't override the cycle-16 `default_fee_rate` bump.

## Session arc (cycles 8 → 18)

Started from the cycle-7 baseline: `irene` had landed Polymarket fee math + market-resolved auto-close + market/news weight gate, but had a stack of open questions parked in `handoffs/2026-05-25.md`. The user then dropped 5 batches of RAG-AI research into `unstructured answers.md` and asked for a structured pipeline.

**Cycle-by-cycle summary:**

| Cycle | Commit  | Theme                                                                 |
|-------|---------|-----------------------------------------------------------------------|
| 8     | `583242c` | Research-distill scaffolding + 3 cycle-7 bug fixes (no-fee-at-resolution; volume24hr; wide-spread gate). |
| 9     | `98677e6` | ET-deadline parsing + similarity-floor docs + scraper audit + Axios pin. |
| 10    | `07a18e8` | Per-market `fee_rate` fetcher (CLOB `/markets/<cid>`) + explicit spread term in edge gate + 30-min spread soak. |
| 11    | `7b608ac` | D1 three-state UMA close machine + W6 `pre_news_drift_15m/60m` + impact-tracker long-tail buckets (out to 24h) + CLOB latency probe. |
| 12    | `13d3ea5` | Closed-but-not-finalized resolution re-poll path + first measured `calibration_seeds.md` (C1 spread, C4/E2 latency). |
| 13    | `2498bba` | Gamma `Retry-After` backoff helper + per-market `min_order_size` gate at arm-time + (failed) low-vol soak v1. |
| 14    | `63389d8` | Per-signal latency-chain instrumentation on `trades` + LLM A/B probe lane (default-off shadow table) + low-vol soak v2 (working). |
| 15    | `9344e2a` | Per-market `fee_rate` refresh cadence (168h) + latency dashboard panel + 500-market floor audit. |
| 16    | `eff6349` | `default_fee_rate` 0.05 → 0.10 + reject-path latency instrumentation on `decisions` + low-vol spread analysis. |
| 17    | `93613eb` | Resolved-market schema verification against 100 live closed markets + sub-$30k spread sampler launch. |
| 18    | `1047864` | `closedTime` authoritative timestamp + cycle-12 active-flicker fix + impact_tracker schema audit. |
| 19    | (this)    | Retrospective + `config.yaml` sanity check. |

**Cumulative test growth:** 395 (session start) → 492 (now). +97 tests across 11 cycles.

## Cumulative new infrastructure

- `irene/cycles/cycle-NN.md` — per-cycle snapshot convention (cycles 8–18 catalogued).
- `irene/plans/*.md` — 9 target-sharded implementation plans (cycle 8).
- `irene/questions/` — `research_questions.md` + `research_answers.md` + `unstructured answers.md` (raw drop zone) + `calibration_seeds.md` (📊 seeds annex).
- `irene/scripts/soak_*` — 4 background-soak scripts (spread high-vol, spread low-vol v1+v2, spread sub-30k, CLOB latency).
- `irene/scripts/audit_market_floors.py` — one-shot 500-market survey.
- `irene/reports/2026-05-25-cycle1[2,5,6,7]-*.md` — 4 dated data-collection reports.

## Major findings worth remembering

1. **Polymarket fee distribution is bimodal on the trading tail.** Cycle-15 500-market survey: 82% of top-volume markets are at 10% taker (1000 bps); 17% at 0%. The 5% category default was wrong as a baseline; raised to 10% in cycle 16. Per-market fetch (cycle 10) routes around the category table anyway.

2. **Spread distribution is roughly volume-invariant down to $30k.** Cycles 12 + 16 combined: 1602 samples across $30k–$5M+ volume, median spread $0.001, p99 $0.03, max $0.045. The RAG's "$0.05+ wide" claim is invalidated for everything we'd actually trade. `wide_spread_threshold = 0.10` is very conservative and could likely tighten to 0.05.

3. **CLOB round-trip from our host: p50 149.7 ms, p95 160.8 ms.** Sub-second but ~50 ms above pro floors (RAG cites sub-100 ms). Confirms the AWS eu-west-2 hosting recommendation.

4. **Polymarket's `/midpoint` returns last-trade (not mid) when spread > $0.10.** Live-verified the cycle-8 fix; the `/book?token_id=` endpoint is the source of truth. Path-param `/book/<id>` 404s.

5. **UMA's 2hr challenge window is too short to catch State 2 in post-hoc samples.** Cycle-17 probe of 100 most-recently-closed markets: all 100 already `resolved`. Production exercise of the cycle-11 state machine States 2 / 3 requires cycle-12's live `_resolution_poll_loop`.

6. **`resolvedBy` field is 49% NULL on resolved markets.** Don't depend on it for finalization signal; use `umaResolutionStatus='resolved'` + valid `outcomePrices` (matches the cycle-11 design).

7. **The "0.07 vs 0.072 crypto fee" question was the wrong question.** Per-market fees vary far more than the RAG suggested. Cycle 10's per-market CLOB fetch made the category table fallback-only.

## What's still open (cycle 20+ candidates)

- **Sub-$30k spread soak analysis** (cycle 19 background — completes ~15 min from now).
- **Tighten `wide_spread_threshold` from 0.10 → 0.05** — gated on the sub-$30k confirmation.
- **Reject-path latency dashboard rendering against real data** — needs a soak run with live trades.
- **LLM probe lane enablement** — pick a challenger list (Haiku 4.5 + DeepSeek), confirm OpenRouter credits, extend budget tracker for probe spend.
- **CLOB hosting region experiment** (eu-west-2 VPS to close the latency gap) — requires infra.
- **Three-state-machine production exercise** — needs a real market transitioning closed → proposed → resolved with one of our positions on it.

## What we're NOT doing (and why)

- **Production model swap** — saved-memory rule from the 2026-05-21 incident. The cycle-14 probe lane is the safe alternative.
- **Editing `irene/docs/philosophy.md`** — declared contradiction-winner in `master-plan.md`. Flagged amendments to the 200ms / 2s budget claims are noted in plans but never applied without human approval.
- **Touching the locked contracts** `irene/docs/dkg_spec.md`, `p1_contracts.md`, `p2_contracts.md`, `p3_contracts.md`.

## `config.yaml` sanity-check

`grep default_fee_rate irene/config.yaml` returns nothing — the cycle-16 bump in `app/config.py` is the operative production default. No stale yaml override. The only `impact` override (`max_age_hours: 24.0`) correctly matches the cycle-11 default.

## Push history this session

`origin/main` last green at `1047864` (cycle 18). All cycles 8–18 pushed.
