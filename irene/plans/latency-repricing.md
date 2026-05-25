# Plan — latency budget & repricing

> Source: `irene/questions/unstructured answers.md` lines 54-62, batch
> `2026-05-25-unstructured-1`. Cross-refs: `research_questions.md`
> O-E1 / O-E2, `research_answers.md` §W1, §W4, §B5, §C4.

## Context

The single most important PROTOCOL finding in this slice is that
**Polymarket removed the 500ms artificial taker-delay penalty** (line 62).
With it gone, competitive bots are operating with a true latency floor
under 100ms — sub-second WebSocket on co-located AWS infra. The slice
also asserts CLOB host region `eu-west-2 London` (line 60), a matching-
engine cost of 70–85ms (line 62), and a "dual-phase" repricing model
(line 57): an algorithmic millisecond shock followed by a multi-hour
retail tail.

`docs/philosophy.md` (§"What this prioritises in operations", lines
73–89) currently anchors the latency thesis at a 200ms / 2s / 30s
ladder: 200ms is "competing well", 2s is "a real shot", 30s is "no edge."
The RAG critique says **2s is dangerously slow for the breaking-news
phase** of a real-money path. That is the headline tension this plan has
to flag.

The statistical claims — 80% reversion in hours, 24–48h spread recovery,
top-1%-capture-84% — are seeds only. The bot already owns the right
table (`price_impacts`, `app/stages/impact_tracker.py:94–122`) to
produce our own half-life numbers from real trades; we should not
hard-code RAG numbers as config defaults.

## Findings

### Protocol facts (act on these)

- **500ms taker-penalty removed** (line 62). True operating floor for
  news bots is now <100ms. This is the structural change that breaks
  philosophy.md's "2s is a real shot" framing for the real-money path.
- **CLOB hosted in AWS `eu-west-2` (London)** (line 60). Informs the
  hosting-region decision: any host outside eu-west-2 already loses
  10–80ms before doing anything useful.
- **Matching engine adds 70–85ms** (line 62). This is the irreducible
  per-order cost; nothing we do client-side recovers it.
- **High-volume traders use persistent WebSocket + co-location** (line
  60), not multi-second HTTP polling. Confirms the priority of moving
  hot-path price reads off polled HTTP onto a CLOB WebSocket
  subscription. (Already flagged in `markets-gamma.md` for the same
  reason; cross-cuts with `research_answers.md` §B5.)
- **Batching multiple orders in one payload + persistent TCP eliminates
  TLS handshake overhead** (line 62). Concrete engineering levers for
  the real-money execution wrapper (P3+).

### Seeds only (measure ourselves, do NOT bake into config)

- **80% price reversion in hours, 24–48h spread recovery** (line 55).
  These are useful as priors for plot-axis scaling, not as defaults.
  Our `price_impacts` buckets currently top out at 60 minutes
  (`impact_tracker.py:107–117`); validating these RAG numbers requires
  extending the bucket ladder (see "Impact-tracker extension" below).
- **Top 1% of traders capture ~84% of gains** (line 57). Anecdotal and
  unactionable as config. Useful only as background — it argues that
  the "informed liquidity" tier is real, which justifies a closing-
  speed obsession, but no parameter follows.
- **Dual-phase: ms shock + hours-long retail tail** (line 57). A
  hypothesis to validate against our own `price_impacts`, not a
  defaulted-on policy. If real, it has a concrete config implication
  (per-window edge floor — see "trade.py possible dual-window" below)
  but we measure first.

### Contradictions to flag (do NOT auto-edit philosophy)

- `docs/philosophy.md:75–77` cites 200ms/2s/30s as the latency ladder.
  RAG calls 2s "dangerously slow." **philosophy.md is the contradiction
  winner per its own §"Status" preamble (lines 6–8); this plan only
  flags the amendment, it does not write it.**
- The amendment, *if* the human accepts it, would update the budget
  table to reflect that the competitive floor is now <100ms post-
  penalty-removal, and clarify that 2s remains acceptable for the
  paper-mode "interpret and run" thesis but is a non-starter for the
  real-money cycle-10+ path.

## Proposed changes

### philosophy.md amendment (HUMAN REVIEW — do not auto-edit)

- Update the 200ms / 2s / 30s ladder (lines 75–77) to acknowledge the
  500ms-penalty removal and the resulting <100ms floor for
  competitive bots. Recommended wording (for the human to apply):
  > "200ms competes with the top tier post-penalty-removal; 2s is the
  > paper-mode target and the realistic ceiling for an RSS-driven
  > pipeline; >2s loses the breaking-news shock window entirely but
  > can still capture the retail tail."
- Add a one-line note that the "200ms is competing well" anchor
  predates Polymarket's removal of the 500ms taker penalty and may need
  re-baselining once we measure end-to-end on co-located infrastructure.

### `closer.py` — timeout + (no) BREAKING-bypass

- **Finding:** `closer.py` does not currently have a BREAKING-aware
  timeout. The close-rule cascade (`closer.py:387–437`) is
  `edge_decay → take_profit → stop_loss → stagnation → timeout`, and
  `cfg.closer.timeout_hours` defaults to 1.5h (`config.py:190`). The
  BREAKING-bypass logic referenced in the original brief lives on
  the *entry* side, not the exit side (`trade.py:679, 687–699`: BREAKING
  freshness relaxes `require_breaking_for_extreme_mid` and
  `require_breaking_for_far_future`).
- **RAG-driven proposal (hypothesis-grade):** the "dual-phase" model
  argues an open position from a BREAKING signal might benefit from a
  *longer* timeout to capture the retail tail, not a shorter one. The
  current 1.5h timeout sits inside the alleged "80% reversion in hours"
  window and may be cutting positions before the retail wave finishes
  pricing in. **Do NOT change `timeout_hours` from this batch alone**;
  resolve via i-D2 (our own repricing-half-life measurement) first.
- **Concrete deferred work:** once the half-life data is in,
  consider freshness-conditional close rules — e.g. a
  `cfg.closer.timeout_hours_breaking` distinct from the default. Track
  as a stub here; do not ship.

### `trade.py` — possible dual-window edge floor (hypothesis only)

- **Hypothesis (from line 57):** during the first N seconds after a
  BREAKING article, require a tight edge floor (we're racing
  algorithms); after that window, widen the floor (we're harvesting
  retail tail and don't need to be as cheap).
- **Why we don't ship a default yet:** N is not knowable from the RAG
  numbers. The slice says "milliseconds to seconds" for the shock and
  "several hours" for the tail; the boundary is a config knob we have
  no data to set. Picking N=60s or N=300s from this RAG slice would be
  exactly the kind of statistical-claim-as-config-default the trust-tier
  rule forbids.
- **What to do instead:** once `price_impacts` has the extended-bucket
  data (see below), fit per-trade `time-to-first-50%-move` distributions
  and *then* propose a boundary. Mark this as 📊-seed-driven and gate
  it behind the half-life measurement (i-D2 in `research_questions.md`).

### `impact_tracker.py` — extension to measure half-life ourselves

- **Add buckets** beyond the current +60s/+5m/+15m/+30m/+60m ladder
  (`impact_tracker.py:107–117`). The RAG claim of "80% reversion in
  hours" can only be measured if we have +2h, +4h, +8h, +24h, +48h
  buckets. Cost: more rows held open by the poller until
  `cfg.impact.max_age_hours` (`config.py:214`, currently 2.0h) elapses.
  - Raise `max_age_hours` to **48** so the longest bucket can fill.
  - Add the new bucket offsets to the seconds list inside the poller.
  - Add `delta_t7200s`, `delta_t14400s`, `delta_t28800s`, `delta_t86400s`,
    `delta_t172800s` and the matching `edge_realised_*` columns to the
    DDL (with `ALTER TABLE … ADD COLUMN` migrations).
- **Add an analysis query** in `scripts/eval/` (new file) that, for
  every closed trade, finds the smallest bucket where
  `abs(edge_realised) >= 0.5 * (target_mid − entry_mid)`. The
  distribution of that smallest-bucket value across the cycle is our
  empirical half-life — the number that should replace the RAG seeds in
  any future edge-floor / closer-timeout discussion.
- **Cross-cut with W6** (`research_answers.md` §W6): the same
  `price_impacts` extension also wants a `pre_news_drift_15m` column so
  "priced-in" can be distinguished from "no readers." Do both
  migrations in one shot.

### Hosting region recommendation (cross-cut with `legal-ops-hosting.md`)

- Move the bot's CLOB read path (and, later, the WebSocket subscription)
  to **AWS `eu-west-2`** to match Polymarket's matching engine region
  (line 60). This is dirt-cheap latency improvement — sub-10ms RTT to
  the order book vs the current arbitrary egress point — and should
  precede any other latency work because every other measurement is
  contaminated by current-region round-trip.

## Open questions

- **Region migration: when?** Hosting region change is a one-time
  switching cost. Should it happen *before* we benchmark, so all
  benchmarks are at the floor we'll actually operate at? Or *after*,
  so we have a delta to prove the move paid off? Recommendation: do
  both — baseline now from current region, move, baseline again.
- **Verify 500ms-removal recency.** The RAG slice asserts removal but
  cites no date. Before predicating any plan on it, check
  `docs.polymarket.com` changelog or a recent Polymarket eng blog post
  to confirm the penalty really is gone and not just deprecated. If
  the penalty is still in effect for some account tier, our latency
  budget arithmetic changes.
- **Is WebSocket subscription actually faster end-to-end for our
  cadence?** Polling at `cfg.markets.poll_seconds = 60` for market
  metadata is fine; the question is whether the hot-path `get_mid`
  call inside the closer (`closer.py:368`, fires every
  `cfg.closer.poll_seconds = 15` per open trade) is dominated by HTTP
  TLS setup. If yes, a WebSocket subscription with persistent state
  pays off. If no (because `httpx` is already keep-alive'ing), the
  win is smaller than the slice implies. Measure before refactoring.
- **Should the BREAKING-bypass live on the close side at all?** The
  current architecture has BREAKING gating only entry. The RAG
  framing ("widen edge floor for retail tail") implies a *separate*
  policy for the post-entry window, which is a new concept in the
  codebase. Decide whether it belongs in `closer.py` (close-rule list)
  or in a new "edge-floor schedule" inside `trade.py`. Either way,
  data-driven, not RAG-default-driven.

## Verification

- **Before any change:** dump current per-stage timings from
  `throughput.txt` and the per-stage `Timer` data. Compute p50/p95/p99
  for the full article → trade chain (`ingest.poll` → `dedupe.handle`
  → `match.handle` → `ai.direction` → `trade.handle` → `execute`).
  This is i-D1; it doubles as the latency-budget baseline.
- **Region move (if accepted):** repeat the same measurement from the
  new region. Expected: 10–80ms savings on CLOB-bound calls; near-zero
  change on LLM-bound calls (those route through OpenRouter, see W4).
- **Impact-tracker extension (if accepted):** after the migration is
  applied, hold a week of trades to populate the new long buckets.
  Then compute the empirical half-life distribution and compare to the
  RAG seed of "hours." If the seed is right, we can recommend
  freshness-conditional close rules with a real boundary; if it's
  wrong (faster or slower), we adjust.
- **CLOB WebSocket roundtrip** (separate measurement): write a small
  one-shot script (`scripts/inspect/clob_ws_rtt.py`) that opens a
  WebSocket subscription to a known liquid token, records the wall-
  clock interval between server-side ticks, and compares against our
  current `httpx`-polled `get_mid` cadence. Decision input for the
  hot-path migration.

## Cross-cutting notes

- **Hosting region** overlaps with `legal-ops-hosting.md` (cycle 10+
  real-money path). Both plans should converge on `eu-west-2` if the
  legal review accepts the jurisdiction.
- **WebSocket migration** overlaps with `markets-gamma.md`, which
  already recommends CLOB/WebSocket for hot-path prices because Gamma
  lags >30s (`research_answers.md` §B5).
- **W1 (serial LLM calls)** in `research_answers.md` and **W4
  (OpenRouter routing latency)** compound the latency problem this
  plan describes. W1 says ~150ms × 25 candidates = ~3.75s of serial
  LLM cost; W4 adds 20–100ms of aggregator routing tax on every call.
  Together they swamp every other latency improvement in this plan —
  fixing them is a prerequisite for any "<100ms competitive floor"
  ambition having teeth. Treat W1/W4 fixes as higher-priority than
  region migration if scheduling forces a choice.
- **Cycle 7 BREAKING-conditional gates** (`trade.py:687–699`) already
  encode the "BREAKING is special" intuition on entry. Any dual-window
  close-side or edge-floor policy should reuse the same freshness
  signal source so the entry and exit policies stay coherent.
