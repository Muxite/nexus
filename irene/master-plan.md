# Master plan — Irene

> The canonical roadmap. For current state, see [`architecture.md`](architecture.md). For the design north star, see [`docs/dkg_spec.md`](docs/dkg_spec.md). The full plan that authorised this build lives at `~/.claude/plans/based-on-the-new-lovely-coral.md` — this file is the in-repo working copy.

- **Last updated:** 2026-05-27 (**cycles 24 + 25 + 26 shipped** — cycle 24 fanout (B–H) committed; cycle 25 dead-market force-close + binary-event dedup + wallet fee-cash + market-vanished re-check landed in the working tree with audit run clean; cycle 26 Gamma events-endpoint dual-read + CLOB WebSocket live cache landed behind 4 default-off flags. 836 tests pass (1 pre-existing dedupe failure).
- **Current phase:** post-cycle-26 promotion. Next session promotes the cycle-26 flags one at a time per the ledger below — shadow events-endpoint first, then probe WS envelope, then turn on WS feed. Cadence remains **implement-then-soak-long**.

---

## Context

We're building **irene** to run in parallel with [seulgi](../seulgi/), sharing only the `scraper:9000` HTTP service. The goals:

1. Iterate on a cleaner architecture without breaking the live seulgi pipeline.
2. Layer in a **DKG (Dynamic Knowledge Graph) design** over multiple phases.
3. Compare old-vs-new before promoting any irene decision into the trading path.

Seulgi's lessons (banked over months of cycles): WAL deadlocks, dual-container HNSW corruption, confidence/p_alpha overloading, NEUTRAL drought, migration sprawl, variant attribution collapse. Irene is designed to avoid each of those traps from day one — see `docs/pitfalls.md` (TBD P1) for the full list.

The user studies-while-building. Every paper they drop gets a folder under `docs/papers/`. Every phase ends with a dated report. Every module gets a `## Specification` block (format below).

### What irene actually does (and what it doesn't)

> **Authoritative philosophy doc:** [`docs/philosophy.md`](docs/philosophy.md).
> If the rest of this plan ever contradicts that file, that file wins.

**Irene does not predict anything.** Irene reads news, confirms the
wording is decisive for a known market, and buys before the market
reprices. The edge is **time**, not insight.

The LLM's role is **natural-language understanding** — "does this
article say the FDA approved or rejected drug X?" — not probability
estimation. The `direction` + `confidence` fields on `predictions`
are interpretation outputs, not forecasts. Every downstream gate
uses them as if they were probabilities because the math is the
same, but their *meaning* is "how decisively does this news resolve
the market".

What this means for the plan:

- **Latency observability is the first-class diagnostic.** Per-stage
  p50/p95/p99 and end-to-end article→trade time matter more than
  almost anything else. The plan reserves a dashboard surface for
  this from P1 forward.
- **The impact tracker is how we know we have edge.** For every
  executed trade we poll CLOB at +1m / +5m / +15m / +30m / +60m and
  ask "did the price move our way?". Lands in P3+ alongside the
  observability port from seulgi.
- **We de-emphasise calibration.** A miscalibrated LLM lets noisy
  trades through the gate — fixable with a stricter confidence floor.
  It is not a six-month research project.
- **Short-horizon markets are our home.** Long-horizon resolution
  markets give other actors time to absorb news; we don't compete
  there.

---

## User decisions locked in (2026-05-22)

| Question | Decision |
|---|---|
| Starting point | **Start fresh.** Old irene (`122720a`) stays deleted; steal patterns only. |
| Build scope | **Minimum viable clone first.** MVP pipeline (article → embed → match → LLM-direction → paper-trade) before any DKG layer. |
| Sharing with seulgi | **Scraper HTTP service only.** Own DB, chroma, everything else. |
| Bus / messaging | **Redis pub/sub design, in-process v1 impl.** Payloads JSON-dicts only; stages stateless across messages. |
| P0 scope | **Slim** (~6 files: README, architecture, master-plan, dkg_spec, first handoff, folder placeholders). |
| Docker-compose | **Standalone only** — no root compose edits ever. |
| Variant fan-out | **DROP entirely.** Single-decision-per-headline. Comparison axis is `decision_path` (mvp vs dkg). |
| Database | **SQLite v1, Postgres P4.** Migration drafted in P4 once volume justifies it. |
| Tracer bullets | **P1 first-class deliverable.** Synthetic articles tagged via `is_tracer`/`tracer_id`; injector + checker + CI stress test. |
| Replay/backtest | **P1 first-class deliverable.** Capture tables (`decision_inputs`, `llm_calls`) inside the live `db.batch()`; replay scripts under `scripts/replay/`. |

---

## Two living documents (in-repo)

### `architecture.md` — what we have NOW

Updated at the **end of every phase** (not every PR — that aspiration drifted stale in seulgi within days; we adopt the looser cadence up front). Per-PR notes live in `changelogs/cycles.md`. Owner: whichever agent last touched a module is responsible for sections describing that module.

Required sections:
- System overview (one paragraph + ASCII data-flow).
- Modules (one row per load-bearing module; pointer to its in-file `## Specification` block).
- Schema (table list, primary indexes, writer attribution).
- External dependencies.
- Configuration knobs.
- Decision funnel drop rates (refreshed after each soak).
- Known limitations.

### `master-plan.md` — what we do NEXT

This file. The phased roadmap, the open questions, the spec-block convention. Edited freely; this is the canonical source.

---

## Modularization

The full rules are in [`docs/modularization.md`](docs/modularization.md) (user directive 2026-05-23, canonical). Every PR is held to those rules. CI enforces:

1. No cross-stage imports (`app/stages/*.py` cannot import each other).
2. JSON-round-trip on every bus payload in tests.
3. Table-ownership lint (each `INSERT`/`UPDATE` matches the table's declared owner module).

Headlines: one responsibility per module, bus is the only inter-stage channel, JSON-dict payloads only, one owner per table, no shared mutable state, dependencies injected, tests sit next to code, each module independently runnable.

## Module specification convention

Every load-bearing module gets a `## Specification` block at the top of its docstring. Format:

```
## Specification
**Purpose.** One sentence.
**Public interface.** Functions / classes other modules import.
**Invariants.** Must-hold properties.
**Side effects.** DB tables read/written, files, external APIs.
**Failure modes.** What can crash, how it recovers.
**Non-goals.** What this module is NOT responsible for.
```

Specs are written in the same PR that creates or substantially modifies the module. Any PR that changes a module's public behaviour without updating its spec is reject-on-review.

---

## Phase plan

Each phase ends with: (a) code merged, (b) a dated `reports/YYYY-MM-DD-phase-N.md`, (c) `architecture.md` updated, (d) a handoff describing what's running and what's next.

| Phase | Theme | Deliverable | Status |
|---|---|---|---|
| **P0** | Scaffold + docs | `irene/` directory tree, 5 root docs, dkg_spec verbatim, first handoff | **✓ complete 2026-05-23** |
| **P1** | MVP pipeline + tracer + replay | Dockerfile, compose entry, schema, all P1 modules, `scripts/tracer/`, `scripts/replay/accuracy.py`. Runs in parallel with seulgi. | **✓ complete 2026-05-23** ([soak report](reports/2026-05-23-irene-p1-soak.md)) |
| **P2** | Extraction layer (shadow) | `stages/extract.py` + entities/facts tables. Trade path STILL uses MVP. Reports compare extraction precision/recall on held-out samples. | **✓ complete 2026-05-23** ([extraction report](reports/2026-05-23-irene-p2-extraction.md)) |
| **P3 ch1** | Entity resolver | `stages/resolve.py` + alias-aware metric. Acronym/alias/suffix/self rule chain over the extracted entities. | **✓ complete 2026-05-23** ([resolver report](reports/2026-05-23-irene-p3-resolver.md)) |
| **P3 ch1.5** | DKG data fill | seulgi → irene one-time migration (27,914 articles + 10,462 markets); production DKG update pipeline (`scripts/dkg/extract_to_irene.py`); SBERT EntityIndex for hot-path dedup; SampledTimer for sampled critical-path latency. | **✓ complete 2026-05-24** ([migration report](reports/2026-05-23-irene-seulgi-migration.md)) |
| **P3 ch2** | Eventize + linker | `stages/eventize.py` + `stages/link.py`. Clustering algorithm locked in [`docs/papers/eventization/notes.md`](docs/papers/eventization/notes.md). Live in `wire_pipeline()`. | **✓ complete 2026-05-24** ([linker report](reports/2026-05-24-irene-link-backfill.md)) |
| **P3 ch3** | Dual-path trader + wallet + closer | `decision_path='dkg'` wired alongside `'mvp'`. Paper-trade wallet with cash + equity + identity check. Closer rules: edge_decay → take_profit → stop_loss → stagnation → timeout. Real Kelly via v6 confidence digit. Per-cycle dashboard isolation. | **✓ complete 2026-05-24** ([handoff](handoffs/2026-05-24.md)) |
| **P3 ch4** | scraper-v2 migration + HTML ingestion | Switched to `jennie-scraper-v2:9001`. `/feed` is shape-compatible; new `_run_html_loop` polls `/scrape mode=links` for 11 HTML homepages (apnews, reuters, bbc, etc.). Three-guard boilerplate filter. | **✓ complete 2026-05-24** ([handoff](handoffs/2026-05-24.md)) |
| **P3 ch5+** | Iterative cycle loop (cycles 8–23 chronicle) | Polymarket fee math, three-state UMA close machine, per-market fee_rate fetcher, soak-monitoring harness, durable systemd timer, backtest preflight, pre-trade safety gates, Gamma pagination fix. | **in progress — cycles 8–23 shipped** ([cycle history](#cycle-history-chunk-3-onwards--iterative-loop), [open work](#current-open-work--issue-list)) |
| **P4** | Dual-write comparison + Postgres draft | Reports quantify mvp vs dkg per-trade-path. Postgres DDL drafted (not run). Schema-reviewer agent owns this. | sketch |
| **P5** | Cutover candidates | One non-critical read (market context) routed through DKG. Then candidate filtering. Final trade decision only after ≥5 consecutive 24h windows of DKG ≥ MVP. | TBD |

Phases are sized so each can be its own conversation. We don't pre-commit to P4+ details until P3 lands.

---

## Phase 1 — shipped 2026-05-23

P1 modules (config, db, bus, ai, embed, budget, stages/{markets,ingest,dedupe,match,trade}, main, Dockerfile + compose) all landed in the first P1 PR. Tracer + replay-accuracy ship gates were verified. Full chronicle: [`reports/2026-05-23-irene-p1-soak.md`](reports/2026-05-23-irene-p1-soak.md).

---

## Phase 2+ (sketches only)

**P2 — Extraction (shadow).** `stages/extract.py` does structured-output LLM call per article, ontology-constrained (small fixed entity-type + relation list defined in `docs/ontology.md` when written). Writes `entities`, `entity_aliases`, `extraction_runs`, `facts`. **The trade path NEVER reads from `facts` in P2.** P2 is data accumulation. End-of-P2 report quantifies extraction precision/recall on a held-out sample and decides whether P3 can proceed.

**P3 — Event + linker.** Three open research problems must be answered in a dedicated research session at the start of P3 before any code:
1. **Eventization clustering** — facts → canonical events needs a similarity metric, time window, and tie-break rule. Research deliverable: `docs/papers/eventization/notes.md`.
2. **`directness_score` and friends** — each component of the DKG scoring formula needs a real implementation. If a component is provably uniform on real data, drop it from the formula rather than carry it as noise.
3. **Dual-path trader risk** — irene v1 ships MVP-only. P3 introduces `decision_path='dkg'` side-by-side. The failure mode to measure: DKG path makes the same trades with extra steps. P3 report quantifies overlap; if >85% with no PnL lift, kill the DKG path.

**P4 — Dual-write comparison + Postgres draft.** Reports quantify mvp vs dkg per-trade-path. Postgres DDL drafted (not run); schema-reviewer agent owns the SQLite-vs-Postgres diff.

**P5 — Cutover candidates.** Per DKG spec — start with one non-critical read (market context), then candidate filtering, then full trade decision only after sustained parity (operationalised: 5+ consecutive 24h windows where DKG PnL ≥ MVP PnL within noise band). Live trading still out of scope.

---

## Open questions

(See also `questions/design_questions.md` when written.)

1. **News freshness store** — separate chroma at `irene/data/news_chroma/` (current plan) vs lighter weight (hash-based) dedup. Doubled embedding writes are the cost; isolation from seulgi's chroma is the benefit. Cost grows with corpus size; revisit in P2.
2. **LLM key sharing** — current plan is shared `OPENROUTER_API_KEY` env var, separate budget tracking. Revisit if either bot hits the monthly cap and we need per-bot pre-allocation.
3. **Tracer template ontology** — what event-shapes should `scripts/tracer/templates/` cover? P1 starts with `regulatory_decision.yaml`, `merger.yaml`, `economic_indicator.yaml`; P2 adds extraction-targeted shapes (entity-rich vs entity-sparse).
4. **DKG scoring formula** — directness/novelty/resolution_fit/deadline_fit/evidence/confirmation components need concrete implementations. Research in P3.

---

## Subagent usage discipline

**Per-phase pattern:**

1. **Start of phase** — 1–3 parallel Explore agents load context (what seulgi does, what the DKG spec says, what irene already has).
2. **Design step** — Plan agent drafts the implementation strategy; for high-stakes design (P3), launch a second independent Plan agent for adversarial review.
3. **Build step** — write code in foreground. Spawn general-purpose agents only for independent parallel one-shots.
4. **End of phase** — three specialised agents:
   - **Diff-checklist agent (Explore):** audits the diff against the deliverables checklist.
   - **Schema-reviewer agent (Explore):** flags missing indexes, JSON casts, Postgres-incompatible types. Especially at P4.
   - **Data-shape audit agent (Explore):** reads `irene.db` after the first soak and reports row counts vs expected ratios.
5. **Every ~2 phases** — **doc-drift agent (Explore):** grep-tests doc claims against code. Output: `reports/YYYY-MM-DD-doc-drift.md`.

Never delegate understanding. The plan, the verification, the phase report — those stay in the main thread.

---

## Hard rules (replicated from README so this doc stands alone)

1. **No DB sharing with seulgi.** Runtime assertion in `db.py`.
2. **Standalone docker-compose only.** Root compose untouched.
3. **Single-decision-per-headline.** No variant fan-out in v1.
4. **Tracers and live trades cannot mix.** `is_tracer=1 AND paper=0` is a hard write rejection.
5. **Capture lives inside the live batch.** `decision_inputs` and `llm_calls` writes share `db.batch()`.
6. **(2026-05-24) Implement-then-soak-long.** Don't ship multiple changes mid-soak. Land a cycle, restart, soak ≥4h, then re-plan from the data — not from the previous plan.
7. **(2026-05-24) Per-cycle data hygiene.** `articles`/`markets`/`entities`/`facts`/`events`/`signals`/`market_links` are SHARED across cycles (news universe). `trades`/`decisions`/`predictions`/`trade_closures`/`llm_calls`/`llm_parse_failures`/`decision_inputs`/`trade_features` are PER-CYCLE; snapshot via `scripts/analysis/snapshot_cycle.py` before recycling.

---

## Cycle history (chunk-3 onwards — iterative loop)

Each row = one implementation cycle the user explicitly scoped. Detail in commit messages; full state-of-system snapshot at the start of every session in `handoffs/`.

| Date | Cycle | Headline change | Soak result |
|---|---|---|---|
| 2026-05-24 | chunk-3 base | Dual-path trader (`decision_path='dkg'`), live eventize+link in `wire_pipeline()`, dashboard PnL page. | 0 trades — ingest stage was broken (GET instead of POST on /feed). |
| 2026-05-24 | follow-up #1 | Ingest GET→POST, `MarketIndex.load()` on start (silent "irene never traded" bug), eventize COALESCE for null published_at. Dashboard observability (AI panel, decisions audit, fewer files). | 18 trades fired, all stuck open (CLOB integration broken — `/book/<id>` 404s). |
| 2026-05-24 | follow-up #2 | Prompt v4 (JSON envelope), matcher tightened (0.5 floor / top-K 8), Tighten title+summary embed, chunker tool. | 350 decisions, parser bug discovered: `{"decision":"NEUTRAL"}` misread as NO. |
| 2026-05-24 | cycle 1 | v5 5-label taxonomy (Y/N/P/U/R), entity-overlap stage-0 gate. | Healthy label spread (Y 2, N 40, P 9, U 21, R 10 in 82). |
| 2026-05-24 | cycle 2 | Wallet (cash/equity/identity), closer stop/target rules, cash-aware sizing, dashboard wallet panel. | Cash went negative immediately (cycle-1 over-allocation), exposed CLOB bug — all trades at 0.5 stub. |
| 2026-05-24 | CLOB fix | `/midpoint?token_id=<id>` is the real endpoint, not `/book/<id>`. | 14 trades closed in seconds; 5 target / 9 stop / -$600 net (cycle-1 mess unwound). |
| 2026-05-24 | cycle 3 | Parser bug fix (strict-match), `target_mid` + `edge_decay` rule, real Kelly via `c` digit, cash clips instead of rejects, 15s poll, stagnation rule, per-cycle dashboard isolation, missed-edge plot. | `cycle3-fresh` boots clean (cycle_id=20). |
| 2026-05-24 | target_mid fix | NO trades had a double-flip sign bug → `target = p_hat + fee_buffer` (was `(1-p_hat) + fee_buffer`). | Locked in by regression test. |
| 2026-05-24 | snapshot tool | `scripts/analysis/snapshot_cycle.py` archives per-cycle data to `data/snapshots/cycle_<id>/`. | cycle_0003 snapshot baseline: equity $435, realized -$565. |
| 2026-05-24 | chunk-4 | scraper-v2 migration — `/feed` shape-compatible; new HTML watcher via `/scrape mode=links` for 11 homepages. Three-guard boilerplate filter. | Live: 316 HTML + 1266 RSS articles in first 30 min (~10× cycle-1 ingest rate). |
| 2026-05-25 | cycle 4 (`08f573b`) | Accounting: degenerate-mid entry filter (`min_mid=0.02`, `max_mid=0.98`); pnl.txt cycle filter; dashboard timestamp 1h-filter bug fixed (cycle isolation across 13 query sites). | Wallet $1000 identity OK; parse_fail dropped from "fake" 77% → 0% (was a stale-data display bug). |
| 2026-05-25 | cycle 5 (`087bc78`) | scraper-v2 `/scrape mode=combined` — parallel RSS+HTML merge; article body fan-out for top-N HTML-only links; 13 tier-1 RSS feeds pinned in `per_host.yaml`. Irene `combined_sources:` config (16 homepages). | First 15 min: 245 articles ingested vs cycle 25's 14; 3 trades fired (cycle 25: 0); LLM R-rate 87%→37%. |
| 2026-05-25 | cycle 5.1 (`a085da2`) | Filter polish: tracking-param strip (`at_*`, `traffic_source`, `update`); numeric-prefix strip on titles; photo-credit detection (Getty/AP Photo/via REUTERS/etc); prefer-better-text dedup; 30-char title floor in scraper (irene's filter removed); RSS-path filtering inside combined; DKG mid-degenerate gate. | 0 junk patterns in cycle 28 first 5 min across BBC/CNN/AJ/AP samples. |
| 2026-05-25 | cycle 6a (`e81fa22`) | Site expansion: 40 subsection RSS (BBC/NYT/Guardian/WSJ/Politico/Hill/NPR/Sky/CBS/ABC) + sitemap_news for AP (761 entries) + Reuters (50 entries) + 18 tier-2 sources. Total ~33 → ~120 effective sources. | First 2 min: 1,112 articles ingested vs cycle 6's 45 (25× rate). 3 trades opened within 1 min. |
| 2026-05-25 | throughput (`d04cad4`) | Dashboard: `throughput.txt` gains TIMINGS table (p1/p50/mean/p95/p99) + CLT STATS (n, σ, SE, 95% CI, required-n for ±10%/±1% accuracy). New plots: `throughput_overlay.png` (5 stages, hourly, log-y), `speed_overlay.png` (per-stage p50/p95/p99 grouped bars, log-y). | Catches `match→trade` gate cascade at n=990 already-converged (need only 145 for ±10% of mean); ingest→trade variance dominated by poll-wait. |
| 2026-05-25 | cycle 7 (`11f8819`) | Polymarket parabolic taker-fee model folded into closure PnL (per-leg `shares * fee_rate * p * (1-p)`; category table); `trade_closures` gains `entry_fee_usdc` + `exit_fee_usdc` audit columns; markets-stage `_deactivate_missing` auto-closes orphaned trades with `close_reason='market_resolved'`; new `market_news_imbalance` trade gate with 5 sub-rules (thin market, extreme-mid w/o BREAKING, far-future w/o BREAKING, near-expiry, singleton-on-extreme). | 390 tests pass. Cycle 31 booting; gate verified via unit tests against cycle 29's 5 trades (rejects NATO + Rajasthan, allows Epstein/Steyer/Delhi). |
| 2026-05-25 | cycle 8 (`583242c`) | Research-distill scaffolding + 3 cycle-7 bug fixes (no-fee-at-resolution; volume24hr; wide-spread gate). | |
| 2026-05-25 | cycle 9 (`98677e6`) | ET-deadline parsing + similarity-floor docs + scraper audit + Axios pin. | |
| 2026-05-25 | cycle 10 (`07a18e8`) | Per-market `fee_rate` fetcher (CLOB `/markets/<cid>`) + explicit spread term in edge gate + 30-min spread soak. | |
| 2026-05-25 | cycle 11 (`7b608ac`) | D1 three-state UMA close machine + W6 `pre_news_drift_15m/60m` + impact-tracker long-tail buckets (out to 24h) + CLOB latency probe. | |
| 2026-05-25 | cycle 12 (`13d3ea5`) | Closed-but-not-finalized resolution re-poll path + first measured `calibration_seeds.md` (C1 spread, C4/E2 latency). | |
| 2026-05-25 | cycle 13 (`2498bba`) | Gamma `Retry-After` backoff helper + per-market `min_order_size` gate at arm-time + (failed) low-vol soak v1. | |
| 2026-05-25 | cycle 14 (`63389d8`) | Per-signal latency-chain instrumentation on `trades` + LLM A/B probe lane (default-off shadow table) + low-vol soak v2 (working). | |
| 2026-05-25 | cycle 15 (`9344e2a`) | Per-market `fee_rate` refresh cadence (168h) + latency dashboard panel + 500-market floor audit. | 82/18% bimodal fee distribution observed on the trading tail. |
| 2026-05-25 | cycle 16 (`eff6349`) | `default_fee_rate` 0.05 → 0.10 + reject-path latency instrumentation on `decisions` + low-vol spread analysis. | |
| 2026-05-25 | cycle 17 (`93613eb`) | Resolved-market schema verification against 100 live closed markets + sub-$30k spread sampler launch. | |
| 2026-05-25 | cycle 18 (`1047864`) | `closedTime` authoritative timestamp + cycle-12 active-flicker fix + impact_tracker schema audit. | |
| 2026-05-25 | cycle 19 (`9793f3c`) | Session retrospective (cycles 8–18) + `config.yaml` sanity-check. No code changes. | 492 tests pass (+97 across cycles 8–18). |
| 2026-05-25 | cycle 20 (`f08588f`) | Production rebuild on cycle-18 code; launched `scripts/soak_check.py` (10-dimension health script) + session-local 2h cron `b4b3e445` for 24h soak. | Container Up; 3 baseline alerts (state 1/2, RESOLUTION_TIMEOUT, fee_rate coverage). |
| 2026-05-25 | cycle 21 (`23e051c`) | 12h soak audit (3 parallel Explore agents) + 5-fix queue handoff. No code. | Confirmed `0 calls to /markets/<cid>` in 24h vs 25,846 `/midpoint` — but cycle-22 disproved the "clob=None" diagnosis. |
| 2026-05-26 | cycle 22 (this branch) | Durable systemd-user timer for `soak_check.py` + `scripts/replay/preflight.py` (P1 gate as `ExecStartPre`) + Fix B (state 1/2 alert split into actionable vs `legacy_uma_backlog`) + Fix D (`IRENE_CYCLE` → `cycle-22`). Preflight against live DB surfaced gross-vs-net drift on trade 19 ($-1.67 stored vs $+15.65 reconstructed). | Alerts dropped 3 → 2; soaks now survive Claude session boundaries. |
| 2026-05-26 | cycle 23 (this branch) | **Halt blind trading.** Pre-trade hard gates (`_gate_mid_unavailable`, `_gate_market_state`, `_insert_trade` post-condition assert) replace silent `mid=0.5` fallback. Kill switch `IRENE__TRADE__EXECUTION_ENABLED`. **Real root cause found:** Gamma caps pagination at `offset=10000`; `_fetch_all_gamma` dropped accumulated markets on 422 — frozen markets table since 2026-05-23. Fix preserves partial pages. Startup-time resolution rescan. RESOLUTION_TIMEOUT audit (13 markets, all no-position). | 655 tests pass. Deployed; `volume_usdc` + `fee_rate` repopulating live (~225 markets/min); kill-switch confirmed firing then re-enabled. |
| 2026-05-26 | cycle 24 (`6ff9442`…`41bf2e9`) | Iran-funnel handoff fanout: **B** dedupe/freshness (recency-bias + burst + widen + accept REHASH), **C** DKG safety-gate parity + `_gates.py` extract, **D** active-flicker SQL CASE + cleanup migration, **E** wallet honesty (None on closed markets, unpriceable annotation), **F** near-miss audit panel + CLI + `audit_confidence_floor`, **G** tracer stress harness (12 templates, live mode, cleanup), **H** LLM context richness (fuller body + market context + softer NEUTRAL + `max_input_tokens`). Post-merge: idempotent `articles.tier`/`breaking_score` migration, `TradeSection.min_hours_to_expiry`, `TradeSection.execution_enabled`, relaxed 4 trade thresholds, centralized tracer campaign mode + WAL fix. | One pre-existing dedupe regression (`test_burst_window_does_not_trip_with_too_few_articles`) accepted as known; remainder green. |
| 2026-05-26 | cycle 25 (working tree, uncommitted) | **25-A** dead-market force-close (`stages/markets.py:_close_orphaned_trades_for`) after `dead_market_force_close_hours`=24 + state-4 logic + idempotent `close_reason` CHECK rebuild (`closer.py`) + zombie backlog migration (`scripts/cycle25/`). **25-B** binary-event dedup gate (`_gates.py:query_open_sibling_markets` + `_gate_binary_dedup`, called from both MVP and DKG paths). **25-C** wallet fee-cash model (`wallet.cash_balance` debits `fees_paid_total`, dashboard shows "fees paid: $X.XX" not phantom DRIFT, `audit_wallet_harness` PASS at $+0.0000 aggregate drift). Plus market-vanished re-check gate and `scripts/audit/market_tracking_audit.py` (read-only, +~25 tests across 4 new files). | 763 tests pass (1 pre-existing dedupe failure unchanged). Audit run: 0 zombies, 0 binary-event violations, 0 orphan trades, 8,949 markets tradable + fresh. |
| 2026-05-27 | cycle 26 (working tree, uncommitted) | **Gamma events endpoint dual-read** (`MarketsSection.use_events_endpoint`/`events_endpoint_dual_read`, `_fetch_universe` orchestrator, schema columns `polymarket_event_id`/`polymarket_event_slug`/`events_seen_in_last_poll`, per-cycle telemetry line, 422 guard preserves partial pages). **WebSocket live market cache** (`clob_ws.py` client + `clob_ws_pool.py` sharded fanout @ chunk_size=500 + `live_market_cache.py` LiveTick/parsers + `stages/market_index.py` extract + `scripts/probe_clob_ws.py`). `CLOBClient.get_mid`/`get_top_of_book` accept `live_cache`+`prefer_live` (REST authoritative for impact_tracker drift). All four flags default OFF — promote in 5 stages (dual-read shadow → events-only → probe envelope → ws soak → prod). | 836 tests pass (1 pre-existing). Cache-fallback invariant verified: when `enable_ws_feed=False`, ws_pool is None and every read goes straight to REST. |

**836 tests pass** as of cycle 26 (+181 across cycles 24–26 — net of one accepted pre-existing dedupe failure). See `## Current open work — issue list` below for the consolidated post-cycle-26 issue ledger.

---

## Current open work — issue list

Single source of truth for next-cycle picks. **Cycle-24 work is now split into 8 self-contained handoff docs under `irene/handoffs/2026-05-26-{A..H}-*.md`.** Each is independently executable by a subagent; the reference doc is handoff A (the empirical funnel audit). Don't tackle the dependent work without reading A first.

### Cycle-24 handoffs (closed)

All cycle-24 work landed in commits `fff1467`…`41bf2e9`. Handoff docs under `irene/handoffs/2026-05-26-{A..H}-*.md` remain as historical reference.

| ID | Title | Status | Commit |
|---|---|---|---|
| A | iran-funnel-audit | reference doc, complete | (handoff only) |
| B | dedupe-freshness (recency-bias + burst + widen + accept REHASH) | **done** | `6ff9442` |
| C | dkg-safety-gates + `_gates.py` extract | **done** | `a02ee66` |
| D | active-flicker (SQL CASE + cleanup migration) | **done** | `6013ab8` |
| E | wallet-honesty (None on closed markets, unpriceable annotation) | **done** | `fff1467` |
| F | near-miss-audit (panel + CLI + `audit_confidence_floor`) | **done** | `aa1aac8` |
| G | tracer-stress-harness (12 templates, live mode, cleanup) | **done** | `aee8cfc` |
| H | llm-context-richness (fuller context + `max_input_tokens`) | **done** | `89b023c` |

### Cycle-25 / cycle-26 promotion ledger (active)

Cycle 25 + 26 sit uncommitted in the working tree. All four cycle-26 feature flags default OFF; promote them one at a time per the 5-stage sequence.

| Flag / step | Status | Promotion gate |
|---|---|---|
| `markets.use_events_endpoint=true` + `events_endpoint_dual_read=true` | **ready — shadow mode** | Watch `events= markets= both= only_events= only_markets=` per-cycle log for ~3 days |
| `markets.events_endpoint_dual_read=false` | blocked on shadow data | `only_markets < 1%` of total |
| (one-shot) `scripts/probe_clob_ws.py` envelope check | **ready** | Confirm WS field names match `live_market_cache.py:apply_*` before flipping `enable_ws_feed` |
| `markets.enable_ws_feed=true` | blocked on probe | Watch cache-hit % + per-shard connection state |
| Production flip | last | Sustained green soak |

### Long-cycle-history items (deferred or done)

| Area | Issue | Status | Next step |
|---|---|---|---|
| Safety | Pre-trade `mid_unavailable` + `market_state` + post-condition gates (MVP only) | **done (cycle 23)** | DKG parity → handoff C |
| Markets | `volume_usdc` NULL on all active markets (cycle-21 Fix A symptom) | **done (cycle 23)** | Root cause: Gamma 422 pagination, fix in `_fetch_all_gamma` |
| Markets | Gamma `category` field NULL | **wontfix — Polymarket-side** | Gamma returns `category: null` natively |
| Resolution | RESOLUTION_TIMEOUT 11/13 markets (cycle-21 Fix E) | **done (cycle 23)** | All `no-position`; see `reports/2026-05-26-resolution-timeout-audit.md` |
| Soak | State 1/2 alert over-counts (cycle-21 Fix B) | **done (cycle 22)** | Split into actionable vs `legacy_uma_backlog` |
| Soak | `IRENE_CYCLE` stale env (cycle-21 Fix D) | **done (cycle 22 → cycle-23)** | Functional, not cosmetic |
| Soak | Cron `b4b3e445` session-local | **done (cycle 22)** | Replaced by `scripts/systemd/irene-soak.{service,timer}` |
| Replay | Preflight gross-vs-net drift (~$17 on trade 19) | **open, no handoff yet** | `plans/preflight-pnl-reconciliation.md` |
| Resolution | Zero `close_reason='market_resolved'` rows ever | **partially addressed (cycle 25-A)** | New `market_dead_unresolved` force-close path covers dead-but-unresolved markets after 24h; true `market_resolved` via UMA still TBD |
| Wallet | Phantom DRIFT in dashboard (fees not deducted from cash) | **done (cycle 25-C)** | `wallet.cash_balance` debits `fees_paid_total`; `audit_wallet_harness` PASS at $+0.0000 |
| Markets | Binary-event over-allocation (sibling positions on same canonical event) | **done (cycle 25-B)** | `_gate_binary_dedup` blocks at both MVP and DKG entry paths |
| Markets | Market vanishes between match and trade insert | **done (cycle 25)** | `_gate_market_vanished` re-checks fresh DB row; new `market_vanished` reject reason |
| LLM | DKG path missing `market_news_imbalance` gate | **done (cycle 24-C)** | `_gates.py` extract gave DKG parity with MVP (`a02ee66`) |
| Diagnostics | Throughput overlay process-time instrumentation | **partially done (cycle 14)** | Per-signal latency-chain landed |
| Trading | Source-quality weighting | **deferred — needs ≥1 week pre_news_drift data** | Plan stub: `plans/source-quality-weighting.md` |
| Trading | Asymmetric stop-loss | **deferred — needs ≥100 closed trades (~13 today)** | Plan stub: `plans/asymmetric-stop-loss.md` |
| Infra | CLOB hosting region experiment (eu-west-2 VPS) | **deferred — infra, not code** | Plan: `plans/legal-ops-hosting.md` |
| LLM | A/B probe lane (cycle 14) — default-off | **intentional** | Needs deliberate provider list + budget extension |
