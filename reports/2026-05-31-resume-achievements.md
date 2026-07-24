# Jennie — Resume Achievements (evidence-backed)

**Generated:** 2026-05-31
**What this is:** A catalog of resume-worthy engineering achievements from the Jennie
project (a news-driven Polymarket paper-trading bot), each tied to a verifiable source —
a `file:line`, a commit SHA, a committed report, or a read-only query against the live
databases. **Profitability is deliberately out of scope** (the system is not profitable;
trading is hard). Every claim below is an *engineering* or *methodology* claim, not a
trading-performance claim.

**Confidence key:** `verified` = re-run by command or read directly from live DB/code on
2026-05-31; `report` = taken from a committed report and not independently reproduced
(data is not reproducible day-to-day). Money figures are quarantined to the final
appendix and must NOT go on the resume.

**2026-06-26 refresh:** all `verified` live-DB figures below were re-measured against the
current `irene.db` (now 4.5 GB) / `seulgi.db` on 2026-06-26 and updated in place. Where a
metric grew materially since 2026-05-31, the prior value is shown in parentheses. The
system kept collecting through this window (minus one 8-day outage, §B-note), so the
live-DB numbers are roughly **2× the 05-31 snapshot**.

**2026-07-09 refresh:** re-measured again against the live `irene.db` (now **6.0 GB**;
`seulgi.db` remains frozen at its 2026-05-27 retirement state, so gen-1 figures are
unchanged). No commits landed since cycle 30 (`846dd78`, 2026-06-12) — code/test/LOC
figures are unchanged except the pytest collection count, which is corrected below (an
apples-to-oranges collection scope in the 06-26 pass undercounted irene's tests). The
explorer kept running unattended for the full 27-day gap with **zero manual
intervention**, which is itself the headline: a capture-only research pipeline that
survives four weeks of pure background operation. Market-universe growth has
**plateaued** (230,448 → 230,581 markets, only +133; the 60,594 distinct-events figure
is unchanged) since the system is past the "discover the universe" phase and into
steady-state capture; the figures that kept climbing are the ones the system is actually
optimized for now — articles, clusters, explorations, odds observations.

---

## Project at a glance (verified)

| Fact | Value | Source |
|---|---|---|
| Active development window | 2026-03-14 → 2026-06-12 (~3 months; bulk in May) | `git log` |
| Commits | 215 total (was 209) | `git rev-list --count HEAD` |
| Numbered development cycles | 30 (was 26) | `irene/cycles/`, cycle-30 commit `846dd78` |
| Python source (real, excl. vendored) | ~129.9k LOC | `find … -name '*.py'` per module |
| — seulgi (cosine-matching bot) | 47,905 LOC / 187 files | clean `wc -l` |
| — irene (knowledge-graph rebuild) | 68,608 LOC / 249 files (was 65,528) | clean `wc -l` |
| — services (scrapers) | 8,392 LOC / 61 files | clean `wc -l` |
| — main (API + GUI) | 5,045 LOC / 29 files | clean `wc -l` |
| Automated tests collected | ~1,578 (irene 956 + seulgi 509 + scraper-v2 113; was 1,397) | `pytest --co -q` |

Note: a naïve LOC count returns ~1.2M because `services/` vendors a 1,689-file
virtualenv; the ~130k figure is source-only.

---

## Section A — Data / ML Engineering

### A1. Scraper redesign (v1 → v2): combined RSS+HTML ingestion
Rebuilt the news scraper from a per-fetch shell-out (re-rendered every page, no caching,
no boilerplate filtering) into a combined RSS+HTML service with **link-set fingerprint
change-detection** (fingerprints the normalized link set, so ads/timestamps don't trigger
spurious "changed" events), trafilatura boilerplate stripping, per-host concurrency with
429/5xx backoff, and a disk cache.
- **Source coverage grew ~33 → ~120 effective sources.** `irene/master-plan.md:226`; live seulgi DB has **45 distinct news sites** scraped (`headline_articles.site_url`). *(report + verified)*
- Architecture: `services/scraper-v2/README.md`, `services/scraper-v2/app/service.py`. *(verified)*

### A2. Throughput jump after source expansion (cycle 6a)
First-poll ingestion rose from **45 articles to 1,112 articles in the first 2 minutes
(~25×)** after adding 40 subsection RSS feeds plus AP/Reuters news-sitemaps.
- AP news-sitemap returned **761 entries/poll**, Reuters **50/poll**. `irene/master-plan.md:226`. *(report — verbatim quote confirmed)*
- Caveat: a first-poll burst, not steady-state; frame as "first-poll throughput."

### A3. Market-universe discovery — escaping the Gamma offset=10k cap
The legacy Polymarket `/markets` endpoint caps pagination at `offset=10,000`, freezing
the tradable universe. Added a **dual-read of the `/events` + `/markets` endpoints** and
merged them, gated on `only_markets < 1%` telemetry before promotion.
- **Live result (measured 2026-07-09):** irene has indexed **230,581 markets across
  60,594 distinct events** (cumulative; 5,852 currently active — was 230,448 / 6,809 on
  06-26), vs the legacy seulgi index of **10,761 markets** — **~21× the universe** (was
  ~9.3× at 100,084 markets on 05-31). Growth has plateaued (+133 markets in 13 days,
  events count unchanged): the discovery phase is done, the system is now in
  steady-state capture. *(verified — read from live DBs)*
- **99.6% of markets are discoverable via the events endpoint** (only 861 markets-only =
  0.37%, unchanged since 06-26, well under the 1% promotion gate). `irene/app/stages/markets.py`.
  *(verified)*
- Note: the comparison is cumulative-index vs cumulative-index; both grow over time. The
  defensible, decision-grade claims are the coverage figure (99.6%) and the gate telemetry
  (0.37% markets-only). The "6.8× more universe" figure in old notes is **not** in the repo.

### A4. Semantic dedup + freshness classification
Chroma cosine-distance dedup (`breaking_distance = 0.55`) with a four-label freshness
taxonomy (**BREAKING / EVOLVING / REHASH / STALE_RETURN**) using cluster recency, burst
detection, and age windows (24h evolving / 72h rehash).
- `irene/app/stages/dedupe.py:37-41,43,103`. *(verified)*
- **Live dedup ratio (2026-07-09):** 176,603 articles ingested; 143,485 clustered into
  7,915 semantic clusters (**~18.1 articles per cluster**; was 97,306 → 7,112 clusters
  on 06-26; 57,416 → 4,519 on 05-31). *(verified — live irene DB)*
- **236 distinct feed endpoints** now feed irene's ingest (`articles.source_detail`), up
  from the ~33 of the v1 era. *(verified)*

### A5. Embeddings + in-memory market index
SBERT `all-MiniLM-L6-v2` (384-dim) embeddings with an in-memory numpy cosine search over
~10k markets, top-K retrieval with a tunable similarity floor.
- `seulgi/app/markets.py:265,282,309-322`. *(verified)*

### A6. LLM direction classifier — ensemble + robust parsing
N=3 ensemble (temperature 0.5) with majority-vote and a **2/3-agreement gate** (hard
demote to NEUTRAL below 2/3), JSON-schema extraction with regex fallback, batched at 10.
- `seulgi/app/ai.py:24-25,51-54,438-454`. *(verified)*
- **Production JSON parse-failure rate (measured 2026-07-09):** **0.26%** in seulgi
  (40 / 15,393 calls — frozen since gen-1 retirement) and **4.99%** in irene
  (16,640 / 333,516 calls; was 4.61% on 06-26, 2.93% on 05-31),
  every one captured in dedicated `llm_parse_failures` tables. The point is the
  *instrumentation*: a malformed response is detected, logged with its prompt hash, and the
  decision is hard-demoted to NEUTRAL — a parse failure can never silently become a trade.
  *(verified — live DBs)*
- Note: an old "12% → <2% parse failures" claim is **not** sourced anywhere in the repo —
  do not use it. The measured rates above are the defensible numbers.

### A7. Confidence calibration (eval methodology)
Fitted **Platt scaling and isotonic regression per direction** on 501 closed decisions
(YES n=229, NO n=272). Cut Brier score from ~0.35 to **0.17–0.22**, and flipped the
information coefficient IC(confidence, realized_pnl) from **−0.19 (anti-informative) to
+0.12 (Platt) / +0.16 (isotonic)** — quantifying that raw model confidence was
anti-informative and how much calibration restored.
- `seulgi/reports/2026-05-22-a3-2-calibration.md:11-18,73-128`. *(report)*
- Framing: this is *evaluation rigor*, not a profit claim.

### A8. Cost-engineered LLM usage (added 2026-06-26, refreshed 2026-07-09)
The entire LLM workload — **333,516 calls, 191.3M input + 16.4M output tokens** on
`google/gemini-2.5-flash` — cost **≈ $98 total** over the project's life (was $76 at
258,678 calls on 06-26). That is **~$0.0006 per article evaluated** — flat, even as call
volume grew ~30% since the last refresh. The lever was a deliberately tiny prompt
contract (a 1-token Y/N/U direction call), which traded ~100× per call cost for ~5× more
candidate breadth, plus a disk-backed prediction cache and a pause-before-spend budget
governor (§D4).
- `irene/app/ai.py`, `irene/app/budget.py`; spend reconstructed from `llm_calls`
  (tokens × gemini-2.5-flash list price). *(verified — live DB)*
- Cross-check: the 2026-06-12 retrospective independently put the same workload at
  **$38.79** at the 05-31 call count; the figure scales with volume, not surprise.

---

## Section B — Backend / Infra Engineering

### B1. Configurable gate-pipeline migration (cycle 26)
Replaced a single inline relatedness call with a **configurable gate pipeline
(`market_open → dkg_threshold → llm_batch`)** in which every rejected candidate persists
an audit row with its full per-gate decision chain (no silent skips).
- Commit `c4f7489`: **29 files, +2,940 / −179**. `irene/app/gates/`, `irene/docs/exploration_system.md`. *(verified — `git show --stat`)*
- Migration restored a red test suite to green (report states from 30 failing). *(report)*

### B2. Test + code scale
~1,578 tests collected by pytest (irene 956, seulgi 509, scraper-v2 113; was 1,397 on
06-26 — irene grew +181 tests across cycles 27–30 with no new commits since); ~130k LOC of
real Python across two complete system builds (seulgi + irene) and the scraper services.
- `pytest --co -q`; clean `wc -l`. *(verified — 2026-07-09)*
- Reports additionally cite 826 passing for irene at cycle 26 (`irene/master-plan.md:247`); the live collected number above is the conservative one to quote.

### B3. SQLite scaling + atomic batch writes
`db.py` (3,078 LOC) provides a `batch()` context manager for atomic multi-row writes
under a serialization lock, WAL mode, and **busy_timeout tuned 5s → 30s** to survive
transient writer contention; 18 tables (seulgi) / 34 tables (irene).
- `seulgi/app/db.py:617-680`. *(verified)*
- **Scale carried (live, 2026-07-09):** seulgi.db is **8.6 GB** (frozen since 2026-05-27
  retirement); `engine_results` holds **10.98M rows**, `market_price_snaps` 241k,
  `headline_articles` 153,848 — unchanged. irene.db is **6.0 GB** (was 4.5 GB) with
  `odds_observations` **4.77M rows** (was 4.01M), `market_links` 413,574 (was 312,395),
  `decisions` **426,454** (was 302,639), `llm_calls` **333,516** (was 258,678),
  `predictions` 183,607 (was 134,995), `explorations` 274,521 (was 194,528).
  *(verified — live DBs)*

### B4. Dual-container write-hazard fix
Eliminated a data hazard where two containers wrote the same SQLite + Chroma index
concurrently by collapsing to a single hybrid container and moving the bot to a
standalone compose file, removing cross-container WAL lock contention.
- Commit `c8ab745` ("collapse to single hybrid container"); `irene/docker-compose.yml`. *(verified)*

### B5. Per-stage latency instrumentation
Instrumented the decision path with a 7-step `decision_latency_breakdown` JSON
(`fetch_article, fetch_market, ai_direction, resolve_mid, resolve_book, imbalance_check,
db_insert`) plus a p50/p95/p99 dashboard and CLT-based sample-size targets (required-n for
±10% / ±1% accuracy).
- `irene/cycles/cycle-14.md:10`, `cycle-15.md:16-20`. *(report)*
- **Live percentiles (2026-07-09, n=125,130):** decision latency p50 **578 ms** / p95
  **3,060 ms** / p99 **9,868 ms** (was p50 560 / p95 3,298 / p99 10,513 ms, n=97,009 on
  06-26) — stable-to-slightly-improved as sample size grew ~29%, and the median fell
  ~2.7× from the 05-31 snapshot (p50 1,506 ms, n=190). Excludes a long tail of LLM
  budget-pause / back-pressure waits (the unfiltered p99 is dominated by the cost-governor
  pausing the pipeline before the spend wall, §D4 — a deliberate stall, not a slow path).
  *(verified — live DB)*

### B6. Tracer-bullet verification harness
A template-driven tracer-injection harness fires synthetic news through the live pipeline
end-to-end, with a hard invariant that tracer rows can never become live/paper trades.
- 12 templates shipped in cycle 24 (commit `aee8cfc`), now 15. `irene/scripts/tracer/templates/`. *(verified)*

### B7. Observability surface
15 dashboard page modules + 10 plot modules (irene), plus a Docker Engine API poller
(30s cadence) writing per-container CPU/RAM into a `container_stats` table.
- `irene/app/dashboard/pages/`, `irene/app/dashboard/plots/`, `irene/app/system_stats.py`. *(verified)*

---

## Section C — Quant / Experimental Methodology

### C1. Offline backtest harness with reproducible replay
Built a sandbox that replays historical candidates through configurable strategies with
**disk-backed LLM response caching** (SHA1 of system|user|model|version|temperature|key),
making sweeps reproducible and nearly free to re-run (one report run: **56/72 calls
served from cache, ~78% hit rate**).
- `irene/sandbox/{strategies,pnl,chain,llm}.py`, `irene/scripts/sandbox/run_strategy.py`. *(verified)*
- Ran sweeps of **15 strategy variants × multiple filter sets** (the live seulgi DB shows
  **32 distinct strategy variants / 32 engines** A/B-tested in production). *(verified)*

### C2. Loss-attribution decomposition
Built a 4-way PnL factorization — **directional drift / entry-spread cost / exit-spread
cost / fees** — validated to reconstruct realized PnL exactly, applied per trade across
every run. This is the analytical instrument; the dollar outputs are money-numbers
(appendix).
- `irene/scripts/sandbox/loss_attribution.py:6-15,109-158`. *(verified)*

### C3. Oracle-exit (perfect-foresight) counterfactual
Built an exit-ceiling counterfactual that re-prices each position against the best quote
available in the post-news window, isolating how much of the loss is exit timing vs
structural friction — and showing exit policy cannot cross zero (friction is the binding
constraint). Methodology is the deliverable; dollar figures are in the appendix.
- `irene/scripts/sandbox/exit_ceiling.py`, `irene/reports/2026-05-29-sandbox-t3b-friction-floor.md`. *(report)*

### C4. Statistical-rigor practices
- **Wilson 95% score intervals** on per-variant hit-rates to separate real edge from
  small-sample noise across 25+ variants. `seulgi/reports/2026-05-16-baseline.md`. *(report)*
- **Market-microstructure adverse-selection measurement:** pre-/post-news mid repricing
  over n≈2,090 events — mean 6.1¢/share vs median 0.5¢ spread — quantifying why passive
  market-making is infeasible. `irene/reports/2026-05-29-irene-trading-viability-synthesis.md:115-133`. *(report)*
- **Signal-concentration segmentation:** midband + already-moving markets show 2.91¢ mean
  post-move (16% >5¢) vs 0.56¢ in quiet-extreme markets (2% >5¢) — a ~5× move-rate
  amplification isolating the one tradable lane. Same report, `:143-157`. *(report)*

### C5. Defensible negative result
The backtest program produced a clean, well-supported conclusion: post-news directional
edge is ≈0 and the strategy is **friction-bound** (fees + spread dominate; even
perfect-foresight exit loses). This is presented as a rigorous *experimental result* —
knowing when to stop is itself an engineering outcome. `irene/reports/2026-05-29-irene-trading-viability-synthesis.md`. *(report)*

### C6. Labeled research-dataset engine (added 2026-06-26)
After the "no edge" verdict, the system was re-aimed from a PnL engine to a **continuously
running, self-labeling data-collection engine** — the deliverable the synthesis asked for.
Each candidate is captured as an *exploration* row carrying its gate status, full gate
history, t0 market state (spread/mid/volume/hours-to-expiry), LLM verdict + reason, and a
schedule of order-book snapshots at fixed offsets from the news (including a sparse
days-horizon track at +4h/+1d/+2d/+7d, cycle 29).
- **Banked since 2026-05-27 (measured 2026-07-09):** **274,521 gated explorations**
  (10,081 passed gates; rejections audited: `cheap_binary` 197,452, `dkg` 66,988) and
  **4,774,740 odds observations** — every row machine-labeled (was 194,528 explorations /
  4,011,255 odds observations on 06-26). *(verified — live irene DB)*
- Ran at **~$2/day** of LLM cost and survived an **8-day container outage** (a Dockerfile
  missing `capture_schedule.py`, fixed in `5f18fae`) — collection resumed and back-filled
  on restart with no manual intervention, and has since run unattended for a further
  **27 days with zero manual intervention and zero code changes**. *(verified — commit +
  live DB)*

### C7. Cluster-aware accounting & "fix the instrument" rebuild (cycles 27–30, added 2026-06-26)
The 2026-06-12 retrospective found the paper wallet's headline loss was **mostly
measurement artifact** — it filled at mid (no spread), used a fee model ~5× too high, and
minted fictional wins on zero-volume books. The response was a sequence of honesty fixes,
each its own numbered cycle:
- **Wallet honesty** (`f5f9a3f`, c27): fill at bid/ask instead of mid, charge real
  `fee_rate` semantics, work in position-space PnL.
- **Cluster-aware accounting** (`6afb7d1`, c28): one position per article cluster and PnL
  reported per cluster — because the effective sample of 845 trades is only **~52
  independent bets** (the "effective-N" statistic that killed an earlier mirage where a
  N=9 sweep winner collapsed to −9.2% at N=42).
- **Days-horizon capture** (`c6b8512`, c29) and an **H8 slow-catalyst lane**
  (`846dd78`, c30): instrument the one surviving hypothesis (narrative catalysts that
  reprice over hours, not milliseconds) rather than chase the refuted speed thesis.
- Source: `reports/2026-06-12-irene-big-retrospective.md`; commits as cited. *(report + verified)*

---

## Section D — The program in general: system architecture & infrastructure

*(Added 2026-06-05. This section is the "what is this thing, actually" narrative — the
infrastructure story, suitable for portfolio pages, cover letters, and interview
walk-throughs. It deliberately covers **plumbing, not recipes**: no strategy logic,
signal definitions, prompt contents, or tuned thresholds beyond what's already public
above. All claims are `verified` against the repo on 2026-06-05 unless noted.)*

### D0. What the program is

A fully containerized, news-driven prediction-market research platform: a fleet of
Docker services that continuously scrape 100+ news sources, deduplicate and embed
headlines, match them against ~100k live Polymarket markets, run LLM-gated paper
trades, and record everything — every decision, every gate rejection, every LLM call,
every latency sample — into an auditable SQLite store with a live dashboard on top.
Two complete generations were built: **seulgi** (cosine-matching bot, 48k LOC) and
**irene** (event-driven knowledge-graph rebuild, 65k LOC), sharing scraper
microservices and infrastructure patterns but with independent data stores and
lifecycles.

### D1. Container topology — root compose + standalone bot composes

The deployment is split deliberately: a **root `docker-compose.yml`** owns the shared
surface (tkinter GUI via noVNC :6080, FastAPI trading server :8000, React frontend
:3000, scraper v1 :9000, scraper-v2 :9001, plus `test`/`build-only` profiles), while
each bot runs from its **own standalone compose file** joined to a shared external
`jennie_default` network. This boundary is a hard rule ("standalone only — no root
compose edits ever") born from the dual-writer incident (B4): bot lifecycle changes can
never accidentally resurrect a second writer from the root file.
- `docker-compose.yml`, `irene/docker-compose.yml`, `seulgi/docker-compose.yml`. *(verified)*
- Operational details that show production care: `init: true` on scraper containers to
  reap headless-browser zombie processes; JSON log rotation (50 MB × 3 files); a shared
  named volume for the ~500 MB sentence-transformers model cache so two bots don't
  download it twice; config bind-mounted read-only with in-process hot-reload so config
  edits never require a rebuild; the Docker socket mounted **read-only** solely for the
  container-stats collector. *(verified — compose files)*

### D2. Event-driven pipeline on a contract-first pub/sub bus

irene's stages communicate **only** through an in-process async pub/sub bus
(`app/bus.py`) written against a protocol that a Redis backend could satisfy
unchanged. The bus enforces real invariants, not vibes:
- **Strict JSON round-trip**: in strict mode every `publish()` is
  `json.dumps`/`json.loads`-validated, so a non-serializable payload fails at the
  publisher, not three stages downstream.
- **Two-tier back-pressure policy**: trade-path topics *block* the publisher when full
  (signals are never silently dropped); `telemetry.*` topics are drop-first and
  non-blocking, with drops counted and surfaced.
- **Per-subscriber fault isolation**: each subscriber runs in its own task; one
  subscriber crashing is logged and counted, never propagated.
- **Lifecycle enforcement**: subscribing after `start()` raises — catching the classic
  "stage wired too late, silently misses messages" bug at boot instead of in prod.
- `irene/app/bus.py`, `irene/docs/modularization.md`. *(verified)*

`main.py` wires stages in topological order (markets → ingest → dedupe → match →
trade, plus auxiliary loops: position closer, odds capture, impact tracker, explorer)
and stops them in **reverse** order so shutdown drains downstream queues first.
- `irene/app/main.py`, `irene/app/stages/` (17 stage modules). *(verified)*

### D3. Sharded WebSocket market-data pool

Live odds come from a **sharded WebSocket pool** against Polymarket's CLOB feed,
designed for ~18k concurrent token subscriptions: tokens are routed to shards by
stable hashing so a token's updates always land on the same connection, shard count
grows monotonically as subscriptions grow, subscribe/unsubscribe resolve to idempotent
per-shard deltas, and a freshness-checking `LiveMarketCache` makes stale reads safe by
construction rather than by discipline.
- `irene/app/clob_ws_pool.py`, `irene/app/clob_ws.py`, `irene/app/live_market_cache.py`. *(verified)*

### D4. LLM cost governor with file-as-protocol pause flag

A `Budget` watcher polls the OpenRouter key endpoint and pauses the entire LLM
pipeline *before* hitting the spend wall, with **two independent pause sources**
(monthly-cap proximity, and a 402-streak counter for the credit balance the API
doesn't expose). Pause state is published as a JSON **flag file** — dashboard and
ad-hoc scripts read it with zero IPC ("the file IS the protocol"), and `ai.py` checks
`is_paused()` before every call.
- `irene/app/budget.py`. *(verified)*

### D5. Scraper-v2 as a proper microservice

The v2 scraper (A1) is a standalone FastAPI service with a pydantic-validated API:
`/scrape` (links | article | auto modes), `/feed`, `/sitemap`, a `/discover` endpoint
that auto-detects RSS/sitemaps so **new sources are added by URL alone**, social
endpoints (X via chained fallbacks, Mastodon, Truth Social), and `/trends`. It runs an
HTTP fast path with headless-render fallback only when needed, per-host concurrency
config in YAML, an SQLite disk cache on a named volume, and per-response poll-pacing
hints (`next_poll_hint_secs`) plus per-link tier/breaking-score signals so *callers*
can prioritize without the scraper knowing anything about trading.
- `services/scraper-v2/README.md`, `services/scraper-v2/app/`. *(verified)*

### D6. Data layer: disciplined SQLite, not accidental SQLite

Beyond the scale numbers in B3, the *design rules* are the resume-worthy part:
- **Table ownership**: each module owns its tables and registers DDL at import time —
  no central migrations file to merge-conflict, no orphan schemas.
- **One write discipline**: all multi-row writes go through an atomic `batch()`
  context manager under a serialization lock; WAL mode; tuned busy-timeout.
- **Cycle stamping via ContextVar**: a per-deployment `cycle_id` is set once at boot
  and transparently stamped onto every trade-path row, making any historical row
  attributable to the exact code/config generation that wrote it.
- 34 tables (irene) / 18 (seulgi), including dedicated audit tables for LLM parse
  failures, gate rejections, decision latency, and per-container resource stats.
- `irene/app/db.py`, `seulgi/app/db.py`. *(verified)*

### D7. Verification infrastructure as first-class services

Testing isn't a directory, it's part of the topology:
- **Compose test profiles** run pytest in fresh ephemeral containers off the same
  production images (`seulgi-test`, `irene-test`, `scraper-v2-test`).
- A **replay regression suite** (`seulgi-replay`) mounts the production DB read-only,
  replays historical headlines through the *live* runner against an isolated test DB,
  and asserts zero row-ID gaps across all write methods — a write-integrity invariant,
  not just a unit test.
- The **tracer harness** (B6) injects synthetic news through the real pipeline with a
  hard "tracers never become trades" invariant.
- `docker-compose.yml` (profiles), `seulgi/scripts/replay/regression_suite.sh`,
  `irene/scripts/tracer/`. *(verified)*

### D8. Spec-first module convention

Every irene module opens with a structured docstring spec — **Purpose / Public
interface / Invariants / Side effects / Failure modes / Non-goals** — written before
the implementation and kept current. The invariants aren't documentation theater;
several are mechanically enforced (bus strict mode, late-subscribe errors, DB path
assertions that *crash on purpose*). This is what made 26 development cycles by a
single developer sustainable across 127k LOC.
- `irene/app/*.py` module docstrings; `irene/docs/modularization.md`. *(verified)*

---

## Appendix — EXCLUDED money numbers (do NOT put on resume)

These appeared in the source reports but are explicitly excluded per the project owner:
trading P&L is noisy, not reproducible, and not good. Listed only so the framing is clear.

- Friction-floor run: −$124.88 total PnL on 43 trades, 7.0% win-rate, −12.49% ROI;
  oracle-exit −$77.03. (`2026-05-29-sandbox-t3b-friction-floor.md`)
- Sweep ROIs (e.g. t3×short_dated +3.73% → −9.20% across sample sizes). (`2026-05-28-sandbox-larger-set-sweep.md`)
- Baseline snapshot: +$1,145.51 realized, +1.68% ROI, Wilson CI on win-rates. (`2026-05-16-baseline.md`)
- Live irene paper wallet (2026-06-26): −$17,379 over 1,279 closures, 13.0% win-rate. Per
  the 2026-06-12 retrospective this is **artifact-dominated** — mid-fills, a ~5×-overstated
  fee model, and fictional wins on zero-volume books; the liquid subset shows only ~+$1.13
  gross drift/trade, below the round-trip spread. Decision-grade conclusion is unchanged:
  edge ≈ 0, friction-bound. Quarantined; never a performance claim. (`2026-06-12-irene-big-retrospective.md`)

Money figures appear in this project only to *describe the analysis built*, never as a
performance claim. The calibration IC/Brier numbers (A7) and the microstructure ¢-spread
measurements (C4) are evaluation metrics, not P&L, and are fine to use.
