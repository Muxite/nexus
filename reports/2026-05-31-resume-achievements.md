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

---

## Project at a glance (verified)

| Fact | Value | Source |
|---|---|---|
| Active development window | 2026-03-14 → 2026-05-29 (~2.5 months) | `git log` |
| Commits | 209 total (193 in May; 196 by primary author) | `git rev-list --count HEAD`, `git shortlog -sn` |
| Numbered development cycles | 26 | `irene/cycles/`, `irene/master-plan.md` |
| Python source (real, excl. vendored) | ~126.9k LOC | `find … -name '*.py'` per module |
| — seulgi (cosine-matching bot) | 47,905 LOC / 187 files | clean `wc -l` |
| — irene (knowledge-graph rebuild) | 65,528 LOC / 239 files | clean `wc -l` |
| — services (scrapers) | 8,392 LOC / 61 files | clean `wc -l` |
| — main (API + GUI) | 5,045 LOC / 29 files | clean `wc -l` |
| Automated tests collected | ~1,397 (irene 775 + seulgi 509 + scraper-v2 113) | `pytest --co -q` |

Note: a naïve LOC count returns ~1.2M because `services/` vendors a 1,689-file
virtualenv; the ~127k figure is source-only.

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
- **Live result (measured today):** irene indexes **100,084 markets across 22,343
  distinct events (17,979 active)**, vs the legacy seulgi index of **10,761 markets** —
  **~9.3× the universe**. *(verified — read from live DBs)*
- **99.1% of markets are discoverable via the events endpoint** (only 897 markets-only =
  0.90%, under the 1% promotion gate). `irene/app/stages/markets.py`. *(verified)*
- Note: the "6.8× more universe" figure in old notes is **not** in the repo; use the
  measured ~9.3× / 99.1%-coverage numbers instead.

### A4. Semantic dedup + freshness classification
Chroma cosine-distance dedup (`breaking_distance = 0.55`) with a four-label freshness
taxonomy (**BREAKING / EVOLVING / REHASH / STALE_RETURN**) using cluster recency, burst
detection, and age windows (24h evolving / 72h rehash).
- `irene/app/stages/dedupe.py:37-41,43,103`. *(verified)*
- **Live dedup ratio:** 57,416 articles collapsed into 4,519 clusters (~12.7 articles per
  semantic cluster). *(verified — live irene DB)*

### A5. Embeddings + in-memory market index
SBERT `all-MiniLM-L6-v2` (384-dim) embeddings with an in-memory numpy cosine search over
~10k markets, top-K retrieval with a tunable similarity floor.
- `seulgi/app/markets.py:265,282,309-322`. *(verified)*

### A6. LLM direction classifier — ensemble + robust parsing
N=3 ensemble (temperature 0.5) with majority-vote and a **2/3-agreement gate** (hard
demote to NEUTRAL below 2/3), JSON-schema extraction with regex fallback, batched at 10.
- `seulgi/app/ai.py:24-25,51-54,438-454`. *(verified)*
- **Production JSON parse-failure rate (measured):** **0.26%** in seulgi (40 / 15,393
  calls) and **2.93%** in irene (2,233 / 76,274 calls), tracked in dedicated
  `llm_parse_failures` tables. *(verified — live DBs)*
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

---

## Section B — Backend / Infra Engineering

### B1. Configurable gate-pipeline migration (cycle 26)
Replaced a single inline relatedness call with a **configurable gate pipeline
(`market_open → dkg_threshold → llm_batch`)** in which every rejected candidate persists
an audit row with its full per-gate decision chain (no silent skips).
- Commit `c4f7489`: **29 files, +2,940 / −179**. `irene/app/gates/`, `irene/docs/exploration_system.md`. *(verified — `git show --stat`)*
- Migration restored a red test suite to green (report states from 30 failing). *(report)*

### B2. Test + code scale
~1,397 tests collected by pytest (irene 775, seulgi 509, scraper-v2 113); ~127k LOC of
real Python across two complete system builds (seulgi + irene) and the scraper services.
- `pytest --co -q`; clean `wc -l`. *(verified)*
- Reports additionally cite 826 passing for irene at cycle 26 (`irene/master-plan.md:247`); the live collected number above is the conservative one to quote.

### B3. SQLite scaling + atomic batch writes
`db.py` (3,078 LOC) provides a `batch()` context manager for atomic multi-row writes
under a serialization lock, WAL mode, and **busy_timeout tuned 5s → 30s** to survive
transient writer contention; 18 tables (seulgi) / 34 tables (irene).
- `seulgi/app/db.py:617-680`. *(verified)*
- **Scale carried (live):** seulgi.db is **8.6 GB**; `engine_results` holds **10.98M
  rows**, `market_price_snaps` 241k, `headline_articles` 153,848. irene
  `odds_observations` **1.41M rows**, `market_links` 137,698, `decisions` 62,490,
  `llm_calls` 76,274. *(verified — live DBs)*

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
- **Live percentiles:** decision latency p50 **1,506 ms** / p95 **7,732 ms** / p99
  **13,837 ms** (n=190). *(verified — live DB)*

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

---

## Appendix — EXCLUDED money numbers (do NOT put on resume)

These appeared in the source reports but are explicitly excluded per the project owner:
trading P&L is noisy, not reproducible, and not good. Listed only so the framing is clear.

- Friction-floor run: −$124.88 total PnL on 43 trades, 7.0% win-rate, −12.49% ROI;
  oracle-exit −$77.03. (`2026-05-29-sandbox-t3b-friction-floor.md`)
- Sweep ROIs (e.g. t3×short_dated +3.73% → −9.20% across sample sizes). (`2026-05-28-sandbox-larger-set-sweep.md`)
- Baseline snapshot: +$1,145.51 realized, +1.68% ROI, Wilson CI on win-rates. (`2026-05-16-baseline.md`)

Money figures appear in this project only to *describe the analysis built*, never as a
performance claim. The calibration IC/Brier numbers (A7) and the microstructure ¢-spread
measurements (C4) are evaluation metrics, not P&L, and are fine to use.
