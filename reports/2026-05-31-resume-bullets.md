# Jennie — Resume Bullets (paste-ready)

**Generated:** 2026-05-31. Every number here traces to a citation in
`2026-05-31-resume-achievements.md`. **No money/P&L/ROI/win-rate numbers** (excluded as
noisy). Pick the variant that matches the role; don't use all of them at once.

---

## Project one-liner (header)

> **News-driven prediction-market trading system** — built two complete pipelines (~127k
> LOC Python) that scrape 100+ news sources, dedup and embed headlines, match them to
> ~100k live Polymarket markets, and run an LLM-gated paper trader, over 209 commits / 26
> iteration cycles with ~1,400 automated tests.

---

## Data / ML Engineering

- **Redesigned the news scraper (v1→v2)** into a combined RSS+HTML service with link-set
  fingerprint change-detection, boilerplate stripping, and per-host backoff — growing
  coverage from ~33 to ~120 sources and lifting first-poll ingestion ~25× (45 → 1,112
  articles in 2 minutes).
- **Escaped Polymarket's `offset=10k` pagination cap** by dual-reading the events +
  markets endpoints, expanding the indexed universe ~9.3× to **100k+ markets across 22k
  events**, with **99.1%** events-endpoint coverage gated by an `<1%` shortfall threshold.
- **Built a semantic dedup + freshness pipeline** (Chroma cosine, 4-label
  BREAKING/EVOLVING/REHASH taxonomy with burst detection) that collapses ~57k raw
  articles into ~4.5k clusters (~13:1).
- **Engineered an LLM direction classifier** — SBERT (`all-MiniLM-L6-v2`, 384-d) market
  retrieval feeding a 3-way ensemble with majority-vote and a 2/3-agreement gate, JSON-
  schema parsing with regex fallback — holding the JSON parse-failure rate to **0.26%
  over 15k calls** in production.
- **Implemented confidence calibration** (per-direction Platt + isotonic) on 501 closed
  decisions, cutting Brier score from ~0.35 to **0.17–0.22** and flipping the information
  coefficient from **−0.19 to +0.16**, proving raw model confidence was anti-informative.

## Backend / Infra Engineering

- **Migrated the matching engine to a configurable gate pipeline**
  (`market_open → dkg_threshold → llm_batch`) with full per-candidate audit trails and no
  silent skips — a 29-file, +2,940/−179 change that restored a red test suite to green.
- **Scaled the SQLite data layer to 8.6 GB / 11M+ rows** with an atomic batch-write
  context manager, WAL, and tuned busy-timeout (5s→30s); eliminated a multi-container
  write hazard by collapsing concurrent writers onto a single hybrid container.
- **Instrumented end-to-end decision latency** with a 7-stage breakdown and p50/p95/p99
  dashboards backed by CLT sample-size targets; built a 15-template tracer-injection
  harness for live end-to-end verification with a hard "tracers never trade" invariant.
- **Stood up the observability surface**: 15 dashboard pages + 10 plot modules plus a 30s
  per-container CPU/RAM poller, across two systems and ~1,400 automated tests.

## Quant / Experimental Methodology

- **Built a reproducible offline backtest harness** with disk-cached LLM replay (~78%
  cache reuse) to sweep 15+ strategy variants × multiple filter sets (32 variants A/B-
  tested in production) at near-zero marginal cost.
- **Developed a loss-attribution framework** decomposing every trade's PnL into
  directional drift vs entry-spread, exit-spread, and fee costs, plus a perfect-foresight
  "oracle-exit" counterfactual to separate exit timing from structural friction.
- **Applied statistical rigor to noisy trading data** — Wilson score intervals on
  per-variant hit-rates, and a market-microstructure study (n≈2,090 events) measuring
  6.1¢ mean adverse selection against a 0.5¢ median spread — to reach a defensible,
  well-supported negative result rather than overfitting to small samples.

---

## Tips for using these

- **Pick 4–6 bullets**, not all 13 — lead with the scraper-throughput, market-universe,
  and gate-pipeline bullets; they have the strongest, most verifiable numbers.
- The **calibration (Brier/IC)** and **microstructure (¢-spread)** numbers are evaluation
  metrics, not P&L — safe to use and they signal quant maturity.
- If asked "was it profitable?" the honest, strong answer is: *"No — and the work that
  proves it isn't (the loss-attribution + friction-floor analysis) is some of the most
  rigorous in the project."* Framing a negative result as an engineering deliverable is
  credible and senior.
- Don't cite the live latency p50 (~1.5s) as a *fast* number — it's evidence of
  *instrumentation*, not speed. Use it only if asked about observability.
