# Plan — source-quality weighting

> Status: STUB (planned during cycle 20 setup, 2026-05-25). Real design + implementation TBD; this file is the placeholder so it survives session boundaries.

## Why this exists

User request 2026-05-25:
> "we also need some kind of way to weigh new sources based on how important they are, some sites dont drop anything that crazy, while some major sites often produce breaking news and leaks"

Today every source in `irene/config.yaml:ingest.feeds` + `combined_sources` feeds the pipeline at the same priority. Cycle 9 audited the source list and flagged 3 risky sources (bloomberg / ft / nyt obscura renders) but did not differentiate signal quality. Cycle 16's `pre_news_drift` columns (W6) give us the raw data shape needed to derive a quality signal, but nothing reads them yet.

## Design sketch (pre-PR, not implemented)

### Phase A — observability
- Add a `source_quality` view computed from `articles` × `signals` × `decisions` × `trades` × `price_impacts`. Per-source metrics:
  - **Article rate** (per day) — volume firehose.
  - **Signal pass-through rate** — fraction of articles that pass the match stage's similarity floor.
  - **Decision firing rate** — fraction of signals that pass the gate cascade (any outcome).
  - **Execute rate** — fraction of decisions that hit `outcome='execute'`.
  - **Realized-edge signal** — `edge_realised_t60s/300s/900s` aggregated across that source's trades. The W6 `pre_news_drift_*` columns answer the "already priced in" question.
  - **Latency-to-publish** — `articles.published_at` vs first-seen.
- Surface in a new dashboard panel `source_quality.txt`.

### Phase B — weighting
- Two candidate axes:
  1. **Poll cadence** — high-quality sources polled more frequently. Trade-off: scraper-v2 budget.
  2. **Confidence multiplier** — gate threshold per source (e.g., a `source_min_confidence_override` or a `source_quality_score` joined into the trade gate).
- Start with cadence-only: a `cfg.ingest.source_priority` dict mapping host → multiplier (default 1.0); the ingest scheduler treats higher-multiplier sources as polled at a fraction of the base cadence.
- Defer confidence-multiplier work to a separate cycle — confidence is already the LLM's interpretation strength and adding a source axis risks double-counting.

### Phase C — feedback loop
- Auto-compute `source_priority` from the Phase A view (rolling 7-day window). Weekly refresh job. Manual overrides via config.

## Hard constraints
- No source REMOVAL from this plan — cycle 9's audit is the source-removal lane.
- Tier-1 prohibited scrapers (bloomberg / ft / nyt) stay quarantined regardless of quality score.
- Quality score must be DATA-driven (the W6 / impact_tracker columns), not RAG-priored.

## Cross-cuts
- `irene/plans/scraper-news-sources.md` — Phase A absorbs the cycle-9 source-audit list.
- `irene/plans/latency-repricing.md` — W6 `pre_news_drift` provides the "already priced in" signal that distinguishes high-quality (front-running) sources from late-to-the-party ones.
- `irene/questions/research_questions.md` G1 — "highest signal-per-dollar wire sources" question becomes a measurement question rather than a research question once Phase A lands.

## When to act

- Earliest after cycle 20's 24h soak completes (gives us ~24h of real `pre_news_drift_*` data).
- More realistic: 7-day soak before any cadence/priority changes ship.
