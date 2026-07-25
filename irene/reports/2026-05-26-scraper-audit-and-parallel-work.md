# Scraper audit + parallel-work pattern

**Date:** 2026-05-26
**Trigger:** Cycle-23 deploy left the bot running but with ~zero trades — funnel analysis showed all signal dying at the freshness gate; user asked to (a) quantify where signal comes from, (b) check scraper v1/v2 structure, (c) propose a pattern for iterating on the scraper without disrupting the live soak.

Companion artifacts:
- Plots: `irene/data/dashboard/plots/{signal_volume_timeseries, signals_by_source, signal_yield_by_source, signal_yield_gap}.png`
- Per-source breakdown CSV: `irene/data/dashboard/plots/signal_source_breakdown.csv`
- Audit script: `irene/scripts/analysis/signal_source_audit.py`

## Signal-source findings (24h window)

| Metric | Value |
|---|---|
| Active sources | 130 |
| Articles ingested | 6,357 |
| Signals matched | 3,997 |
| Overall sig/article | 0.63 |
| Top-quartile median sig/article | 1.50 |
| Articles from noise sources (yield <0.10) | 1,180 (18%) |
| Articles from healthy sources (yield ≥0.50) | 3,138 (49%) |

**Key observations:**

1. **Signal volume is not the problem.** 3,997 signals/day is high relative to what the LLM can/should process. Yet 0 trades fired in the last hour — every signal died downstream.
2. **The matcher's `similarity_floor=0.50` inflates signal counts.** Top sources show sig/article ratios >1.0 (Yahoo 1.18, Bloomberg 1.05, Al Jazeera 2.0, Yahoo+RSS combined 1.34). A single article fans out to multiple "matching" markets at 0.50-0.55 cosine, none of which are semantic matches.
3. **18% of intake is dead weight.** 1,180 articles/24h come from sources yielding <0.10 sig/article (Repubblica 0.02, Indian Express article+html 0.06, etc.). These articles burn embedding cost + dedupe CPU and produce nothing actionable.
4. **The "healthy baseline" framing is misleading.** Multiplying total articles by top-quartile yield gives 9,535 "potential" signals — but that overstates because the top-Q yield is itself inflated by loose matching. Real fix is matcher tightening, not source expansion.

**Recommended ordering** (deferred to cycle 24+ per the issue list):
1. Prune the <0.10 noise sources from `ingest.feeds` and `combined_sources` in `irene/config.yaml` (1,180 fewer articles/day, ~no signal loss).
2. Tighten `cfg.match.similarity_floor` from 0.50 → 0.35 (matches trade-stage floor). Will halve signal volume but the survivors will be real semantic matches.
3. *Then* run the freshness-gate audit (`plans/freshness-gate-audit.md`) — the freshness gate's "STALE_RETURN" rate is partly a symptom of upstream noise. Tighten matcher first, freshness rejection pattern will become legible.

## Scraper architecture

Both scrapers live under `services/`; both are running today.

### scraper-v1 (`services/scraper/`)

- **Image:** `nexus-ujin:latest`, container `nexus-ujin`, port **9000**.
- **Code:** single `app.py` (~300 lines). FastAPI + aiohttp + feedparser.
- **Endpoints:** `POST /fetch`, `POST /twitter/search`, `POST /feed`.
- **Dependencies:** wraps the `obscura` headless browser binary + Brave Search API.
- **Consumer:** **seulgi only.** Irene does not call this scraper.
- **Status:** Up 3 days.

### scraper-v2 (`services/scraper-v2/`)

- **Image:** `nexus-ujin:latest`, container `nexus-ujin`, port **9001**.
- **Code:** proper module structure (`app/{main,service,config,host_overrides,metrics,…}.py`), `config/per_host.yaml`, comprehensive `tests/` suite (15+ test files).
- **Endpoints:** `/health`, `/scrape` (mode=`links|article|auto|combined`), `/feed`, `/sitemap`, `/discover`, `/social/{twitter,mastodon,truth}`, plus admin.
- **Improvements over v1:** HTTP fast-path with obscura fallback only when needed; trafilatura boilerplate stripping; fingerprint-based change detection; per-host concurrency + backoff; sitemap + discover endpoints; clean article-body extraction.
- **Consumer:** **irene only** (via `SCRAPER_URL=http://nexus-ujin:9001` in `irene/.env` + `irene/docker-compose.yml`).
- **Status:** Up 21 hours.

### Irene's consumption pattern

From `irene/config.yaml` + cycle-4/cycle-5 commit history:

- **`/feed`** for ~60 RSS feeds listed in `ingest.feeds` (BBC, NYT, Guardian, AP, Reuters subsections + tier-2 sources). One worker per feed, jittered poll interval.
- **`/scrape mode=combined`** for ~16 HTML homepages in `combined_sources` (timesofindia, repubblica, apnews, finance.yahoo, indianexpress, cnn, bloomberg, channelnewsasia, thehill, …). Per-host policy via `per_host.yaml`.
- **`/scrape mode=article`** fan-out for top-N HTML-only links per combined cycle (per the cycle-5 PR).

The high noise-source articles in the audit (repubblica.it+html: 397 articles → 8 signals; indianexpress.com+article+html: 145 → 8) are the article-mode fan-out catching dead links. The plain-RSS Indian-language feeds (Hindu, indianexpress.com/feed) yield 0.20–0.40 — better than article-mode but still below the 0.50 healthy floor.

## Parallel-work pattern — recommended setup

**Constraint:** the live soak must keep accumulating data (this is the cycle-22 backtest-data requirement). Restarting `nexus-ujin` would briefly starve irene of new articles.

**Recommendation: dev sibling container, separate image tag, separate port.** Live and dev share the same Dockerfile + source tree but run as independent containers.

### Setup (one-time)

Create `services/scraper-v2/docker-compose.dev.yml`:

```yaml
services:
  nexus-ujin-dev:
    build:
      context: .
      dockerfile: Dockerfile
    image: nexus-ujin:dev
    container_name: nexus-ujin-dev
    ports:
      - "9002:9001"   # host 9002 → container 9001 (same internal port)
    networks:
      - nexus_default
    environment:
      - LOG_LEVEL=DEBUG
    # No `restart: unless-stopped` — dev container may crash on experiments.
networks:
  nexus_default:
    external: true
```

Bring it up: `docker compose -f services/scraper-v2/docker-compose.dev.yml up --build -d`.

### Day-to-day workflow

1. **Edit** files under `services/scraper-v2/` (host filesystem, normal git).
2. **Rebuild** the dev image: `docker compose -f services/scraper-v2/docker-compose.dev.yml up --build -d`. ~30s with cached layers.
3. **Live (port 9001) is untouched.** Irene keeps getting articles.
4. **Compare** dev vs live on the same input:
   ```bash
   # Same /feed request to both
   curl -s -X POST localhost:9001/feed -H 'content-type: application/json' \
     -d '{"url":"https://www.aljazeera.com/xml/rss/all.xml"}' > /tmp/live.json
   curl -s -X POST localhost:9002/feed -H 'content-type: application/json' \
     -d '{"url":"https://www.aljazeera.com/xml/rss/all.xml"}' > /tmp/dev.json
   diff <(jq -S . /tmp/live.json) <(jq -S . /tmp/dev.json)
   ```
5. **Run the test suite** in dev for unit-level confidence: `docker compose -f services/scraper-v2/docker-compose.dev.yml exec nexus-ujin-dev pytest -q`.
6. **Promote to live** when ready:
   ```bash
   # Build the :latest tag from your verified branch
   docker compose -f services/scraper-v2/docker-compose.yml build
   docker compose -f services/scraper-v2/docker-compose.yml up -d --force-recreate
   ```
   This causes a brief (5-10s) gap in scraper availability for irene; the ingest stage absorbs this without issue (jittered polls, INSERT OR IGNORE).

### Optional: A/B comparison harness

Once the dev container is up, a tiny script can quantify the impact of a scraper change on irene's signal yield. Idea:

- Pull a fixed set of N URLs (e.g., today's `combined_sources` list).
- Hit both `/scrape` endpoints, collect the link/article responses.
- Run both result-sets through irene's matcher offline (just the embedder + similarity step, no DB writes).
- Compare: (a) #articles, (b) #unique markets matched, (c) match-similarity distribution.

That gives a closed-loop "did this scraper change actually move the funnel" signal without waiting for a multi-hour irene soak.

Worth writing once you have a concrete change to test.

### What NOT to do

- Don't edit `services/scraper-v2/` files and restart `nexus-ujin` directly — that's the live container, and one bad change starves irene of news for as long as the regression takes to detect.
- Don't reuse the `:latest` image tag during experiments. Tag dev iterations as `:dev` (or `:exp-2026-05-26`) so a rollback is just a container recreate.
- Don't share the `9001` port between live and dev. Different ports keep both reachable, which is the whole point.

### What to do FIRST (before any scraper code change)

The signal-yield CSV (`data/dashboard/plots/signal_source_breakdown.csv`) is your input data for the cleanest first cut: **prune the bottom-yield sources from `irene/config.yaml`**. This is a *config* change in irene, not a scraper change — doesn't need the dev scraper at all and gives immediate measurable funnel improvement (~18% less wasted intake). Touch the scraper code only after the easy config wins are in.
