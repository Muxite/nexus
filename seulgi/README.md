# Seulgi

News-driven paper trading bot for Polymarket.
Watches news sites → matches headlines to markets → classifies direction → places paper trades.

---

## Pipeline

```
Scraper container (POST /fetch)
        │  links extracted every ~20–25s per site
        ▼
  SiteWatcher (app/watcher.py)
        │  new headline URLs filtered + deduped
        ▼
  Embedder — all-MiniLM-L6-v2 (384-dim)
        │  embed headline text
        ▼
  MarketIndex — in-memory cosine similarity (numpy @ matrix)
        │  top-K markets above similarity_threshold
        ▼
  DirectionClassifier — OpenRouter LLM (gemini-2.5-flash-lite)
        │  YES / NO / NEUTRAL + confidence
        ▼
  EngineRunner x3 (aggressive / balanced / conservative)
        │  each applies its own edge/confidence thresholds
        ▼
  Paper trade → SQLite  /data/seulgi.db
        │  position tracker polls every 60s
        ▼
  Close at timeout (24h) → log P&L
        │
  Dashboard files written every 30s → /data/dashboard/
```

---

## Quick start

```bash
# From project root — run both required services:
docker compose up scraper seulgi

# Rebuild seulgi only (after code changes):
docker compose build seulgi && docker compose up -d seulgi

# Rebuild scraper (takes longer — downloads Playwright Chromium):
docker compose build scraper && docker compose up -d scraper

# Full rebuild from scratch:
docker compose build --no-cache scraper seulgi
```

Seulgi waits for the scraper to be healthy before starting
(`depends_on: scraper: condition: service_healthy`).

---

## Environment variables

### Required
| Variable | Description |
|---|---|
| `OPENROUTER_API_KEY` | API key for OpenRouter (AI direction classifier) |

### Optional overrides
| Variable | Default | Description |
|---|---|---|
| `SCRAPER_URL` | `http://scraper:9000` | Scraper service address |
| `SEULGI_CONFIG` | `/config/config.yaml` | Config file path |
| `LOG_LEVEL` | `INFO` | Set `DEBUG` for verbose output |
| `PROFILE` | `1` | Set `0` to disable the profiler entirely |
| `SEULGI__TRADING__BALANCE_USDC` | `1000.0` | Starting paper balance |
| `SEULGI__TRADING__MAX_POSITION_USDC` | `100.0` | Max USDC per trade |
| `SEULGI__TRADING__MIN_EDGE` | `0.04` | Min required edge (p_hat minus mid) |
| `SEULGI__MARKETS__SIMILARITY_THRESHOLD` | `0.35` | Min cosine sim for market match |
| `SEULGI__AI__MIN_CONFIDENCE` | `0.45` | Min LLM confidence to act on direction |

Set via `seulgi/.env` or directly in `docker-compose.yml`.

---

## config.yaml

Path: `seulgi/config.yaml` — mounted read-only at `/config/config.yaml`.
Override any value without rebuilding: `SEULGI__SECTION__KEY=value`.

```yaml
watched_sites:
  - url: "https://apnews.com"
    change_wait_seconds: 20    # base poll interval (+-20% Gaussian jitter applied)
    max_failures: 5            # consecutive failures before exponential backoff

markets:
  refresh_hours: 4             # re-fetch Polymarket market list every N hours
  similarity_threshold: 0.35   # raise to 0.5+ to reduce noise; lower = more trades
  top_k: 15                    # markets evaluated per headline

ai:
  model: "google/gemini-2.5-flash-lite"   # any OpenRouter model ID
  api_key: ""                             # blank = uses OPENROUTER_API_KEY env var
  min_confidence: 0.45                    # raise for fewer, higher-quality trades

trading:
  balance_usdc: 1000.0
  max_position_usdc: 100.0
  min_trade_usdc: 5.0
  min_edge: 0.04               # edge = p_hat - mid_price
  edge_increment: 0.12         # edge at confidence=1.0; scales linearly
  position_timeout_hours: 24
  max_open_positions: 20

engines:                       # each runs same headlines with its own thresholds
  - name: "aggressive"         # each gets its own balance + P&L tracking
    min_edge: 0.03
    max_position_usdc: 150.0
    min_confidence: 0.40
  - name: "conservative"
    min_edge: 0.08
    max_position_usdc: 50.0
    min_confidence: 0.60
  - name: "balanced"
    min_edge: 0.05
    max_position_usdc: 100.0
    min_confidence: 0.50

dashboard:
  data_dir: "/data/dashboard"
  interval_secs: 30
```

---

## Dashboard files

Written to `seulgi/data/dashboard/` (host volume). All writes are atomic.

| File | Contents |
|---|---|
| `dashboard.txt` | Main summary: balance, watcher health, last events |
| `watcher_status.txt` | Per-site: reachable, backoff level, last headline |
| `positions_open.txt` | Live positions with entry price and unrealized P&L |
| `trades_executed.txt` | Last 50 executed trades |
| `trades_declined.txt` | Last 50 skipped trades with skip reason |
| `near_miss.txt` | Trades with positive edge below threshold |
| `news_seen.txt` | Last 50 detected headlines |
| `ai_decisions.txt` | Last 50 LLM classify calls |
| `market_index.txt` | Polymarket index stats |
| `engine_comparison.txt` | Per-engine balance, trade count, skip breakdown |
| `activity_log.txt` | Rolling event log (max 500 lines) |
| `profile.txt` | Profiler: n / avg / p50 / p95 / p99 per span |

---

## Watcher recovery model

Watchers never permanently die. On scraper failures they enter exponential
backoff and recover automatically when the scraper comes back.

```
5 consecutive failures  ->  backoff level 1  (~30s retry)
5 more failures         ->  backoff level 2  (~60s)
...doubles each round...
                        ->  backoff level 6  (~30min, shown as DEAD in dashboard)
any successful poll     ->  immediately resets to normal polling
```

Backoff sequence (seconds): 30 -> 60 -> 120 -> 240 -> 480 -> 900 -> 1800
All intervals have +-20% Gaussian jitter.

---

## Scraper integration

One call per site per poll cycle:

```
POST http://scraper:9000/fetch
{"url": "https://apnews.com", "extract_links": true, "extract_text": false}
```

The scraper selects the fastest available method per domain:
`requests -> cloudscraper -> playwright -> selenium`

Per-domain memory skips methods that have previously failed.
Responses are cached 45s inside the scraper container.
Live scraper method timings: `GET http://localhost:9000/profile`

---

## Profiler

Enabled by default (`PROFILE=1`). Set `PROFILE=0` to disable — spans become true no-ops.

Output: `seulgi/data/dashboard/profile.txt`, refreshed every 10s.

```
-- seulgi  2026-04-19 12:34:56 ------------------------------------------
  ai/classify          n=  142  avg= 890.1ms  p50= 780.3  p95=1800.4  p99=3200.1
  markets/embed        n=  142  avg=  12.3ms  p50=  11.1  p95=  18.4  p99=  45.2
  markets/search       n=  142  avg=   2.1ms  p50=   1.8  p95=   4.2  p99=  12.3
  watcher/fetch        n= 1203  avg= 312.4ms  p50= 280.1  p95= 890.2  p99=2100.0
```

Adding a span:
```python
from profiler import prof

with prof.span("my/operation"):
    do_work()

@prof.traced("my/function")
async def my_fn(): ...
```

---

## Database

SQLite at `/data/seulgi.db` (host: `seulgi/data/seulgi.db`).

```bash
sqlite3 seulgi/data/seulgi.db

.tables
SELECT * FROM trades ORDER BY opened_at DESC LIMIT 20;
SELECT * FROM positions WHERE closed_at IS NULL;
SELECT engine, COUNT(*), ROUND(SUM(realized_pnl),2) FROM trades GROUP BY engine;
```

---

## Logs

```bash
docker compose logs -f seulgi
docker compose logs -f scraper
docker compose logs -f scraper seulgi
```

Log prefixes: `[watcher]` `[markets]` `[engine/<name>]` `[ai]`

---

## Module map

| File | Role |
|---|---|
| `app/main.py` | Asyncio orchestrator — starts all loops, handles shutdown |
| `app/watcher.py` | SiteWatcher — polls scraper, emits RawHeadline to queue |
| `app/markets.py` | MarketIndex — Gamma API fetch, embedding, cosine search |
| `app/ai.py` | DirectionClassifier — OpenRouter LLM with retry |
| `app/engine.py` | EngineRunner — applies trading rules, tracks per-engine state |
| `app/trader.py` | Paper trade execution + position lifecycle |
| `app/dashboard.py` | Writes all .txt dashboard files, manages event ring buffers |
| `app/db.py` | Thin aiosqlite wrapper |
| `app/config.py` | YAML config loader with env var overrides |
| `app/profiler.py` | Lightweight span profiler (`Profiler` class + `prof` singleton) |

---

## Tests

```bash
# From project root
docker compose --profile test run --rm seulgi-test

# Locally
cd seulgi
pip install -r requirements.txt -r requirements.test.txt
python -m pytest app/tests/ -v
```

---

## Tuning guide

| Goal | Change |
|---|---|
| More trades | Lower `similarity_threshold` (0.25), lower `min_confidence` (0.35) |
| Fewer false positives | Raise `similarity_threshold` (0.50), raise `min_confidence` (0.60) |
| Faster news detection | Lower `change_wait_seconds` (10) — watch for scraper rate-limits |
| Reduce scraper load | Raise `change_wait_seconds` (30-60) or remove low-value sites |
| Test a different LLM | Change `ai.model` to any OpenRouter model ID — no rebuild needed |
| Compare strategies | Add entries under `engines:` with different thresholds |
| Debug slow spans | Check `profile.txt` — identify which stage has high p99 |
