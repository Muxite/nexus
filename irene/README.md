# Irene — Polymarket Signal-Driven Trading Bot

Irene is a signal-driven trading bot for Polymarket that exploits the information lag between real-world events and market price updates.

## Core Thesis

Polymarket prices update on minute-to-hour timescales. News wires and public APIs update in seconds. Irene's edge is **faster information incorporation** — detecting events that should move a market probability and placing orders before the crowd reprices.

## Scope

**In scope:**
- News-driven markets across politics, economics, crypto, and world events
- Markets closing within 1 hour to 1 week (configurable window)
- Paper trading mode (log decisions without executing)
- Live trading on Polygon via py-clob-client

**Out of scope:**
- Sports markets (excluded by config keyword filter)
- Cross-market arbitrage
- Market making / passive liquidity provision

## Signal Pipeline

```
NewsAPI + RSS feeds + Brave Search + Watched pages (Playwright)
        │
        ▼
    [feeder]  ── embed signal ──▶ cosine similarity vs market index
        │
        │  Redis pub/sub (irene:signals)
        ▼
    [trader]  ── CLOB price fetch ──▶ edge check ──▶ Kelly sizing ──▶ execute / log
        │
        ▼
  PostgreSQL (trades, positions, market_events, brier_scores)
        │
        ▼
    [monitor] FastAPI + text dashboard (P&L, positions, signals, calibration)
```

The `[scanner]` service runs independently: it polls the Gamma API for open markets, generates sentence-transformer embeddings, and writes the market index to PostgreSQL. The feeder queries this index via cosine similarity to find which markets each signal touches.

## Services

| Service    | Port | Role |
|------------|------|------|
| `scanner`  | —    | Gamma API poller, market index builder |
| `feeder`   | —    | NewsAPI + RSS + Brave ingestion, signal → market mapper |
| `trader`   | —    | Decision engine + py-clob-client execution, position tracker |
| `monitor`  | host `${IRENE_MONITOR_HOST_PORT:-18080}` → 8080 | FastAPI observability |
| `postgres` | 5432 | Durable storage |
| `redis`    | 6379 | Signal pub/sub, rate-limit counters, profiler data |

## Configuration

**Non-secret settings** live in a volume-mounted `config.yaml`. The system rereads this file every 20 seconds — changes take effect without a container restart.

**Secrets and API keys** go in `irene/.env` using the `IRENE__<section>__<key>` form. For NewsAPI and Brave, pass multiple keys as comma-separated values:

```bash
IRENE__SIGNALS__NEWSAPI_KEYS=key1,key2      # round-robin with per-key daily quota
IRENE__SIGNALS__BRAVE_API_KEYS=key1,key2
```

Legacy single-key vars (`IRENE__SIGNALS__NEWSAPI_KEY`) still work and are merged into the list. Compose loads `.env` via `env_file` — keys are not listed in `docker-compose.yml`.

**Host port for the monitor:** set `IRENE_MONITOR_HOST_PORT` in `irene/.env` or shell before `docker compose up`. Compose substitutes it in `docker-compose.yml` (default `18080`).

**Rate limits (UTC day):** `rate_limits` in `config.yaml` sets soft daily caps enforced via `RedisDailyBudgetLimiter` (`shared/irene_shared/ratelimit.py`). For NewsAPI and Brave the limit is **per API key**; `RotatingApiKeyPool` round-robins keys and skips exhausted ones until the next UTC midnight. Default `brave_max_per_day: 500` targets the paid Brave Search API Basic plan — align with your dashboard quota. RSS and CLOB quotas are shared buckets across all feeds/requests. Use `0` for unlimited.

See [docs/config_schema.md](docs/config_schema.md) for the full schema.

## Quick Start

```bash
cd irene
cp config.example.yaml config.yaml
cp .env.example .env
# Edit .env: set IRENE__SIGNALS__NEWSAPI_KEYS and IRENE__SIGNALS__BRAVE_API_KEYS
# (RSS + Gamma work without keys — they just provide fewer signals)
docker compose up -d

# Monitor dashboard
open http://localhost:18080

# Text dashboard (updates every 10s)
watch -n 10 cat irene/data/dashboard.txt

# Live logs from all services
tail -f irene/logs/*.log
```

## Observability

### HTTP API (monitor)

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Service heartbeats, open positions count |
| `GET /markets` | Active market index with per-market trade count and P&L |
| `GET /markets/{id}/events` | Full event timeline for a single market |
| `GET /trades` | Paginated trade log, filterable by condition_id / outcome |
| `GET /signals` | Recent ingested signals with match counts |
| `GET /positions` | Open positions with unrealized P&L |
| `GET /calibration` | Brier score per signal source |
| `GET /metrics` | Per-service operation latency profiles (p50/p95/p99) |

### Text dashboard (`irene/data/dashboard.txt`)

Updated every 10 seconds. Sections:
- **Equity & P&L** — starting balance, implied equity, realized/unrealized breakdown
- **Scorecard** — win/loss/flat counts, win rate, executed vs skipped trades, visual bar chart
- **Pipeline** — active markets, signals ingested, mapped signals, dispatch counts
- **Services** — Redis last-seen timestamps + per-operation latency for scanner/feeder/trader
- **Open positions** — table with direction, size, entry/last price, unrealized P&L, age
- **Recent closed trades** — last 15 with entry/exit, realized P&L, outcome, exit reason
- **Calibration** — Brier scores per signal source vs 0.25 random baseline
- **Skip reasons** — histogram of why trades were rejected
- **Timeline** — first signal ingested, first trade decided, last trade closed

### File logs (`irene/logs/`)

All services write structured logs to `irene/logs/<service>.log` (mounted volume). Docker stdout is also captured with `json-file` rotation (20 MB × 5 files).

```bash
tail -f irene/logs/trader.log       # trade decisions and executions
tail -f irene/logs/feeder.log       # signal ingestion and mapping
tail -f irene/logs/scanner.log      # market index refreshes
tail -f irene/logs/*.log            # all at once
docker compose logs -f trader       # Docker stdout (same content)
```

### Web UI

`http://localhost:<host-port>/` redirects to `/ui/`, a minimal signals viewer that polls `GET /signals` every 30 seconds. Static files live in `frontend/` and are copied into the monitor image.

### JSON export (`irene/data/signals.json`)

With `monitor.signals_export_dir` set (default `/data/exports`), the monitor writes `signals.json` atomically every `signals_export_interval_seconds` (default 300s). Set the path to `""` or interval to `0` to disable.

## Docker Logs — Normal vs. Noise

- **Redis `Memory overcommit must be enabled`:** On the **host**, run `sudo sysctl vm.overcommit_memory=1` and add `vm.overcommit_memory = 1` to `/etc/sysctl.d/99-redis.conf`. This is a host kernel setting — the container cannot change it.
- **Redis `no config file specified`:** Informational; the image runs with defaults.
- **Scanner/feeder Hugging Face unauthenticated warnings:** Harmless. Set `HF_TOKEN` in `.env` for higher Hub rate limits when downloading the embedding model.
- **SentenceTransformer HTTP 404 on `adapter_config.json` / "UNEXPECTED" keys:** Harmless; the loader probes optional paths.
- **Services showing `—` in dashboard `SERVICES` section:** Redis `last_seen` keys have a 5-minute TTL. Give containers ~30 seconds after startup; timestamps will appear once the first work cycle completes.

## 24-Hour Paper Test

1. `irene.mode: paper` in `config.yaml` (safe default — no real orders).
2. Set API keys in `.env` (`IRENE__SIGNALS__NEWSAPI_KEYS`, `IRENE__SIGNALS__BRAVE_API_KEYS`).
3. `docker compose up -d` from `irene/`.
4. Monitor with `watch -n 10 cat irene/data/dashboard.txt` or `http://localhost:28080`.
5. After the run, review `GET /calibration` for Brier scores and `GET /trades` for skip reason patterns.
6. Tune `signals.similarity_threshold`, `trading.min_edge`, `trading.edge_increment_*` live via `config.yaml` — changes apply within 20 seconds.

## Directory Layout

```
irene/
├── config.example.yaml        # template config — copy to config.yaml
├── config.yaml                # live config (hot-reloaded every 20s)
├── .env.example               # template secrets
├── .env                       # live secrets (not committed)
├── docker-compose.yml
├── data/                      # monitor exports (bind-mounted, host-visible)
│   ├── dashboard.txt          # plain-text dashboard (updated every 10s)
│   └── signals.json           # recent signals export
├── logs/                      # service logs (bind-mounted, host-visible)
│   ├── scanner.log
│   ├── feeder.log
│   ├── trader.log
│   └── monitor.log
├── docs/
│   └── config_schema.md       # full config reference
├── frontend/                  # static monitor UI; copied into monitor image
├── shared/                    # irene_shared Python package
│   └── irene_shared/
│       ├── config.py          # IreneConfig Pydantic model + ConfigManager hot-reload
│       ├── db.py              # asyncpg wrapper, schema migrations, CRUD + get_dashboard_stats()
│       ├── models.py          # shared Pydantic models
│       ├── ratelimit.py       # RedisDailyBudgetLimiter + RotatingApiKeyPool
│       ├── redis_client.py    # pub/sub, heartbeats, profiler read/write
│       ├── timing.py          # Timer, ProfileStore, jittered_sleep
│       └── migrations/
│           └── 001_initial.sql
├── scanner/
│   ├── Dockerfile
│   └── app/
│       ├── main.py            # Scanner loop
│       ├── gamma_client.py
│       ├── market_filter.py
│       ├── embedder.py        # sentence-transformers/all-MiniLM-L6-v2
│       └── tests/
├── feeder/
│   ├── Dockerfile
│   └── app/
│       ├── main.py            # Feeder loop (NewsAPI + RSS + Brave concurrent)
│       ├── sources/
│       │   ├── newsapi.py
│       │   ├── rss.py
│       │   ├── brave.py       # Brave Search API source
│       │   └── watched.py     # Playwright persistent tab; MutationObserver on DOM changes
│       ├── mapper.py          # cosine similarity matching
│       ├── direction.py       # keyword-based direction classifier
│       └── tests/
├── trader/
│   ├── Dockerfile
│   └── app/
│       ├── main.py            # Trader loop (Redis subscriber)
│       ├── clob_client.py
│       ├── decision.py        # 5-step edge + Kelly engine
│       ├── executor.py        # order submission (paper: DB only; live: CLOB)
│       ├── position_tracker.py
│       └── tests/
└── monitor/
    ├── Dockerfile
    └── app/
        ├── main.py            # FastAPI app + export loops
        ├── text_dashboard.py  # plain-text dashboard.txt writer
        └── tests/
```
