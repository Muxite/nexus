# Irene — Agent Instructions

## System Purpose

Irene is a Polymarket trading bot targeting news-driven markets. It ingests signals from NewsAPI, RSS feeds, and Brave Search; maps them to relevant markets via embedding similarity; computes a trading edge; and executes orders on Polygon via the Polymarket CLOB.

## Architecture Summary

```
scanner  →  PostgreSQL (market index: embeddings + metadata)
feeder   →  PostgreSQL (signals, mapped_signals) + Redis pub/sub (irene:signals)
trader   ←  Redis pub/sub → PostgreSQL (trades, positions, market_events, brier_scores)
monitor  ←  PostgreSQL + Redis → FastAPI (read-only) + dashboard.txt (volume)
```

## Key Design Rules

1. **Non-secret settings in `config.yaml`** (hot-reloaded every 20s). **Secrets** via `IRENE__<section>__<key>` env vars, typically from `irene/.env` via Compose `env_file`. NewsAPI and Brave support multiple keys: `IRENE__SIGNALS__NEWSAPI_KEYS=key1,key2` or `IRENE__SIGNALS__BRAVE_API_KEYS=key1,key2` (comma-separated). Rate limiting is per-key per UTC day via `RotatingApiKeyPool`. Always call `config_manager.get()` at call-site — never cache the config object.

2. **Tests before implementation.** Every module gets `tests/test_<module>.py` before writing the module. Unit tests must not hit network, real DB, or real Redis.

3. **Paper mode is the default and safe.** `Executor.execute()` in paper mode writes to DB and returns `None` — no CLOB orders. Live mode requires `irene.mode: live` in config and `polymarket.private_key` set.

4. **Per-market observability is non-negotiable.** Every significant event writes a row to `market_events` with `condition_id`. This is how we identify which market types win and which lose.

5. **Async throughout.** Use `asyncio` + `asyncpg` + `aiohttp` + `aioredis`. No blocking I/O on the hot path. Use `run_in_executor` for CPU-bound work (embedding generation).

6. **Prefer reuse within Irene.** Shared models, DB helpers, config, timing, and Redis utilities live in `shared/irene_shared/`. Do not reach into other Jennie projects.

7. **Fail safe.** CLOB unreachable → log + skip, no retry loop. Config reload failure → retain previous config. Embedding failure → skip the market. API key exhausted → rotate to next key; if all exhausted, skip the poll.

## Directory Layout

```
irene/
├── shared/irene_shared/    # installable shared package
│   ├── config.py           # IreneConfig Pydantic + ConfigManager hot-reload
│   ├── db.py               # asyncpg pool, schema init, CRUD, get_dashboard_stats()
│   ├── models.py           # all cross-service Pydantic models
│   ├── ratelimit.py        # RedisDailyBudgetLimiter + RotatingApiKeyPool
│   ├── redis_client.py     # pub/sub, heartbeats, last-seen, profiler
│   ├── timing.py           # Timer, ProfileStore, jittered_sleep
│   └── migrations/
│       └── 001_initial.sql
├── scanner/app/            # Gamma API poller + embedder
│   └── tests/
├── feeder/app/             # NewsAPI + RSS + Brave mapper + direction classifier
│   ├── sources/
│   │   ├── newsapi.py
│   │   ├── rss.py
│   │   └── brave.py        # Brave Search API source (RotatingApiKeyPool)
│   └── tests/
├── trader/app/             # CLOB client + decision + executor + position tracker
│   └── tests/
├── monitor/app/            # FastAPI read-only dashboard + text_dashboard.py
│   └── tests/
├── config.example.yaml
├── .env.example
└── docker-compose.yml
```

## Shared Package (`irene_shared`)

- `config.py` — `IreneConfig` (Pydantic), `ConfigManager` (async hot-reload, env overlay)
- `models.py` — all cross-service models
- `db.py` — `IreneDB`: asyncpg pool, schema, CRUD helpers, `get_dashboard_stats()`
- `redis_client.py` — `IreneRedis`: pub/sub, heartbeats, `get_last_seen()`, `get_profile()`
- `ratelimit.py` — `RedisDailyBudgetLimiter`: per-key UTC-day counter with Redis TTL; `RotatingApiKeyPool`: round-robins keys, skips exhausted keys
- `timing.py` — `Timer`, `ProfileStore` (p50/p95/p99/error_rate), `jittered_sleep`

## Signal Sources (feeder)

| Source | Module | Rate limit key | Notes |
|--------|--------|----------------|-------|
| NewsAPI | `sources/newsapi.py` | `rate_limits.newsapi_max_per_day` per key | `RotatingApiKeyPool`; in-session URL dedup |
| RSS | `sources/rss.py` | `rate_limits.rss_http_max_per_day` shared | HTTP conditional GET (ETag/304); persistent instance per URL; in-session dedup |
| Brave Search | `sources/brave.py` | `rate_limits.brave_max_per_day` per key | `RotatingApiKeyPool`; rotates queries; in-session URL dedup |
| Watched pages | `sources/watched.py` | none (browser, no HTTP polling) | Persistent Playwright tab per URL; MutationObserver fires on DOM change; reload after silence |

### Watched Page Source details

`WatchedPageSource` uses `playwright.async_api` (headless Chromium, no images). Each instance:
1. Opens one tab, navigates to the URL once.
2. Injects `_OBSERVER_JS` — a `MutationObserver` watching `document.body` for `childList` mutations. New `<a href>` nodes are buffered in `window._ireneChanges[]`.
3. Polls `window._ireneReady` every `change_wait_seconds` (20–30s). On `true`: calls `_DRAIN_JS` to extract and reset, yields `RawSignal` objects.
4. If nothing fires for `reload_seconds ± jitter_fraction`, does a `page.reload()` and re-injects the observer.

`Feeder._watched_manager_loop` (runs every 60s) reconciles running tasks against `config.yaml`. Hot-reload aware — new URLs start a tab, removed URLs cancel their task. Crashed tasks are restarted automatically.

Signals flow through the same `_process_signals` path as RSS/NewsAPI (embed → cosine map → Redis publish).

Status of each watched tab is published to Redis via the feeder profile (`_watched_pages` key) and shown in `dashboard.txt` under SERVICES.

## Decision Engine (5 steps)

Located in `trader/app/decision.py`. Evaluated on every Redis signal:

1. **p_hat** — `mid_price + edge_increment_yes` (YES) or `mid_price - edge_increment_no` (NO), clamped to [0.01, 0.99]
2. **Edge check** — `|p_hat - mid| - (spread/2 + fee_buffer) > min_edge` or skip
3. **Position cap** — `open_positions < max_open_positions` or skip
4. **Kelly sizing** — `kelly_fraction × (net_edge / (1 - price_to_buy)) × balance`, capped at `max_position_fraction × balance`
5. **Liquidity check** — `kelly_size ≤ max_liquidity_fraction × available_depth` or cap

## Polymarket APIs

- **Gamma API** (`gamma-api.polymarket.com`) — market metadata, volumes, end dates. Read-only, no auth. Key fields: `conditionId`, `question`, `endDate`, `volume`, `outcomePrices`, `tokens[].token_id`.
- **CLOB API** (`clob.polymarket.com`) — orderbooks, order submission, fill status. EIP-712 signed orders in live mode.

## Embedding Model

`sentence-transformers/all-MiniLM-L6-v2` — loaded once at startup in both scanner and feeder. ~80 MB, cached in Docker volume `irene_model_cache`. Cosine similarity in numpy. No GPU required. Stored in DB as `BYTEA` (pickled `np.ndarray float32`).

## Observability

- **`GET /metrics`** — per-service `ProfileStore` data published to Redis `irene:profile:<service>` every `timing.publish_interval_seconds`
- **`dashboard.txt`** — written to `./data/dashboard.txt` every 10s. Sections: equity, scorecard (win bar chart), pipeline, services + latency, open positions table, recent closed trades, calibration, skip reason histogram, timeline
- **`./logs/<service>.log`** — file logging via `IRENE_LOG_DIR=/logs`; all services write here when the env var is set. Bind-mounted from host `irene/logs/`

## Testing

```bash
docker compose run --rm --profile test scanner-test
docker compose run --rm --profile test feeder-test
docker compose run --rm --profile test trader-test
docker compose run --rm --profile test monitor-test
docker compose run --rm --profile test shared-test

# Integration tests (hit real APIs):
IRENE_INTEGRATION_TESTS=true docker compose run --rm --profile test scanner-test
```

## Common Pitfalls

- `MappedSignal` must include `token_yes` / `token_no` IDs from the market record so the trader can fetch the CLOB book without an extra DB lookup.
- Scanner stores embeddings as `BYTEA` (pickled float32). The feeder loads all active embeddings once and refreshes every 60s — avoid deserialising on every signal.
- Kelly sizing can produce large numbers on high-edge signals. Always apply both `max_position_fraction × balance` AND `max_liquidity_fraction × depth` as hard upper bounds.
- In paper mode, `positions` is still populated and `PositionTracker` still runs. Paper positions are closed at CLOB mid-price (no actual fill).
- API key rotation: always use `IRENE__SIGNALS__NEWSAPI_KEYS` (plural) for multi-key configs. The legacy `NEWSAPI_KEY` single-key var still works but `NEWSAPI_KEY2` etc. do **not** — they are ignored. Comma-separate multiple keys in the plural var.
- `RotatingApiKeyPool` tracks usage in Redis with a per-key fingerprint (SHA-256 first 16 chars). Counters reset at UTC midnight automatically via TTL.
