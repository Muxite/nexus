# Irene — Roadmap

All phases follow the same discipline: **write tests first, then write the implementation**.
Tests live in `service/app/tests/`. Integration tests are skipped unless `IRENE_INTEGRATION_TESTS=true`.

---

## Phase 0 — Foundation

**Goal:** Everything compiles, containers start, config hot-reload works.

### 0.1 Shared Package (`irene_shared`)

> `timing.py` is the profiler and jitter module. It must be implemented before any service loop so all loops can import it from day one.

- [ ] `models.py` — Pydantic models: `MarketRecord`, `RawSignal`, `MappedSignal`, `TradeDecision`, `OpenPosition`, `MarketEvent`, `BrierScore`
- [ ] `config.py` — `IreneConfig` Pydantic model (full YAML schema) + `ConfigManager` (async hot-reload every 20s); includes `TimingSection` (`jitter_fraction`, `profile_window`, `publish_interval_seconds`) and per-section `jitter_fraction` overrides on `ScannerSection` and `SignalsSection`
- [ ] `timing.py` — `Timer` context manager, `ProfileStore` (per-operation ring-buffer of p50/p95/p99/error-rate), `jittered_sleep(base, fraction, rng)` for thundering-herd prevention; `_jitter_seconds` and `_percentile` are pure functions, fully unit-tested
- [ ] `db.py` — async PostgreSQL wrapper (asyncpg), schema migrations on startup, per-table CRUD helpers
- [ ] `redis_client.py` — async Redis wrapper (aioredis), pub/sub helpers for `irene:signals`
- [ ] `tests/test_config.py` — hot-reload test (write file, wait, check new value applied)
- [ ] `tests/test_models.py` — serialisation round-trips for all models
- [ ] `tests/test_timing.py` — Timer, ProfileStore, jitter bounds, jittered_sleep duration

### 0.2 Docker skeleton

- [ ] `docker-compose.yml` — postgres, redis, scanner, feeder, trader, monitor services
- [ ] `config.example.yaml` — all keys with documented defaults
- [ ] Each service: `Dockerfile` (python:3.12-slim base, install `irene_shared` + service deps)
- [ ] `shared/setup.py` — installable package

### 0.3 Database

- [ ] Migration script in `shared/irene_shared/migrations/001_initial.sql` — all tables from ARCHITECTURE.md
- [ ] `db.py` runs migration on first connect (idempotent `CREATE TABLE IF NOT EXISTS`)
- [ ] `tests/test_db.py` — CRUD for each table using pytest-asyncio + test DB

---

## Phase 1 — Scanner

**Goal:** Scanner populates the `markets` table with fresh embeddings. Can be run standalone.

### 1.1 Gamma API client (`scanner/app/gamma_client.py`)

- [ ] `GammaClient` — async, wraps `aiohttp.ClientSession`
- [ ] `fetch_open_markets(limit, offset) -> list[dict]` — paginates the Gamma API
- [ ] `tests/test_gamma_client.py` — mock HTTP, test pagination, test field extraction
- [ ] `@pytest.mark.integration` test hitting real Gamma API

### 1.2 Market filter (`scanner/app/market_filter.py`)

- [ ] `MarketFilter` — reads config on each call (`config.markets.*`)
- [ ] `filter(markets: list[dict]) -> list[dict]`
  - end_date within [min_hours, max_hours] window
  - volume ≥ min_volume_usdc
  - question does not match any exclude_keyword (case-insensitive)
- [ ] `tests/test_market_filter.py` — parametrized cases for each filter rule

### 1.3 Embedder (`scanner/app/embedder.py`)

- [ ] `Embedder` — loads `sentence-transformers/all-MiniLM-L6-v2` once on init
- [ ] `embed(text: str) -> np.ndarray` — returns float32 vector, normalised
- [ ] `embed_batch(texts: list[str]) -> list[np.ndarray]` — for bulk rebuild
- [ ] `tests/test_embedder.py` — shape check, determinism check, similarity sanity check

### 1.4 Scanner main loop (`scanner/app/main.py`)

- [ ] `Scanner` class — orchestrates GammaClient → MarketFilter → Embedder → DB upsert
- [ ] Async loop: sleep `scanner.poll_interval_seconds`, then run
- [ ] Writes `market_events` row (`event_type='scanner_refresh'`) with count of new/updated markets
- [ ] Graceful shutdown on SIGTERM (finishes current batch)
- [ ] `tests/test_scanner.py` — mock all dependencies, verify upsert logic

---

## Phase 2 — Feeder

**Goal:** Feeder ingests news, maps to markets, publishes to Redis.

### 2.1 NewsAPI source (`feeder/app/sources/newsapi.py`)

- [ ] `NewsAPISource` — async, uses `aiohttp`
- [ ] `fetch(since: datetime) -> list[RawSignal]`
- [ ] Deduplicates by URL (checks `signals` table before inserting)
- [ ] `tests/test_newsapi.py` — mock HTTP response, test dedup logic

### 2.2 RSS source (`feeder/app/sources/rss.py`)

- [ ] `RSSSource(url: str)` — async fetch + `feedparser` parse
- [ ] `fetch(since: datetime) -> list[RawSignal]`
- [ ] `tests/test_rss.py` — mock HTTP, test with sample RSS XML

### 2.3 Direction classifier (`feeder/app/direction.py`)

- [ ] `DirectionClassifier` — keyword-based, no LLM
- [ ] `classify(text: str) -> tuple[Direction, float]` — direction + confidence
- [ ] Keyword lists from ARCHITECTURE.md; config can override via `signals.yes_keywords` / `signals.no_keywords`
- [ ] `tests/test_direction.py` — parametrized yes/no/neutral headlines

### 2.4 Mapper (`feeder/app/mapper.py`)

- [ ] `SignalMapper` — loads active market embeddings from DB (cached, refreshed every 60s)
- [ ] `map(signal: RawSignal) -> list[MappedSignal]`
  - Embed signal text
  - Cosine similarity against market index
  - Filter ≥ similarity_threshold, take top_k
  - Attach direction from classifier
- [ ] `tests/test_mapper.py` — inject known embeddings, verify top-K and threshold filtering

### 2.5 Feeder main loop (`feeder/app/main.py`)

- [ ] `Feeder` class — runs ingestion and mapping concurrently
- [ ] On each mapped signal: write to `mapped_signals`, write `market_events`, publish to Redis
- [ ] `tests/test_feeder.py` — end-to-end mock (no network, no Redis): verify Redis publish called

---

## Phase 3 — Trader

**Goal:** Trader subscribes to signals, makes decisions, executes in paper mode.

### 3.1 CLOB client (`trader/app/clob_client.py`)

- [ ] `ClobClient` — async, wraps Polymarket CLOB REST API (no signing in paper mode)
- [ ] `get_orderbook(token_id: str) -> Orderbook` — returns best bid/ask + depth
- [ ] `get_mid_price(token_id: str) -> float`
- [ ] `tests/test_clob_client.py` — mock HTTP, verify bid/ask extraction

### 3.2 Decision engine (`trader/app/decision.py`)

- [ ] `DecisionEngine` — reads config on each call
- [ ] `evaluate(signal: MappedSignal, orderbook: Orderbook, balance: float) -> TradeDecision | None`
  - Compute `p_hat` from direction + edge_increment
  - Edge check: `|p_hat - mid| > min_edge + spread_estimate + fee`
  - Kelly: `size = kelly_fraction * (edge / odds) * balance`; clamp to `max_position_fraction * balance`
  - Liquidity: size ≤ `max_liquidity_fraction * available_depth`
  - Return `None` if any check fails (with reason logged)
- [ ] `tests/test_decision.py` — parametrized: edge too small, size too large, all checks pass

### 3.3 Executor (`trader/app/executor.py`)

- [ ] `Executor` — wraps py-clob-client for live mode; no-ops in paper mode
- [ ] `execute(decision: TradeDecision) -> FillResult | None`
  - Paper mode: return `None`, log decision
  - Live mode: sign + submit FOK/FAK order, poll for fill, return `FillResult`
- [ ] `tests/test_executor.py` — paper mode: verify decision is written to DB, no order submitted

### 3.4 Position tracker (`trader/app/position_tracker.py`)

- [ ] `PositionTracker` — polls open positions every 60s
- [ ] Check each position:
  - Fetch current CLOB mid-price
  - If market resolved (price ≥ 0.97): close + compute P&L + write `brier_scores`
  - If position age > timeout (config): close at market price
- [ ] Write `market_events` on each position close
- [ ] `tests/test_position_tracker.py` — mock CLOB prices, verify close logic

### 3.5 Trader main loop (`trader/app/main.py`)

- [ ] `Trader` class — subscribes to Redis `irene:signals`
- [ ] For each message: run freshness check → exposure check → `DecisionEngine.evaluate` → `Executor.execute`
- [ ] Spawns `PositionTracker` as separate coroutine
- [ ] `tests/test_trader.py` — mock Redis, CLOB, executor; verify full pipeline

---

## Phase 4 — Monitor

**Goal:** FastAPI dashboard for per-market observability.

### 4.1 Routers

- [ ] `routers/health.py` — `GET /health`: scanner last-seen, feeder last-seen, open positions count, config hash
- [ ] `routers/markets.py` — `GET /markets`: all active markets with trade count, win rate, total P&L; `GET /markets/{condition_id}/events`: paginated event log
- [ ] `routers/trades.py` — `GET /trades`: paginated, filter by date/outcome/condition_id
- [ ] `routers/signals.py` — `GET /signals`: recent signals with mapping results
- [ ] `routers/positions.py` — `GET /positions`: open positions with unrealized P&L
- [ ] `routers/calibration.py` — `GET /calibration`: Brier score per signal source, rolling 7d/30d/all-time

### 4.2 Monitor main (`monitor/app/main.py`)

- [ ] FastAPI app, lifespan: open DB pool on startup, close on shutdown
- [ ] CORS enabled for local dev
- [ ] `tests/test_routers.py` — TestClient, mock DB rows, verify response shapes

---

## Phase 5 — Integration & Paper Trading

**Goal:** Run the full system in paper mode. Verify the signal → decision pipeline end-to-end.

- [ ] End-to-end test: inject a synthetic signal, verify `market_events` contains `signal_match` + `trade_decided`
- [ ] Run for 24 hours in paper mode with real NewsAPI + Gamma API
- [ ] Review monitor dashboard: are signals matching sensible markets? Are directions reasonable?
- [ ] Tune `signals.similarity_threshold`, `trading.min_edge`, `trading.edge_increment` via config.yaml (no restart)
- [ ] Confirm Brier scores are being recorded on market resolution

---

## Phase 6 — Live Trading

**Goal:** Enable `irene.mode: live` with real capital.

Pre-conditions (all must pass before enabling live mode):
- [ ] Paper mode has logged ≥ 50 decisions with plausible P&L
- [ ] At least one full market resolution cycle recorded (brier score computed)
- [ ] Executor tested in live mode with minimum size (1 USDC) on a low-stakes market
- [ ] Circuit breaker tested: set `trading.max_open_positions: 0` → verify no orders submitted
- [ ] Drawdown limit tested: verify system enters paper mode if daily loss > configured threshold

---

## Phase 7 — Profiler Surface (Monitor)

**Goal:** Make timing data visible in the monitor so it's easy to see where latency lives.

- [ ] Each service publishes its `ProfileStore.summary_as_dict()` to Redis key `irene:profile:<service>` every `timing.publish_interval_seconds` (default 60s)
- [ ] `monitor/app/routers/metrics.py` — `GET /metrics`: reads all `irene:profile:*` keys from Redis, returns merged JSON with p50/p95/p99/error_rate per operation per service
- [ ] `monitor/app/routers/health.py` updated to include last-seen timestamps from Redis and a link to `/metrics`
- [ ] `tests/test_metrics.py` — mock Redis responses, verify response shape

---

## Deferred / Future

- LLM-based direction classification (`signals.use_llm_direction: true`)
- Brier score feedback loop: auto-adjust `edge_increment` per signal source based on rolling accuracy
- Additional signal sources: crypto exchange price feeds, social velocity (X/Twitter via Nitter)
- Frontend dashboard (React) for the monitor API
- Backtest mode: replay historical signals + prices against logged decisions
