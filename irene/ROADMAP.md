# Irene — Roadmap

All phases follow the same discipline: **write tests first, then write the implementation**.
Tests live in `service/app/tests/`. Integration tests are skipped unless `IRENE_INTEGRATION_TESTS=true`.

---

## Phase 0 — Foundation ✅

**Goal:** Everything compiles, containers start, config hot-reload works.

### 0.1 Shared Package (`irene_shared`)

- [x] `models.py` — Pydantic models: `MarketRecord`, `RawSignal`, `MappedSignal`, `TradeDecision`, `OpenPosition`, `MarketEvent`, `BrierScore`
- [x] `config.py` — `IreneConfig` Pydantic model (full YAML schema) + `ConfigManager` (async hot-reload every 20s); includes `TimingSection`, `RateLimitsSection`, per-section `jitter_fraction` overrides
- [x] `timing.py` — `Timer`, `ProfileStore` (p50/p95/p99/error-rate ring-buffer), `jittered_sleep`
- [x] `db.py` — asyncpg wrapper, idempotent schema migrations, per-table CRUD helpers, `get_dashboard_stats()`
- [x] `redis_client.py` — aioredis pub/sub, heartbeats, last-seen, profile read/write
- [x] `ratelimit.py` — `RedisDailyBudgetLimiter` (per-key UTC-day quota), `RotatingApiKeyPool` (round-robin across multiple API keys)
- [x] `tests/test_config.py`, `test_db.py`, `test_redis_ratelimit.py`

### 0.2 Docker skeleton

- [x] `docker-compose.yml` — postgres, redis, scanner, feeder, trader, monitor services
- [x] All services: bind-mount `./logs:/logs`, `IRENE_LOG_DIR=/logs`, `PYTHONUNBUFFERED=1`, Docker json-file log rotation (20 MB × 5 files)
- [x] `config.example.yaml` — all keys with documented defaults
- [x] `.env.example` — template with all `IRENE__*` variables documented

### 0.3 Database

- [x] Migration in `shared/irene_shared/migrations/001_initial.sql` — all tables (markets, signals, mapped_signals, trades, positions, market_events, brier_scores)
- [x] `db.py` runs migration on first connect (idempotent)
- [x] `tests/test_db.py`

---

## Phase 1 — Scanner ✅

**Goal:** Scanner populates the `markets` table with fresh embeddings.

- [x] `GammaClient` — async paginated Gamma API fetch
- [x] `MarketFilter` — configurable time window, volume floor, keyword exclusions
- [x] `Embedder` — `sentence-transformers/all-MiniLM-L6-v2`, cached in Docker volume
- [x] `Scanner` main loop — Gamma → filter → embed → DB upsert → expire stale markets → heartbeat
- [x] Jitter on poll interval (`scanner.jitter_fraction`)
- [x] Embedding rebuild on `scanner.embedding_rebuild_interval_seconds`
- [x] Tests: gamma client, market filter, embedder, scanner integration

---

## Phase 2 — Feeder ✅

**Goal:** Feeder ingests news, maps to markets, publishes to Redis.

- [x] `NewsAPISource` — async fetch with dedup by URL; `RotatingApiKeyPool` for multiple keys
- [x] `RSSSource` — async fetch + feedparser; configurable feed list; startup stagger
- [x] `BraveNewsSource` — Brave Search API; `RotatingApiKeyPool` for multiple keys
- [x] `WatchedPageSource` — persistent Playwright (Chromium) tab per URL; MutationObserver detects DOM changes within seconds; falls back to jittered page reload on silence; in-session URL dedup; status published to Redis for dashboard visibility
- [x] `DirectionClassifier` — keyword-based YES/NO/NEUTRAL with confidence score
- [x] `SignalMapper` — cosine similarity against market embeddings; `similarity_threshold`, `top_k_markets`
- [x] Feeder main loop — concurrent NewsAPI + RSS + Brave polling; per-key rate limiting via Redis
- [x] Tests: newsapi, rss, brave, direction, mapper, feeder integration

---

## Phase 3 — Trader ✅

**Goal:** Trader subscribes to signals, makes decisions, executes in paper mode.

- [x] `ClobClient` — async CLOB orderbook fetch (no auth in paper mode)
- [x] `DecisionEngine` — 5-step: p_hat estimate → edge check → position cap → Kelly sizing → liquidity check
- [x] `Executor` — paper: DB write only; live: EIP-712 signed FOK/FAK order + fill poll
- [x] `PositionTracker` — background coroutine; closes resolved (price ≥ 0.97) or timed-out positions; writes Brier scores
- [x] Trader main loop — Redis subscriber; signal freshness check; exposure check; decision → execute
- [x] Tests: clob client, decision engine, executor (paper), position tracker, trader integration

---

## Phase 4 — Monitor ✅

**Goal:** FastAPI observability dashboard + plain-text volume export.

- [x] `GET /health` — service heartbeats (Redis last-seen), open positions count
- [x] `GET /markets` — active market index with trade count and realized P&L
- [x] `GET /markets/{id}/events` — paginated event timeline
- [x] `GET /trades` — paginated, filterable by condition_id / outcome
- [x] `GET /signals` — recent signals with match counts
- [x] `GET /positions` — open positions with unrealized P&L
- [x] `GET /calibration` — Brier score per signal source
- [x] `GET /metrics` — per-service operation latency profiles from Redis (p50/p95/p99)
- [x] `text_dashboard.py` — `dashboard.txt` written every 10s with: equity, scorecard (win bar chart), pipeline, services + latency, open positions table, recent closed trades, calibration table, skip reason histogram, timeline

---

## Phase 5 — Integration & Paper Trading ✅ / 🔄

**Goal:** Run the full system in paper mode; verify the signal → decision pipeline end-to-end.

- [x] End-to-end integration test: seed market + signal, run feeder mapper + trader, verify `market_events` contains `signal_match` + `trade_decided`
- [x] `config.yaml` and `.env` documented and wired; Compose `env_file` loads secrets; monitor on configurable host port
- [x] API key rotation tested: `IRENE__SIGNALS__NEWSAPI_KEYS=key1,key2` → both keys used, exhausted keys skipped until UTC midnight
- [x] File-based logging: `./logs/<service>.log` on host via bind-mount; `IRENE_LOG_DIR` env var
- [x] **24-hour demo run started** — aggressive config: `kelly_fraction: 0.75`, `min_edge: 0.02`, `max_open_positions: 64`, `similarity_threshold: 0.65`, markets window 1h–168h
- [ ] Review results after 24h: win rate, Brier scores, skip reason distribution
- [ ] Tune `similarity_threshold`, `edge_increment_*`, `kelly_fraction` based on results
- [ ] Confirm Brier scores recorded on market resolution

---

## Phase 6 — Live Trading

**Goal:** Enable `irene.mode: live` with real capital.

Pre-conditions (all must pass):
- [ ] Paper mode has logged ≥ 50 decisions with positive net P&L
- [ ] At least one full market resolution cycle (Brier score recorded)
- [ ] Executor tested in live mode with minimum size (1 USDC) on a low-stakes market
- [ ] Circuit breaker confirmed: `trading.max_open_positions: 0` → no orders submitted
- [ ] Drawdown limit confirmed: `irene.drawdown_limit` triggers paper fallback on daily loss

---

## Deferred / Future

- LLM-based direction classification (`signals.use_llm_direction: true`) — config plumbing exists, classifier not implemented
- Brier score feedback loop: auto-adjust `edge_increment_*` per signal source based on rolling accuracy
- Additional signal sources: crypto exchange feeds, social velocity (X/Twitter)
- Backtest mode: replay historical signals + prices against logged decisions
- React frontend for the monitor API (currently minimal vanilla JS)
