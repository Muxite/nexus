# Irene — Architecture

## Design Principles

1. **Async throughout** — all services use `asyncio`. No blocking I/O on the hot path.
2. **Hot-reload config** — `config.yaml` is re-read every 20 seconds. All services hold a reference to the shared `Config` object, which is swapped atomically.
3. **Paper-first** — every trade decision is logged to PostgreSQL regardless of mode. Live execution is gated by `irene.mode: live` in config.
4. **Per-market observability** — every market gets its own event log row-stream so the monitor can show which market types win and lose.
5. **Fail-safe defaults** — missing config keys fall back to conservative defaults. If the CLOB is unreachable, the trader logs the intended order and skips.

---

## Inter-Service Communication

```
┌─────────┐   Gamma API   ┌──────────────────────────────────────────┐
│ scanner │──────────────▶│         PostgreSQL (irene_db)            │
└─────────┘               │                                          │
                          │  markets       (index + embeddings)      │
┌─────────┐               │  signals       (raw ingested signals)    │
│ feeder  │──────────────▶│  mapped_signals (signal↔market matches)  │
│         │               │  trades        (all decisions + fills)   │
│         │──────────────▶│  positions     (open positions)          │
│         │  Redis        │  market_events (per-market event log)    │
│         │  pub/sub      │  brier_scores  (calibration tracking)    │
└────┬────┘               └──────────────────────────────────────────┘
     │ irene:signals                ▲
     ▼                              │
┌─────────┐                         │
│ trader  │─────────────────────────┘
└─────────┘
     │
     ▼ CLOB API (read price + submit order)
```

Redis is used only for the hot-path signal channel (`irene:signals`). All durable state lives in PostgreSQL. The trader subscribes to the Redis channel and writes every decision (with outcome) back to PostgreSQL.

---

## Services

### scanner

**Responsibility:** Build and refresh the market index.

**Loop (every `scanner.poll_interval_seconds`, default 120s):**
1. Fetch all open markets from `gamma-api.polymarket.com/markets`
2. Filter by:
   - `endDate` within configured window (`markets.min_hours_to_close` – `markets.max_hours_to_close`)
   - Volume ≥ `markets.min_volume_usdc`
   - Question does not contain any `markets.exclude_keywords`
3. For markets not already in the index (or stale > `scanner.embedding_rebuild_interval_seconds`):
   - Embed `question + description` with `sentence-transformers/all-MiniLM-L6-v2`
   - Upsert row in `markets` table with `embedding` (stored as `REAL[]` / binary blob)
4. Expire markets past their `endDate` (set `active = false`)

**Config keys used:** `polymarket.gamma_url`, `markets.*`, `scanner.*`

---

### feeder

**Responsibility:** Ingest news signals and map them to open markets.

**Two concurrent loops:**

**A. Signal ingestion (every `signals.newsapi_poll_seconds`):**
1. Poll NewsAPI `everything` endpoint with a broad query
2. Poll each RSS feed URL in `signals.rss_feeds`
3. Deduplicate by URL + published-at timestamp
4. For each new article: store raw in `signals` table

**B. Signal mapping (runs after each ingestion batch):**
1. For each unmapped signal in `signals`:
   - Embed `title + description` with the same model as the scanner
   - Load all `active` market embeddings from PostgreSQL
   - Compute cosine similarity; keep matches ≥ `signals.similarity_threshold`
   - Take top-K (`signals.top_k_markets`) markets
2. For each match:
   - Write row to `mapped_signals`
   - Publish `MappedSignal` JSON to Redis channel `irene:signals`

**Config keys used:** `signals.*`

---

### trader

**Responsibility:** Turn a mapped signal into a trade decision, then optionally execute it.

**Loop:** Subscribes to Redis `irene:signals`. For each `MappedSignal`:

1. **Freshness check** — skip if signal older than `signals.max_signal_age_seconds`
2. **Exposure check** — skip if open positions ≥ `trading.max_open_positions`
3. **CLOB price fetch** — GET `/book` for the market's YES token; extract best bid/ask
4. **Probability estimate** — derive `p_hat` from signal (see Decision Logic below)
5. **Edge check** — `|p_hat - mid_price| > trading.min_edge + estimated_spread + fee_buffer`
6. **Liquidity check** — desired size ≤ `trading.max_liquidity_fraction × available_depth`
7. **Kelly sizing** — `size = kelly_fraction × (edge / odds) × balance`; clamp to `max_position_fraction × balance`
8. **Execute or log:**
   - `paper` mode: write `TradeDecision` to PostgreSQL with `executed=false`
   - `live` mode: submit FOK/FAK order via py-clob-client; write fill result to PostgreSQL

**Position exit:** A separate polling coroutine (every 60s) checks open positions:
- If market resolved (price → 0.98+): close position, record P&L
- If signal staleness timeout exceeded: close position at market

**Decision Logic (probability estimation):**

The signal → `p_hat` mapping is deliberately simple in v1. The feeder publishes a `direction` hint (YES / NO / NEUTRAL) derived from headline sentiment keywords (up/down, win/lose, pass/fail, etc.). The trader then:

```
if direction == YES:   p_hat = mid_price + edge_increment   (default +0.12)
if direction == NO:    p_hat = mid_price - edge_increment
if direction == NEUTRAL: skip (no actionable estimate)
```

`edge_increment` is tunable per market category via config. This is intentionally naive — the Brier score tracker will tell us if it's calibrated. Upgrading to an LLM-based estimator is a later iteration.

**Config keys used:** `trading.*`, `signals.max_signal_age_seconds`, `polymarket.clob_url`

---

### monitor

**Responsibility:** FastAPI observability layer. No writes. Read-only queries on PostgreSQL.

**Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | System status (scanner last-seen, feeder last-seen, positions count) |
| GET | `/markets` | Active market index with per-market P&L summary |
| GET | `/markets/{condition_id}/events` | Full event log for one market |
| GET | `/trades` | Trade log (paginated, filterable by date/outcome) |
| GET | `/signals` | Recent signals with match info |
| GET | `/positions` | Open positions |
| GET | `/calibration` | Brier score per signal source |

---

## Database Schema

```sql
-- Market index
CREATE TABLE markets (
    condition_id        TEXT PRIMARY KEY,
    slug                TEXT,
    question            TEXT NOT NULL,
    description         TEXT,
    category            TEXT,
    end_date            TIMESTAMPTZ NOT NULL,
    volume_usdc         REAL,
    token_yes           TEXT,
    token_no            TEXT,
    embedding           BYTEA,          -- pickle of np.ndarray float32
    embedding_updated_at TIMESTAMPTZ,
    active              BOOLEAN DEFAULT true,
    first_seen_at       TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
);

-- Raw ingested signals
CREATE TABLE signals (
    id                  SERIAL PRIMARY KEY,
    source              TEXT NOT NULL,  -- 'newsapi' | 'rss:<url>'
    url                 TEXT UNIQUE,
    title               TEXT NOT NULL,
    description         TEXT,
    published_at        TIMESTAMPTZ,
    ingested_at         TIMESTAMPTZ NOT NULL,
    embedding           BYTEA,
    mapped              BOOLEAN DEFAULT false
);

-- Signal → Market matches
CREATE TABLE mapped_signals (
    id                  SERIAL PRIMARY KEY,
    signal_id           INTEGER REFERENCES signals(id),
    condition_id        TEXT REFERENCES markets(condition_id),
    similarity_score    REAL NOT NULL,
    direction           TEXT,           -- 'YES' | 'NO' | 'NEUTRAL'
    direction_confidence REAL,
    created_at          TIMESTAMPTZ NOT NULL,
    dispatched          BOOLEAN DEFAULT false
);

-- All trade decisions (paper + live)
CREATE TABLE trades (
    id                  SERIAL PRIMARY KEY,
    mapped_signal_id    INTEGER REFERENCES mapped_signals(id),
    condition_id        TEXT NOT NULL,
    token_id            TEXT NOT NULL,
    direction           TEXT NOT NULL,   -- 'YES' | 'NO'
    p_hat               REAL NOT NULL,   -- our probability estimate
    mid_price           REAL NOT NULL,   -- market price at decision time
    edge                REAL NOT NULL,
    kelly_size_usdc     REAL NOT NULL,
    executed            BOOLEAN NOT NULL,
    order_type          TEXT,            -- 'FOK' | 'FAK'
    fill_price          REAL,
    fill_size_usdc      REAL,
    fill_at             TIMESTAMPTZ,
    order_id            TEXT,            -- CLOB order ID
    decided_at          TIMESTAMPTZ NOT NULL,
    closed_at           TIMESTAMPTZ,
    exit_price          REAL,
    exit_reason         TEXT,            -- 'resolved' | 'timeout' | 'manual'
    realized_pnl        REAL,
    outcome             TEXT,            -- 'WIN' | 'LOSS' | 'PUSH' (post-resolution)
    notes               TEXT
);

-- Open position tracker (derived, but kept for fast queries)
CREATE TABLE positions (
    trade_id            INTEGER PRIMARY KEY REFERENCES trades(id),
    condition_id        TEXT NOT NULL,
    token_id            TEXT NOT NULL,
    direction           TEXT NOT NULL,
    size_usdc           REAL NOT NULL,
    entry_price         REAL NOT NULL,
    opened_at           TIMESTAMPTZ NOT NULL,
    last_price          REAL,
    unrealized_pnl      REAL
);

-- Per-market event log (for observability)
CREATE TABLE market_events (
    id                  SERIAL PRIMARY KEY,
    condition_id        TEXT NOT NULL,
    event_type          TEXT NOT NULL,  -- 'signal_match' | 'trade_decided' | 'order_filled' | 'position_closed' | 'market_resolved'
    payload             JSONB,
    created_at          TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_market_events_condition ON market_events(condition_id, created_at DESC);

-- Calibration: actual outcome vs. our p_hat, grouped by signal source
CREATE TABLE brier_scores (
    id                  SERIAL PRIMARY KEY,
    trade_id            INTEGER REFERENCES trades(id),
    signal_source       TEXT NOT NULL,
    p_hat               REAL NOT NULL,
    actual_outcome      REAL NOT NULL,  -- 1.0 = YES won, 0.0 = NO won
    brier_component     REAL NOT NULL,  -- (p_hat - actual)^2
    recorded_at         TIMESTAMPTZ NOT NULL
);
```

---

## Jitter — Thundering-Herd Prevention

Every polling loop calls `jittered_sleep(base, jitter_fraction, rng)` instead of `asyncio.sleep(base)`. This spreads poll bursts so multiple services (or future scaled instances) don't all hit the Gamma API, NewsAPI, or RSS feeds in the same second.

```
jittered_sleep(120, 0.20)  →  actual sleep in [96s, 144s]
```

Three layers of jitter are applied:

| Layer | Where | Effect |
|-------|-------|--------|
| **Loop interval** | `scanner._loop`, `feeder._newsapi_loop`, `feeder._rss_loop` | Each iteration sleeps `base ± 20%` |
| **Startup offset** | `feeder._newsapi_loop`, `feeder._rss_loop` | Random delay before the first fetch (up to 50% of poll interval) so scanner and feeder don't both make their first external call at container-start t=0 |
| **Per-feed offset** | Inside `feeder._ingest_rss_feeds` | Each RSS feed gets an independent random delay (up to 50% of poll interval) before fetching, so N feeds don't fire simultaneously |

Each service holds its own `random.Random()` instance (`self._rng`) seeded from OS entropy at init. This does not touch the global `random` state.

**Config:**

```yaml
scanner:
  jitter_fraction: 0.20    # ±20% on poll_interval_seconds

signals:
  jitter_fraction: 0.20    # ±20% on newsapi_poll_seconds and rss_poll_seconds
  rss_startup_spread_fraction: 1.0  # startup delay = rng(0, rss_poll_seconds * this)
```

---

## Time Profiler

Every significant blocking operation is wrapped in a `Timer` context manager from `irene_shared.timing`. The timer:

1. Measures wall time via `time.perf_counter()`
2. Records a sample in the service's `ProfileStore` (per-operation ring buffer, default 200 samples)
3. Logs a structured JSON line at DEBUG level: `{"service": "scanner", "operation": "gamma_fetch", "duration_ms": 312.4, "ok": true}`
4. Marks the sample as `ok=False` if an exception propagates through the block

**Operations timed:**

| Service | Operation | What it covers |
|---------|-----------|----------------|
| scanner | `gamma_fetch` | Full Gamma API fetch (all pages) |
| scanner | `market_filter` | CPU filter pass over parsed markets |
| scanner | `embed_batch` | sentence-transformer batch encode |
| scanner | `db_upsert` | PostgreSQL upsert loop for all filtered markets |
| feeder | `newsapi_ingest` | Full NewsAPI poll + signal processing |
| feeder | `rss_ingest` | All RSS feeds + signal processing |
| feeder | `signal_embed` | Single signal embedding |
| feeder | `signal_map` | Cosine similarity scan + direction classification |

**ProfileStore** accumulates samples in a `deque(maxlen=window)` per operation. `summary()` computes mean, p50, p95, p99, error rate, and last value. The monitor surfaces this via `GET /metrics` by reading per-service summaries published to Redis every `timing.publish_interval_seconds`.

---

## Config Hot-Reload

The `ConfigManager` class in `irene_shared/config.py` runs a background coroutine that calls `asyncio.get_event_loop().run_in_executor` to re-read and re-parse `config.yaml` every 20 seconds. All services hold a `ConfigManager` reference and call `.get()` on each use — never cache the config object directly.

```python
class ConfigManager:
    def __init__(self, path: str): ...
    async def start(self): ...          # begins background reload loop
    def get(self) -> IreneConfig: ...   # returns current config snapshot
    async def stop(self): ...
```

If the YAML is invalid on reload, the previous config is kept and a warning is logged.

---

## Pydantic Models (shared)

```python
# irene_shared/models.py

class SignalSource(str, Enum):
    NEWSAPI = "newsapi"
    RSS = "rss"

class Direction(str, Enum):
    YES = "YES"
    NO = "NO"
    NEUTRAL = "NEUTRAL"

class TradeMode(str, Enum):
    PAPER = "paper"
    LIVE = "live"

class MarketRecord(BaseModel):          # from DB
class RawSignal(BaseModel):             # from feeder
class MappedSignal(BaseModel):          # published to Redis + stored
class TradeDecision(BaseModel):         # trader output
class OpenPosition(BaseModel):          # from positions table
class MarketEvent(BaseModel):           # event log entry
```

---

## Observability Per Market

Every significant event in a market's lifetime writes to `market_events`:

- `signal_match`: feeder found a match (similarity score, signal title)
- `trade_decided`: trader made a decision (p_hat, edge, size, executed flag)
- `order_filled`: CLOB fill received (fill_price, fill_size)
- `position_closed`: exit with reason and realized P&L
- `market_resolved`: outcome recorded (YES/NO, final price)

This makes it possible to replay exactly what happened in any market and diagnose false signals, mispriced edges, or execution failures.

The monitor `/markets/{condition_id}/events` endpoint returns this stream as a timeline. Aggregate P&L by market category (`market.category`) to identify which types of markets the signal pipeline wins vs. loses on.

---

## Signal Direction Extraction

A fast, zero-LLM keyword classifier assigns direction to each signal before dispatching:

```
YES signals: "passes", "wins", "approves", "reaches", "rises above", "elected",
             "signed", "confirmed", "hits", "exceeds", "above"

NO signals:  "fails", "loses", "rejects", "falls below", "suspended", "cancelled",
             "below", "vetoed", "denied", "misses"
```

Direction confidence = (matched_yes_count - matched_no_count) / total_matched, clamped to [0, 1]. Signals with `|confidence| < 0.3` are marked NEUTRAL and skipped.

This is tunable: a future config option `signals.use_llm_direction: true` can route unclear signals through a cheap LLM call (e.g. Haiku) for direction classification.

---

## Testing Strategy

Each service has its own `tests/` directory. The pattern:

```
service/app/tests/
├── conftest.py            # shared fixtures (in-memory DB, mock Redis, mock APIs)
├── test_<module>.py       # unit tests per module (no network, no real DB)
└── test_integration.py    # @pytest.mark.integration (real APIs, skipped in CI)
```

Unit tests use:
- `pytest-asyncio` for async tests
- `unittest.mock` / `respx` for HTTP mocking
- In-memory PostgreSQL via `pytest-postgresql` or a test-scoped Docker container

Integration tests are marked `@pytest.mark.integration` and skipped unless `IRENE_INTEGRATION_TESTS=true`.

---

## Code Reuse & Language Decisions

**Within Irene:** `irene_shared` is the single shared package. All cross-service models, config, DB helpers, and Redis utilities live there. Services import only from `irene_shared` — no cross-service imports.

**Outside Irene:** No dependency on Jennie or WebRAG libraries. If a utility already exists in those projects (e.g. Jennie's scraper), a clean rewrite in `irene_shared` is preferred over importing across project boundaries — it avoids coupling and keeps each system independently deployable.

**C++/Bazel:** Not used in v1. The only candidate would be the cosine similarity computation in the mapper (feeder service). At Irene's scale (~2000 markets, signals arriving every 30-60s), numpy batch cosine similarity completes in <5ms — C++ would shave microseconds at the cost of significant build complexity. If the market universe grows to 50,000+ markets or signal frequency rises to sub-second, a C++ similarity kernel (compiled to a Python extension via pybind11) is the right call. Flag this in code with a `# PERF: consider C++ if market_count > 50000` comment.

**Folder principle:** Each file belongs to the service that owns it. Cross-cutting concerns go in `shared/irene_shared/`. Within a service, group by responsibility (e.g. `sources/`, `routers/`) rather than flat files.

---

## Deployment

```
docker compose up          # all services
docker compose up scanner  # just the scanner
```

All containers mount the same `./config.yaml` at `/config/config.yaml`. The `irene_shared` package is volume-mounted into each service so local edits are reflected without rebuild during development.

Production: EC2 t3.medium or equivalent, Docker service with restart-always. PostgreSQL on RDS or a managed instance. Redis on ElastiCache or same host.
