# Irene — Agent Instructions

## System Purpose

Irene is a Polymarket trading bot targeting **time-sensitive resolution markets** (closing within 72 hours). It ingests news wire signals, maps them to relevant markets via embedding similarity, computes a trading edge, and executes orders on Polygon via the Polymarket CLOB.

## Architecture Summary

```
scanner  →  PostgreSQL (market index with embeddings)
feeder   →  PostgreSQL (signals, mapped_signals) + Redis pub/sub (irene:signals)
trader   ←  Redis pub/sub → PostgreSQL (trades, positions, market_events)
monitor  ←  PostgreSQL → FastAPI (read-only observability)
```

See ARCHITECTURE.md for full detail.

## Key Design Rules

1. **All config comes from `config.yaml`** (hot-reloaded every 20s). Never hardcode thresholds. Always call `config_manager.get()` at call-site — do not cache the config object.

2. **Tests before implementation.** Every module gets a `tests/test_<module>.py` before the module is written. Unit tests must not hit the network, real DB, or real Redis.

3. **Paper mode is the default and safe.** In paper mode, `Executor.execute()` writes to the DB and returns `None` — no CLOB orders. Live mode is gated behind `irene.mode: live` in config.

4. **Per-market observability is non-negotiable.** Every significant event writes a row to `market_events` with a `condition_id`. This is how we identify which market types win and which lose.

5. **Async throughout.** Use `asyncio` + `asyncpg` + `aiohttp` + `aioredis`. No blocking I/O on the hot path. Use `run_in_executor` only for CPU-bound work (embedding generation).

6. **Prefer reuse within Irene.** Shared models, DB helpers, config, and Redis utilities live in `shared/irene_shared/`. Don't duplicate. Do not reach into Jennie or WebRAG shared libraries.

7. **Fail safe.** If the CLOB is unreachable, the trader logs the intended order and skips — it does not retry in a loop. If config reload fails, the previous config is used. If an embedding fails, the market is skipped.

## Directory Layout

```
irene/
├── shared/irene_shared/    # installable shared package — models, config, db, redis
├── scanner/app/            # Gamma API poller + embedder
│   └── tests/
├── feeder/app/             # NewsAPI + RSS + mapper + direction classifier
│   └── tests/
├── trader/app/             # CLOB client + decision + executor + position tracker
│   └── tests/
├── monitor/app/            # FastAPI read-only dashboard
│   └── tests/
├── config.example.yaml
└── docker-compose.yml
```

## Shared Package (`irene_shared`)

- `config.py` — `IreneConfig` (Pydantic), `ConfigManager` (async hot-reload)
- `models.py` — all cross-service Pydantic models
- `db.py` — `IreneDB` (asyncpg pool, schema init, CRUD helpers)
- `redis_client.py` — `IreneRedis` (aioredis, pub/sub helpers)

## Polymarket APIs

- **Gamma API** (`gamma-api.polymarket.com`) — market metadata, questions, volumes, end dates, categories. Read-only, no auth.
- **CLOB API** (`clob.polymarket.com`) — order books, order submission, fill status. EIP-712 signed orders for live mode.

Key Gamma API fields: `conditionId`, `question`, `description`, `endDate`, `volume`, `outcomePrices`, `category`, `tokens[].token_id`.

## Signal → Direction Keywords

Located in `feeder/app/direction.py`. The lists are the ground truth for the keyword classifier. Config can extend them via `signals.yes_keywords` / `signals.no_keywords`. Do not move this logic into the shared package — it belongs to the feeder.

## Embedding Model

`sentence-transformers/all-MiniLM-L6-v2` — loaded once at startup via `embedder.py` in both scanner and feeder. The model is ~80 MB and is cached in a Docker volume (`irene_model_cache`). Cosine similarity is computed in numpy. No GPU required.

## Database

PostgreSQL via `asyncpg`. Schema is in `shared/irene_shared/migrations/001_initial.sql` and applied idempotently on startup. All timestamps are `TIMESTAMPTZ` stored in UTC.

## Testing

```bash
docker compose run --profile test scanner-test
docker compose run --profile test feeder-test
docker compose run --profile test trader-test
docker compose run --profile test monitor-test
docker compose run --profile test shared-test
```

Integration tests require `IRENE_INTEGRATION_TESTS=true` in the test environment.

## Common Pitfalls

- `MappedSignal` must include `token_yes` / `token_no` IDs (from the market record) so the trader can fetch the CLOB book without another DB lookup.
- The scanner stores embeddings as `BYTEA` (pickled `np.ndarray float32`). The feeder loads these and computes similarity in numpy — avoid unnecessary deserialisation on every signal.
- Kelly sizing can produce very large numbers for high-edge signals. Always apply both `max_position_fraction * balance` AND the `max_liquidity_fraction * depth` cap as hard upper bounds.
- In paper mode, `positions` table is still populated — the tracker still runs. Paper positions are closed at the CLOB mid-price (no actual fill).
