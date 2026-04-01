# Irene — Polymarket Time-Sensitive Resolution Bot

Irene is a signal-driven trading bot for Polymarket that exploits the information lag between real-world events and market price updates.

## Core Thesis

Polymarket prices update on minute-to-hour timescales. News wires and public APIs update in seconds. Irene's edge is **faster information incorporation** — detecting events that should move a market probability and placing orders before the crowd reprices.

## Scope

**In scope:**
- Time-sensitive resolution markets (closing within 72 hours)
- Political, economic, crypto, weather, and general news-driven markets
- Paper trading mode (log decisions without executing)
- Live trading on Polygon via py-clob-client

**Out of scope:**
- Sports markets (excluded by config)
- Cross-market arbitrage
- Market making / passive liquidity provision

## Signal Pipeline

```
NewsAPI + RSS feeds
        │
        ▼
    [feeder]  ── entity extraction ──▶ signal embeddings
        │
        │  Redis pub/sub (irene:signals)
        ▼
    [trader]  ── CLOB price fetch ──▶ edge check ──▶ Kelly sizing ──▶ execute / log
        │
        ▼
  PostgreSQL (trades, positions, market_events)
        │
        ▼
    [monitor] FastAPI  (per-market P&L, signal log, Brier scores)
```

The `[scanner]` service runs independently: it polls the Gamma API for open markets near resolution, generates sentence-transformer embeddings, and writes the market index to PostgreSQL. The feeder queries this index via cosine similarity to find which markets a signal touches.

## Services

| Service   | Port | Role |
|-----------|------|------|
| `scanner` | —    | Gamma API poller, market index builder |
| `feeder`  | —    | NewsAPI + RSS ingestion, signal → market mapper |
| `trader`  | —    | Decision engine + py-clob-client execution |
| `monitor` | 8080 | FastAPI observability (trades, signals, P&L) |
| `postgres` | 5432 | Durable storage |
| `redis`   | 6379 | Hot-path signal pub/sub |

## Configuration

All behaviour is controlled by a volume-mounted `config.yaml`. The system rereads this file every 20 seconds — changes take effect without a container restart.

See [docs/config_schema.md](docs/config_schema.md) for the full schema.

## Quick Start

```bash
# Paper trading (no keys needed)
cp config.example.yaml config.yaml
docker compose up

# Monitor dashboard
open http://localhost:8080
```

## Directory Layout

```
irene/
├── config.example.yaml     # template config
├── docker-compose.yml
├── shared/                 # irene_shared Python package
│   └── irene_shared/
│       ├── config.py       # YAML hot-reload
│       ├── db.py           # PostgreSQL wrapper
│       ├── models.py       # shared Pydantic models
│       └── redis_client.py
├── scanner/                # Market index service
│   ├── Dockerfile
│   └── app/
│       ├── main.py
│       ├── gamma_client.py
│       ├── market_filter.py
│       ├── embedder.py
│       └── tests/
├── feeder/                 # Signal ingestion service
│   ├── Dockerfile
│   └── app/
│       ├── main.py
│       ├── sources/
│       │   ├── newsapi.py
│       │   └── rss.py
│       ├── mapper.py       # cosine similarity matching
│       └── tests/
├── trader/                 # Decision + execution service
│   ├── Dockerfile
│   └── app/
│       ├── main.py
│       ├── clob_client.py
│       ├── decision.py     # edge + Kelly
│       ├── executor.py     # order submission
│       ├── position_tracker.py
│       └── tests/
└── monitor/                # Observability FastAPI service
    ├── Dockerfile
    └── app/
        ├── main.py
        ├── routers/
        └── tests/
```
