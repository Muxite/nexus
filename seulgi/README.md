# Seulgi

**One container. One process. Dead simple news → trade pipeline.**

Seulgi is a stripped-down paper-trading bot for Polymarket. It watches a list of
news websites for content changes, matches changed headlines to open markets via
cosine similarity, asks Gemini whether the headline is directional, and places a
paper trade if it is.

## How it works

```
News sites (Playwright)
        │ DOM change detected
        ▼
  Headline text
        │ embed (all-MiniLM-L6-v2)
        ▼
  Market index (in-memory numpy matrix)
        │ top-K cosine similarity matches
        ▼
  Gemini Flash (per matched market)
        │ YES / NO / NEUTRAL
        ▼
  Paper trade → SQLite
        │ position tracker polls every 60s
        ▼
  Close at resolution or timeout → log P&L
```

## Architecture

Everything runs in a **single Python process** inside a single Docker container.
SQLite is the only persistence layer — no Postgres, no Redis, no separate services.

| Module | Responsibility |
|---|---|
| `main.py` | Asyncio orchestrator — starts all loops |
| `watcher.py` | Playwright MutationObserver per news site |
| `markets.py` | Gamma API fetch + embedding + cosine similarity |
| `ai.py` | Gemini Flash REST API direction classifier |
| `trader.py` | Paper trade decision + position tracking |
| `db.py` | Thin SQLite/aiosqlite wrapper |
| `config.py` | Config from `config.yaml` + env vars |

## Quickstart

```bash
cd jennie/seulgi

# Set your Gemini key
export GEMINI_API_KEY=your_key_here

# Build and run
docker compose up --build
```

Logs stream directly to stdout. SQLite DB is at `/data/seulgi.db` inside the
container (volume-mounted to `./data/` on host).

## Configuration

Edit `config.yaml`. Key settings:

```yaml
watched_sites:
  - url: "https://apnews.com"
    change_wait_seconds: 30      # how often to poll for DOM changes
    reload_seconds: 300          # reload page if silent for this long

markets:
  refresh_hours: 4              # re-fetch market list every N hours
  similarity_threshold: 0.35   # minimum cosine similarity to consider a match
  top_k: 10                    # max markets per headline

trading:
  balance_usdc: 1000.0
  min_edge: 0.04
  position_timeout_hours: 24

ai:
  model: "gemini-2.5-flash"
  api_key: ""                  # falls back to GEMINI_API_KEY env var
```

## Debugging

- All decisions logged with `[SEULGI]` prefix at INFO level
- Every signal match, AI call, and trade is recorded in SQLite
- `sqlite3 data/seulgi.db` to inspect tables: `signals`, `trades`, `positions`
- Set `LOG_LEVEL=DEBUG` env var for verbose output

## Running tests

```bash
docker build -f Dockerfile.test -t seulgi-test .
docker run --rm seulgi-test
```
