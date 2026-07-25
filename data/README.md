# Data

Nothing in this directory tree is tracked. This file documents the layout so
that a fresh clone knows what to create and what can be shared.

## Layout

```
irene/data/
  irene.db                  SQLite — markets, articles, DKG nodes/edges, trades
  irene.db-wal, -shm        WAL sidecars (present while irene is running)
  chroma/                   vector store for article embeddings
  dashboard/                rendered text panels + plots
  sandbox/{runs,cache,sets,eval,verdicts}
                            backtest harness output

seulgi/data/
  seulgi.db                 SQLite — markets, news memory, decisions, trades
  news_chroma/              vector store for the news corpus
  dashboard/
```

The two agents **must not** share a database. `irene/app/db.py` enforces this
at runtime: `DB.connect()` raises `IreneDBPathError` if the resolved path is
named `seulgi.db` or contains a directory named `seulgi`. That guard exists
because a shared chroma directory corrupted an HNSW index on 2026-05-21.

## What is shareable

**Source rows are shareable; derived stores are not.**

The scraped articles, resolved markets, and trade decisions in each `.db` are
the real asset — they are expensive to collect (rate limits, per-host policy,
months of wall-clock) and are what a backtest actually needs. Copy those.

The chroma directories are *derived*: embeddings computed from rows already in
the database, plus an HNSW index built from those embeddings. They are large,
they do not survive being copied between machines with a different embedding
model, and they can be rebuilt. Do not treat them as data to preserve.

This is why `seulgi/snapshots/` was removed from git history during the
extraction — 246 MB across 1139 blobs, about 89% of the repository's total
weight, all of it rebuildable. See `docs/rename-from-jennie.md`.

## Copying a live database

Both agents run with SQLite in WAL mode. A plain `cp` of a live `.db` produces
a torn file — the WAL may hold committed pages the main file does not.

Use the online backup API instead, which takes a consistent snapshot without
blocking the running agent:

```python
import sqlite3
src = sqlite3.connect("file:irene/data/irene.db?mode=ro", uri=True, timeout=60)
dst = sqlite3.connect("/path/to/irene.db.snapshot")
src.backup(dst)
dst.close(); src.close()
```

Then verify before trusting it:

```python
sqlite3.connect("file:/path/to/irene.db.snapshot?mode=ro", uri=True) \
       .execute("PRAGMA integrity_check").fetchone()
```

## Rebuilding

Vector stores rebuild from rows already in the database — see each agent's
`scripts/` directory for the backfill entry points. Dashboard output regenerates
on the next cycle. The sandbox backtest outputs regenerate from
`irene/sandbox/` plus `irene/scripts/sandbox/`, both of which *are* tracked.
