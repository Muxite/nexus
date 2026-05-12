# Seulgi Architecture

## Overview

Seulgi is a news-driven paper trading bot for Polymarket prediction markets. The core hypothesis
is "rising edge" arbitrage: breaking news moves market odds before most traders react. We are
**the fastest slow bot** — we can't match FPGA latency, but we're faster than human analysts
because embedding lookups and batch AI calls are cached and parallelized. Our edge window is
roughly the gap between headline publication and when the crowd catches up (~30s–5min).

## Component Flow

```
News Sites (15)       ──┐
Twitter (@elonmusk)   ──┼──► [Watchers] ──► Queue ──► [EngineRunner]
Truth Social (@Trump) ──┘                                    │
                                                             ├──► embed(headline)
                                                             │    [all-MiniLM-L6-v2, 384D]
                                                             │
                                                             ├──► NewsMemory.check()
                                                             │    [ChromaDB dedup]
                                                             │
                                                             ├──► MarketIndex.find_matches()
                                                             │    [cosine similarity, numpy]
                                                             │
                                                             ├──► DB.bump_market_heat()
                                                             │    [match_count tracking]
                                                             │
                                                             └──► fan out to N engines ──┐
                                                                                         │
[Engine: aggressive] ◄──────────────────────────────────────────────────────────────────┤
[Engine: balanced  ] ◄──────────────────────────────────────────────────────────────────┤
[Engine: conservative] ◄────────────────────────────────────────────────────────────────┘
         │
         └──► PaperTrader.process()
                   │
                   ├──► DirectionClassifier.classify_batch()
                   │    [Gemini via OpenRouter, batched — all markets in one call]
                   │
                   ├──► _fetch_mid_price()   [Polymarket CLOB API]
                   │
                   └──► edge model → execute / skip
                              │
                              ├──► SeulgiDB  [aiosqlite, SQLite]
                              │
                              └──► PriceImpactTracker
                                   [records t0 price, checks t1m/t5m/t15m/t30m/t60m]
                                          │
                                          └──► Dashboard ──► /data/dashboard/*.txt
```

## Modules

| File | Responsibility |
|------|---------------|
| `main.py` | Entry point, wires everything together, runs background loops |
| `config.py` | Pydantic-free YAML config dataclasses; `_build_config()` parser |
| `watcher.py` | `SiteWatcher` (Playwright), `TwitterAccountWatcher` (Brave), `TruthSocialWatcher` (RSS) |
| `markets.py` | Gamma API fetcher, `Embedder` (sentence-transformers), `MarketIndex` (numpy similarity) |
| `engine.py` | `EngineRunner` (fan-out), `Engine` dataclass, `build_engines` factory |
| `trader.py` | `PaperTrader` — edge model, balance accounting, position lifecycle |
| `ai.py` | `DirectionClassifier` — batched Gemini calls, YES/NO/NEUTRAL + confidence |
| `db.py` | `SeulgiDB` — aiosqlite wrapper, idempotent migrations, all DB methods |
| `dashboard.py` | File-based dashboard; writes ~14 `.txt` files every 30s atomically |
| `news_memory.py` | `NewsMemory` — ChromaDB semantic deduplication (~0.88 similarity = republish) |
| `impact_tracker.py` | `PriceImpactTracker` — background loop filling price buckets at t1m/t5m/t15m/t30m/t60m |
| `profiler.py` | `@prof.traced` decorator for timing hot paths |

## Edge Model

```
edge = edge_increment × (2 × confidence − 1)
p_hat = mid_price + edge

Execute when: edge ≥ min_edge
```

| Engine | min_edge | edge_increment | min_confidence | Fire threshold |
|--------|----------|----------------|----------------|---------------|
| aggressive | 0.005 | 0.20 | 0.25 | conf ≥ 0.51 |
| balanced | 0.03 | 0.12 | 0.40 | conf ≥ 0.625 |
| conservative | 0.06 | 0.10 | 0.55 | conf ≥ 0.80 |

## Market Heat System

Every time a market matches a headline, its `match_count` is incremented in the DB
and `last_matched_at` is updated. On the next similarity search, frequently-matched
markets get a small score boost (capped at +5% at ≥5 matches). This surfaces "hot"
markets without hiding new ones.

Cold-position culling: if a position is losing and its market hasn't matched any
headline in `cold_position_hours` (default 12h), the position is closed at the current
price during the next monitor cycle. This prevents capital being tied up in markets
that news has stopped touching.

## Database Schema

```
markets         — Polymarket markets with embeddings (match_count, last_matched_at)
signals         — Raw headlines detected by watchers
trades          — Every trade decision (executed + skipped)
positions       — Currently open paper positions
engine_results  — Per-engine trade decisions (signal_latency_ms column)
price_impacts   — Hypothesis tracker: t0/t1m/t5m/t15m/t30m/t60m price buckets
```

Schema migrations are idempotent `ALTER TABLE` statements in `_MIGRATIONS` — silently
ignored if the column already exists. Safe to restart against an old DB.

## Configuration

All settings in `config.yaml`. Override with env vars:
```
SEULGI__TRADING__BALANCE_USDC=500
SEULGI__AI__MIN_CONFIDENCE=0.30
```

Key limits to tune:
- `markets.similarity_threshold` — lower = more matches (more AI calls, more noise)
- `trading.max_unique_markets` — cap on distinct markets in portfolio
- `trading.cold_position_hours` — how long before a losing cold market is culled
- `search_budget.brave_per_hour` — Brave API rate limit for Twitter watcher
