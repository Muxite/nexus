# Seulgi — Change Log

## Current (yujin branch)

### Scraper overhaul (`services/scraper`)

**Multi-method fallback stack**
- New method order: `requests → cloudscraper → playwright → selenium`
- `_PLAYWRIGHT_PREFERRED` frozenset routes bloomberg, ft, wsj, nytimes, washingtonpost directly to playwright, bypassing requests/cloudscraper
- `_SELENIUM_REQUIRED` frozenset routes x.com, twitter, instagram, facebook, linkedin to selenium
- Per-domain promotion: `min_method_idx` advances when a method repeatedly fails; recovers one step on consecutive successes

**Preflight system**
- `_preflight(url)` issues a HEAD request on first visit to an unknown domain
- Returns `"reachable"` → start at requests, `"challenge"` → start at cloudscraper, `"unreachable"` → start at playwright
- Preflight result is cached per-domain for 300 s (`_PREFLIGHT_CACHE_TTL`)

**Adaptive rate limiting**
- Per-domain `per_domain_interval` with Gaussian jitter (`random.gauss(mu, mu*0.3)`)
- `_on_429`: bumps interval × 1.5 (cap 30 s)
- `_on_success`: after 5 consecutive successes, decays × 0.9 toward `_DOMAIN_MIN_INTERVAL`

**Caching**
- L1: in-memory TTL cache (default 45 s, `CACHE_TTL_SECONDS`)
- L2: SQLite persistent cache for `_SLOW_CHANGING_HOSTS` (default 300 s, `PERSISTENT_CACHE_TTL_SECONDS`); survives restarts
- L1 hit → return immediately; L2 hit → return and promote to L1

**Trace IDs**
- Every `fetch()` call carries a `trace_id` (auto-generated `uuid4().hex[:8]` if not provided)
- Logged at every step: `[tid=X] fetch start`, `trying method=`, `success method=`, `L1/L2 cache hit`
- Returned in result dict as `"trace_id"`

**IP rotation hooks (future-ready)**
- `ProxyManager` class with `get_proxy(url) → str | None` (default returns `None`)
- Subclass and assign to `scraper._proxy_manager` to activate

**New API endpoints**
- `GET /methods` — per-domain stats (preferred method, success rate, avg latency, preflight result, adaptive interval)
- `GET /cache/stats` — memory and SQLite hit/miss/eviction counts

**Profiler integration**
- `prof.span("fetch/cache")`, `prof.span("fetch/preflight")`, `prof.span("fetch/{method}")` on every code path
- `GET /profile` returns profiler snapshot

---

### Seulgi core changes

#### Market cap (`markets.py`)
- `max_tracked_markets: 300` in config (was unbounded)
- `_fetch_gamma_markets_capped()` sorts Gamma results by volume descending, slices to top N
- Prevents embedding thousands of low-liquidity markets that would never trade

#### Headline traceability (`watcher.py`, `db.py`, `dashboard.py`, `engine.py`, `trader.py`)
- `RawHeadline` now has `headline_id: str` — 12-char hex UUID generated at detection time
- `headline_id` is propagated through: `signals` table → `trades` table → `engine_results` table
- All log lines that touch a headline include `[hid=X]`
- `Dashboard.record_news()`, `record_trade()` accept and log `headline_id`
- `Dashboard.record_trace()` adds a line to `trace_log.txt`:
  `hid=X | site | headline | market | AI direction | trade outcome`

#### News deduplication (`news_memory.py`)
- New `NewsMemory` class backed by ChromaDB (persistent HNSW, cosine space)
- `check(headline, embedding, site_url, article_url) → (is_republish, similar_to)`
- Distance threshold 0.12 (≈ cosine similarity 0.88) — configurable via `news_memory.dedup_distance_threshold`
- Gracefully degrades to no-op if `chromadb` is not installed
- Wired into `EngineRunner.process()` before market matching

#### AI republish awareness (`ai.py`)
- When `is_republish=True` and `similar_to` is set, injects `_REPUBLISH_BLOCK` into the prompt
- LLM is told the market may have already priced in the news and to return NEUTRAL if no new info

#### AI batched classification (`ai.py`) — new
- `classify_batch(headline, matches)` evaluates ALL matched markets in a single API call
- **10-15× cheaper** than per-market `classify()` for headlines with many matches
- Falls back to per-market on batch parse failure
- Tiered model routing: set `ai.premium_model` to route high-similarity batches (≥ `premium_sim_threshold: 0.70`) through a better model
- `response_format: json_object` forces clean JSON output
- System message added for better LLM role priming
- Mid-price and similarity score now included in the prompt as context

#### Profiler (`profiler.py`, `seulgi/app/profiler.py`)
- Lightweight span profiler, zero external deps
- `with prof.span("name")` or `@prof.traced("name")` decorator
- Writes `profile.txt` to dashboard dir every cycle
- `prof.snapshot()` returns dict for API/logging use

#### Configuration additions (`config.py`, `config.yaml`)
- `markets.max_tracked_markets: 300`
- `news_memory.path` and `news_memory.dedup_distance_threshold`
- `ai.premium_model` (optional) and `ai.premium_sim_threshold: 0.70`

---

### Tests added

| File | Coverage |
|------|---------|
| `services/scraper/tests/test_scraper.py` | +35 tests: preflight, adaptive interval, ProxyManager, trace_id, SQLite cache, `/methods`, `/cache/stats` |
| `services/scraper/tests/test_integration_live.py` | Live parametrized tests for all 15 watched sites |
| `seulgi/app/tests/test_news_memory.py` | ChromaDB dedup: empty, identical, similar, dissimilar, thread safety, disabled mode |
| `seulgi/app/tests/test_watcher.py` | `headline_id` is 12-char hex, unique, stable |
| `seulgi/tests/test_system_live.py` | Full 1h live soak test with per-poll observability report |

---

### Files created or significantly changed

```
services/scraper/app/scraper.py         — complete rewrite (multi-method, preflight, cache, trace)
services/scraper/app/main.py            — v4.0.0, /methods, /cache/stats, trace_id forwarding
services/scraper/app/profiler.py        — new
services/scraper/tests/test_integration_live.py  — new
seulgi/app/ai.py                        — classify_batch, system message, JSON format, price context
seulgi/app/config.py                    — premium_model, news_memory section
seulgi/app/config.yaml                  — all new fields
seulgi/app/dashboard.py                 — headline_id, record_trace, trace_log.txt
seulgi/app/db.py                        — headline_id migration in signals/trades/engine_results
seulgi/app/engine.py                    — news_memory integration, hid logging
seulgi/app/markets.py                   — max_tracked_markets cap
seulgi/app/news_memory.py               — new (ChromaDB dedup)
seulgi/app/profiler.py                  — new
seulgi/app/trader.py                    — classify_batch, headline_id propagation
seulgi/app/watcher.py                   — headline_id on RawHeadline
seulgi/app/tests/test_news_memory.py    — new
seulgi/app/tests/test_watcher.py        — headline_id tests added
seulgi/requirements.txt                 — chromadb>=0.5
seulgi/tests/test_system_live.py        — new
```
