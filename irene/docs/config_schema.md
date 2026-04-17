# Irene Config Schema

`config.yaml` is volume-mounted at `/config/config.yaml` in all containers.
It is re-read every **20 seconds**. Changes take effect on the next cycle — no restart needed.
If the file is invalid YAML or fails Pydantic validation, the previous config is retained and a warning is logged.

---

## Full Schema with Defaults

```yaml
irene:
  # Trading mode. "paper" logs decisions without submitting orders.
  # Switch to "live" only after validating paper performance.
  mode: paper          # "paper" | "live"

  # If daily realised loss exceeds this fraction of starting balance,
  # the system automatically switches back to paper mode.
  drawdown_limit: 0.10  # 10%

polymarket:
  clob_url: "https://clob.polymarket.com"
  gamma_url: "https://gamma-api.polymarket.com"

  # Required only in live mode. Leave blank for paper.
  private_key: ""   # EVM private key (hex, 0x-prefixed)
  api_key: ""       # Polymarket CLOB API key
  api_secret: ""
  api_passphrase: ""
  chain_id: 137     # 137 = Polygon mainnet, 80002 = Amoy testnet

trading:
  balance_usdc: 100.0          # starting balance (paper) or wallet cap (live)
  max_position_fraction: 0.20  # max single position as fraction of balance
  kelly_fraction: 0.25         # fractional Kelly multiplier (0 < x ≤ 1.0)
  min_edge: 0.04               # minimum net edge (after spread + fee_buffer) before trading
  fee_buffer: 0.02             # assumed taker fee + slippage buffer subtracted from edge
  max_liquidity_fraction: 0.15 # max fraction of available book depth to consume
  max_open_positions: 8        # hard cap on simultaneous open positions
  order_type: "FOK"            # "FOK" (fill-or-kill) | "FAK" (fill-and-kill)
  position_timeout_hours: 48   # force-close positions older than this

  # Per-direction probability bump applied to mid-price to form p_hat.
  # Tune based on Brier scores from GET /calibration.
  edge_increment_yes: 0.12     # added to mid when direction=YES
  edge_increment_no:  0.12     # subtracted from mid when direction=NO

markets:
  min_volume_usdc: 5000        # skip illiquid markets
  max_hours_to_close: 72       # do not trade markets resolving beyond this window
  min_hours_to_close: 2        # avoid markets resolving imminently (wide spreads)
  exclude_keywords:            # case-insensitive substring match on market question
    - "nfl"
    - "nba"
    - "mlb"
    - "nhl"
    - "premier league"
    - "champions league"
    - "laliga"
    - "bundesliga"
    - "world cup"
    - "super bowl"
    - "stanley cup"
    - "march madness"

signals:
  # NewsAPI
  newsapi_key: ""              # legacy single-key; merged into newsapi_keys if list is empty
  newsapi_keys: []             # preferred: list of keys; or IRENE__SIGNALS__NEWSAPI_KEYS=key1,key2
  newsapi_poll_seconds: 900    # polling interval (per key, with jitter)
  newsapi_page_size: 100       # articles per request

  # Brave Search
  brave_api_key: ""            # legacy single-key
  brave_api_keys: []           # preferred: list; or IRENE__SIGNALS__BRAVE_API_KEYS=key1,key2
  brave_poll_seconds: 900      # polling interval (per key, with jitter)

  # RSS
  rss_feeds:
    - url: "https://www.theguardian.com/world/rss"
    - url: "https://feeds.bbci.co.uk/news/rss.xml"
    - url: "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"
  rss_poll_seconds: 45         # polling interval per feed
  rss_startup_spread_fraction: 1.0  # feeds stagger initial poll by up to 1× rss_poll_seconds

  # Watched pages (persistent Playwright browser tabs)
  # Each entry opens one headless Chromium tab that stays on the page permanently.
  # A MutationObserver fires as soon as new DOM nodes appear (new articles), so
  # signals are extracted within seconds of a site update — no repeated HTTP polling.
  # A full page reload is forced after `reload_seconds` of silence (±jitter_fraction).
  watched_pages: []
  # Example:
  # watched_pages:
  #   - url: "https://apnews.com/"
  #     reload_seconds: 240       # reload after 4 min of silence
  #     jitter_fraction: 0.20     # ±20% on reload_seconds
  #     change_wait_seconds: 20   # inner poll interval checking observer flag

  # Signal → market matching
  similarity_threshold: 0.72   # minimum cosine similarity to match signal → market
  top_k_markets: 3             # max markets matched per signal
  max_signal_age_seconds: 180  # skip signals older than this when the trader receives them

  # Direction classification
  direction_min_confidence: 0.30  # skip NEUTRAL (confidence below threshold)
  use_llm_direction: false        # route low-confidence signals through LLM (requires llm.*)

  jitter_fraction: 0.20        # ±% applied to newsapi and rss poll intervals to prevent thundering herd

scanner:
  poll_interval_seconds: 120          # how often to fetch new markets from Gamma API
  embedding_rebuild_interval_seconds: 3600  # rebuild stale embeddings on this cycle
  gamma_page_size: 200                # markets per Gamma API page
  gamma_max_pages: 20                 # pagination cap per poll (4000 markets max)
  jitter_fraction: 0.20               # ±% applied to poll_interval_seconds

timing:
  jitter_fraction: 0.20        # global default jitter; section-level overrides take precedence
  profile_window: 200          # samples retained per operation in the in-memory ProfileStore
  publish_interval_seconds: 60 # how often each service pushes profiler data to Redis (0 = off)

monitor:
  port: 8080                              # in-container port (host port set via IRENE_MONITOR_HOST_PORT in .env)
  per_market_log_days: 30                 # days of market_events to return per GET /markets/{id}/events
  signals_export_dir: "/data/exports"     # directory for signals.json; "" = disabled
  signals_export_interval_seconds: 300    # how often to write signals.json
  signals_export_limit: 5000              # max signals included in export
  dashboard_text_path: "/data/exports/dashboard.txt"  # plain-text dashboard; "" = disabled
  dashboard_text_interval_seconds: 10     # how often to refresh dashboard.txt

# Required only if signals.use_llm_direction: true
llm:
  provider: "anthropic"      # "anthropic" | "openai"
  api_key: ""
  model: "claude-haiku-4-5-20251001"

database:
  url: "postgresql://irene:irene@postgres:5432/irene"
  pool_size: 5

redis:
  url: "redis://redis:6379/0"
  signal_channel: "irene:signals"
  heartbeat_channel: "irene:heartbeat"

rate_limits:
  newsapi_max_per_day: 90      # per API key (free tier: ~96/day at 15-min polls)
  brave_max_per_day: 500       # per API key (paid Brave Basic plan; align with your dashboard)
  rss_http_max_per_day: 3000   # shared across all RSS feeds
  clob_max_per_day: 20000      # shared across all CLOB orderbook fetches
  # Use 0 for unlimited. Limits are enforced via Redis daily counters (reset UTC midnight).
```

---

## Environment Variable Overrides

Any config value can be overridden via `IRENE__<SECTION>__<KEY>` (double-underscore delimiters).

```bash
IRENE__IRENE__MODE=live
IRENE__POLYMARKET__PRIVATE_KEY=0xabc...
IRENE__TRADING__MIN_EDGE=0.06
IRENE__TRADING__KELLY_FRACTION=0.5
IRENE__SIGNALS__NEWSAPI_KEYS=key1,key2        # comma-separated → list
IRENE__SIGNALS__BRAVE_API_KEYS=key1,key2
IRENE__SIGNALS__SIMILARITY_THRESHOLD=0.68
```

Environment variables take precedence over `config.yaml`. Type coercion is automatic: `"true"/"false"` → bool, `"123"` → int, `"1.5"` → float, `"a,b,c"` → list.

### API Key Rotation

Multiple keys for NewsAPI or Brave: set `IRENE__SIGNALS__NEWSAPI_KEYS=key1,key2` (plural form, comma-separated). The `RotatingApiKeyPool` tracks per-key daily usage in Redis and round-robins, skipping any key that has hit its `rate_limits.*_max_per_day` quota. Counters reset at UTC midnight automatically via Redis TTL.

**Do not** use `NEWSAPI_KEY2`, `NEWSAPI_KEY3`, etc. — those variable names are not read. Only the plural `NEWSAPI_KEYS` or the singular `NEWSAPI_KEY` (merged into a one-element list) are understood.

---

## Validation Rules

| Field | Constraint |
|-------|------------|
| `trading.kelly_fraction` | 0 < x ≤ 1.0 |
| `trading.max_position_fraction` | 0 < x ≤ 1.0 |
| `trading.min_edge` | x > 0 |
| `trading.max_liquidity_fraction` | 0 < x ≤ 1.0 |
| `trading.max_open_positions` | x ≥ 1 |
| `markets.min_hours_to_close` | x ≥ 0 |
| `markets.max_hours_to_close` | > `markets.min_hours_to_close` |
| `signals.similarity_threshold` | 0 < x < 1.0 |
| `signals.top_k_markets` | x ≥ 1 |
| `irene.mode` | `"paper"` or `"live"` |
| live mode | requires `polymarket.private_key` non-empty |

---

## Profitability Tuning Guide

All of these can be changed in `config.yaml` without restarting containers (hot-reload in ~20s).

| Goal | Parameter | Direction | Notes |
|------|-----------|-----------|-------|
| More trades (wider net) | `signals.similarity_threshold` | ↓ lower | More market matches per signal; watch false positives via Brier |
| More trades | `trading.min_edge` | ↓ lower | Accept smaller edges; increases trade count but lowers average return |
| Larger positions | `trading.kelly_fraction` | ↑ higher | Max 1.0 (full Kelly); 0.25–0.5 is typical; higher = more volatility |
| More simultaneous positions | `trading.max_open_positions` | ↑ higher | Capped by available balance |
| Wider market universe | `markets.max_hours_to_close` | ↑ higher | More markets eligible; may reduce edge (further from resolution) |
| Signal recency | `signals.max_signal_age_seconds` | ↓ lower | Tighter freshness filter; trader rejects older signals |
| Direction accuracy | `trading.edge_increment_yes/no` | tune | Check Brier scores at `GET /calibration`; if winning > 50% → increment may be undersized |
| Faster signals | `signals.newsapi_poll_seconds` | ↓ lower | Constrained by daily quota: `quota / poll_seconds_per_day ≤ max_per_day` |
| More signal sources | `signals.rss_feeds` | add feeds | Free; `rss_http_max_per_day` shared |

---

## Current Demo Config (24-hour paper run)

These are the values tuned for the 24h demo run (aggressive, to maximise trade volume and get a clear signal on edge quality):

```yaml
trading:
  kelly_fraction: 0.75        # near full-Kelly
  min_edge: 0.02              # very low bar — take almost everything
  fee_buffer: 0.01
  max_liquidity_fraction: 0.30
  max_open_positions: 64
  position_timeout_hours: 24

markets:
  min_volume_usdc: 1000
  max_hours_to_close: 2160    # 90 days — most Polymarket markets resolve weeks/months out
  min_hours_to_close: 1

signals:
  newsapi_poll_seconds: 480   # 2 keys × 90/day = 180 → 1 per 480s
  brave_poll_seconds: 300     # 1 key × 500/day → 288/day at 300s
  rss_poll_seconds: 30
  similarity_threshold: 0.65
  top_k_markets: 5
  max_signal_age_seconds: 300
  direction_min_confidence: 0.25

scanner:
  poll_interval_seconds: 90
  embedding_rebuild_interval_seconds: 1800

monitor:
  dashboard_text_interval_seconds: 10
```
