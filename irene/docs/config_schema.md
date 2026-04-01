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

  # If daily realised loss exceeds this fraction of initial balance,
  # the system switches to paper mode automatically.
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
  balance_usdc: 100.0          # starting balance (paper) or wallet balance cap (live)
  max_position_fraction: 0.20  # max single position as fraction of balance
  kelly_fraction: 0.25         # fractional Kelly multiplier
  min_edge: 0.04               # minimum |p_hat - mid| before trading (after spread+fee)
  fee_buffer: 0.02             # assumed taker fee + slippage buffer
  max_liquidity_fraction: 0.15 # max fraction of available book depth to consume
  max_open_positions: 8        # hard cap on simultaneous open positions
  order_type: "FOK"            # "FOK" (fill-or-kill) | "FAK" (fill-and-kill)
  position_timeout_hours: 48   # force-close positions older than this

  # Per-direction probability bump applied to mid-price to form p_hat.
  # Keep these small — the Brier tracker will show if they're calibrated.
  edge_increment_yes: 0.12     # added to mid when direction=YES
  edge_increment_no:  0.12     # subtracted from mid when direction=NO

markets:
  min_volume_usdc: 5000        # skip illiquid markets
  max_hours_to_close: 72       # only trade near-resolution markets
  min_hours_to_close: 2        # avoid markets resolving imminently
  exclude_keywords:            # case-insensitive substring match on question
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
  newsapi_key: ""              # get one at newsapi.org (free tier: 100 req/day)
  newsapi_poll_seconds: 60     # how often to poll NewsAPI
  newsapi_page_size: 100       # articles per request

  rss_feeds:
    - url: "https://feeds.reuters.com/reuters/topNews"
    - url: "https://feeds.bbci.co.uk/news/rss.xml"
    - url: "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"
    - url: "https://feeds.washingtonpost.com/rss/world"

  rss_poll_seconds: 45

  similarity_threshold: 0.72   # minimum cosine similarity to match signal → market
  top_k_markets: 3             # max markets to match per signal
  max_signal_age_seconds: 180  # skip signals older than this before trading

  # Direction classification thresholds
  direction_min_confidence: 0.30  # skip if |confidence| below this (NEUTRAL)

  # Optional: route low-confidence signals through an LLM for direction classification.
  # Disabled by default to avoid API cost. Requires llm.* config below.
  use_llm_direction: false
  llm_direction_confidence_threshold: 0.50  # only route signals below this to LLM

scanner:
  poll_interval_seconds: 120          # how often to fetch new markets from Gamma API
  embedding_rebuild_interval_seconds: 3600  # rebuild stale embeddings
  gamma_page_size: 200                # markets per Gamma API page

monitor:
  port: 8080
  per_market_log_days: 30    # how many days of market_events to return per market

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
```

---

## Environment Variable Overrides

Any config value can be overridden via environment variable using the pattern `IRENE__<SECTION>__<KEY>` (double underscore delimiters). For example:

```bash
IRENE__IRENE__MODE=live
IRENE__POLYMARKET__PRIVATE_KEY=0xabc...
IRENE__TRADING__MIN_EDGE=0.06
IRENE__SIGNALS__NEWSAPI_KEY=abc123
```

Environment variables take precedence over `config.yaml`. Useful for secrets — keep keys out of the YAML file and inject them via Docker secrets or `.env`.

---

## Validation Rules

The config loader enforces these constraints (Pydantic validators):

| Field | Constraint |
|-------|-----------|
| `trading.kelly_fraction` | 0 < x ≤ 1.0 |
| `trading.max_position_fraction` | 0 < x ≤ 1.0 |
| `trading.min_edge` | x > 0 |
| `trading.max_liquidity_fraction` | 0 < x ≤ 1.0 |
| `trading.max_open_positions` | x ≥ 1 |
| `markets.min_hours_to_close` | x ≥ 0 |
| `markets.max_hours_to_close` | > `markets.min_hours_to_close` |
| `signals.similarity_threshold` | 0 < x < 1.0 |
| `signals.top_k_markets` | x ≥ 1 |
| `irene.mode` | must be "paper" or "live" |
| Live mode requires | `polymarket.private_key` non-empty |
