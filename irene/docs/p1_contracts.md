# P1 cross-module contracts

> This doc locks the interfaces that span modules so parallel work doesn't
> diverge. Every module in P1 references this doc; any change here that
> alters a signature is a PR-of-its-own and updates every dependent module
> in the same diff.

- **Owner.** Whoever lands the next contract-touching PR. Currently: P1 author.
- **Status as of 2026-05-23.** Bus + StageDeps + cycle context locked. Module-level interfaces (Embedder / AIClient / Budget / Timing) locked. Stage lifecycle locked (§9 below).

---

## 1. StageDeps (modularization rule 6)

The dataclass every `Stage.__init__` takes. Lives at `app/deps.py`.

```python
@dataclass(frozen=True)
class StageDeps:
    db: DB                       # app/db.py — DB connection + batch()
    bus: Bus                     # app/bus.py — publish/subscribe
    config: Config               # app/config.py — frozen snapshot
    embedder: Embedder | None = None         # app/embed.py — sentence-transformers
    ai: AIClient | None = None               # app/ai.py — OpenRouter wrapper
    budget: Budget | None = None             # app/budget.py — OpenRouter cap watcher
    timing: ProfileStore | None = None       # app/timing.py — Timer samples
    market_index: MarketIndex | None = None  # app/stages/markets.py — shared cosine index
    clob: CLOBClient | None = None           # app/clob.py — Polymarket CLOB mid lookups
```

`market_index` lives in `StageDeps` because it's a process-singleton **resource** (owned by `MarketsStage`, consumed by `MatchStage`) — same pattern as `db` or `bus`. This is the only stage-owned object that crosses into another stage's hands; it's the documented exception per modularization rule 5 and is mediated by an `asyncio.Lock`.

`clob` is a pure utility client — no tables, no shared mutable state, no stage-ownership baggage. Consumed by `trade.py` (entry mid-price for the edge gate) and `stages/closer.py` (exit mid-price for PnL).

`None`-by-default for the optional deps: not every stage needs an embedder or an AI client. The wiring step in `main.py` provides only what the stage declares it needs (the stage's `__init__` raises if a required dep is missing).

---

## 2. Bus topics + payload schemas

Topic names live as constants in `app/topics.py`. Payloads are JSON-dicts only (modularization rule 3). All datetime values cross the bus as **ISO-8601 strings**; embeddings cross as **row IDs** the receiver re-fetches from the DB.

**Telemetry vs trade-path.** A topic name starting with `telemetry.` is dropped first under back-pressure. Every other topic is trade-path and is **never** dropped — publishers block when the bounded queue is full.

| Constant | Topic string | Publisher | Subscribers | Payload |
|---|---|---|---|---|
| `TOPIC_ARTICLE_INGESTED` | `article.ingested` | `stages/ingest.py` | `dedupe` | `{article_id: int, url: str, is_tracer: bool, tracer_id: str \| None}` |
| `TOPIC_ARTICLE_DEDUPED` | `article.deduped` | `stages/dedupe.py` | `match` | `{article_id: int, cluster_id: int \| None, freshness: str, is_tracer: bool, tracer_id: str \| None}` |
| `TOPIC_MARKETS_REFRESHED` | `markets.refreshed` | `stages/markets.py` | `match` (advisory only) | `{count: int, refreshed_at: str}` |
| `TOPIC_SIGNAL_CREATED` | `signal.created` | `stages/match.py` | `trade` | `{signal_id: int, article_id: int, market_id: str, similarity: float, freshness: str, is_tracer: bool, tracer_id: str \| None}` |
| `TOPIC_TRADE_DECIDED` | `trade.decided` | `stages/trade.py` | (none — diagnostic) | `{trade_id: int \| None, decision_id: int, outcome: str, reason: str, is_tracer: bool, tracer_id: str \| None}` |
| `TOPIC_TELEMETRY_TIMING` | `telemetry.timing` | any (via `timing.ProfileStore`) | (collector) | `{op: str, ms: float, sampled_at: str}` |
| `TOPIC_TELEMETRY_BUDGET` | `telemetry.budget` | `budget.py` | (collector) | `{remaining_usd: float, sampled_at: str}` |

**Freshness labels** (set by dedupe): `BREAKING | EVOLVING | REHASH | STALE_RETURN`. Carried on `article.deduped` and persisted on `article_clusters.freshness`.

**Tracer flag propagation.** Every trade-path payload carries `is_tracer` and `tracer_id`. The bus middleware threads these into every row written under that handle's `db.batch()` — stages don't manually pass the flags around.

---

## 3. Bus interface

```python
class Bus(Protocol):
    async def publish(self, topic: str, payload: dict) -> None: ...
    def subscribe(self, topic: str, handler: Callable[[dict], Awaitable[None]]) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

**Implementation guarantees:**

- `publish()` to a trade-path topic blocks if the topic's bounded queue is full (back-pressure into the publisher).
- `publish()` to a `telemetry.*` topic is **non-blocking**: full queue → silently drop, increment `bus_drops_telemetry` counter.
- In strict mode (env `IRENE_BUS_STRICT=1`, default ON in tests), every published payload is JSON-roundtripped before delivery. Production may skip the roundtrip for speed.
- `subscribe()` is called at wire-time only (from `main.py` after `bus.start()` would be too late). Topic with no subscribers: `publish()` is a no-op.
- Each subscriber runs in its own task; one subscriber's exception does not affect siblings.

---

## 4. Module-level interfaces (utilities)

### `app/embed.py` — `Embedder`

```python
class Embedder:
    def __init__(self, model_name: str, dim: int) -> None: ...
    @property
    def embedder_id(self) -> str:
        """Returns `{model_name}@{dim}` — stamped on every embedding row to
        prevent silent dimension/model space mixing (a seulgi lesson)."""
    def embed(self, text: str) -> np.ndarray: ...           # shape (dim,), float32
    def embed_batch(self, texts: list[str]) -> np.ndarray:  # shape (N, dim), float32
```

The embedder is loaded once per process and shared across stages via `StageDeps.embedder`. Cache layer is in-process LRU keyed by `sha1(text)`; cache is **NOT** persisted to DB in P1 (revisit in P2 when extraction-call volume grows).

### `app/budget.py` — `Budget`

```python
class Budget:
    def __init__(self, *, api_key: str, monthly_cap_usd: float,
                 pause_at_remaining_usd: float, poll_seconds: int,
                 pause_flag_path: str) -> None: ...
    async def start(self) -> None: ...     # background poll loop
    async def stop(self) -> None: ...
    def is_paused(self) -> bool: ...       # ai.py checks this before every call
    def note_402(self) -> None: ...        # ai.py bumps on Insufficient credits
    def note_ok(self) -> None: ...         # ai.py bumps on 200
    @property
    def remaining_usd(self) -> float | None: ...
```

Pause behaviour: writes a file at `pause_flag_path` (default `/data/budget_paused.flag`) with the reason. Dashboard / ad-hoc scripts read the file without IPC (seulgi lesson). `is_paused()` is fast (in-process attr).

### `app/ai.py` — `AIClient`

```python
@dataclass(frozen=True)
class DirectionResult:
    direction: str          # 'YES' | 'NO' | 'NEUTRAL'
    confidence: float       # [0.0, 1.0]
    prediction_id: int      # row id in predictions
    cached: bool            # served from llm_calls.prompt_sha cache
    raw_response: str       # the LLM's text (for debugging only)

class AIClient:
    PROMPT_VERSION: str = "v1"

    def __init__(self, *, db: DB, budget: Budget, config: Config) -> None: ...
    async def direction(self, *, signal_id: int, prompt: str,
                        is_tracer: bool = False,
                        tracer_id: str | None = None) -> DirectionResult: ...
    async def close(self) -> None: ...
```

**Cycle:** `direction()` reads `cycle.current_cycle_id()` internally — no caller passes it. Same pattern as every other trade-path writer.

**`ai.py` owns three tables** (modularization rule 4): `predictions`, `llm_calls`, `llm_parse_failures`. All writes go through `db.batch()`; on a cache hit the `llm_calls` row is not duplicated but the `predictions` row IS — one prediction per (signal × model × prompt_version) is the invariant.

**Prompt contract.** Callers compose the full prompt as a string. The LLM is instructed by the prompt itself to respond with JSON of shape `{"direction": "YES"|"NO"|"NEUTRAL", "confidence": 0.0..1.0, "reasoning": "..."}`. ai.py appends a constant suffix that pins this format; if the model returns malformed JSON, ai.py writes `llm_parse_failures` with `reason='parse_error'` and returns NEUTRAL.

**Budget paused.** If `budget.is_paused()` returns True at call time, `direction()` short-circuits: writes `llm_parse_failures(reason='budget_paused')`, returns a `DirectionResult(direction='NEUTRAL', confidence=0.0, cached=False, prediction_id=…)`. No HTTP call, but a `predictions` row IS written (NEUTRAL with confidence 0) so the funnel stays consistent.

**402 handling.** A 402 response triggers `budget.note_402()` and is treated as a parse failure (`reason='402_credits'`). A 200 with parseable JSON triggers `budget.note_ok()`.

### `app/timing.py` — `Timer` + `ProfileStore`

```python
class Timer:
    """Context manager: `with timer('op_name'): ...`"""

class ProfileStore:
    def __init__(self, *, window: int = 200) -> None: ...
    def sample(self, op: str, ms: float) -> None: ...
    def percentiles(self, op: str) -> dict[str, float]:
        """Returns {'p50': ..., 'p95': ..., 'p99': ..., 'count': int}"""
```

`ProfileStore` is in-memory; flushed to the bus as `telemetry.timing` payloads on a fixed cadence (controlled by `Config.timing.publish_interval_seconds` — to be added when timing.py needs it; default 60s).

---

## 5. Cycle context (`app/cycle.py`)

```python
CYCLE_CTX: ContextVar[int | None] = ContextVar("cycle_id", default=None)

def current_cycle_id() -> int:
    """Returns the cycle_id set by main.py. Raises if unset (every trade-path
    write must stamp cycle_id; an unset context is a wiring bug, not a
    runtime condition we recover from)."""

def set_cycle_id(cycle_id: int) -> None:
    """Called once from main.py after ensure_current_cycle()."""
```

Modules that write trade-path rows call `current_cycle_id()` at write-time. This is a contextvar (not thread-local) so it works across asyncio tasks.

---

## 6. Table ownership (the full P1 list)

Source of truth for the table-ownership CI lint (`docs/modularization.md` rule 4 + §"Verification"):

| Table | Owner module | DDL registered via |
|---|---|---|
| `schema_migrations` | `app/db.py` | inline (db.py owns it) |
| `cycles` | `app/main.py` | `db_module.register_schema("app/main.py", _CYCLES_DDL)` |
| `articles` | `app/stages/ingest.py` | `_ARTICLES_DDL` registered at import |
| `article_clusters` + `article_cluster_members` | `app/stages/dedupe.py` | `_DEDUPE_DDL` |
| `markets` | `app/stages/markets.py` | `_MARKETS_DDL` |
| `signals` | `app/stages/match.py` | `_SIGNALS_DDL` |
| `predictions` + `llm_calls` + `llm_parse_failures` | `app/ai.py` | `_AI_DDL` |
| `decisions` | written by whichever gate rejects; gate's owning stage is `app/stages/trade.py` for trade-side gates | `_DECISIONS_DDL` in `trade.py` |
| `trades` + `trade_features` | `app/stages/trade.py` | (same `_DECISIONS_DDL` / shared file) |
| `trade_closures` | `app/stages/closer.py` | `_CLOSER_DDL` |
| `decision_inputs` | `app/db.py` (DDL) + the upstream stage that calls `db.capture(cur, ...)` (writes) | inline DDL in `db.py` |

---

## 7. Capture (where `decision_inputs` lives)

`decision_inputs` is the capture row for replay. It is written **inside the same `db.batch()`** as the live write that triggered it — not in a separate transaction (modularization §"Cross-cutting concerns" + master-plan "Capture cannot lag" gate).

**Implementation (final).** `DB` exposes a helper:

```python
async def db.capture(
    cur: aiosqlite.Cursor,
    *,
    kind: str,          # 'signal' in P1
    ref_id: int,        # signals.id in P1
    payload: dict,      # the bus payload that triggered the live write
    snapshot: dict,     # the stage's view of decision-time inputs (see below)
    is_tracer: bool,
    tracer_id: str | None,
) -> None
```

Stages call `db.capture(cur, …)` **inside their existing `db.batch()`**, immediately after the live write. The cursor `cur` is the same cursor the stage just used — so the live row and the capture row commit or roll back together. Idempotent on `(kind, ref_id)` (INSERT OR IGNORE).

Earlier drafts of this doc described this as "bus middleware". The bus has no DB awareness; putting capture on `DB` keeps the bus pure and aligns the writer with the transaction it must share.

**Snapshot shape for `kind='signal'`** (written by `app/stages/match.py`):

```json
{
  "article_id": int,
  "market_id": str,
  "similarity": float,
  "cluster_id": int | null,
  "freshness": str | null,
  "embedder_id": str,        // {model}@{dim}; lets replay detect space mixing
  "title": str               // article title at decision time, denormalised
}
```

DDL for `decision_inputs` is registered inline by `db.py` (housekeeping, like `schema_migrations`).

---

## 9. Stage lifecycle

Every stage under `app/stages/` exposes this surface — even when not all four methods are used. `main.py` calls them in this order at boot:

```python
class Stage:
    """## Specification block at the top."""

    def __init__(self, deps: StageDeps) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def handle(self, payload: dict) -> None: ...
```

- **`__init__(deps)`** — pure assembly: copy refs out of `deps`, no I/O. Raise immediately if a required optional dep (e.g. `deps.embedder`) is `None`.
- **`start()`** — called once at boot, after all subscriptions are wired. Pollers launch their background `asyncio.Task` here. Subscribers usually no-op.
- **`stop()`** — called at shutdown. Cancels background tasks, closes HTTP clients, flushes batches. Idempotent (calling twice is fine).
- **`handle(payload)`** — invoked by the bus for each topic the stage subscribed to. Pure pollers may raise `NotImplementedError` here; the bus only calls it for topics the stage was subscribed to.

**Wiring contract in main.py:**

```python
markets = MarketsStage(deps)
ingest  = IngestStage(deps)
dedupe  = DedupeStage(deps)
match   = MatchStage(deps)
trade   = TradeStage(deps)

bus.subscribe(TOPIC_ARTICLE_INGESTED, dedupe.handle)
bus.subscribe(TOPIC_ARTICLE_DEDUPED, match.handle)
bus.subscribe(TOPIC_SIGNAL_CREATED, trade.handle)

await bus.start()
for stage in (markets, ingest, dedupe, match, trade):
    await stage.start()

# … run …

for stage in reversed((markets, ingest, dedupe, match, trade)):
    await stage.stop()
await bus.stop()
```

**Pure-poller vs subscriber stages in P1:**

| Stage | start() launches | subscribes to |
|---|---|---|
| `MarketsStage` | Gamma poll loop + embedding refresh loop | (none) |
| `IngestStage` | scraper:9000 poll loop (HTTP + RSS + Twitter) | (none) |
| `DedupeStage` | (no background task) | `TOPIC_ARTICLE_INGESTED` |
| `MatchStage` | (no background task) | `TOPIC_ARTICLE_DEDUPED` |
| `TradeStage` | (no background task) | `TOPIC_SIGNAL_CREATED` |
| `CloserStage` | closer poll loop (every `cfg.closer.poll_seconds`) | (none) |

**Trade lifecycle and the closer.** `trades` is **append-only** (entry-time
fact). The closer owns a separate table `trade_closures` (exit-time fact)
joined to trades by `trade_id`. Replay reconstructs PnL from `(trades
JOIN trade_closures)`.

For P1 the only close rule is **timeout**: a trade `decided_at` more than
`cfg.closer.timeout_hours` ago and not yet resolved gets closed at the
current CLOB mid with `close_reason='timeout'`. Target/stop/resolution
rules land in later phases.

PnL formula:
- `YES`: `pnl = size_usdc * (exit_mid - entry_mid) / entry_mid`
- `NO`:  `pnl = size_usdc * (entry_mid - exit_mid) / (1 - entry_mid)`

These are the formulas `scripts/replay/accuracy.py` uses to validate
stored `trade_closures.realized_pnl_usdc` to within ±1¢.

---

## 10. What gets verified at the P1 ship gate

(Replicated from `master-plan.md` §"P1 verification" so this doc stands alone.)

- `docker compose -f irene/docker-compose.yml up` boots and runs cleanly for 1+ hour.
- `irene.db` accumulates rows in every P1-owned table.
- `scripts/tracer/fire.py --count 1000 --concurrency 50` shows 0 dropped non-telemetry bullets.
- `scripts/replay/accuracy.py` reconstructs every closed paper trade's `pnl_usdc` to ±1¢.
- Soak report compares first-hour numbers vs seulgi.
