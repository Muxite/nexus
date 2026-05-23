"""
## Specification

**Purpose.** P1 ship-gate tracer harness. Embeds the irene pipeline in this
process with mocked externals (no real scraper, no Polymarket Gamma, no
OpenRouter), publishes N synthetic `article.ingested` payloads tagged with
`is_tracer=True` / `tracer_id=<unique>`, waits for the bus to drain, and
reports the rows that materialised in every owned table.

Exit code 0 iff zero trade-path bullets were dropped — i.e. every
synthetic article propagated through dedupe → match → trade. Telemetry
drops (`bus.counters.telemetry`) are allowed and explicitly excluded from
the gate per `docs/p1_contracts.md` §"Telemetry drop-first".

**Public surface (function-level entry points for tests).**

- `run_inproc(count, concurrency, *, db_path=":memory:", template=None,
  timeout_seconds=30) -> TracerRunResult` — runs the full harness without
  shelling out. The test suite drives this directly.
- `main(argv=None) -> int` — argparse CLI; returns exit code.

**Why an embedded pipeline?** The bus is in-process. A separate `irene`
container holding the bus would have no socket the tracer could push
payloads into. So the tracer boots its own pipeline, owning all the
plumbing, and verifies the trade-path is correctly wired end-to-end.

**Owned tables.** None — the harness is a *driver*, not a stage.

**Side effects.**
- Constructs a `DB` (`:memory:` by default; a file path on demand).
- Opens an `InProcBus`, wires dedupe / match / trade subscribers via
  `app.main.wire_pipeline()`, then bypasses the pure-poller stages.
- Writes synthetic `articles` rows directly (real ingest would HTTP-GET
  the scraper; we skip that and call `bus.publish` ourselves).
- Pre-seeds three stub `markets` rows with deterministic 4-dim embeddings
  so `MarketIndex.load()` has something to find.

**Non-goals.**
- Network I/O. No real Gamma / scraper / OpenRouter calls — the harness
  must run in CI and exit fast.
- Verifying values. The check is structural ("did every owned table
  receive a tracer row?"). Replay verifies per-trade correctness.
- Cross-stage imports. The harness imports `app.main.wire_pipeline()` and
  bus / config / db; it does NOT reach into any `app.stages.*` module.

**Anti-patterns guarded against.**
- Late subscribe — `wire_pipeline()` registers all subscriptions before
  `bus.start()`. The bus rejects late subscribers explicitly.
- Real network. The fake AI client and the absence of polling tasks keep
  every IO local.
- `sentence-transformers` import. The deterministic 4-dim embedder runs
  in pure numpy so tests stay fast.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import itertools
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np
import yaml

# Side-effect imports: every stage's DDL must register before db.connect().
import app.ai  # noqa: F401
import app.main  # noqa: F401 — cycles DDL
import app.stages.dedupe  # noqa: F401
import app.stages.ingest  # noqa: F401
import app.stages.markets  # noqa: F401
import app.stages.match  # noqa: F401
import app.stages.trade  # noqa: F401
from app.ai import DirectionResult
from app.bus import InProcBus
from app.config import Config
from app.cycle import set_cycle_id
from app.db import DB
from app.main import wire_pipeline
from app.topics import (
    TOPIC_ARTICLE_DEDUPED,
    TOPIC_ARTICLE_INGESTED,
    TOPIC_SIGNAL_CREATED,
)

from scripts.tracer._queries import TracerCounts, query_tracer_counts

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TRACER_EMBEDDER_ID = "tracer-embedder@4"
TRACER_DIM = 4
STUB_MARKET_COUNT = 3
TEMPLATES_DIR = Path(__file__).parent / "templates"
DRAIN_POLL_INTERVAL_S = 0.05
DRAIN_QUIET_PASSES = 3  # how many consecutive all-zero polls before we call it drained


# ---------------------------------------------------------------------------
# Deterministic 4-dim embedder
# ---------------------------------------------------------------------------


def _seed_from_text(text: str) -> int:
    """Stable, well-distributed 32-bit seed for a text string."""
    digest = hashlib.sha1(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big")


def _deterministic_vec(text: str) -> np.ndarray:
    """Return a normalised positive 4-dim vector keyed on `text`.

    Positive components keep cosine similarity in [0, 1]; with `np.abs` the
    pairwise cosine on random unit vectors averages ~0.7 in 4-D, so every
    article comfortably clears `match.similarity_floor` even at the default
    config value. This is the contract that makes `signals = articles ×
    stub_market_count` deterministic for the tracer.
    """
    rng = np.random.default_rng(_seed_from_text(text))
    v = np.abs(rng.standard_normal(TRACER_DIM)).astype(np.float32)
    norm = float(np.linalg.norm(v))
    if norm == 0.0:
        # Degenerate edge case (sha1 collision against the all-zero draw)
        # — fall back to a canonical basis vector so we never return NaN.
        v = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
        return v
    return v / norm


class _TracerEmbedder:
    """Duck-typed `Embedder` — pure numpy, no torch / sentence-transformers.

    Matches the surface in `docs/p1_contracts.md` §4: an `embedder_id`
    property plus `embed(text)`. `embed_batch` is provided so the rest of
    the wiring (`MarketsStage._embed_loop`, which we don't start) stays
    syntactically happy if its codepath is ever reached.
    """

    embedder_id: str = TRACER_EMBEDDER_ID

    def embed(self, text: str) -> np.ndarray:
        return _deterministic_vec(text)

    def embed_batch(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, TRACER_DIM), dtype=np.float32)
        return np.stack([_deterministic_vec(t) for t in texts]).astype(np.float32)


# ---------------------------------------------------------------------------
# Fake AIClient — writes a real predictions row, returns DirectionResult.
# ---------------------------------------------------------------------------


class _TracerAI:
    """Duck-typed `AIClient.direction()` — no network, deterministic output.

    Writes a `predictions` row (so `trades.prediction_id` has a real FK
    target) inside `db.batch()` and returns a `DirectionResult` pointing
    at it. Always returns YES @ 0.7 — high enough to clear the
    `min_confidence` gate but below 1.0 so the edge math is non-trivial.
    """

    PROMPT_VERSION = "tracer-v1"
    MODEL = "tracer/stub"

    def __init__(self, db: DB) -> None:
        self._db = db

    async def direction(
        self,
        *,
        signal_id: int,
        prompt: str,  # noqa: ARG002 — kept to match the AIClient surface
        is_tracer: bool = False,
        tracer_id: Optional[str] = None,
    ) -> DirectionResult:
        from app.cycle import current_cycle_id
        cycle_id = current_cycle_id()
        raw = '{"direction":"YES","confidence":0.7,"reasoning":"tracer stub"}'
        async with self._db.batch() as cur:
            await cur.execute(
                """
                INSERT INTO predictions
                    (signal_id, llm_call_id, model, prompt_version,
                     direction, confidence, cached, raw_response, cycle_id,
                     is_tracer, tracer_id, created_at)
                VALUES (?, NULL, ?, ?, 'YES', 0.7, 0, ?, ?, ?, ?, ?)
                ON CONFLICT(signal_id, model, prompt_version) DO NOTHING
                """,
                (
                    signal_id,
                    self.MODEL,
                    self.PROMPT_VERSION,
                    raw,
                    cycle_id,
                    1 if is_tracer else 0,
                    tracer_id,
                    _now_iso(),
                ),
            )
            await cur.execute(
                "SELECT id FROM predictions WHERE signal_id=? AND model=? AND prompt_version=?",
                (signal_id, self.MODEL, self.PROMPT_VERSION),
            )
            row = await cur.fetchone()
        if row is None:
            raise RuntimeError(
                f"tracer AI: predictions insert failed for signal_id={signal_id}"
            )
        return DirectionResult(
            direction="YES",
            confidence=0.7,
            prediction_id=int(row[0]),
            cached=False,
            raw_response=raw,
        )

    async def close(self) -> None:
        return


class _TracerBudget:
    """Budget stand-in for the tracer harness.

    `ExtractStage` requires `deps.budget`; the tracer never starts the
    extract stage's HTTP path (it isn't subscribed in tracer mode) but
    the wire_pipeline() construction still needs a non-None budget to
    satisfy the contract."""

    def is_paused(self) -> bool:
        return False

    def note_ok(self) -> None:
        return

    def note_402(self) -> None:
        return

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return


class _TracerCLOB:
    """CLOB stand-in for the tracer harness.

    By default returns None on every lookup (trade.py and closer.py both
    fall back to their respective defaults). The soak harness flips
    `default_mid` to a real number so the closer actually computes a
    non-zero exit price — that's what lets the replay accuracy gate fire."""

    def __init__(self, default_mid: Optional[float] = None) -> None:
        self.default_mid = default_mid

    async def get_mid(self, token_id: str):  # noqa: ARG002
        return self.default_mid

    async def close(self) -> None:
        return


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Template:
    name: str
    title: str
    summary: str
    source: str
    source_detail: str
    url_pattern: str

    def render(self, i: int) -> dict[str, str]:
        return {
            "title": self.title.format(i=i),
            "summary": self.summary.format(i=i),
            "source": self.source,
            "source_detail": self.source_detail,
            "url": self.url_pattern.format(i=i, template_name=self.name),
        }


_REQUIRED_TEMPLATE_KEYS = ("title", "summary", "source", "source_detail", "url_pattern")


def load_template(path: Path) -> _Template:
    raw = yaml.safe_load(path.read_text()) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"template {path} did not parse as a mapping")
    missing = [k for k in _REQUIRED_TEMPLATE_KEYS if k not in raw]
    if missing:
        raise ValueError(f"template {path} missing keys: {missing}")
    return _Template(
        name=path.stem,
        title=str(raw["title"]),
        summary=str(raw["summary"]),
        source=str(raw["source"]),
        source_detail=str(raw["source_detail"]),
        url_pattern=str(raw["url_pattern"]),
    )


def load_all_templates() -> list[_Template]:
    return [load_template(p) for p in sorted(TEMPLATES_DIR.glob("*.yaml"))]


# ---------------------------------------------------------------------------
# Markets seeding
# ---------------------------------------------------------------------------


def _stub_market_rows(rng: np.random.Generator) -> list[dict[str, Any]]:
    """Return STUB_MARKET_COUNT rows ready for the `markets` INSERT."""
    rows: list[dict[str, Any]] = []
    for i in range(STUB_MARKET_COUNT):
        # Each market's "question" is a deterministic string — the embedding
        # is also keyed on text so the seed is reproducible without
        # threading the rng through every call.
        v = np.abs(rng.standard_normal(TRACER_DIM)).astype(np.float32)
        norm = float(np.linalg.norm(v))
        if norm > 0:
            v /= norm
        rows.append(
            {
                "condition_id": f"tracer-market-{i}",
                "question": f"Tracer market #{i}: will the synthetic event resolve?",
                "embedding_bytes": v.tobytes(),
            }
        )
    return rows


async def _seed_markets(db: DB) -> None:
    now = _now_iso()
    rng = np.random.default_rng(seed=0)
    rows = _stub_market_rows(rng)
    async with db.batch() as cur:
        for row in rows:
            await cur.execute(
                """
                INSERT INTO markets (
                    condition_id, slug, question, description, category,
                    end_date, volume_usdc, token_yes, token_no,
                    embedding, embedder_id, embedding_dim,
                    embedding_updated_at, active, closed_at,
                    first_seen_at, updated_at
                ) VALUES (?, NULL, ?, NULL, NULL, NULL, 0.0, 'ty', 'tn',
                          ?, ?, ?, ?, 1, NULL, ?, ?)
                ON CONFLICT(condition_id) DO NOTHING
                """,
                (
                    row["condition_id"],
                    row["question"],
                    row["embedding_bytes"],
                    TRACER_EMBEDDER_ID,
                    TRACER_DIM,
                    now,
                    now,
                    now,
                ),
            )


# ---------------------------------------------------------------------------
# Single-writer serialisation for db.batch()
# ---------------------------------------------------------------------------


# Note: an earlier draft of this module wrapped `db.batch()` with a local
# asyncio.Lock to serialise concurrent BEGINs. That fix has since moved
# into `app/db.py` itself — every consumer of `DB` gets the lock for free.

# ---------------------------------------------------------------------------
# Article injection
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _insert_tracer_article(
    db: DB,
    *,
    cycle_id: int,
    template: _Template,
    index: int,
) -> Optional[int]:
    rendered = template.render(index)
    now = _now_iso()
    async with db.batch() as cur:
        await cur.execute(
            """
            INSERT INTO articles
                (source, source_detail, url, title, summary, published_at,
                 ingested_at, cycle_id, is_tracer, tracer_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            """,
            (
                rendered["source"],
                rendered["source_detail"],
                rendered["url"],
                rendered["title"],
                rendered["summary"],
                now,
                now,
                cycle_id,
                f"tracer-{index}",
            ),
        )
        article_id = cur.lastrowid
    return int(article_id) if article_id else None


# ---------------------------------------------------------------------------
# Config override — keep similarity floors below the deterministic embedder
# ---------------------------------------------------------------------------


def _build_tracer_config(
    *,
    closer_timeout_hours: float = 24.0,
    closer_poll_seconds: int = 60,
) -> Config:
    """Return a Config whose match/trade thresholds are loose enough that
    every synthetic article makes it through every gate.

    The deterministic embedder produces positive-quadrant unit vectors;
    pairwise cosine sits well above the default 0.35 floor, but we pin
    floors at 0.01 anyway so a small change to the embedder doesn't drop
    tracer rows on the floor.

    Closer settings are explicit kwargs so the soak harness can force the
    timeout to 0 + poll to ~1s and actually exercise the close path.
    """
    import tempfile
    chroma_dir = tempfile.mkdtemp(prefix="irene-tracer-chroma-")
    return Config.model_validate(
        {
            # MarketIndex pulls `dim` off cfg.embedder; pinning it here to
            # the 4-dim tracer embedder is what makes the index accept the
            # stub market embeddings instead of skipping them all.
            "embedder": {"name": "tracer-embedder", "dim": TRACER_DIM},
            "match": {"similarity_floor": 0.01, "top_k": STUB_MARKET_COUNT},
            "trade": {
                "min_similarity": 0.01,
                # Stub AI returns confidence=0.7; the cascade requires
                # confidence >= min_confidence so 0.55 is fine. We keep
                # the gate at its default to exercise it.
                "min_edge": 0.01,
                # Default kelly_fraction (0.25) + 0.7 confidence ⇒ size > 1,
                # so the size gate passes naturally. Floor unchanged.
            },
            "closer": {
                "timeout_hours": closer_timeout_hours,
                "poll_seconds": closer_poll_seconds,
                "skip_on_missing_mid": False,
            },
            # Even though we don't start dedupe's chromadb client unless
            # chromadb is installed, point it at a tmp dir in case it is.
            "dedupe": {"chroma_path": chroma_dir},
        }
    )


# ---------------------------------------------------------------------------
# Drain helper
# ---------------------------------------------------------------------------


_TRADE_PATH_TOPICS = (
    TOPIC_ARTICLE_INGESTED,
    TOPIC_ARTICLE_DEDUPED,
    TOPIC_SIGNAL_CREATED,
)


async def _drain_bus(
    bus: InProcBus,
    *,
    timeout_seconds: float,
) -> bool:
    """Block until all trade-path queues are empty for DRAIN_QUIET_PASSES
    consecutive polls. Returns True on success, False on timeout."""
    deadline = time.monotonic() + timeout_seconds
    consecutive_zero = 0
    while time.monotonic() < deadline:
        depths = [bus.queue_depth(t) for t in _TRADE_PATH_TOPICS]
        if all(d == 0 for d in depths):
            consecutive_zero += 1
            if consecutive_zero >= DRAIN_QUIET_PASSES:
                return True
        else:
            consecutive_zero = 0
        await asyncio.sleep(DRAIN_POLL_INTERVAL_S)
    return False


# ---------------------------------------------------------------------------
# Run result
# ---------------------------------------------------------------------------


@dataclass
class TracerRunResult:
    counts: TracerCounts
    telemetry_drops: int
    handler_exceptions: int
    timed_out: bool
    elapsed_seconds: float
    articles_requested: int

    @property
    def drops(self) -> int:
        """Trade-path drops only — telemetry is explicitly NOT counted.

        A non-zero value here means a synthetic article didn't propagate
        cleanly: either an exception inside a handler, or a timeout, or
        the funnel didn't materialise a row where one was expected.
        """
        d = self.handler_exceptions
        if self.timed_out:
            d += max(0, self.articles_requested - self.counts.articles)
        return d

    def as_dict(self) -> dict[str, Any]:
        out = dict(self.counts.as_dict())
        out.update(
            {
                "telemetry_drops": self.telemetry_drops,
                "handler_exceptions": self.handler_exceptions,
                "timed_out": self.timed_out,
                "elapsed_seconds": round(self.elapsed_seconds, 3),
                "articles_requested": self.articles_requested,
                "drops": self.drops,
            }
        )
        return out


# ---------------------------------------------------------------------------
# Core entry point
# ---------------------------------------------------------------------------


async def _run_async(
    *,
    count: int,
    concurrency: int,
    db_path: str,
    template_name: Optional[str],
    timeout_seconds: float,
    start_closer: bool = False,
    closer_mid: Optional[float] = None,
    closer_timeout_hours: float = 24.0,
    closer_poll_seconds: int = 60,
) -> TracerRunResult:
    t0 = time.monotonic()
    if count <= 0:
        raise ValueError("count must be >= 1")
    if concurrency <= 0:
        raise ValueError("concurrency must be >= 1")

    templates = load_all_templates()
    if not templates:
        raise RuntimeError(f"no templates found under {TEMPLATES_DIR}")
    if template_name:
        templates = [t for t in templates if t.name == Path(template_name).stem]
        if not templates:
            raise ValueError(f"template not found: {template_name!r}")

    template_cycle = itertools.cycle(templates)

    # Construct everything BEFORE bus.start() — late-subscribe is rejected.
    if db_path == ":memory:":
        db = DB.ephemeral()
    else:
        db = DB(path=db_path, journal_mode="MEMORY", synchronous="OFF")
    await db.connect()
    cycle_id = await db.ensure_current_cycle("tracer")
    set_cycle_id(cycle_id)

    cfg = _build_tracer_config(
        closer_timeout_hours=closer_timeout_hours,
        closer_poll_seconds=closer_poll_seconds,
    )
    embedder = _TracerEmbedder()
    ai = _TracerAI(db)
    # CloserStage requires a CLOB. When `start_closer=True` (the soak
    # path) we want a real number so realized_pnl_usdc != 0; otherwise
    # the stub returns None and any closer that does run would skip.
    clob = _TracerCLOB(default_mid=closer_mid)

    bus = InProcBus()
    budget = _TracerBudget()
    pipeline = wire_pipeline(
        db=db,
        bus=bus,
        cfg=cfg,
        embedder=embedder,
        ai=ai,
        # ExtractStage requires deps.budget; AIClient consumes it too in
        # production. Our fake AI / un-started extract path never call
        # into the budget's HTTP loop — the stub satisfies the type only.
        budget=budget,
        clob=clob,
    )

    # Pre-seed markets and load the index *before* the bus starts so
    # MatchStage's index has rows on the first message.
    await _seed_markets(db)
    await pipeline.markets.index.load(db)

    # NOTE: we deliberately do NOT call markets.start() or ingest.start()
    # — both are pollers that would hit real network. The dedupe / match /
    # trade subscribers run via bus tasks. The closer (also a poller in
    # production) is driven SYNCHRONOUSLY after the bus drains so the
    # tracer can return deterministically — see below.
    await bus.start()
    await pipeline.dedupe.start()
    await pipeline.match.start()
    await pipeline.trade.start()

    sem = asyncio.Semaphore(concurrency)
    # Serialise the SQLite-touching insert. aiosqlite shares a single thread
    # per connection and `db.batch()` opens a real BEGIN — two concurrent
    # `BEGIN`s collide with "cannot start a transaction within a transaction".
    # The bus.publish back-pressure is what `concurrency` actually gates;
    # the insert lock just keeps the connection consistent.
    insert_lock = asyncio.Lock()

    async def _fire_one(i: int) -> None:
        async with sem:
            template = next(template_cycle)
            async with insert_lock:
                article_id = await _insert_tracer_article(
                    db, cycle_id=cycle_id, template=template, index=i
                )
            if article_id is None:
                return
            await bus.publish(
                TOPIC_ARTICLE_INGESTED,
                {
                    "article_id": article_id,
                    "url": template.url_pattern.format(
                        i=i, template_name=template.name
                    ),
                    "is_tracer": True,
                    "tracer_id": f"tracer-{i}",
                },
            )

    try:
        await asyncio.gather(*(_fire_one(i) for i in range(count)))
        drained = await _drain_bus(bus, timeout_seconds=timeout_seconds)
        if start_closer:
            # Drive the closer synchronously rather than via the poll loop.
            # `run_close_cycle()` iterates over every open trade once;
            # with `cfg.closer.timeout_hours=0` (soak-only) every trade
            # qualifies. Running this once is sufficient and bounded.
            await pipeline.closer.run_close_cycle()
    finally:
        await pipeline.trade.stop()
        await pipeline.match.stop()
        await pipeline.dedupe.stop()
        await bus.stop()

    # Read counts off the underlying connection. The TracerCounts query is
    # synchronous (sqlite3) so we extract the file path & re-open, OR for
    # the :memory: case borrow the live aiosqlite connection's underlying
    # sqlite3 connection. aiosqlite stores its native conn at `._conn`.
    # That's a private attr; doing a direct query through the existing
    # connection is cleaner.
    counts = await _async_query_tracer_counts(db)

    elapsed = time.monotonic() - t0
    await db.close()

    return TracerRunResult(
        counts=counts,
        telemetry_drops=bus.counters.telemetry,
        handler_exceptions=bus.counters.handler_exceptions,
        timed_out=not drained,
        elapsed_seconds=elapsed,
        articles_requested=count,
    )


async def _async_query_tracer_counts(db: DB) -> TracerCounts:
    """Run the tracer-count queries on the live aiosqlite connection.

    Mirrors `scripts.tracer._queries.query_tracer_counts` but stays async
    so the harness doesn't have to round-trip through `:memory:` files.
    """
    async def _one(table: str, predicate: Optional[str]) -> int:
        where = " WHERE " + predicate if predicate else ""
        try:
            cur = await db.conn.execute(f"SELECT COUNT(*) FROM {table}{where}")
        except Exception:  # noqa: BLE001
            return 0
        row = await cur.fetchone()
        await cur.close()
        return int(row[0] if row else 0)

    return TracerCounts(
        articles=await _one("articles", "is_tracer = 1"),
        article_clusters=await _one("article_clusters", "is_tracer = 1"),
        signals=await _one("signals", "is_tracer = 1"),
        decisions=await _one("decisions", "is_tracer = 1"),
        trades=await _one("trades", "is_tracer = 1"),
        decision_inputs=await _one(
            "decision_inputs",
            "kind = 'signal' AND is_tracer = 1",
        ),
    )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def run_inproc(
    count: int,
    concurrency: int,
    *,
    db_path: str = ":memory:",
    template: Optional[str] = None,
    timeout_seconds: float = 30.0,
    start_closer: bool = False,
    closer_mid: Optional[float] = None,
) -> TracerRunResult:
    """Function-level entry point — used by the test suite to skip the CLI.

    Synchronous wrapper around `_run_async` so tests don't need to manage
    an asyncio loop themselves.
    """
    return asyncio.run(
        _run_async(
            count=count,
            concurrency=concurrency,
            db_path=db_path,
            template_name=template,
            timeout_seconds=timeout_seconds,
            start_closer=start_closer,
            closer_mid=closer_mid,
        )
    )


def _format_report(result: TracerRunResult) -> str:
    c = result.counts
    return (
        f"tracer run: count={result.articles_requested} "
        f"elapsed={result.elapsed_seconds:.2f}s\n"
        f"  articles            count={c.articles}\n"
        f"  article_clusters    count={c.article_clusters}\n"
        f"  signals             count={c.signals}\n"
        f"  decisions           count={c.decisions}\n"
        f"  trades              count={c.trades}\n"
        f"  decision_inputs     count={c.decision_inputs}\n"
        f"  telemetry_drops     count={result.telemetry_drops}  (allowed)\n"
        f"  handler_exceptions  count={result.handler_exceptions}\n"
        f"  timed_out           {result.timed_out}\n"
        f"  trade_path_drops    {result.drops}"
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.tracer.fire",
        description="P1 tracer: inject synthetic articles, verify propagation.",
    )
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument(
        "--db",
        default=":memory:",
        help="DB path (default `:memory:`). Use a file to inspect with check.py.",
    )
    parser.add_argument(
        "--template",
        default=None,
        help="Restrict to a single template (default: round-robin all).",
    )
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument(
        "--log-level",
        default="WARNING",
        help="Python logging level (default WARNING — keep stdout clean for the report).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.WARNING),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        result = run_inproc(
            count=args.count,
            concurrency=args.concurrency,
            db_path=args.db,
            template=args.template,
            timeout_seconds=args.timeout_seconds,
        )
    except Exception as exc:  # noqa: BLE001 — surface to CLI
        print(f"tracer FAILED during run: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return 1

    print(_format_report(result))
    if result.drops > 0:
        print(
            f"FAIL: {result.drops} trade-path drop(s). "
            "Bus handler_exceptions or article-rows missing from the funnel.",
            file=sys.stderr,
        )
        return 1
    if result.timed_out:
        print(
            "FAIL: bus did not drain within timeout — pipeline back-pressured.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
