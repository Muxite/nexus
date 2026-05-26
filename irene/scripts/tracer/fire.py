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
import os
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
                ON CONFLICT(signal_id, model, prompt_version)
                    WHERE signal_id IS NOT NULL
                    DO NOTHING
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
    """A tracer scenario template.

    Backward compatible with the legacy "single title + summary + url_pattern"
    schema. NEW (cycle 24): two optional fields enable a single template to
    produce ≥50 distinct articles deterministically by `i`:

      title_variants: list[str]
          Alternative title format strings. Round-robin'd by i so consecutive
          fires of the same template don't repeat the same headline.
          The legacy `title` is appended as variant 0 when this list is set,
          ensuring summary's primary phrasing still surfaces.

      variables: dict[str, list[str]]
          A mapping of placeholder name → choice list. Render selects a
          deterministic choice via `i // multiplier % len(list)` so every
          variable rotates at a different rate, maximising variety.

    `render(i)` substitutes `{i}` and every variable into title + summary +
    url. Unknown placeholders in a particular variant simply don't expand
    (str.format with a missing key would raise; we use a safe-substitute
    that leaves unknowns as the empty string).
    """

    name: str
    title: str
    summary: str
    source: str
    source_detail: str
    url_pattern: str
    title_variants: tuple[str, ...] = ()
    variables: tuple[tuple[str, tuple[str, ...]], ...] = ()
    high_quality_overrides: tuple[tuple[str, tuple[str, ...]], ...] = ()

    @property
    def _all_titles(self) -> tuple[str, ...]:
        # Legacy `title` always sits at index 0; variants extend it.
        if not self.title_variants:
            return (self.title,)
        seen = [self.title]
        for v in self.title_variants:
            if v not in seen:
                seen.append(v)
        return tuple(seen)

    def _pick_vars(
        self,
        i: int,
        *,
        prefer_high_quality: bool = False,
    ) -> dict[str, str]:
        """Deterministic per-variable choice. Each variable rotates at a
        unique stride so {target, location, count} all advance independently
        instead of in lock-step.
        """
        overrides = dict(self.high_quality_overrides) if prefer_high_quality else {}
        chosen: dict[str, str] = {}
        for offset, (name, choices) in enumerate(self.variables):
            if not choices:
                continue
            # Stride = prime-ish offset so two variables of equal length
            # don't share a phase. `i // (offset+1) % len(choices)` works.
            stride = offset + 1
            if name in overrides and overrides[name]:
                pool = overrides[name]
            else:
                pool = choices
            chosen[name] = pool[(i // stride) % len(pool)]
        return chosen

    def _safe_format(self, template: str, **kwargs: Any) -> str:
        """Like str.format but missing placeholders → empty string (so a
        variant referencing {price} doesn't crash on a template that didn't
        declare a `price` variable).
        """
        class _Defaulting(dict):
            def __missing__(self, key):  # noqa: D401
                return ""

        try:
            return template.format_map(_Defaulting(kwargs))
        except (IndexError, KeyError, ValueError):
            return template

    def render(
        self,
        i: int,
        *,
        prefer_high_quality: bool = False,
        url_salt: str = "",
    ) -> dict[str, str]:
        titles = self._all_titles
        title_tmpl = titles[i % len(titles)]
        vars_map = self._pick_vars(i, prefer_high_quality=prefer_high_quality)
        ctx = {
            "i": i,
            "source": self.source,
            "source_detail": self.source_detail,
            "template_name": self.name,
            "url_salt": url_salt,
            **vars_map,
        }
        url = self._safe_format(self.url_pattern, **ctx)
        # `url_salt` lets the caller (a tracer prefix) guarantee URL
        # uniqueness across concurrent bursts that re-use the same
        # template + i. We append it as a query parameter so the URL
        # remains valid even if the template doesn't reference the salt.
        if url_salt and "{url_salt}" not in self.url_pattern:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}salt={url_salt}"
        return {
            "title": self._safe_format(title_tmpl, **ctx),
            "summary": self._safe_format(self.summary, **ctx),
            "source": self.source,
            "source_detail": self.source_detail,
            "url": url,
        }

    def estimate_distinct(self) -> int:
        """Upper bound on distinct (title, summary, url) tuples this
        template can produce. Tests use this to assert ≥50.
        """
        n_titles = len(self._all_titles)
        n = n_titles
        for _, choices in self.variables:
            if choices:
                n *= len(choices)
        return n


_REQUIRED_TEMPLATE_KEYS = ("title", "summary", "source", "source_detail", "url_pattern")


def load_template(path: Path) -> _Template:
    raw = yaml.safe_load(path.read_text()) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"template {path} did not parse as a mapping")
    missing = [k for k in _REQUIRED_TEMPLATE_KEYS if k not in raw]
    if missing:
        raise ValueError(f"template {path} missing keys: {missing}")

    raw_variants = raw.get("title_variants") or []
    if not isinstance(raw_variants, list):
        raise ValueError(f"template {path}: title_variants must be a list")
    variants = tuple(str(v) for v in raw_variants)

    raw_vars = raw.get("variables") or {}
    if not isinstance(raw_vars, dict):
        raise ValueError(f"template {path}: variables must be a mapping")
    var_pairs: list[tuple[str, tuple[str, ...]]] = []
    for name, choices in raw_vars.items():
        if not isinstance(choices, list) or not choices:
            raise ValueError(
                f"template {path}: variables[{name!r}] must be a non-empty list"
            )
        var_pairs.append((str(name), tuple(str(c) for c in choices)))

    raw_hq = raw.get("high_quality_overrides") or {}
    if not isinstance(raw_hq, dict):
        raise ValueError(
            f"template {path}: high_quality_overrides must be a mapping"
        )
    hq_pairs: list[tuple[str, tuple[str, ...]]] = []
    for name, choices in raw_hq.items():
        if not isinstance(choices, list) or not choices:
            raise ValueError(
                f"template {path}: high_quality_overrides[{name!r}] must be a non-empty list"
            )
        hq_pairs.append((str(name), tuple(str(c) for c in choices)))

    return _Template(
        name=path.stem,
        title=str(raw["title"]),
        summary=str(raw["summary"]),
        source=str(raw["source"]),
        source_detail=str(raw["source_detail"]),
        url_pattern=str(raw["url_pattern"]),
        title_variants=variants,
        variables=tuple(var_pairs),
        high_quality_overrides=tuple(hq_pairs),
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
    tracer_id: str,
    prefer_high_quality: bool = False,
    url_salt: str = "",
) -> Optional[int]:
    rendered = template.render(
        index,
        prefer_high_quality=prefer_high_quality,
        url_salt=url_salt,
    )
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
                tracer_id,
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
    wallet_pre: Optional[dict[str, Any]] = None
    wallet_post: Optional[dict[str, Any]] = None
    tracer_wallet: Optional[dict[str, Any]] = None
    budget_baseline_usd: Optional[float] = None
    budget_final_usd: Optional[float] = None
    tracer_id_prefix: Optional[str] = None

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
                "wallet_pre": self.wallet_pre,
                "wallet_post": self.wallet_post,
                "tracer_wallet": self.tracer_wallet,
                "budget_baseline_usd": self.budget_baseline_usd,
                "budget_final_usd": self.budget_final_usd,
                "tracer_id_prefix": self.tracer_id_prefix,
            }
        )
        return out


# ---------------------------------------------------------------------------
# Live-externals helpers (Step 2 + Step 4.5 + Step 5)
# ---------------------------------------------------------------------------


async def _live_externals_preflight(
    db: DB,
    *,
    tracer_id_prefix: str,
    force: bool,
) -> None:
    """Safety pre-flight before publishing into the live DB.

    Aborts (or warns + requires --force) when another tracer burst with a
    different prefix has written into `articles` within the last hour —
    catches the "two concurrent tracer prefixes mid-flight" footgun where
    audit_burst.py / cleanup.py output would conflate the two prefixes.
    """
    try:
        cur = await db.conn.execute(
            "SELECT COUNT(*) FROM articles "
            "WHERE is_tracer = 1 "
            "AND COALESCE(tracer_id, '') NOT LIKE ? "
            "AND ingested_at > datetime('now', '-1 hour')",
            (f"{tracer_id_prefix}%",),
        )
        row = await cur.fetchone()
        await cur.close()
    except Exception as exc:  # noqa: BLE001
        # Live DB might be missing the schema entirely (fresh file). Treat
        # an inaccessible articles table as "no concurrent burst".
        logger.warning("live preflight: articles probe failed: %r", exc)
        return
    other_recent = int(row[0] if row else 0)
    if other_recent > 0 and not force:
        raise RuntimeError(
            f"live preflight: {other_recent} tracer article(s) with a "
            f"different prefix landed in the last hour. Pass --force to "
            f"proceed anyway. (My prefix: {tracer_id_prefix!r})"
        )


async def _snapshot_live_wallet(db: DB) -> dict[str, Any]:
    """Read the LIVE (is_tracer=0) wallet aggregates so we can prove the
    burst didn't perturb production state. Best-effort: each query that
    fails contributes a 0 / None to the snapshot rather than aborting.
    """
    async def _scalar(sql: str, params: tuple = ()) -> Any:
        try:
            cur = await db.conn.execute(sql, params)
            row = await cur.fetchone()
            await cur.close()
        except Exception:  # noqa: BLE001
            return None
        return row[0] if row else None

    return {
        "live_trades": await _scalar(
            "SELECT COUNT(*) FROM trades WHERE is_tracer = 0"
        ),
        "live_open_trades": await _scalar(
            "SELECT COUNT(*) FROM trades "
            "WHERE is_tracer = 0 AND closed_at IS NULL"
        ),
        "live_size_total": await _scalar(
            "SELECT COALESCE(SUM(size_usdc), 0) FROM trades "
            "WHERE is_tracer = 0"
        ),
        "live_realized_total": await _scalar(
            "SELECT COALESCE(SUM(realized_pnl_usdc), 0) FROM trade_closures "
            "WHERE is_tracer = 0"
        ),
        "live_articles": await _scalar(
            "SELECT COUNT(*) FROM articles WHERE is_tracer = 0"
        ),
        "snapshot_at": _now_iso(),
    }


async def _snapshot_tracer_wallet(
    db: DB,
    *,
    tracer_id_prefix: str,
) -> dict[str, Any]:
    """Aggregate the tracer-only paper-wallet view for this prefix.

    `is_tracer=1 AND paper=0` is rejected by rule #4, so any trade row
    here is paper. The query proves the lifecycle ran end-to-end:
    executes → closes → realized PnL with fees subtracted.
    """
    pat = f"{tracer_id_prefix}%"

    async def _scalar(sql: str, params: tuple = ()) -> Any:
        try:
            cur = await db.conn.execute(sql, params)
            row = await cur.fetchone()
            await cur.close()
        except Exception:  # noqa: BLE001
            return None
        return row[0] if row else None

    return {
        "tracer_trades": await _scalar(
            "SELECT COUNT(*) FROM trades "
            "WHERE is_tracer = 1 AND tracer_id LIKE ?",
            (pat,),
        ),
        "tracer_open": await _scalar(
            "SELECT COUNT(*) FROM trades "
            "WHERE is_tracer = 1 AND tracer_id LIKE ? AND closed_at IS NULL",
            (pat,),
        ),
        "tracer_closed": await _scalar(
            "SELECT COUNT(*) FROM trade_closures "
            "WHERE is_tracer = 1 AND tracer_id LIKE ?",
            (pat,),
        ),
        "tracer_invested_usdc": await _scalar(
            "SELECT COALESCE(SUM(size_usdc), 0) FROM trades "
            "WHERE is_tracer = 1 AND tracer_id LIKE ?",
            (pat,),
        ),
        "tracer_realized_pnl_usdc": await _scalar(
            "SELECT COALESCE(SUM(realized_pnl_usdc), 0) FROM trade_closures "
            "WHERE is_tracer = 1 AND tracer_id LIKE ?",
            (pat,),
        ),
        "tracer_fees_usdc": await _scalar(
            "SELECT COALESCE(SUM(COALESCE(entry_fee_usdc,0) + COALESCE(exit_fee_usdc,0)), 0) "
            "FROM trade_closures "
            "WHERE is_tracer = 1 AND tracer_id LIKE ?",
            (pat,),
        ),
    }


async def _build_live_externals(
    db: DB,
    *,
    cfg: Config,
    ai_mock: bool,
    budget_cap_usd: Optional[float],
) -> tuple[Any, Any, Any, Any]:
    """Construct the REAL embedder / AIClient / CLOB / budget when
    `--live-externals` is set.

    Returns a 4-tuple matching the slot the tracer stubs fill normally.
    `ai_mock=True` keeps the real embedder + CLOB + budget but swaps the
    LLM for the deterministic `_TracerAI` — this is what tests use to
    exercise the full pipeline without burning OpenRouter credits.

    Budget-cap enforcement happens lazily: the AIClient sees the live
    Budget instance; if Budget reports `remaining_usd` below the cap we
    raise before any spend.
    """
    # Real embedder (sentence-transformers if installed).
    try:
        from app.embed import Embedder  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"live-externals: real Embedder unavailable: {exc!r}"
        ) from exc
    embedder = Embedder()

    # Real CLOB client.
    try:
        from app.clob import CLOBClient  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"live-externals: real CLOBClient unavailable: {exc!r}"
        ) from exc
    clob = CLOBClient()

    # Budget: snapshot, enforce cap before any spend.
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key and not ai_mock:
        raise RuntimeError(
            "live-externals: OPENROUTER_API_KEY not set — refusing to "
            "boot a real AIClient with no auth."
        )
    try:
        from app.budget import Budget  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"live-externals: Budget unavailable: {exc!r}"
        ) from exc
    # Use a conservative pause threshold; the cap below is what actually
    # gates spend per burst.
    budget = Budget(
        api_key=api_key or "",
        monthly_cap_usd=float(budget_cap_usd or 5.0),
        pause_at_remaining_usd=float(budget_cap_usd or 5.0) * 0.1,
        poll_seconds=30,
        pause_flag_path="/tmp/tracer-budget-pause.flag",
    )
    if not ai_mock:
        await budget.start()
    # Cap check: if remaining is known and below cap, abort.
    remaining = getattr(budget, "remaining_usd", None)
    if (
        budget_cap_usd is not None
        and remaining is not None
        and remaining < float(budget_cap_usd)
    ):
        raise RuntimeError(
            f"live-externals: OpenRouter remaining ${remaining:.2f} below "
            f"cap ${budget_cap_usd:.2f}. Aborting before any spend."
        )

    if ai_mock:
        ai = _TracerAI(db)
    else:
        try:
            from app.ai import AIClient  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"live-externals: real AIClient unavailable: {exc!r}"
            ) from exc
        try:
            ai = AIClient(api_key=api_key, db=db, budget=budget, cfg=cfg)
        except TypeError:
            # Best-effort: the AIClient ctor surface differs between
            # checkouts. Fall back to a minimal constructor.
            try:
                ai = AIClient(api_key=api_key, db=db, budget=budget)
            except TypeError:
                ai = AIClient(db=db)  # type: ignore

    return embedder, ai, clob, budget


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
    tracer_id_prefix: str = "tracer",
    prefer_high_quality: bool = False,
    live_externals: bool = False,
    live_externals_ai_mock: bool = False,
    budget_cap_usd: Optional[float] = None,
    force: bool = False,
    config_path: Optional[str] = None,
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

    # ---- Live-externals safety pre-flight (Step 5) -----------------------
    # If another tracer prefix has fired recently into this DB, warn and
    # require --force. Catches the accidental concurrent-burst footgun.
    pre_burst_wallet: Optional[dict[str, Any]] = None
    live_budget_baseline: Optional[float] = None
    if live_externals:
        await _live_externals_preflight(
            db,
            tracer_id_prefix=tracer_id_prefix,
            force=force,
        )
        # Snapshot live wallet before publishing, so post-burst we can
        # prove isolation. Best-effort: failures here are warnings only.
        try:
            pre_burst_wallet = await _snapshot_live_wallet(db)
        except Exception as exc:  # noqa: BLE001
            logger.warning("live wallet snapshot failed: %r", exc)

    # ---- Build config ----------------------------------------------------
    if live_externals and config_path:
        try:
            cfg = Config.from_yaml(config_path)
        except AttributeError:
            # Older Config without classmethod loader; fall back to manual.
            with open(config_path, "r", encoding="utf-8") as fh:
                cfg = Config.model_validate(yaml.safe_load(fh) or {})
    else:
        cfg = _build_tracer_config(
            closer_timeout_hours=closer_timeout_hours,
            closer_poll_seconds=closer_poll_seconds,
        )

    # ---- Build embedder / ai / clob / budget -----------------------------
    if live_externals:
        embedder, ai, clob, budget = await _build_live_externals(
            db,
            cfg=cfg,
            ai_mock=live_externals_ai_mock,
            budget_cap_usd=budget_cap_usd,
        )
        live_budget_baseline = (
            getattr(budget, "remaining_usd", None)
            if budget is not None
            else None
        )
    else:
        embedder = _TracerEmbedder()
        ai = _TracerAI(db)
        # CloserStage requires a CLOB. When `start_closer=True` (the soak
        # path) we want a real number so realized_pnl_usdc != 0; otherwise
        # the stub returns None and any closer that does run would skip.
        clob = _TracerCLOB(default_mid=closer_mid)
        budget = _TracerBudget()

    bus = InProcBus()
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
    # Skip seeding when running against the live DB — it already holds
    # real markets that the live bot has been populating.
    if not live_externals:
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
            tracer_id = f"{tracer_id_prefix}-{i}"
            # Carry the prefix into the URL so two concurrent bursts on the
            # same DB don't collide on `articles.url`'s UNIQUE constraint.
            async with insert_lock:
                article_id = await _insert_tracer_article(
                    db,
                    cycle_id=cycle_id,
                    template=template,
                    index=i,
                    tracer_id=tracer_id,
                    prefer_high_quality=prefer_high_quality,
                    url_salt=tracer_id_prefix,
                )
            if article_id is None:
                return
            rendered = template.render(
                i,
                prefer_high_quality=prefer_high_quality,
                url_salt=tracer_id_prefix,
            )
            await bus.publish(
                TOPIC_ARTICLE_INGESTED,
                {
                    "article_id": article_id,
                    "url": rendered["url"],
                    "is_tracer": True,
                    "tracer_id": tracer_id,
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

    # Step 4.5 — post-flight wallet snapshot + tracer-wallet view.
    wallet_post: Optional[dict[str, Any]] = None
    tracer_wallet: Optional[dict[str, Any]] = None
    if live_externals:
        try:
            wallet_post = await _snapshot_live_wallet(db)
        except Exception as exc:  # noqa: BLE001
            logger.warning("post-burst live wallet snapshot failed: %r", exc)
        try:
            tracer_wallet = await _snapshot_tracer_wallet(
                db, tracer_id_prefix=tracer_id_prefix
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("tracer wallet snapshot failed: %r", exc)

    budget_final = (
        getattr(budget, "remaining_usd", None) if live_externals else None
    )

    elapsed = time.monotonic() - t0
    await db.close()

    return TracerRunResult(
        counts=counts,
        telemetry_drops=bus.counters.telemetry,
        handler_exceptions=bus.counters.handler_exceptions,
        timed_out=not drained,
        elapsed_seconds=elapsed,
        articles_requested=count,
        wallet_pre=pre_burst_wallet,
        wallet_post=wallet_post,
        tracer_wallet=tracer_wallet,
        budget_baseline_usd=live_budget_baseline,
        budget_final_usd=budget_final,
        tracer_id_prefix=tracer_id_prefix,
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
    tracer_id_prefix: str = "tracer",
    prefer_high_quality: bool = False,
    live_externals: bool = False,
    live_externals_ai_mock: bool = False,
    budget_cap_usd: Optional[float] = None,
    force: bool = False,
    config_path: Optional[str] = None,
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
            tracer_id_prefix=tracer_id_prefix,
            prefer_high_quality=prefer_high_quality,
            live_externals=live_externals,
            live_externals_ai_mock=live_externals_ai_mock,
            budget_cap_usd=budget_cap_usd,
            force=force,
            config_path=config_path,
        )
    )


def _format_report(result: TracerRunResult) -> str:
    c = result.counts
    parts = [
        f"tracer run: count={result.articles_requested} "
        f"elapsed={result.elapsed_seconds:.2f}s",
        f"  tracer_id_prefix    {result.tracer_id_prefix}",
        f"  articles            count={c.articles}",
        f"  article_clusters    count={c.article_clusters}",
        f"  signals             count={c.signals}",
        f"  decisions           count={c.decisions}",
        f"  trades              count={c.trades}",
        f"  decision_inputs     count={c.decision_inputs}",
        f"  telemetry_drops     count={result.telemetry_drops}  (allowed)",
        f"  handler_exceptions  count={result.handler_exceptions}",
        f"  timed_out           {result.timed_out}",
        f"  trade_path_drops    {result.drops}",
    ]
    if result.tracer_wallet is not None:
        tw = result.tracer_wallet
        parts.extend(
            [
                "",
                "  tracer wallet (paper, isolated by rule #4):",
                f"    trades_opened     {tw.get('tracer_trades')}",
                f"    trades_open       {tw.get('tracer_open')}",
                f"    trades_closed     {tw.get('tracer_closed')}",
                f"    invested_usdc     {tw.get('tracer_invested_usdc')}",
                f"    realized_pnl_usdc {tw.get('tracer_realized_pnl_usdc')}",
                f"    fees_usdc         {tw.get('tracer_fees_usdc')}",
            ]
        )
    if result.wallet_pre is not None and result.wallet_post is not None:
        pre = result.wallet_pre
        post = result.wallet_post
        delta_realized = (
            (post.get("live_realized_total") or 0)
            - (pre.get("live_realized_total") or 0)
        )
        delta_trades = (
            (post.get("live_trades") or 0) - (pre.get("live_trades") or 0)
        )
        parts.extend(
            [
                "",
                "  live wallet isolation check (should both be ~0):",
                f"    delta_realized_usdc  {delta_realized}",
                f"    delta_live_trades    {delta_trades}",
            ]
        )
    if result.budget_baseline_usd is not None:
        parts.append(
            f"  openrouter_budget   baseline=${result.budget_baseline_usd} "
            f"final=${result.budget_final_usd}"
        )
    return "\n".join(parts)


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
    parser.add_argument(
        "--tracer-id-prefix",
        default="tracer",
        help=(
            "Prefix for the per-row tracer_id. Each fire's tracer_id is "
            "<prefix>-<seq>. Used by audit_burst.py / cleanup.py to scope "
            "their queries. Default 'tracer'."
        ),
    )
    parser.add_argument(
        "--live-externals",
        action="store_true",
        help=(
            "Boot real Embedder + AIClient + CLOBClient against the live "
            "DB instead of the in-process stubs. BURNS REAL OPENROUTER "
            "CREDITS. Use --budget-cap-usd to bound spend."
        ),
    )
    parser.add_argument(
        "--budget-cap-usd",
        type=float,
        default=None,
        help=(
            "When --live-externals is set, abort the burst if OpenRouter "
            "remaining drops below this dollar cap. Default 5.0 USD."
        ),
    )
    parser.add_argument(
        "--config",
        default=None,
        help=(
            "Path to live config.yaml. Required with --live-externals when "
            "matcher / dedupe / closer settings should mirror production."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Skip the live-externals concurrent-burst guard. Without this, "
            "fire.py refuses to publish if another tracer prefix wrote "
            "into `articles` within the last hour."
        ),
    )
    parser.add_argument(
        "--exercise-wallet",
        action="store_true",
        help=(
            "Force high-quality payload variants and run a post-burst "
            "tracer-wallet snapshot. Validates that tracer trades exercise "
            "the paper-wallet lifecycle while live wallet equity stays "
            "unchanged (rule #4 isolation)."
        ),
    )
    parser.add_argument(
        "--live-externals-ai-mock",
        action="store_true",
        help=(
            "Internal test toggle: use --live-externals plumbing (real DB, "
            "real Embedder/CLOB/Budget) but keep the deterministic stub AI "
            "to avoid spending OpenRouter credits in unit tests."
        ),
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
            tracer_id_prefix=args.tracer_id_prefix,
            prefer_high_quality=args.exercise_wallet,
            live_externals=args.live_externals,
            live_externals_ai_mock=args.live_externals_ai_mock,
            budget_cap_usd=args.budget_cap_usd,
            force=args.force,
            config_path=args.config,
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
