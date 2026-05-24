"""
## Specification

**Purpose.** Boot the irene process, construct every resource + stage,
wire the bus subscriptions, drive the asyncio loop, and shut everything
down in reverse on SIGTERM.

**Public interface.**
- `run()` — synchronous entry point called from `__main__.py`.
- `main()` — the async lifecycle coroutine; testable in isolation.
- `wire_pipeline(deps_base, *, market_index)` — small helper that builds
  the five stages and registers their bus subscriptions; exposed for the
  boot smoke test.

**Owned tables.** `cycles` (modularization rule 4). The DDL is registered
with `db.py` at module import time below.

**Invariants.**
- `cycles` row is upserted before any stage starts; `cycle.set_cycle_id`
  propagates the value into the ContextVar so every trade-path writer can
  stamp `cycle_id`.
- `bus.subscribe()` is called BEFORE `bus.start()` (rule enforced by
  `app/bus.py` — late-subscribe raises).
- Stages are started in topological-ish order (markets → ingest → dedupe
  → match → trade) and stopped in reverse so a stop-cascading shutdown
  drains downstream queues first.
- The OpenRouter `Budget` poll loop is only started when an API key is
  present — boots with an empty key (e.g. CI smoke) don't fire HTTP at a
  blank credential.

**Side effects.** Opens the DB; starts every poll/loop task; installs
signal handlers; logs at INFO on every lifecycle event.

**Failure modes.** Any exception during construction propagates; the
process exits non-zero. `IreneDBPathError` from `db.py` is treated the
same way — the assertion exists to crash us.

**Non-goals.** Pipeline logic — each stage owns its own loop /
subscription; main.py only wires.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Optional

from app import db as db_module
from app.ai import AIClient
from app.budget import Budget
from app.bus import InProcBus
from app.clob import CLOBClient
from app.config import Config, ConfigManager, load_config
from app.cycle import set_cycle_id
from app.db import DB
from app.deps import StageDeps
from app.embed import Embedder
from app.dashboard import Dashboard
from app.stages.closer import CloserStage
from app.stages.dedupe import DedupeStage
from app.stages.dkg_trade import DKGTradeStage
# Import order matters: extract.py owns `facts`; eventize.py's DDL contains
# an ALTER TABLE facts ADD COLUMN event_id. Schema apply runs in
# registration order, so extract MUST register before eventize. (Mirror
# constraint for link.py — it references events(id) via FK, so eventize
# must register before link.) Keep the explicit ordering below; do not
# resort alphabetically.
from app.stages.extract import ExtractStage
from app.stages.eventize import EventizeStage
from app.stages.link import LinkerStage
from app.stages.impact_tracker import ImpactTrackerStage
from app.stages.ingest import IngestStage
from app.stages.markets import MarketIndex, MarketsStage
from app.stages.match import MatchStage
from app.stages.resolve import ResolveStage
from app.stages.trade import TradeStage
from app.topics import (
    TOPIC_ARTICLE_DEDUPED,
    TOPIC_ARTICLE_INGESTED,
    TOPIC_EVENT_UPDATED,
    TOPIC_EXTRACTION_COMPLETED,
    TOPIC_MARKET_LINKS_UPDATED,
    TOPIC_RESOLUTION_COMPLETED,
    TOPIC_SIGNAL_CREATED,
    TOPIC_TRADE_DECIDED,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema owned by main.py (per modularization rule 4)
# ---------------------------------------------------------------------------

_CYCLES_DDL = [
    """
    CREATE TABLE IF NOT EXISTS cycles (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL UNIQUE,
        opened_at   TEXT NOT NULL,
        closed_at   TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_cycles_opened_at ON cycles(opened_at)",
]
db_module.register_schema("app/main.py", _CYCLES_DDL)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------


def _build_budget(cfg: Config) -> Budget:
    return Budget(
        api_key=cfg.openrouter.api_key,
        monthly_cap_usd=cfg.budget.monthly_cap_usd,
        pause_at_remaining_usd=cfg.budget.pause_at_remaining_usd,
        poll_seconds=cfg.budget.poll_seconds,
        pause_flag_path="/data/budget_paused.flag",
    )


class Pipeline:
    """Bag of running stages + resources, returned from `wire_pipeline()`.

    Carries enough handle state for a clean shutdown — main.py simply
    iterates `stages_in_stop_order` and calls `.stop()` on each.
    """

    def __init__(
        self,
        *,
        markets: MarketsStage,
        ingest: IngestStage,
        dedupe: DedupeStage,
        match: MatchStage,
        trade: TradeStage,
        closer: CloserStage,
        extract: ExtractStage,
        resolve: ResolveStage,
        eventize: EventizeStage,
        link: LinkerStage,
        impact_tracker: ImpactTrackerStage,
        dkg_trade: Optional["DKGTradeStage"] = None,
    ) -> None:
        self.markets = markets
        self.ingest = ingest
        self.dedupe = dedupe
        self.match = match
        self.trade = trade
        self.closer = closer
        self.extract = extract
        self.resolve = resolve
        self.eventize = eventize
        self.link = link
        self.impact_tracker = impact_tracker
        # P3 chunk 3 (workstream B): DKG trade path. Optional in the
        # ctor so older test wirings that pre-date this param keep
        # working; production wiring (wire_pipeline) always sets it.
        self.dkg_trade = dkg_trade

    @property
    def start_order(self) -> tuple:
        # P2: extract is wired alongside match on article.deduped.
        # P3 chunk 1: resolve subscribes to extraction.completed.
        # P3 chunk 3 (workstream A): eventize subscribes to
        # resolution.completed; link subscribes to event.updated. Together
        # they finish the DKG pipeline live (extract → resolve → eventize
        # → link → market_links). Trade path does not read market_links
        # yet (workstream B wires that).
        # Impact tracker (observability): subscribes to trade.decided and
        # polls CLOB at +1m/5m/15m/30m/60m. Pure diagnostic — never feeds
        # the trade path (docs/philosophy.md "we don't predict; we
        # verify the speed thesis holds via this data").
        # P3 chunk 3 (workstream B): dkg_trade is started AFTER link so
        # `market_links.updated` has a producer before the subscriber
        # turns on. Filtered out when unset so older test wirings that
        # don't construct it continue to boot.
        stages = (
            self.markets, self.ingest, self.dedupe, self.match,
            self.trade, self.closer, self.extract, self.resolve,
            self.eventize, self.link,
            self.impact_tracker,
        )
        if self.dkg_trade is not None:
            stages = stages + (self.dkg_trade,)
        return stages

    @property
    def stop_order(self) -> tuple:
        return tuple(reversed(self.start_order))


def wire_pipeline(
    *,
    db: DB,
    bus: InProcBus,
    cfg: Config,
    embedder: Embedder,
    ai: AIClient,
    budget: Budget,
    clob: Optional[CLOBClient] = None,
) -> Pipeline:
    """Construct every stage and register their subscriptions.

    Returns the `Pipeline` handle. Caller is responsible for `bus.start()`
    + `stage.start()` calls (this function only wires; it doesn't run).
    """
    base_deps = StageDeps(
        db=db,
        bus=bus,
        config=cfg,
        embedder=embedder,
        ai=ai,
        budget=budget,
        clob=clob,
    )
    markets = MarketsStage(base_deps)

    full_deps = StageDeps(
        db=db,
        bus=bus,
        config=cfg,
        embedder=embedder,
        ai=ai,
        budget=budget,
        market_index=markets.index,
        clob=clob,
    )
    ingest = IngestStage(full_deps)
    dedupe = DedupeStage(full_deps)
    match = MatchStage(full_deps)
    trade = TradeStage(full_deps)
    # Closer needs `deps.clob`; if `clob is None`, construction raises and
    # the boot fails loudly — a paper-trading bot without a price source
    # can't reason about exits.
    closer = CloserStage(full_deps)
    # Extract (P2) needs `deps.budget`. It runs alongside match on the
    # same `article.deduped` topic — the bus fans out to both subscribers.
    extract = ExtractStage(full_deps)
    # Resolve (P3 chunk 1) subscribes to extraction.completed. It owns
    # the entity_resolutions overlay; never mutates extract's tables.
    resolve = ResolveStage(full_deps)
    # Eventize (P3 chunk 2a) subscribes to resolution.completed. Clusters
    # facts into canonical `events`; writes `event_edges` + stamps
    # `facts.event_id`. Publishes `event.updated` for the linker.
    eventize = EventizeStage(full_deps)
    # Linker (P3 chunk 2b) subscribes to event.updated. Scores each
    # updated event against every active market and persists the top-K
    # candidates to `market_links`. `start()` loads ALL market embeddings
    # into memory — takes a beat at boot. Owns `market_links`.
    link = LinkerStage(full_deps)
    # Impact tracker (observability): subscribes to trade.decided. Polls
    # CLOB at +1m/5m/15m/30m/60m to measure whether the price moved the
    # way we bet. Pure diagnostic; never reads back into the trade path.
    impact_tracker = ImpactTrackerStage(full_deps)
    # DKG trade (P3 chunk 3 workstream B). Subscribes to
    # `market_links.updated`; for every link above
    # `cfg.dkg_trade.score_floor` re-runs the LLM direction call against
    # the article behind the event and writes a `trades` row with
    # `decision_path='dkg'`. Sister of TradeStage; both feed the impact
    # tracker via TOPIC_TRADE_DECIDED.
    dkg_trade = DKGTradeStage(full_deps)

    # Subscriptions MUST be registered before bus.start() — the bus
    # rejects late subscribers (catches the wiring bug where a stage
    # silently misses messages).
    bus.subscribe(TOPIC_ARTICLE_INGESTED, dedupe.handle)
    bus.subscribe(TOPIC_ARTICLE_DEDUPED, match.handle)
    bus.subscribe(TOPIC_ARTICLE_DEDUPED, extract.handle)
    bus.subscribe(TOPIC_SIGNAL_CREATED, trade.handle)
    bus.subscribe(TOPIC_EXTRACTION_COMPLETED, resolve.handle)
    bus.subscribe(TOPIC_RESOLUTION_COMPLETED, eventize.handle)
    bus.subscribe(TOPIC_EVENT_UPDATED, link.handle)
    bus.subscribe(TOPIC_TRADE_DECIDED, impact_tracker.handle)
    bus.subscribe(TOPIC_MARKET_LINKS_UPDATED, dkg_trade.handle)

    return Pipeline(
        markets=markets,
        ingest=ingest,
        dedupe=dedupe,
        match=match,
        trade=trade,
        closer=closer,
        extract=extract,
        resolve=resolve,
        eventize=eventize,
        link=link,
        dkg_trade=dkg_trade,
        impact_tracker=impact_tracker,
    )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def main() -> int:
    config_path = os.environ.get("IRENE_CONFIG", "/config/config.yaml")
    cfg = load_config(config_path)
    _setup_logging(cfg.irene.log_level)

    logger.info("irene boot starting (config=%s)", config_path)

    cfg_mgr = ConfigManager(config_path)
    await cfg_mgr.start()

    db = DB(
        path=cfg.db.path,
        journal_mode=cfg.db.journal_mode,
        synchronous=cfg.db.synchronous,
    )
    await db.connect()
    cycle_id = await db.ensure_current_cycle(cfg.irene.cycle)
    set_cycle_id(cycle_id)

    embedder = Embedder(cfg.embedder.name, cfg.embedder.dim)
    budget = _build_budget(cfg)
    ai = AIClient(db=db, budget=budget, config=cfg)
    clob = CLOBClient(base_url=cfg.clob.base_url, timeout_seconds=cfg.clob.timeout_seconds)
    bus = InProcBus()

    pipeline = wire_pipeline(
        db=db, bus=bus, cfg=cfg, embedder=embedder, ai=ai, budget=budget, clob=clob,
    )

    # Dashboard runs in the background, writing text panels every
    # interval_secs. Self-contained; reads the DB but never writes.
    # Dashboard directory derives from the DB path so dev runs with
    # `:memory:` route to /tmp and don't try to write to /data (which
    # only the container has permissions for).
    if cfg.db.path == ":memory:":
        dashboard_dir = "/tmp/irene_dashboard"
    else:
        dashboard_dir = str(Path(cfg.db.path).parent / "dashboard")
    dashboard = Dashboard(
        data_dir=dashboard_dir,
        db=db,
        interval_secs=30,
    )

    # Only start the OpenRouter poll if we have a credential — a blank
    # key would loop on 401s and clutter the boot log.
    if cfg.openrouter.api_key:
        await budget.start()

    await bus.start()
    for stage in pipeline.start_order:
        await stage.start()
    dashboard._task = asyncio.create_task(dashboard.run())

    logger.info(
        "irene boot OK, %d stages wired (cycle=%s id=%d db=%s budget=%s extract=shadow resolve=shadow eventize=shadow link=shadow impact=tracking)",
        len(pipeline.start_order),
        cfg.irene.cycle,
        cycle_id,
        cfg.db.path,
        "polling" if cfg.openrouter.api_key else "idle (no api_key)",
    )

    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: stop.done() or stop.set_result(s))

    try:
        await stop
    finally:
        logger.info("irene shutting down")
        await dashboard.stop()
        for stage in pipeline.stop_order:
            try:
                await stage.stop()
            except Exception:  # noqa: BLE001
                logger.exception("error stopping %s", stage.__class__.__name__)
        await bus.stop()
        await ai.close()
        await clob.close()
        if cfg.openrouter.api_key:
            await budget.stop()
        await cfg_mgr.stop()
        await db.close()
    return 0


def run() -> None:
    try:
        rc = asyncio.run(main())
    except KeyboardInterrupt:
        rc = 0
    sys.exit(rc)
