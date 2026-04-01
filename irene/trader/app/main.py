"""
Irene Trader service.

Subscribes to Redis irene:signals, evaluates each MappedSignal via
DecisionEngine, and executes orders via Executor.  Runs PositionTracker
as a background coroutine to close resolved/timed-out positions.

Config is hot-reloaded every 20 s via ConfigManager; all thresholds are
read from cfg at call-site, never cached.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal as _signal
from datetime import datetime, timezone

from irene_shared.config import ConfigManager
from irene_shared.db import IreneDB
from irene_shared.models import Direction, MappedSignal, MarketEventType
from irene_shared.redis_client import IreneRedis
from irene_shared.timing import ProfileStore, Timer

from clob_client import ClobClient
from decision import DecisionEngine
from executor import Executor
from position_tracker import PositionTracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [trader] %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_CONFIG_PATH = os.getenv("IRENE_CONFIG_PATH", "/config/config.yaml")


class Trader:
    """
    Orchestrates the signal → decision → execution pipeline.

    Signal consumption is event-driven (Redis pub/sub).
    Position management runs on a periodic background task.
    """

    def __init__(
        self,
        config_manager: ConfigManager,
        db: IreneDB,
        redis: IreneRedis,
    ) -> None:
        self._cfg = config_manager
        self._db = db
        self._redis = redis
        self._engine = DecisionEngine()
        self._executor = Executor(db)
        self._profile = ProfileStore(window=200)
        self._running = False

    async def start(self) -> None:
        logger.info("Trader starting")
        self._running = True
        tracker = PositionTracker(self._cfg, self._db, self._profile)
        await asyncio.gather(
            self._signal_loop(),
            tracker.start(),
        )

    async def stop(self) -> None:
        logger.info("Trader stopping")
        self._running = False

    # ------------------------------------------------------------------
    # Signal loop
    # ------------------------------------------------------------------

    async def _signal_loop(self) -> None:
        async for raw_json in self._redis.subscribe_signals():
            if not self._running:
                break
            try:
                ms = MappedSignal.model_validate_json(raw_json)
                await self._handle_signal(ms)
            except Exception as exc:
                logger.exception("Signal handling error: %s", exc)

    async def _handle_signal(self, ms: MappedSignal) -> None:
        cfg = self._cfg.get()

        # Fetch orderbook for the relevant token
        token_id = ms.token_yes if ms.direction == Direction.YES else ms.token_no

        with Timer("trader", "clob_fetch", self._profile):
            async with ClobClient(base_url=cfg.polymarket.clob_url) as clob:
                ob = await clob.get_orderbook(token_id)

        if ob is None:
            logger.warning(
                "No orderbook for token %s (cond=%s) — skipping signal",
                token_id[:8], ms.condition_id[:8],
            )
            return

        open_positions = await self._db.count_open_positions()

        with Timer("trader", "decision", self._profile):
            decision = self._engine.evaluate(
                signal=ms,
                orderbook=ob,
                balance_usdc=cfg.trading.balance_usdc,
                open_positions=open_positions,
                cfg=cfg.trading,
            )

        # Record every decision (including skips) for per-market analysis
        await self._db.log_event(
            condition_id=ms.condition_id,
            event_type=MarketEventType.TRADE_DECIDED,
            payload={
                "mapped_signal_id": ms.signal_id,
                "direction": ms.direction.value,
                "p_hat": decision.p_hat,
                "mid_price": decision.mid_price,
                "edge": decision.edge,
                "kelly_size_usdc": decision.kelly_size_usdc,
                "skip_reason": decision.skip_reason,
            },
        )

        if decision.skip_reason:
            return

        with Timer("trader", "execute", self._profile):
            await self._executor.execute(decision, cfg)

        # Heartbeat so the monitor knows the trader is alive
        await self._redis.set_last_seen(
            "trader", datetime.now(timezone.utc).isoformat()
        )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


async def main() -> None:
    cfg_manager = ConfigManager(_CONFIG_PATH)
    await cfg_manager.start()

    cfg = cfg_manager.get()
    db = IreneDB(cfg.database.url, pool_size=cfg.database.pool_size)
    await db.connect()

    redis = IreneRedis(
        url=cfg.redis.url,
        signal_channel=cfg.redis.signal_channel,
        heartbeat_channel=cfg.redis.heartbeat_channel,
    )
    await redis.connect()

    trader = Trader(cfg_manager, db, redis)

    loop = asyncio.get_event_loop()

    def _shutdown(*_):
        logger.info("Shutdown signal received")
        loop.create_task(trader.stop())

    _signal.signal(_signal.SIGTERM, _shutdown)
    _signal.signal(_signal.SIGINT, _shutdown)

    try:
        await trader.start()
    finally:
        await redis.close()
        await db.close()
        await cfg_manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
