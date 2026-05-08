"""
Irene Scanner service.

Polls the Polymarket Gamma API for open markets near resolution,
builds sentence-transformer embeddings, and writes the market index
to PostgreSQL.

Lifecycle:
  1. Load config
  2. Connect to DB
  3. Load embedding model
  4. Loop: fetch → filter → upsert → expire
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
from datetime import datetime, timezone

import aiohttp

from irene_shared.config import ConfigManager
from irene_shared.db import IreneDB
from irene_shared.models import MarketEventType
from irene_shared.redis_client import IreneRedis
from irene_shared.timing import ProfileStore, Timer, jittered_sleep

from gamma_client import GammaClient, parse_market
from market_filter import MarketFilter
from embedder import Embedder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [scanner] %(levelname)s %(name)s: %(message)s",
)
if _log_dir := os.getenv("IRENE_LOG_DIR"):
    os.makedirs(_log_dir, exist_ok=True)
    _fh = logging.FileHandler(os.path.join(_log_dir, "scanner.log"))
    _fh.setFormatter(logging.Formatter("%(asctime)s [scanner] %(levelname)s %(name)s: %(message)s"))
    logging.getLogger().addHandler(_fh)
logger = logging.getLogger(__name__)

_CONFIG_PATH = os.getenv("IRENE_CONFIG_PATH", "/config/config.yaml")


class Scanner:
    def __init__(
        self,
        config_manager: ConfigManager,
        db: IreneDB,
        redis: IreneRedis,
    ) -> None:
        self._cfg = config_manager
        self._db = db
        self._redis = redis
        self._filter = MarketFilter()
        self._embedder = Embedder()
        self._running = False
        self._profile = ProfileStore(window=200)
        self._rng = random.Random()

    async def start(self) -> None:
        logger.info("Scanner starting")
        self._embedder.load()  # blocks briefly; call in executor in prod if needed
        self._running = True
        await self._loop()

    async def stop(self) -> None:
        logger.info("Scanner stopping")
        self._running = False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._run_once()
            except Exception as exc:
                logger.exception("Scanner iteration failed: %s", exc)

            cfg = self._cfg.get()
            interval = cfg.scanner.poll_interval_seconds
            jitter = cfg.scanner.jitter_fraction
            actual = await jittered_sleep(interval, jitter_fraction=jitter, rng=self._rng)
            logger.info("Scanner sleeping %.1fs (base=%ds jitter=±%.0f%%)", actual, interval, jitter * 100)

    async def _run_once(self) -> None:
        cfg = self._cfg.get()
        now = datetime.now(timezone.utc)
        loop = asyncio.get_event_loop()

        # --- Gamma API fetch ---
        with Timer("scanner", "gamma_fetch", self._profile) as t_fetch:
            async with GammaClient(
                base_url=cfg.polymarket.gamma_url,
                timeout_seconds=15,
            ) as client:
                raw_markets = await client.fetch_open_markets(
                    page_size=cfg.scanner.gamma_page_size,
                    max_pages=cfg.scanner.gamma_max_pages,
                )
        logger.debug("gamma_fetch: %d markets in %.0fms", len(raw_markets), t_fetch.duration_ms)

        parsed = [m for raw in raw_markets if (m := parse_market(raw)) is not None]

        # --- Market filter ---
        with Timer("scanner", "market_filter", self._profile):
            filtered = self._filter.filter(parsed, cfg.markets, now)

        # --- Batch embedding ---
        # Embed question only (not description) so market vectors stay in the
        # same semantic space as signal embeddings (title + description).
        # Including the full market description (often 500+ words) pulls the
        # vector away from short news headlines and kills cosine similarity.
        texts = [m["question"] for m in filtered]
        with Timer("scanner", "embed_batch", self._profile) as t_embed:
            embeddings = await loop.run_in_executor(
                None, self._embedder.embed_batch, texts
            )
        logger.debug(
            "embed_batch: %d markets in %.0fms (%.1fms/market)",
            len(filtered), t_embed.duration_ms,
            t_embed.duration_ms / len(filtered) if filtered else 0,
        )

        # --- DB upserts ---
        with Timer("scanner", "db_upsert", self._profile):
            for market, embedding in zip(filtered, embeddings):
                await self._db.upsert_market(
                    condition_id=market["condition_id"],
                    slug=market.get("slug"),
                    question=market["question"],
                    description=market.get("description"),
                    category=market.get("category"),
                    end_date=market["end_date"],
                    volume_usdc=market.get("volume_usdc"),
                    token_yes=market.get("token_yes"),
                    token_no=market.get("token_no"),
                    embedding=embedding,
                )

        expired = await self._db.deactivate_expired_markets(before=now)
        if expired:
            logger.info("Deactivated %d expired markets", expired)

        # Log summary event + timing snapshot
        timing_summary = self._profile.summary_as_dict()
        await self._db.log_event(
            condition_id="__scanner__",
            event_type=MarketEventType.SCANNER_REFRESH,
            payload={
                "fetched": len(raw_markets),
                "parsed": len(parsed),
                "filtered": len(filtered),
                "embedded": len(filtered),
                "expired": expired,
                "timing": timing_summary,
            },
        )

        logger.info(
            "Scanner: fetched=%d parsed=%d filtered=%d expired=%d "
            "| fetch=%.0fms embed=%.0fms",
            len(raw_markets), len(parsed), len(filtered), expired,
            t_fetch.duration_ms, t_embed.duration_ms,
        )

        await self._redis.publish_heartbeat("scanner")
        await self._redis.set_last_seen("scanner", now.isoformat())


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

    scanner = Scanner(cfg_manager, db, redis)

    loop = asyncio.get_event_loop()

    def _shutdown(*_):
        logger.info("Shutdown signal received")
        loop.create_task(scanner.stop())

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        await scanner.start()
    finally:
        await redis.close()
        await db.close()
        await cfg_manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
