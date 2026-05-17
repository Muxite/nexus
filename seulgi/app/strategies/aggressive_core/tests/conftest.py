"""Shared fixtures for aggressive_core tests.

We use a tmp on-disk SQLite (rather than :memory:) so SeulgiDB's
file-URI connection works. EventBus runs in 'ring-only' mode by writing
to a tmp path that we clean up.
"""
from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from typing import AsyncIterator

import pytest
import pytest_asyncio

try:
    from db import SeulgiDB
except ImportError:
    from app.db import SeulgiDB  # type: ignore[no-redef]

from ..config import CoreConfig
from ..observability import EventBus
from ..paper_trader import PaperTrader
from ..strategy import CoreStrategy


@pytest.fixture
def core_cfg() -> CoreConfig:
    cfg = CoreConfig()
    # Make tests deterministic: no latency, no per-market hourly throttle.
    cfg.entry_delay_ms = 0
    cfg.max_size_per_market_per_hour_usdc = 1_000_000.0
    cfg.min_seconds_between_entries = 0.0
    return cfg


@pytest_asyncio.fixture
async def tmp_db() -> AsyncIterator[SeulgiDB]:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = SeulgiDB(path)
    await db.connect()
    try:
        yield db
    finally:
        await db.close()
        Path(path).unlink(missing_ok=True)


@pytest_asyncio.fixture
async def bus(tmp_path) -> AsyncIterator[EventBus]:
    b = EventBus(ndjson_path=tmp_path / "events.ndjson")
    await b.start()
    try:
        yield b
    finally:
        await b.stop()


@pytest_asyncio.fixture
async def paper_trader(core_cfg, tmp_db, bus) -> AsyncIterator[PaperTrader]:
    t = PaperTrader(cfg=core_cfg, db=tmp_db, bus=bus)
    yield t


@pytest_asyncio.fixture
async def strategy(core_cfg, tmp_db, bus, paper_trader) -> AsyncIterator[CoreStrategy]:
    s = CoreStrategy(cfg=core_cfg, db=tmp_db, trader=paper_trader, bus=bus)
    yield s
    await s.close()
