"""
Irene Monitor — read-only FastAPI observability layer.

Endpoints:
  GET /                         Redirect to `/ui/` (signals viewer)
  GET /ui/                      Static minimal signals UI
  GET /health                   System status, service heartbeats
  GET /markets                  Active market index with P&L summary
  GET /markets/{id}/events      Per-market event timeline
  GET /trades                   Trade log (paginated, filterable)
  GET /signals                  Recent signals with match counts
  GET /positions                Open positions
  GET /calibration              Brier score per signal source
  GET /metrics                  Service profiler summaries from Redis

Also writes ``dashboard.txt`` when ``monitor.dashboard_text_path`` is set
(Kazuha-style plain-text snapshot on a volume).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from irene_shared.config import ConfigManager
from irene_shared.db import IreneDB
from irene_shared.redis_client import IreneRedis
from text_dashboard import write_dashboard_text_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [monitor] %(levelname)s %(name)s: %(message)s",
)
if _log_dir := os.getenv("IRENE_LOG_DIR"):
    os.makedirs(_log_dir, exist_ok=True)
    _fh = logging.FileHandler(os.path.join(_log_dir, "monitor.log"))
    _fh.setFormatter(logging.Formatter("%(asctime)s [monitor] %(levelname)s %(name)s: %(message)s"))
    logging.getLogger().addHandler(_fh)
logger = logging.getLogger(__name__)

_CONFIG_PATH = os.getenv("IRENE_CONFIG_PATH", "/config/config.yaml")
_STATIC_DIR = Path(__file__).resolve().parent / "static"


async def _signals_export_loop(app: FastAPI) -> None:
    """
    Writes `signals.json` under `monitor.signals_export_dir` on each interval.

    :param app: FastAPI app with `state.db` and `state.cfg`.
    """
    while True:
        try:
            cm: ConfigManager = app.state.cfg
            cfg = cm.get()
            export_dir = (cfg.monitor.signals_export_dir or "").strip()
            interval = cfg.monitor.signals_export_interval_seconds
            if not export_dir or interval <= 0:
                await asyncio.sleep(15)
                continue
            db: IreneDB = app.state.db
            rows = await db.get_recent_signals(
                limit=cfg.monitor.signals_export_limit,
                offset=0,
            )
            out = Path(export_dir)
            out.mkdir(parents=True, exist_ok=True)
            target = out / "signals.json"
            payload = {
                "exported_at": datetime.now(timezone.utc).isoformat(),
                "count": len(rows),
                "signals": rows,
            }
            tmp = out / "signals.json.tmp"
            tmp.write_text(
                json.dumps(payload, indent=2, default=str),
                encoding="utf-8",
            )
            tmp.replace(target)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("signals export failed")
        await asyncio.sleep(max(interval, 1))


async def _dashboard_text_loop(app: FastAPI) -> None:
    """
    Overwrite ``monitor.dashboard_text_path`` on a schedule (Kazuha-style).

    :param app: FastAPI app with ``state.db``, ``state.redis``, ``state.cfg``.
    """
    while True:
        try:
            cm: ConfigManager = app.state.cfg
            cfg = cm.get()
            raw_path = (cfg.monitor.dashboard_text_path or "").strip()
            interval = cfg.monitor.dashboard_text_interval_seconds
            if not raw_path or interval <= 0:
                await asyncio.sleep(15)
                continue
            db: IreneDB = app.state.db
            redis: IreneRedis = app.state.redis
            await write_dashboard_text_file(
                db,
                redis,
                Path(raw_path),
                starting_balance_usdc=cfg.trading.balance_usdc,
                mode=cfg.irene.mode,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("text dashboard write failed")
        await asyncio.sleep(max(interval, 1))


@asynccontextmanager
async def lifespan(app: FastAPI):
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

    app.state.db = db
    app.state.redis = redis
    app.state.cfg = cfg_manager

    export_task = asyncio.create_task(_signals_export_loop(app))
    dashboard_task = asyncio.create_task(_dashboard_text_loop(app))

    logger.info("Monitor started on port %s", cfg.monitor.port)
    yield

    export_task.cancel()
    dashboard_task.cancel()
    try:
        await export_task
    except asyncio.CancelledError:
        pass
    try:
        await dashboard_task
    except asyncio.CancelledError:
        pass

    await redis.close()
    await db.close()
    await cfg_manager.stop()
    logger.info("Monitor stopped")


app = FastAPI(title="Irene Monitor", version="1.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/health")
async def health(request: Request) -> dict:
    """System status: service heartbeats and open position count."""
    db: IreneDB = request.app.state.db
    redis: IreneRedis = request.app.state.redis

    open_positions = await db.count_open_positions()
    scanner_last = await redis.get_last_seen("scanner")
    feeder_last = await redis.get_last_seen("feeder")
    trader_last = await redis.get_last_seen("trader")

    return {
        "status": "ok",
        "open_positions": open_positions,
        "last_seen": {
            "scanner": scanner_last,
            "feeder": feeder_last,
            "trader": trader_last,
        },
    }


# ---------------------------------------------------------------------------
# Markets
# ---------------------------------------------------------------------------


@app.get("/markets")
async def markets(
    request: Request,
    limit: int = Query(default=200, ge=1, le=500),
) -> dict:
    """Active market index with aggregated trade counts and realized P&L."""
    db: IreneDB = request.app.state.db
    rows = await db.get_active_markets(limit=limit)
    return {"markets": rows, "count": len(rows)}


@app.get("/markets/{condition_id}/events")
async def market_events(
    condition_id: str,
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Full event timeline for a single market."""
    db: IreneDB = request.app.state.db
    events = await db.get_market_events(condition_id, limit=limit, offset=offset)
    return {"condition_id": condition_id, "events": events, "count": len(events)}


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------


@app.get("/trades")
async def trades(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    condition_id: Optional[str] = None,
    outcome: Optional[str] = None,
) -> dict:
    """Trade log, newest first. Filterable by condition_id and outcome."""
    db: IreneDB = request.app.state.db
    rows = await db.get_recent_trades(
        limit=limit,
        offset=offset,
        condition_id=condition_id,
        outcome=outcome,
    )
    return {"trades": rows, "count": len(rows)}


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------


@app.get("/signals")
async def signals(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Recent ingested signals with mapped-market match counts."""
    db: IreneDB = request.app.state.db
    rows = await db.get_recent_signals(limit=limit, offset=offset)
    return {"signals": rows, "count": len(rows)}


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------


@app.get("/positions")
async def positions(request: Request) -> dict:
    """Currently open positions."""
    db: IreneDB = request.app.state.db
    rows = await db.get_open_positions()
    return {"positions": rows, "count": len(rows)}


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------


@app.get("/calibration")
async def calibration(request: Request) -> dict:
    """Brier score summary per signal source."""
    db: IreneDB = request.app.state.db
    rows = await db.get_brier_summary()
    return {"calibration": rows}


# ---------------------------------------------------------------------------
# Metrics (Phase 7 — profiler surface)
# ---------------------------------------------------------------------------


@app.get("/metrics")
async def metrics(request: Request) -> dict:
    """Profiler summaries published by each service to Redis."""
    redis: IreneRedis = request.app.state.redis
    services = ["scanner", "feeder", "trader"]
    result = {}
    for svc in services:
        profile = await redis.get_profile(svc)
        if profile is not None:
            result[svc] = profile
    return {"metrics": result}


@app.get("/")
async def root() -> RedirectResponse:
    """Serve the minimal UI at `/ui/`."""
    return RedirectResponse(url="/ui/", status_code=302)


if _STATIC_DIR.is_dir():
    app.mount(
        "/ui",
        StaticFiles(directory=str(_STATIC_DIR), html=True),
        name="ui",
    )


if __name__ == "__main__":
    _cfg = ConfigManager(_CONFIG_PATH).get()
    _port = _cfg.monitor.port
    logger.info("Monitor binding to port %s", _port)
    uvicorn.run("main:app", host="0.0.0.0", port=_port, reload=False)
