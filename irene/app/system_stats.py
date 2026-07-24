"""
## Specification

**Purpose.** Periodic per-container CPU and memory sampler. Targets a
fixed set of sibling containers (irene, jennie-scraper-v2), reads
`/containers/{name}/stats?stream=false` over
the Docker Engine API UDS, and appends one row per target per tick to
the `container_stats` SQLite table so the dashboard page + plot can
render the last hour without hitting the docker socket on render.

**Public interface.**
- `TARGETS: tuple[str, ...]` — container names sampled each tick.
- `async def run_collector(db, *, interval_secs=30, retain_minutes=120,
  docker_sock="/var/run/docker.sock") -> None` — infinite loop. One bad
  tick (socket missing, container absent, malformed stats) is logged at
  WARNING/DEBUG and the loop continues.
- `_cpu_pct(payload)` / `_mem_used_bytes(payload)` — pure helpers
  exported for tests.

**Owned tables.** `container_stats`. DDL registered with
`db_module.register_schema()` at import time.

**Invariants.**
- One row per (container, tick). Missing target → no row, no exception.
- Retention prune runs once per collector tick (cheap: indexed `ts`).
- Cycle-independent — system load isn't a cycle-scoped metric.

**Failure modes.**
- Docker socket absent (running outside the irene container): the first
  tick logs WARNING and the loop continues sampling nothing. No retry
  backoff — the dashboard simply stays empty.
- A target stops/restarts: 404 → DEBUG log, no row for that tick.
- Malformed stats payload (missing precpu_stats fields after a fresh
  restart): treated as `cpu_pct = 0.0`. Mem still recorded if available.

**Non-goals.**
- Per-process breakdown inside a container.
- Network / disk I/O metrics — out of scope for cycle 26.
- Host-level (non-container) CPU/RAM.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Iterable, Optional

import httpx

from app import db as db_module

logger = logging.getLogger(__name__)


TARGETS: tuple[str, ...] = (
    "irene",
    "jennie-scraper-v2",
)


_CONTAINER_STATS_DDL = [
    """
    CREATE TABLE IF NOT EXISTS container_stats (
        ts              TEXT NOT NULL,
        container       TEXT NOT NULL,
        cpu_pct         REAL NOT NULL,
        mem_bytes       INTEGER NOT NULL,
        mem_limit_bytes INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_container_stats_ts ON container_stats(ts)",
    "CREATE INDEX IF NOT EXISTS idx_container_stats_container_ts "
    "ON container_stats(container, ts)",
]
db_module.register_schema("app/system_stats.py", _CONTAINER_STATS_DDL)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cpu_pct(payload: dict) -> float:
    """Standard docker-stats CPU% formula.

    Both `cpu_stats` and `precpu_stats` come back in a single
    `stream=false` response (docker daemon samples twice internally).
    On a freshly-started container, `precpu_stats` may be zeroed — we
    return 0.0 rather than guess.
    """
    try:
        cpu = payload["cpu_stats"]
        pre = payload["precpu_stats"]
        cpu_total = cpu["cpu_usage"]["total_usage"]
        pre_total = pre.get("cpu_usage", {}).get("total_usage", 0)
        sys_total = cpu.get("system_cpu_usage", 0)
        pre_sys = pre.get("system_cpu_usage", 0)
    except (KeyError, TypeError):
        return 0.0
    cpu_delta = cpu_total - pre_total
    sys_delta = sys_total - pre_sys
    if cpu_delta <= 0 or sys_delta <= 0:
        return 0.0
    online = (
        cpu.get("online_cpus")
        or len(cpu.get("cpu_usage", {}).get("percpu_usage") or []) or 1
    )
    return (cpu_delta / sys_delta) * online * 100.0


def _mem_used_bytes(payload: dict) -> tuple[int, int]:
    """Returns (used, limit). Matches `docker stats` semantics: subtract
    `inactive_file` (cgroup v2) or `cache` (cgroup v1) from raw usage so
    page-cache doesn't look like working set.
    """
    mem = payload.get("memory_stats") or {}
    usage = int(mem.get("usage") or 0)
    limit = int(mem.get("limit") or 0)
    stats = mem.get("stats") or {}
    # cgroupv2 exposes inactive_file; cgroupv1 exposes cache. Prefer the
    # former when both are present (modern docker).
    deduct = int(stats.get("inactive_file") or stats.get("cache") or 0)
    used = max(usage - deduct, 0)
    return used, limit


async def _sample_one(client: httpx.AsyncClient, name: str) -> Optional[dict]:
    """Poll one container. Returns a row-shaped dict or None.

    `?stream=false` blocks ~1s on the daemon side while it takes two
    samples internally, then returns a single payload with both
    `cpu_stats` and `precpu_stats` populated.
    """
    try:
        resp = await client.get(f"/containers/{name}/stats", params={"stream": "false"})
    except httpx.HTTPError as exc:
        logger.debug("system_stats: %s sample failed: %s", name, exc)
        return None
    if resp.status_code == 404:
        logger.debug("system_stats: container %s not found", name)
        return None
    if resp.status_code >= 400:
        logger.debug("system_stats: %s HTTP %d", name, resp.status_code)
        return None
    try:
        payload = resp.json()
    except ValueError:
        logger.debug("system_stats: %s payload not JSON", name)
        return None
    used, limit = _mem_used_bytes(payload)
    return {
        "container": name,
        "cpu_pct": _cpu_pct(payload),
        "mem_bytes": used,
        "mem_limit_bytes": limit,
    }


async def _write_rows(db, rows: Iterable[dict], ts: str) -> None:
    rows = list(rows)
    if not rows:
        return
    async with db.batch() as cur:
        for r in rows:
            await cur.execute(
                "INSERT INTO container_stats "
                "(ts, container, cpu_pct, mem_bytes, mem_limit_bytes) "
                "VALUES (?, ?, ?, ?, ?)",
                (ts, r["container"], r["cpu_pct"],
                 r["mem_bytes"], r["mem_limit_bytes"]),
            )


async def _prune(db, retain_minutes: int) -> None:
    async with db.batch() as cur:
        await cur.execute(
            "DELETE FROM container_stats "
            f"WHERE datetime(ts) < datetime('now','-{int(retain_minutes)} minutes')"
        )


async def _sample_all(
    targets: Iterable[str], docker_sock: str
) -> list[dict]:
    transport = httpx.AsyncHTTPTransport(uds=docker_sock)
    # `http://docker` is a stand-in Host — UDS transport ignores it.
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://docker",
        timeout=httpx.Timeout(5.0, read=10.0),
    ) as client:
        results = await asyncio.gather(
            *(_sample_one(client, t) for t in targets),
            return_exceptions=False,
        )
    return [r for r in results if r is not None]


async def run_collector(
    db,
    *,
    interval_secs: int = 30,
    retain_minutes: int = 120,
    docker_sock: str = "/var/run/docker.sock",
    targets: Iterable[str] = TARGETS,
) -> None:
    """Sample TARGETS forever. Mirrors `Dashboard.run()` resilience:
    one bad tick logs and the loop continues."""
    targets = tuple(targets)
    logger.info(
        "system_stats: collector starting (every %ds, retain %dm, %d targets)",
        interval_secs, retain_minutes, len(targets),
    )
    while True:
        try:
            rows = await _sample_all(targets, docker_sock)
            await _write_rows(db, rows, _now_iso())
            await _prune(db, retain_minutes)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("system_stats: tick failed; continuing")
        await asyncio.sleep(interval_secs)
