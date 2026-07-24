#!/usr/bin/env python3
"""Regenerate the live IRENE dashboard panels embedded in slide 08, on demand.

Drives the *actual* plot modules in `irene/app/dashboard/plots/` against a
read-only clone of the live irene.db, so the panels match exactly what the
running dashboard renders — but produced here, now, without waiting on (or
racing) the container's write directory. Output -> reports/linkedin-reel/_panels/.

SEULGI's gen-1 panels are intentionally NOT regenerated: that bot was retired
on 2026-05-27 and its DB is frozen, so its equity-curve / confidence-bucket
plots are the final historical record, not stale data.
"""
import asyncio
import os
import sys

import aiosqlite

ROOT = "/home/muk/projects/jennie"
sys.path.insert(0, os.path.join(ROOT, "irene"))

from app.dashboard.plots import per_gate_rejection, throughput_overlay  # noqa: E402

OUT = os.path.join(ROOT, "reports/linkedin-reel/_panels")
os.makedirs(OUT, exist_ok=True)


class _StubDB:
    def __init__(self, conn):
        self.conn = conn


class _StubDash:
    """Minimal surface the plot render() functions touch."""
    def __init__(self, conn):
        self.db = _StubDB(conn)
        self.current_cycle_id = None   # None -> 24h window across all cycles


async def main():
    conn = await aiosqlite.connect(f"file:{ROOT}/irene/data/irene.db?mode=ro", uri=True)
    conn.row_factory = aiosqlite.Row
    dash = _StubDash(conn)
    loop = asyncio.get_running_loop()

    for mod in (throughput_overlay, per_gate_rejection):
        png = await mod.render(dash, loop=loop)
        if not png:
            print(f"  {mod.FILENAME:32s} EMPTY (no rows in window)")
            continue
        path = os.path.join(OUT, mod.FILENAME)
        with open(path, "wb") as f:
            f.write(png)
        print(f"  {mod.FILENAME:32s} {len(png)/1024:.1f} KB -> {path}")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
