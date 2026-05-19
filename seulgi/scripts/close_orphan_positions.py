"""One-shot cleanup: close positions whose engine no longer exists.

When variants are retired mid-cycle, their open positions stay in the DB but
no engine is monitoring them. This script closes them at the most recent
mid (positions.last_price), records realized_pnl honestly on the trade row,
and removes them from the open-positions table. Run inside the container:

    docker exec jennie-seulgi python /app/scripts/close_orphan_positions.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone

import aiosqlite

ACTIVE_ENGINES = {
    "balanced/conf40_tp020_sl030_lowvol",
    "balanced/conf40_tp020_sl030_slpct10",
    "balanced/conf40_tp030_sl050",
    "inverted_highconf/conf40_tp020_sl030_inv75_lowvol",
    "inverted_highconf/conf40_tp030_sl050_inv75",
}

DB_PATH = "/data/seulgi.db"


def realized_pnl(direction: str, size_usdc: float, entry: float, exit_: float) -> float:
    shares = size_usdc / entry
    if direction.upper() == "YES":
        return shares * (exit_ - entry)
    return shares * (entry - exit_)


async def main() -> int:
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT p.trade_id, p.condition_id, p.direction, p.size_usdc,
                   p.entry_price, p.last_price, t.engine_name
              FROM positions p
              JOIN trades t ON t.id = p.trade_id
            """
        )
        rows = await cur.fetchall()
        await cur.close()

        orphans = [r for r in rows if r["engine_name"] not in ACTIVE_ENGINES]
        active = [r for r in rows if r["engine_name"] in ACTIVE_ENGINES]
        print(f"open positions total: {len(rows)}  active: {len(active)}  orphans: {len(orphans)}")

        if not orphans:
            return 0

        total_realized = 0.0
        skipped = 0
        for r in orphans:
            exit_ = r["last_price"] if r["last_price"] is not None else r["entry_price"]
            pnl = realized_pnl(r["direction"], r["size_usdc"], r["entry_price"], exit_)
            total_realized += pnl
            await db.execute(
                """UPDATE trades
                      SET realized_pnl = ?, closed_at = ?, close_reason = 'engine_retired'
                    WHERE id = ?""",
                (pnl, now, r["trade_id"]),
            )
            await db.execute("DELETE FROM positions WHERE trade_id = ?", (r["trade_id"],))
            if r["last_price"] is None:
                skipped += 1

        await db.commit()
        print(f"closed {len(orphans)} orphan positions  realized_total=${total_realized:+.2f}")
        if skipped:
            print(f"  (note: {skipped} used entry price as exit — no last_price recorded)")

        cur = await db.execute("SELECT COUNT(*) FROM positions")
        remaining = (await cur.fetchone())[0]
        await cur.close()
        print(f"positions remaining: {remaining}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
