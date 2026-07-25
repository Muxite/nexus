"""One-shot cycle reset: close all open positions, end the current cycle,
start a new one. Used at the boundary between strategy populations
(e.g. Phase E.7.3 evolutionary engine expansion).

Closes positions honestly at positions.last_price (or entry_price as
fallback), records realized_pnl on the trade row, deletes the position
row, then inserts a new cycle. Engine balances reset to the configured
per-engine allocation on next startup via init_from_db (which sums
realized_pnl since the new cycle's started_at).

Phase 1 R4: writes are chunked with explicit COMMITs every CHUNK_SIZE
positions instead of one giant transaction at the end. The live
container's writer never waits more than one chunk for the lock —
previously it would block for the entire UPDATE loop, hit the 30 s
busy_timeout, and silently abort in-flight `insert_trade` calls
(B-WRITES root cause).

Usage:
    docker exec nexus-seulgi python /app/scripts/reset_cycle.py NEW_CYCLE_NAME
"""
from __future__ import annotations

import asyncio
import sys
import time
from datetime import datetime, timezone

import aiosqlite

DB_PATH = "/data/seulgi.db"
# Chunk size for position-close UPDATEs. Each chunk commits independently
# so the live writer's busy_timeout (30 s) never elapses waiting on us.
CHUNK_SIZE = 50


def realized_pnl(direction: str, size_usdc: float, entry: float, exit_: float) -> float:
    shares = size_usdc / entry
    if direction.upper() == "YES":
        return shares * (exit_ - entry)
    return shares * (entry - exit_)


async def main(new_cycle_name: str) -> int:
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        # Wait up to 60s for the live writer to release the lock instead
        # of failing immediately with "database is locked".
        await db.execute("PRAGMA busy_timeout=60000")
        db.row_factory = aiosqlite.Row

        # 1) Snapshot all open positions. The SELECT itself is RO under
        #    WAL — never blocks the writer.
        cur = await db.execute(
            """SELECT p.trade_id, p.direction, p.size_usdc, p.entry_price,
                      p.last_price, t.engine_name
                 FROM positions p JOIN trades t ON t.id = p.trade_id"""
        )
        rows = await cur.fetchall()
        await cur.close()
        print(f"open positions: {len(rows)}")

        # 2) Chunked close. Each chunk is one BEGIN..COMMIT — the live
        #    writer gets a turn between chunks.
        total = 0.0
        fallback = 0
        n_rows = len(rows)
        for chunk_start in range(0, n_rows, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, n_rows)
            t0 = time.monotonic()
            for r in rows[chunk_start:chunk_end]:
                exit_ = r["last_price"] if r["last_price"] is not None else r["entry_price"]
                if r["last_price"] is None:
                    fallback += 1
                pnl = realized_pnl(r["direction"], r["size_usdc"], r["entry_price"], exit_)
                total += pnl
                await db.execute(
                    """UPDATE trades
                          SET realized_pnl = ?, closed_at = ?, close_reason = 'cycle_reset'
                        WHERE id = ?""",
                    (pnl, now, r["trade_id"]),
                )
                await db.execute(
                    "DELETE FROM positions WHERE trade_id = ?", (r["trade_id"],),
                )
            await db.commit()
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            print(
                f"[reset_cycle] committed chunk {chunk_start}-{chunk_end} / "
                f"{n_rows}  ({elapsed_ms:.0f} ms)"
            )

        # 3) End the current cycle + insert the new one — small atomic txn.
        await db.execute(
            "UPDATE cycles SET ended_at = ? WHERE ended_at IS NULL",
            (now,),
        )
        await db.execute(
            "INSERT INTO cycles (name, started_at, notes) VALUES (?, ?, ?)",
            (new_cycle_name, now,
             "Phase E.7.3 evolutionary engine expansion: 3 parents + 5 mutants, $100/engine"),
        )
        await db.commit()

        cur = await db.execute(
            "SELECT id, name, started_at FROM cycles ORDER BY id DESC LIMIT 1"
        )
        row = await cur.fetchone()
        await cur.close()
        print(
            f"closed {n_rows} positions, realized_total=${total:+.4f} "
            f"(fallback to entry on {fallback})"
        )
        print(
            f"new cycle: id={row['id']} name='{row['name']}' "
            f"started_at={row['started_at']}"
        )
    return 0


if __name__ == "__main__":
    name = (
        sys.argv[1] if len(sys.argv) > 1
        else f"cycle_phase_e7_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    )
    sys.exit(asyncio.run(main(name)))
