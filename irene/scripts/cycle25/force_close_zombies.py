"""Cycle 25-A — one-shot migration to close existing zombie positions.

Zombie = an open trade on a market with `closed_at IS NOT NULL` where
`last_resolution_status` is not yet `'resolved'`. Pre-cycle-25 the
closer would reschedule these forever because `_current_mid_for_trade`
returns None for closed markets and the closer needs a non-None mid to
proceed. The new `_close_orphaned_trades_for` State-4 path (cycle 25-A,
in `markets.py`) handles these in steady state; this script catches the
backlog without waiting for the next markets-stage poll cycle.

Identical accounting to the live path: `write_trade_closure` is called
with `exit_mid=entry_mid` (gross PnL = 0) and `close_reason=
'market_dead_unresolved'` so the exit fee is zeroed (no taker fill ever
occurred).

Usage:
    # Dry-run: shows what WOULD be closed, doesn't touch the DB.
    python3 -m scripts.cycle25.force_close_zombies --db /data/irene.db --dry-run

    # Apply:
    python3 -m scripts.cycle25.force_close_zombies --db /data/irene.db --apply
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

# Project root on the path so `app.*` imports work when invoked via
# `python3 -m scripts.cycle25.force_close_zombies`.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from app.cycle import set_cycle_id  # noqa: E402
from app.db import DB  # noqa: E402

# Register schema (every owning module's DDL must run before connect()).
import app.ai  # noqa: F401, E402
import app.stages.closer  # noqa: F401, E402
import app.stages.dkg_trade  # noqa: F401, E402
import app.stages.markets  # noqa: F401, E402
import app.stages.trade  # noqa: F401, E402

from app.stages.closer import write_trade_closure  # noqa: E402


def _hours_since(iso_string: str | None, now_dt: datetime) -> float | None:
    if not iso_string:
        return None
    try:
        s = iso_string.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (now_dt - dt).total_seconds() / 3600.0
    except (ValueError, TypeError):
        return None


async def _fetch_zombies(db: DB, *, min_age_hours: float) -> list[dict]:
    """Open trades on markets with closed_at IS NOT NULL and either no
    UMA status or non-resolved status, where (now - closed_at) > min_age.
    """
    now_dt = datetime.now(timezone.utc)
    cur = await db.conn.execute(
        """
        SELECT t.id, t.signal_id, t.market_id, t.direction,
               t.mid_price, t.size_usdc, t.decided_at,
               t.is_tracer, t.tracer_id, t.target_mid,
               m.category, m.fee_rate, m.closed_at,
               m.last_resolution_status, m.question
        FROM trades t
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        LEFT JOIN markets m ON m.condition_id = t.market_id
        WHERE c.id IS NULL
          AND m.closed_at IS NOT NULL
          AND (m.last_resolution_status IS NULL
               OR LOWER(m.last_resolution_status) != 'resolved')
        ORDER BY t.decided_at
        """
    )
    rows = await cur.fetchall()
    await cur.close()

    out: list[dict] = []
    for r in rows:
        age = _hours_since(r["closed_at"], now_dt)
        if age is None or age < min_age_hours:
            continue
        out.append({
            "id": r["id"],
            "signal_id": r["signal_id"],
            "market_id": r["market_id"],
            "direction": r["direction"],
            "mid_price": float(r["mid_price"]),
            "size_usdc": float(r["size_usdc"]),
            "decided_at": r["decided_at"],
            "is_tracer": int(r["is_tracer"] or 0),
            "tracer_id": r["tracer_id"],
            "target_mid": (
                float(r["target_mid"]) if r["target_mid"] is not None else None
            ),
            "category": r["category"],
            "market_fee_rate": r["fee_rate"],
            "closed_at": r["closed_at"],
            "last_resolution_status": r["last_resolution_status"],
            "question": r["question"],
            "age_hours": age,
        })
    return out


async def _make_trade_cfg():
    """Build a TradeSection-shaped object so write_trade_closure can read
    `default_fee_rate` and `gas_cost_usdc`. We don't need the full Config
    tree — pyrocketscience the surface that's actually read."""
    from app.config import Config
    cfg = Config.model_validate({})
    return cfg.trade


async def _run(db_path: str, *, dry_run: bool, min_age_hours: float) -> int:
    db = DB(path=db_path, journal_mode="WAL", synchronous="NORMAL")
    await db.connect()
    cycle_id = await db.ensure_current_cycle("cycle25-zombie-migration")
    set_cycle_id(cycle_id)

    zombies = await _fetch_zombies(db, min_age_hours=min_age_hours)
    if not zombies:
        print("No zombies meeting the criteria. Nothing to do.")
        await db.close()
        return 0

    print(f"Found {len(zombies)} zombie position(s):")
    for z in zombies:
        q = (z["question"] or "<no row>")[:60]
        print(
            f"  trade_id={z['id']:>4} "
            f"{z['direction']} ${z['size_usdc']:>7.2f} @ {z['mid_price']:.4f} "
            f"closed_at={z['closed_at'][:19]} age={z['age_hours']:.1f}h "
            f"status={z['last_resolution_status'] or '(none)'} "
            f"\"{q}\""
        )

    if dry_run:
        print("\nDry-run only — no writes. Pass --apply to force-close.")
        await db.close()
        return 0

    trade_cfg = await _make_trade_cfg()
    closed_count = 0
    for z in zombies:
        ok = await write_trade_closure(
            db,
            trade={
                "id": z["id"],
                "signal_id": z["signal_id"],
                "market_id": z["market_id"],
                "direction": z["direction"],
                "mid_price": z["mid_price"],
                "size_usdc": z["size_usdc"],
                "decided_at": z["decided_at"],
                "is_tracer": z["is_tracer"],
                "tracer_id": z["tracer_id"],
                "target_mid": z["target_mid"],
            },
            exit_mid=z["mid_price"],  # gross PnL = 0
            close_reason="market_dead_unresolved",
            trade_cfg=trade_cfg,
            market_fee_rate=z["market_fee_rate"],
        )
        if ok:
            closed_count += 1
            print(f"  closed trade_id={z['id']}")
        else:
            print(f"  trade_id={z['id']} already closed (idempotent skip)")

    print(f"\nForce-closed {closed_count}/{len(zombies)} zombie(s).")
    await db.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Cycle 25-A zombie migration")
    p.add_argument("--db", required=True, help="Path to irene SQLite DB.")
    p.add_argument(
        "--min-age-hours",
        type=float,
        default=12.0,
        help=(
            "Minimum (now - market.closed_at) for a trade to qualify. "
            "Defaults to 12h so this script catches the current backlog "
            "without stomping on markets that just transitioned."
        ),
    )
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true",
                   help="Show what would close without touching the DB.")
    g.add_argument("--apply", action="store_true",
                   help="Actually force-close the zombies.")
    args = p.parse_args(argv)

    return asyncio.run(
        _run(args.db, dry_run=args.dry_run, min_age_hours=args.min_age_hours)
    )


if __name__ == "__main__":
    sys.exit(main())
