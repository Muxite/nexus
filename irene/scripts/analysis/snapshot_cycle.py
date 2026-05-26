"""Snapshot a cycle's wallet / trade history to disk.

Per the cycle-3 architecture decision (2026-05-24): articles, markets,
entities, facts, events, signals, market_links are SHARED across cycles
(the news/extraction universe). Trades, decisions, predictions,
closures, llm_calls, llm_parse_failures are PER-CYCLE (the trading
record). Each cycle's per-cycle data is also archived to
`data/snapshots/cycle_<id>/` as JSONL + a manifest so we have a
durable record per cycle even if we later truncate live tables.

Usage:
    cd /home/muk/projects/jennie/irene
    # Snapshot a specific cycle:
    PYTHONPATH=. python3 -m scripts.analysis.snapshot_cycle --cycle-id 20

    # Snapshot the current cycle (latest cycle row):
    PYTHONPATH=. python3 -m scripts.analysis.snapshot_cycle

    # Snapshot + delete the live rows for that cycle (so next boot looks
    # fresh and the DB stays small). Articles/markets/etc untouched.
    PYTHONPATH=. python3 -m scripts.analysis.snapshot_cycle --cycle-id 20 --prune
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from app import wallet

DEFAULT_DB = "data/irene.db"
DEFAULT_OUT = "data/snapshots"
INITIAL_BALANCE_FALLBACK = 1000.0

# Per-cycle tables — these get archived + (optionally) pruned. Anything
# NOT in this list is shared (articles, markets, entities, facts, events,
# signals, market_links) and stays untouched.
PER_CYCLE_TABLES = (
    "trades",
    "decisions",
    "predictions",
    "trade_closures",
    "llm_calls",
    "llm_parse_failures",
    "decision_inputs",
    "trade_features",
)


# Replay-required tables added in PR-B (post-cycle-21). These do NOT have
# a `cycle_id` column of their own (price_impacts is global diagnostic
# data — see `app/stages/impact_tracker.py`; llm_probe_calls predates the
# convention; signals is shared cross-cycle). For replay we need the
# cycle-relevant subset, so each is dumped via a JOIN through a table
# that *does* carry cycle_id. Snapshots are read-only — these are NEVER
# pruned.
#
# Keyed by output filename → SELECT statement template. Each query takes
# exactly one parameter (the cycle_id). The SELECT must include every
# column the replay needs.
_JOIN_DUMPS: dict[str, str] = {
    # Every signal that has a decision in this cycle. Signals predate the
    # cycle are still emitted (since their `decisions` row IS in-cycle).
    "signals": (
        "SELECT DISTINCT s.* "
        "FROM signals s "
        "JOIN decisions d ON d.signal_id = s.id "
        "WHERE d.cycle_id = ?"
    ),
    # Every price_impacts row whose anchoring trade fired in this cycle.
    # `price_impacts.cycle_id` does not exist (see impact_tracker spec).
    "price_impacts": (
        "SELECT pi.* "
        "FROM price_impacts pi "
        "JOIN trades t ON t.id = pi.trade_id "
        "WHERE t.cycle_id = ?"
    ),
    # Every probe call whose linked production prediction was made in
    # this cycle. Probes may have NULL prediction_id on failure — those
    # are not snapshotted (no cycle context to attribute them to).
    "llm_probe_calls": (
        "SELECT pc.* "
        "FROM llm_probe_calls pc "
        "JOIN predictions p ON p.id = pc.prediction_id "
        "WHERE p.cycle_id = ?"
    ),
    # Articles referenced by any signal that produced a decision in this
    # cycle. PR-F needs `articles.source` for per-source PnL attribution.
    # Articles is a SHARED table; we dump only the cycle-relevant subset.
    "articles": (
        "SELECT DISTINCT a.* "
        "FROM articles a "
        "JOIN signals s ON s.article_id = a.id "
        "JOIN decisions d ON d.signal_id = s.id "
        "WHERE d.cycle_id = ?"
    ),
}


def _resolve_cycle(db: sqlite3.Connection, cycle_id: int | None) -> tuple[int, str, str]:
    if cycle_id is None:
        row = db.execute("SELECT id, name, opened_at FROM cycles ORDER BY id DESC LIMIT 1").fetchone()
    else:
        row = db.execute("SELECT id, name, opened_at FROM cycles WHERE id = ?", (cycle_id,)).fetchone()
    if row is None:
        raise SystemExit(f"cycle not found: {cycle_id}")
    return int(row[0]), str(row[1] or ""), str(row[2] or "")


def _table_has_cycle_id(db: sqlite3.Connection, table: str) -> bool:
    cols = [r[1] for r in db.execute(f"PRAGMA table_info({table})")]
    return "cycle_id" in cols


def _dump_table(
    db: sqlite3.Connection,
    table: str,
    cycle_id: int,
    out_dir: Path,
) -> int:
    if not _table_has_cycle_id(db, table):
        return 0
    out_path = out_dir / f"{table}.jsonl"
    n = 0
    with out_path.open("w") as f:
        cur = db.execute(f"SELECT * FROM {table} WHERE cycle_id = ?", (cycle_id,))
        cols = [c[0] for c in cur.description]
        for row in cur:
            f.write(json.dumps(dict(zip(cols, row)), default=str) + "\n")
            n += 1
    return n


def _dump_via_query(
    db: sqlite3.Connection,
    name: str,
    sql: str,
    cycle_id: int,
    out_dir: Path,
) -> int:
    """Dump rows from a parametrised SELECT to `<name>.jsonl`. Used for
    tables that don't carry `cycle_id` themselves (price_impacts,
    llm_probe_calls) or that are shared cross-cycle (signals) — the SQL
    JOIN narrows to the relevant subset for this cycle."""
    out_path = out_dir / f"{name}.jsonl"
    n = 0
    with out_path.open("w") as f:
        try:
            cur = db.execute(sql, (cycle_id,))
        except sqlite3.OperationalError as exc:
            # Pre-cycle-14 DB may not have llm_probe_calls; pre-cycle-11
            # may not have price_impacts. Write an empty file rather than
            # crashing the snapshot.
            print(f"  warn: {name}: {exc}", file=sys.stderr)
            return 0
        cols = [c[0] for c in cur.description]
        for row in cur:
            f.write(json.dumps(dict(zip(cols, row)), default=str) + "\n")
            n += 1
    return n


def _compute_wallet_summary(
    db: sqlite3.Connection,
    cycle_id: int,
    initial_balance: float,
) -> dict:
    """Final wallet state for the cycle. Mirrors the dashboard panel."""
    cur = db.execute(
        """
        SELECT t.id, t.size_usdc, t.direction, t.mid_price AS entry_mid,
               t.decision_path,
               c.exit_mid_price, c.realized_pnl_usdc, c.close_reason
        FROM trades t
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        WHERE t.is_tracer = 0 AND t.cycle_id = ?
        """,
        (cycle_id,),
    )
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur]
    closed = [r for r in rows if r["realized_pnl_usdc"] is not None]
    opens = [r for r in rows if r["realized_pnl_usdc"] is None]

    closed_pairs = [
        {"direction": r["direction"], "entry_mid": r["entry_mid"],
         "exit_mid_price": r["exit_mid_price"], "size_usdc": r["size_usdc"]}
        for r in closed
    ]
    closures = [{"realized_pnl_usdc": r["realized_pnl_usdc"]} for r in closed]

    open_size = wallet.sum_open_size_invested(opens)
    closed_size = wallet.sum_closed_size_invested(closed)
    closed_exit_value = wallet.sum_closed_exit_value(closed_pairs)
    realized = wallet.sum_realized_pnl(closures)
    cash = wallet.cash_balance(initial_balance, open_size, closed_exit_value, closed_size)
    # No mark-to-market in snapshots (no live CLOB) — open positions
    # valued at entry mid → unrealized=0.
    open_with_mid = [
        {"direction": r["direction"], "entry_mid": r["entry_mid"],
         "current_mid": None, "size_usdc": r["size_usdc"]}
        for r in opens
    ]
    open_val = wallet.sum_open_positions_value(open_with_mid)
    eq = wallet.equity(cash, open_val)
    ok, drift = wallet.accounting_identity_check(initial_balance, realized, 0.0, eq)

    # Per-path breakdown
    by_path: dict[str, dict] = {}
    for r in rows:
        path = r["decision_path"] or "mvp"
        bp = by_path.setdefault(path, {"trades": 0, "closed": 0, "realized": 0.0, "open": 0})
        bp["trades"] += 1
        if r["realized_pnl_usdc"] is not None:
            bp["closed"] += 1
            bp["realized"] += float(r["realized_pnl_usdc"])
        else:
            bp["open"] += 1

    by_reason: dict[str, int] = {}
    for r in closed:
        by_reason[r["close_reason"]] = by_reason.get(r["close_reason"], 0) + 1

    return {
        "initial_balance": initial_balance,
        "cash": round(cash, 4),
        "open_invested": round(open_size, 4),
        "open_current_value": round(open_val, 4),
        "realized_pnl": round(realized, 4),
        "equity": round(eq, 4),
        "accounting_identity_ok": ok,
        "accounting_drift": round(drift, 6),
        "n_trades": len(rows),
        "n_open": len(opens),
        "n_closed": len(closed),
        "by_decision_path": by_path,
        "by_close_reason": by_reason,
    }


def snapshot(
    db_path: str,
    out_root: str,
    cycle_id: int | None,
    *,
    initial_balance: float = INITIAL_BALANCE_FALLBACK,
    prune: bool = False,
) -> Path:
    db = sqlite3.connect(db_path)
    cid, cname, copened = _resolve_cycle(db, cycle_id)
    out_dir = Path(out_root) / f"cycle_{cid:04d}_{cname}"
    out_dir.mkdir(parents=True, exist_ok=True)

    table_counts: dict[str, int] = {}
    for table in PER_CYCLE_TABLES:
        try:
            table_counts[table] = _dump_table(db, table, cid, out_dir)
        except sqlite3.OperationalError as exc:
            table_counts[table] = -1
            print(f"  warn: {table}: {exc}", file=sys.stderr)

    # PR-B: replay-required join dumps. These never get pruned (the
    # underlying tables are shared cross-cycle or are global diagnostic
    # data).
    for name, sql in _JOIN_DUMPS.items():
        table_counts[name] = _dump_via_query(db, name, sql, cid, out_dir)

    summary = _compute_wallet_summary(db, cid, initial_balance)

    manifest = {
        "cycle_id": cid,
        "cycle_name": cname,
        "opened_at": copened,
        "snapshotted_at": datetime.now(timezone.utc).isoformat(),
        "tables": table_counts,
        "wallet": summary,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))

    if prune:
        print(f"--- pruning live rows for cycle {cid} ---")
        for table in PER_CYCLE_TABLES:
            if not _table_has_cycle_id(db, table):
                continue
            n = db.execute(f"DELETE FROM {table} WHERE cycle_id = ?", (cid,)).rowcount
            print(f"  {table}: deleted {n}")
        db.commit()

    db.close()
    return out_dir


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--out", default=DEFAULT_OUT)
    p.add_argument("--cycle-id", type=int, default=None)
    p.add_argument("--initial-balance", type=float, default=INITIAL_BALANCE_FALLBACK)
    p.add_argument("--prune", action="store_true",
                   help="DELETE the live per-cycle rows after archiving them.")
    args = p.parse_args(argv)
    out_dir = snapshot(args.db, args.out, args.cycle_id,
                       initial_balance=args.initial_balance, prune=args.prune)
    print(f"snapshot written to {out_dir}")
    print(json.dumps(json.loads((out_dir / "manifest.json").read_text()), indent=2)[:1500])
    return 0


if __name__ == "__main__":
    sys.exit(main())
