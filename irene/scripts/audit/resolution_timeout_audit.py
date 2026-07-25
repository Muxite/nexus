"""Audit the markets stuck in RESOLUTION_TIMEOUT (closed > 7d, UMA not finalized).

Cycle-21 Fix E, executed in cycle 23. For each timed-out market: print
question, slug, closed_at, age in days, whether we ever held a position,
and a Polymarket URL hint for manual lookup. SQL-only; no Gamma calls.

Usage:  python -m scripts.audit.resolution_timeout_audit [--db PATH]
"""
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone


DEFAULT_DB = "/home/muk/projects/nexus/irene/data/irene.db"

SQL = """
SELECT
    m.condition_id,
    m.slug,
    m.question,
    m.closed_at,
    m.end_date,
    m.last_resolution_status,
    m.last_outcome_prices,
    (julianday('now') - julianday(m.closed_at)) AS age_days,
    (SELECT COUNT(*) FROM trades t WHERE t.market_id = m.condition_id)
        AS total_trades,
    (SELECT COUNT(*) FROM trades t
       LEFT JOIN trade_closures c ON c.trade_id = t.id
       WHERE t.market_id = m.condition_id AND c.id IS NULL)
        AS open_trades_now
FROM markets m
WHERE m.closed_at IS NOT NULL
  AND m.uma_finalized_at IS NULL
  AND (julianday('now') - julianday(m.closed_at)) * 24 >= 168
ORDER BY m.closed_at ASC
"""


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default=DEFAULT_DB)
    args = p.parse_args(argv)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(SQL).fetchall()
    conn.close()

    if not rows:
        print("RESOLUTION_TIMEOUT count: 0 — nothing to audit.")
        return 0

    held_now = sum(1 for r in rows if r["open_trades_now"] > 0)
    held_ever = sum(1 for r in rows if r["total_trades"] > 0)
    print(f"RESOLUTION_TIMEOUT count: {len(rows)}")
    print(f"  with open trades NOW:    {held_now}")
    print(f"  with any historical pos: {held_ever}")
    print(f"  ran at:                  {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    print()
    for r in rows:
        url = f"https://polymarket.com/market/{r['slug']}" if r["slug"] else "(no slug)"
        actionable = "ACTIONABLE" if r["open_trades_now"] > 0 else (
            "historical" if r["total_trades"] > 0 else "no-position"
        )
        print(
            f"- [{actionable}] closed={r['closed_at']} "
            f"age={r['age_days']:.1f}d uma_status={r['last_resolution_status']!r} "
            f"outcomes={r['last_outcome_prices']!r}"
        )
        print(f"    cid:      {r['condition_id']}")
        print(f"    end_date: {r['end_date']}")
        print(f"    question: {r['question']}")
        print(f"    trades:   ever={r['total_trades']} open_now={r['open_trades_now']}")
        print(f"    url:      {url}")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
