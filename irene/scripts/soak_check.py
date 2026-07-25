#!/usr/bin/env python3
"""24h-soak check script — re-runnable every 2h to monitor the post-cycle-18
production bot.

Outputs a short structured report to stdout AND appends to
`/tmp/irene_soak_24h.log`. Anomalies are tagged `[ALERT]`; healthy stats
are tagged `[ok]`. The goal: catch wrong-or-out-of-place behavior
mid-soak without a human staring at the dashboard.

Usage: `python3 irene/scripts/soak_check.py` (no args). Idempotent.
"""
from __future__ import annotations

import sqlite3
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

DB = "/home/muk/projects/nexus/irene/data/irene.db"
LOG = Path("/tmp/irene_soak_24h.log")


def query(cur, sql: str, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


def alert_or_ok(label: str, ok: bool, detail: str) -> str:
    tag = "[ok]   " if ok else "[ALERT]"
    return f"{tag} {label:32s} {detail}"


def main():
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    out: list[str] = [f"\n===== soak check {ts} ====="]

    # 1. Container alive?
    try:
        ps = subprocess.run(
            ["docker", "ps", "--filter", "name=^irene$", "--format", "{{.Status}}"],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
    except Exception as exc:
        ps = f"<exec error: {exc}>"
    container_up = ps.startswith("Up")
    out.append(alert_or_ok("container", container_up, ps or "<not found>"))

    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # 2. Decision rate (last 2h)
    rows = query(cur, """
        SELECT COUNT(*) FROM decisions
        WHERE created_at >= datetime('now', '-2 hours')
    """)
    dec_2h = rows[0][0]
    # Healthy if 50+ decisions in 2h (= ~25/hr); alert if zero or negligible.
    dec_ok = dec_2h >= 50
    out.append(alert_or_ok("decisions (last 2h)", dec_ok, f"{dec_2h}"))

    # 3. Outcome distribution (last 2h)
    rows = query(cur, """
        SELECT outcome, reason, COUNT(*) as n FROM decisions
        WHERE created_at >= datetime('now', '-2 hours')
        GROUP BY outcome, reason ORDER BY n DESC LIMIT 8
    """)
    execute_count = sum(r["n"] for r in rows if r["outcome"] == "execute")
    reject_count = sum(r["n"] for r in rows if r["outcome"] == "reject")
    out.append(alert_or_ok(
        "execute / reject", True,
        f"execute={execute_count}, reject={reject_count}"
    ))
    if rows:
        top = ", ".join(f"{r['reason']}={r['n']}" for r in rows[:4])
        out.append(f"        top reasons: {top}")

    # 4. Open trades (should be small + bounded)
    rows = query(cur, """
        SELECT COUNT(*) FROM trades t
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        WHERE c.id IS NULL
    """)
    open_n = rows[0][0]
    open_ok = open_n < 30  # arbitrary safety: more than 30 stuck-open is weird
    out.append(alert_or_ok("open trades", open_ok, f"{open_n}"))

    # 5. Trades closed in last 2h
    rows = query(cur, """
        SELECT close_reason, COUNT(*) as n, AVG(realized_pnl_usdc) as avg_pnl
        FROM trade_closures
        WHERE closed_at >= datetime('now', '-2 hours')
        GROUP BY close_reason ORDER BY n DESC
    """)
    if rows:
        for r in rows:
            out.append(f"        close: {r['close_reason']:18s} n={r['n']:>3d} avg_pnl=${r['avg_pnl']:+.4f}")
    else:
        out.append("        no closures last 2h")

    # 6. Decision latency p50/p95 (cycle 14 instrumentation)
    rows = query(cur, """
        SELECT decision_latency_ms FROM trades
        WHERE decision_latency_ms IS NOT NULL
          AND decided_at >= datetime('now', '-2 hours')
        ORDER BY id DESC LIMIT 500
    """)
    if rows:
        ms = sorted(r["decision_latency_ms"] for r in rows)
        p50 = statistics.median(ms)
        p95 = ms[int(len(ms) * 0.95)] if len(ms) >= 20 else ms[-1]
        # Healthy: p95 < 2000 ms (philosophy.md budget); alert if > 5s.
        lat_ok = p95 < 5000
        out.append(alert_or_ok(
            "trade latency (n=" + str(len(ms)) + ")", lat_ok,
            f"p50={p50:.0f}ms p95={p95:.0f}ms"
        ))

    # 7. Markets stuck at State 1/2 (cycle 11/12)
    # Split per cycle-22: the cycle-12 _resolution_poll_loop only acts on
    # markets where we still hold an open trade. Counting every closed-not-
    # finalized market over-reports because the legacy backlog (no-open-trade
    # rows) sits in this bucket indefinitely by design.
    rows = query(cur, """
        SELECT COUNT(*) FROM markets m
        WHERE m.closed_at IS NOT NULL
          AND m.uma_finalized_at IS NULL
          AND (julianday('now') - julianday(m.closed_at)) * 24 < 168
          AND EXISTS (
            SELECT 1 FROM trades t
            LEFT JOIN trade_closures c ON c.trade_id = t.id
            WHERE t.market_id = m.condition_id AND c.id IS NULL
          )
    """)
    actionable_n = rows[0][0]
    # Actionable = markets the resolution poll should be draining. Any
    # non-zero count means the poll is failing to advance positions we hold.
    actionable_ok = actionable_n == 0
    out.append(alert_or_ok(
        "state 1/2 actionable", actionable_ok,
        f"{actionable_n} markets with open trades awaiting finalization",
    ))

    rows = query(cur, """
        SELECT COUNT(*) FROM markets m
        WHERE m.closed_at IS NOT NULL
          AND m.uma_finalized_at IS NULL
          AND (julianday('now') - julianday(m.closed_at)) * 24 < 168
          AND NOT EXISTS (
            SELECT 1 FROM trades t
            LEFT JOIN trade_closures c ON c.trade_id = t.id
            WHERE t.market_id = m.condition_id AND c.id IS NULL
          )
    """)
    passive_n = rows[0][0]
    # Passive = legacy backlog with no open positions; informational only.
    out.append(f"[info]  {'legacy_uma_backlog':32s} {passive_n} closed-not-finalized (no open trades)")

    # 8. Markets in RESOLUTION_TIMEOUT (> 7d closed unresolved)
    rows = query(cur, """
        SELECT COUNT(*) FROM markets
        WHERE closed_at IS NOT NULL
          AND uma_finalized_at IS NULL
          AND (julianday('now') - julianday(closed_at)) * 24 >= 168
    """)
    timeout_n = rows[0][0]
    timeout_ok = timeout_n == 0
    out.append(alert_or_ok("RESOLUTION_TIMEOUT", timeout_ok,
                            f"{timeout_n} markets need human review"))

    # 9. fee_rate fetch coverage
    rows = query(cur, """
        SELECT
            SUM(CASE WHEN active=1 AND fee_rate IS NULL THEN 1 ELSE 0 END) as nulls,
            SUM(CASE WHEN active=1 THEN 1 ELSE 0 END) as active_total
        FROM markets
    """)
    if rows:
        nulls = rows[0]["nulls"] or 0
        tot = rows[0]["active_total"] or 0
        # Healthy if < 20% of active markets are still unfetched.
        pct_null = (nulls / tot * 100.0) if tot else 0.0
        fee_ok = pct_null < 50.0  # tolerant — first poll cycle takes time
        out.append(alert_or_ok(
            "fee_rate coverage", fee_ok,
            f"{nulls}/{tot} active markets NULL ({pct_null:.0f}%)"
        ))

    # 10. Error count in container logs (last 2h)
    try:
        logs = subprocess.run(
            ["docker", "logs", "--since", "2h", "irene"],
            capture_output=True, text=True, timeout=10,
        ).stderr + subprocess.run(
            ["docker", "logs", "--since", "2h", "irene"],
            capture_output=True, text=True, timeout=10,
        ).stdout
    except Exception as exc:
        logs = ""
    err_count = sum(
        1 for line in logs.splitlines()
        if (" ERROR " in line) or (" CRITICAL " in line) or (" Traceback" in line)
    )
    # Healthy if 0; some are tolerable but anything > 50 in 2h needs review.
    err_ok = err_count < 50
    out.append(alert_or_ok("ERROR-level logs (2h)", err_ok, f"{err_count}"))

    db.close()

    # Final summary
    n_alerts = sum(1 for line in out if line.startswith("[ALERT]"))
    summary = (f"SUMMARY: {n_alerts} alert(s); see above. "
               f"Next check in ~2h.")
    out.append(summary)

    report = "\n".join(out)
    print(report)
    with LOG.open("a") as f:
        f.write(report + "\n")


if __name__ == "__main__":
    main()
