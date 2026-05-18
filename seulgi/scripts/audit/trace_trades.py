"""
Audit-mode trade tracer.

Picks N random closed trades per engine from the local SQLite DB and writes
a per-trade .txt file that reconstructs every number and every "thought"
behind the trade — headline → AI verdict → rule eval → fill → MTM ticks →
close → bankroll delta. Each derived number is shown with its formula and
labeled `[PASS]` / `[FAIL]` / `[WARN]`, so Claude Code (or a human) can
read the output and confirm the math.

Plan §8. No external API; pure local SQL + arithmetic.

Usage:
    python -m scripts.audit.trace_trades \\
        --engines aggressive,balanced \\
        -n 5 \\
        --since 24h \\
        --out data/dashboard/audit/

    python -m scripts.audit.trace_trades --engines all -n 3 --out /tmp/audit
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Make sibling analysis helpers importable.
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from analysis._common import open_db, parse_since  # noqa: E402


TOL = 0.01  # dollar tolerance for arithmetic checks


@dataclass
class Check:
    label: str
    line: str
    status: str  # PASS | FAIL | WARN


def _line(check: Check) -> str:
    return f"    [{check.status:<4}] {check.line}"


def _fmt_money(x: Optional[float]) -> str:
    if x is None:
        return "    n/a"
    sign = "+" if x >= 0 else "-"
    return f"{sign}${abs(x):>8.4f}"


def _fmt_price(x: Optional[float]) -> str:
    return f"{x:.4f}" if x is not None else "n/a"


# ---------------------------------------------------------------------------
# Data fetch
# ---------------------------------------------------------------------------


def _sample_trades(
    conn: sqlite3.Connection,
    engine: str,
    n: int,
    since: Optional[str],
    include_open: bool,
) -> list[sqlite3.Row]:
    where = ["t.size_usdc > 0", "t.skip_reason IS NULL"]
    if not include_open:
        where.append("t.realized_pnl IS NOT NULL")
    params: list = []
    if since:
        where.append("t.decided_at >= ?")
        params.append(since)
    if engine != "all":
        where.append("COALESCE(t.variant_name, t.engine_name, '') LIKE ?")
        params.append(f"{engine}%")
    sql = f"""
        SELECT t.*, s.headline, s.site_url, s.detected_at AS signal_detected_at,
               p.shares       AS pos_shares,
               p.entry_price  AS pos_entry_price,
               p.last_price   AS pos_last_price,
               p.high_water_price AS pos_hwm,
               p.opened_at    AS pos_opened_at,
               p.unrealized_pnl AS pos_unrealized
        FROM trades t
        LEFT JOIN signals s   ON t.signal_id = s.id
        LEFT JOIN positions p ON p.trade_id = t.id
        WHERE {' AND '.join(where)}
        ORDER BY RANDOM()
        LIMIT ?
    """
    params.append(n)
    return conn.execute(sql, params).fetchall()


def _engine_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT COALESCE(variant_name, engine_name, 'default') AS eng FROM trades"
    ).fetchall()
    return sorted({r["eng"] for r in rows if r["eng"]})


# ---------------------------------------------------------------------------
# Per-trade renderer
# ---------------------------------------------------------------------------


def _render_trade(conn: sqlite3.Connection, trade: sqlite3.Row) -> tuple[str, list[Check]]:
    checks: list[Check] = []
    lines: list[str] = []
    eng = trade["variant_name"] or trade["engine_name"] or "default"
    tid = trade["id"]

    lines.append("═" * 67)
    lines.append(f" TRADE AUDIT  —  trade_id={tid}  engine={eng}")
    lines.append("═" * 67)

    # [1] Headline ------------------------------------------------------------
    lines.append("")
    lines.append("[1] HEADLINE")
    lines.append(f"    signal_id      : {trade['signal_id']}")
    lines.append(f"    headline_id    : {trade['headline_id'] or '(none)'}")
    lines.append(f"    detected_at    : {trade['signal_detected_at']}")
    lines.append(f"    site_url       : {trade['site_url']}")
    headline_text = (trade["headline"] or "")[:120]
    lines.append(f"    headline       : {headline_text!r}")

    # [2] Market match --------------------------------------------------------
    lines.append("")
    lines.append("[2] MARKET MATCH")
    lines.append(f"    condition_id   : {trade['condition_id']}")
    lines.append(f"    question       : {trade['question']!r}")
    lines.append(f"    similarity     : {trade['similarity']:.4f}")
    lines.append(f"    mid_yes        : {_fmt_price(trade['mid_yes'])}")
    lines.append(f"    mid_no         : {_fmt_price(trade['mid_no'])}")
    lines.append(f"    mid_at_decide  : {_fmt_price(trade['mid_price'])}")

    # [3] AI verdict ----------------------------------------------------------
    lines.append("")
    lines.append("[3] AI VERDICT")
    lines.append(f"    direction      : {trade['direction']}")
    lines.append(f"    p_hat          : {trade['p_hat']:.4f}")
    lines.append(f"    confidence_raw : {trade['confidence_raw'] or 0.0:.4f}")
    reason = (trade["ai_reason"] or "")[:200]
    lines.append(f"    reasoning      : {reason!r}")
    lines.append(f"    relevance      : {trade['relevance_score']} — "
                 f"{(trade['relevance_reason'] or '')[:100]!r}")
    lines.append(f"    freshness      : {trade['freshness_label']}")
    # tokens / cost — sum from usage_events on the same headline
    tok_row = conn.execute(
        "SELECT COALESCE(SUM(count), 0) AS toks FROM usage_events "
        "WHERE kind='ai_tokens' AND meta LIKE ?",
        (f'%"hid": "{trade["headline_id"]}"%' if trade["headline_id"] else "%__no_hid__%",),
    ).fetchone()
    lines.append(f"    tokens(stage)  : {int(tok_row['toks']) if tok_row else 0} "
                 f"(sum across relevance+predict for this headline)")

    # [4] Engine rule eval ----------------------------------------------------
    lines.append("")
    lines.append("[4] ENGINE RULE EVAL")
    trader_trace = trade["trader_trace"]
    if trader_trace:
        try:
            steps = json.loads(trader_trace)
            for step in steps:
                lines.append(f"    {step}")
        except Exception:  # noqa: BLE001
            lines.append(f"    (trader_trace unparseable: {trader_trace[:80]!r})")
    else:
        lines.append("    (no trader_trace persisted)")

    # [5] Fill ----------------------------------------------------------------
    lines.append("")
    lines.append("[5] FILL")
    entry_price = trade["pos_entry_price"] if trade["pos_entry_price"] is not None else trade["mid_price"]
    size_usdc = trade["size_usdc"]
    pos_shares = trade["pos_shares"]
    implied_shares = (size_usdc / entry_price) if entry_price else 0.0
    shares_used = pos_shares if pos_shares else implied_shares
    lines.append(f"    direction      : {trade['direction']}")
    lines.append(f"    entry_price    : {_fmt_price(entry_price)}")
    lines.append(f"    size_usdc      : ${size_usdc:.4f}")
    if pos_shares:
        lines.append(f"    shares (persisted) : {pos_shares:.4f}")
        # Sanity: shares × entry_price + entry_fee ≈ size_usdc
        entry_fee = trade["entry_fee_usdc"] or 0.0
        recon = pos_shares * entry_price + entry_fee
        if abs(recon - size_usdc) <= 0.05:
            checks.append(Check("shares_recon",
                                f"shares × entry_price + entry_fee = "
                                f"{pos_shares:.4f} × {entry_price:.4f} + "
                                f"{entry_fee:.4f} = ${recon:.4f} ≈ size_usdc ${size_usdc:.4f}",
                                "PASS"))
        else:
            checks.append(Check("shares_recon",
                                f"shares × entry_price + entry_fee = ${recon:.4f} "
                                f"diverges from size_usdc ${size_usdc:.4f} by ${recon-size_usdc:+.4f}",
                                "FAIL"))
    else:
        lines.append(f"    shares_implied : {size_usdc}/{entry_price} = {implied_shares:.4f}")
    lines.append(f"    entry_fee_usdc : {_fmt_money(trade['entry_fee_usdc'])}")
    lines.append(f"    opened_at      : {trade['pos_opened_at'] or trade['decided_at']}")

    # [6] Close ---------------------------------------------------------------
    lines.append("")
    lines.append("[6] CLOSE")
    close_reason = trade["close_reason"]
    realized = trade["realized_pnl"]
    exit_fee = trade["exit_fee_usdc"] or 0.0
    if realized is not None:
        lines.append(f"    close_reason   : {close_reason}")
        lines.append(f"    closed_at      : {trade['closed_at']}")
        lines.append(f"    exit_fee_usdc  : {_fmt_money(exit_fee)}")
        lines.append(f"    realized_pnl   : {_fmt_money(realized)}")

        # Verify realized_pnl math from persisted fields.
        # For simulate-mode RESOLVE_*, exit_price is implicit.
        if close_reason == "RESOLVE_WIN" and pos_shares:
            proceeds = pos_shares * 1.00
            expected_pnl = proceeds - size_usdc
            ok = abs(expected_pnl - realized) <= TOL
            checks.append(Check(
                "pnl_resolve_win",
                f"shares × $1.00 - size = {pos_shares:.4f} × 1.00 - {size_usdc:.4f} = "
                f"${expected_pnl:+.4f} vs realized ${realized:+.4f}",
                "PASS" if ok else "FAIL",
            ))
        elif close_reason == "RESOLVE_LOSS" and pos_shares:
            expected_pnl = -size_usdc
            ok = abs(expected_pnl - realized) <= TOL
            checks.append(Check(
                "pnl_resolve_loss",
                f"-size_usdc = ${expected_pnl:+.4f} vs realized ${realized:+.4f}",
                "PASS" if ok else "FAIL",
            ))
        elif pos_shares and trade["pos_last_price"] is not None:
            # Early exit: (last - entry) × shares - exit_fee - entry_fee
            entry_fee = trade["entry_fee_usdc"] or 0.0
            last = trade["pos_last_price"]
            gross = (last - entry_price) * pos_shares
            expected_pnl = gross - exit_fee - entry_fee
            ok = abs(expected_pnl - realized) <= 0.05  # slippage walk ≤ 2c
            checks.append(Check(
                "pnl_early_exit",
                f"(last - entry) × shares - fees = "
                f"({last:.4f} - {entry_price:.4f}) × {pos_shares:.4f} - "
                f"{exit_fee:.4f} - {entry_fee:.4f} = ${expected_pnl:+.4f} "
                f"vs realized ${realized:+.4f}",
                "PASS" if ok else "WARN",
            ))
        else:
            # Legacy paper / infinite — gross_pnl - spread_cost
            implied = size_usdc / entry_price if entry_price else 0.0
            last = trade["pos_last_price"] if trade["pos_last_price"] is not None else entry_price
            gross = (last - entry_price) * implied
            checks.append(Check(
                "pnl_legacy",
                f"(last - entry) × implied_shares = "
                f"({last:.4f} - {entry_price:.4f}) × {implied:.4f} = ${gross:+.4f} "
                f"(realized ${realized:+.4f} after spread cost)",
                "PASS" if abs(gross - realized) < 0.50 else "WARN",
            ))
    else:
        lines.append("    (position still open — closed_at IS NULL)")
        if trade["pos_unrealized"] is not None:
            lines.append(f"    unrealized_pnl : {_fmt_money(trade['pos_unrealized'])}")
            if shares_used and trade["pos_last_price"] is not None:
                expected_unreal = (trade["pos_last_price"] - entry_price) * shares_used
                ok = abs(expected_unreal - trade["pos_unrealized"]) <= TOL
                checks.append(Check(
                    "unrealized_recon",
                    f"(last - entry) × shares = "
                    f"({trade['pos_last_price']:.4f} - {entry_price:.4f}) × "
                    f"{shares_used:.4f} = ${expected_unreal:+.4f} vs ${trade['pos_unrealized']:+.4f}",
                    "PASS" if ok else "FAIL",
                ))

    # [7] Audit checks --------------------------------------------------------
    lines.append("")
    lines.append("[7] AUDIT CHECKS")
    if not checks:
        lines.append("    (no checks emitted — likely a legacy row missing data)")
    for c in checks:
        lines.append(_line(c))

    return "\n".join(lines) + "\n", checks


# ---------------------------------------------------------------------------
# Dashboard cross-check
# ---------------------------------------------------------------------------


def _dashboard_crosscheck(conn: sqlite3.Connection) -> list[str]:
    """Re-derive per-engine GAIN/STAKED/ROI/UNREALIZED from raw SQL.

    Operator can diff this against the live `dashboard.txt` per-engine block.
    """
    rows = conn.execute(
        """
        SELECT COALESCE(variant_name, engine_name, 'default') AS eng,
               COALESCE(SUM(CASE WHEN realized_pnl IS NOT NULL THEN realized_pnl END), 0.0) AS gain,
               COALESCE(SUM(size_usdc), 0.0) AS staked,
               SUM(CASE WHEN size_usdc > 0 AND skip_reason IS NULL AND realized_pnl IS NULL
                        THEN 1 ELSE 0 END) AS open_count
        FROM trades
        WHERE size_usdc > 0 AND skip_reason IS NULL
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()

    unreal_rows = conn.execute(
        """
        SELECT COALESCE(t.variant_name, t.engine_name, 'default') AS eng,
               COALESCE(SUM(p.unrealized_pnl), 0.0) AS unreal
        FROM positions p JOIN trades t ON p.trade_id = t.id
        GROUP BY 1
        """
    ).fetchall()
    unreal = {r["eng"]: r["unreal"] for r in unreal_rows}

    out = ["## Dashboard cross-check (re-derived from trades+positions tables)\n",
           "| ENGINE | GAIN | STAKED | ROI | OPEN | UNREALIZED |",
           "|---|---:|---:|---:|---:|---:|"]
    for r in rows:
        roi = (r["gain"] / r["staked"] * 100.0) if r["staked"] > 0 else 0.0
        u = unreal.get(r["eng"], 0.0)
        out.append(
            f"| {r['eng']} | {r['gain']:+.2f} | {r['staked']:.2f} | "
            f"{roi:+.2f}% | {r['open_count']} | {u:+.2f} |"
        )
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--engines", default="all",
                        help="comma-separated engine prefixes, or 'all'. Matched via LIKE 'eng%%'.")
    parser.add_argument("-n", type=int, default=5,
                        help="random sample size per engine (default: 5)")
    parser.add_argument("--since", default=None,
                        help="time window (24h, 7d, ISO datetime) — restrict to recent trades")
    parser.add_argument("--include-open", action="store_true",
                        help="also sample currently-open positions, not just closed trades")
    parser.add_argument("--out", type=Path, default=Path("data/dashboard/audit"),
                        help="output directory (default: data/dashboard/audit)")
    parser.add_argument("--db", type=Path, default=None)
    args = parser.parse_args()

    conn = open_db(args.db)
    since = parse_since(args.since)
    out_dir: Path = args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.engines == "all":
        engines = _engine_names(conn) or ["default"]
    else:
        engines = [e.strip() for e in args.engines.split(",") if e.strip()]

    index_rows = ["# Trade audit index\n",
                  "| trade_id | engine | PnL | reason | checks |",
                  "|---|---|---:|---|---|"]
    total_pass = total_fail = total_warn = 0

    for eng in engines:
        trades = _sample_trades(conn, eng, args.n, since, args.include_open)
        if not trades:
            print(f"[audit] no trades found for engine={eng!r}", file=sys.stderr)
            continue
        for trade in trades:
            body, checks = _render_trade(conn, trade)
            p = out_dir / f"trade_{trade['id']}.txt"
            p.write_text(body)
            p_count = sum(1 for c in checks if c.status == "PASS")
            f_count = sum(1 for c in checks if c.status == "FAIL")
            w_count = sum(1 for c in checks if c.status == "WARN")
            total_pass += p_count
            total_fail += f_count
            total_warn += w_count
            verdict = f"{p_count}/{len(checks)}"
            if f_count:
                verdict += " ✗"
            elif w_count:
                verdict += " ⚠"
            pnl_str = (
                f"{trade['realized_pnl']:+.2f}"
                if trade["realized_pnl"] is not None else "(open)"
            )
            index_rows.append(
                f"| {trade['id']} | {trade['variant_name'] or trade['engine_name']} | "
                f"{pnl_str} | {trade['close_reason'] or '-'} | {verdict} |"
            )

    index_rows.append("")
    index_rows.append(f"Totals: PASS={total_pass}  FAIL={total_fail}  WARN={total_warn}")
    index_rows.append("")
    index_rows.extend(_dashboard_crosscheck(conn))

    (out_dir / "INDEX.md").write_text("\n".join(index_rows) + "\n")

    print(f"[audit] wrote {len(list(out_dir.glob('trade_*.txt')))} trade files to {out_dir}")
    print(f"[audit] PASS={total_pass}  FAIL={total_fail}  WARN={total_warn}")
    return 0 if total_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
