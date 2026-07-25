"""Operator dashboard for the explorer firehose.

Read-only. Prints recent throughput + admission-cap utilization so the
operator can decide whether to raise `max_active_explorations`, lower
`score_floor`, or do nothing.

    python -m irene.scripts.explorer.firehose_stats
    python -m irene.scripts.explorer.firehose_stats --db /custom/path.db
"""

from __future__ import annotations

import argparse
import datetime as dt
import sqlite3
from typing import Optional

DEFAULT_DB = "/home/muk/projects/nexus/irene/data/irene.db"


def _con(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def _utc_iso(minus_seconds: float = 0.0) -> str:
    now = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=minus_seconds)
    return now.strftime("%Y-%m-%dT%H:%M:%S")


def _scalar(con, sql: str, params: tuple = ()) -> Optional[float]:
    cur = con.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--cap", type=int, default=100,
                    help="max_active_explorations from explorer.yaml; "
                         "drives the utilization %")
    ap.add_argument("--poll-secs", type=int, default=15,
                    help="odds_poll_secs; drives the calls/sec estimate")
    args = ap.parse_args()

    con = _con(args.db)

    hour_ago = _utc_iso(3600)
    day_ago = _utc_iso(86400)

    explorations_1h = _scalar(
        con, "SELECT COUNT(*) FROM explorations WHERE created_at >= ?", (hour_ago,)
    ) or 0
    explorations_24h = _scalar(
        con, "SELECT COUNT(*) FROM explorations WHERE created_at >= ?", (day_ago,)
    ) or 0
    explorations_total = _scalar(
        con, "SELECT COUNT(*) FROM explorations"
    ) or 0
    active = _scalar(
        con, "SELECT COUNT(*) FROM explorations WHERE capture_status='active'"
    ) or 0
    odds_1h = _scalar(
        con, "SELECT COUNT(*) FROM odds_observations WHERE sampled_at >= ?",
        (hour_ago,),
    ) or 0
    odds_total = _scalar(
        con, "SELECT COUNT(*) FROM odds_observations"
    ) or 0

    judged_total = _scalar(
        con,
        "SELECT COUNT(*) FROM explorations WHERE llm_prompt_hash IS NOT NULL",
    ) or 0
    unjudged_total = _scalar(
        con,
        "SELECT COUNT(*) FROM explorations WHERE llm_prompt_hash IS NULL",
    ) or 0

    cap_util = (active / args.cap * 100) if args.cap else 0.0
    est_calls_per_sec = active * (1.0 / args.poll_secs) if args.poll_secs else 0.0
    polymarket_budget_pct = est_calls_per_sec / 400.0 * 100.0  # 400 req/s ceiling

    print(f"Firehose stats  ({_utc_iso()}Z)")
    print(f"  DB: {args.db}")
    print()
    print("Explorations:")
    print(f"  last 1h:    {explorations_1h:>6,}    (avg {explorations_1h / 60:.1f}/min)")
    print(f"  last 24h:   {explorations_24h:>6,}   (avg {explorations_24h / 1440:.2f}/min)")
    print(f"  all-time:   {explorations_total:>6,}")
    print(f"  judged:     {judged_total:>6,}    (LLM-gate ran)")
    print(f"  unjudged:   {unjudged_total:>6,}    (gate_skipped — capture-mode)")
    print()
    print("Admission cap:")
    print(f"  active:     {int(active):>6}/{args.cap}    ({cap_util:.1f}% of cap)")
    if cap_util >= 95:
        print("  ⚠  At/near cap — consider raising max_active_explorations.")
    elif cap_util <= 20:
        print("  ℹ  Plenty of room — consider lowering score_floor for more volume.")
    print()
    print("CLOB call estimate (sequential poll loop):")
    print(f"  calls/sec:  ~{est_calls_per_sec:.2f}    (active × 1/poll_secs)")
    print(f"  Polymarket: {polymarket_budget_pct:.2f}% of 400 req/s budget")
    if polymarket_budget_pct >= 25:
        print("  ⚠  Approaching API budget — next lever is concurrent poll loop.")
    print()
    print("Odds observations:")
    print(f"  last 1h:    {odds_1h:>6,}    ({odds_1h / 3600:.2f}/sec)")
    print(f"  all-time:   {odds_total:>6,}")
    print()

    # Capture-end-reason breakdown — added cycle 26. Lets the operator
    # tell whether captures are reaching the configured window
    # (`window_complete`) or being cut short. If `evicted_by_cap` or
    # `capture_aborted` start dominating, raise max_active_explorations
    # or investigate.
    cur = con.execute(
        "SELECT capture_end_reason, COUNT(*) AS n "
        "FROM explorations "
        "WHERE created_at >= ? AND capture_end_reason IS NOT NULL "
        "GROUP BY capture_end_reason ORDER BY n DESC",
        (day_ago,),
    )
    end_reasons = list(cur.fetchall())
    print("Capture end reasons (last 24h, completed only):")
    if not end_reasons:
        print("  (none yet — first captures still in window or pre-migration)")
    else:
        total_ended = sum(int(r["n"]) for r in end_reasons)
        for r in end_reasons:
            pct = (int(r["n"]) / total_ended * 100) if total_ended else 0.0
            print(f"  {r['capture_end_reason']:<18} {int(r['n']):>6,}  ({pct:5.1f}%)")

    # Reject-sample throughput — added cycle 26. Confirms the 5%
    # reject-capture knob is firing and isn't hitting its own cap.
    reject_sampled_24h = _scalar(
        con,
        "SELECT COUNT(*) FROM explorations "
        "WHERE created_at >= ? AND capture_sample_reason='rejected_sample'",
        (day_ago,),
    ) or 0
    rejected_24h = _scalar(
        con,
        "SELECT COUNT(*) FROM explorations "
        "WHERE created_at >= ? AND gate_status='rejected'",
        (day_ago,),
    ) or 0
    sample_rate = (
        (reject_sampled_24h / rejected_24h * 100) if rejected_24h else 0.0
    )
    print()
    print("Reject-sample capture (last 24h):")
    print(f"  rejected:        {int(rejected_24h):>6,}")
    print(f"  sampled:         {int(reject_sampled_24h):>6,}  ({sample_rate:.2f}% of rejected)")


if __name__ == "__main__":
    main()
