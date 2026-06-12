"""Operator landing page — `dashboard.txt`.

Everything an operator needs to know in one screen: uptime, 1h counts,
AI subsystem health, end-to-end latency, last 5 articles, last 5
decisions. The AI panel absorbed what was `llm_calls.txt` so the
prompt-quality signal (NEUTRAL rate, parse failures, cache hit) is
immediately visible on the landing page.
"""

from __future__ import annotations

import time

from app import wallet

from ..db_helpers import end_to_end_latency_ms, scalar
from ..io import _now, _percentiles


async def _fetch_open_trades(dash) -> list[dict]:
    # Cycle 3: per-cycle isolation — only current-cycle trades.
    cycle_id = dash.current_cycle_id
    cycle_sql = f"AND t.cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    cur = await dash.db.conn.execute(
        f"""
        SELECT t.id, t.market_id, t.direction, t.mid_price AS entry_mid,
               t.entry_fill_price,
               t.size_usdc, t.decided_at, t.decision_path, t.target_mid,
               m.token_yes, m.token_no, m.question,
               m.active AS market_active, m.closed_at AS market_closed_at
        FROM trades t
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        LEFT JOIN markets m ON m.condition_id = t.market_id
        WHERE c.id IS NULL AND t.is_tracer = 0 {cycle_sql}
        ORDER BY t.decided_at DESC
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    return [dict(r) for r in rows]


async def _fetch_closed_pairs(dash) -> tuple[list[dict], list[dict], list[dict]]:
    """Returns (closed_trades, closed_pairs_for_exit_value, closures)."""
    cycle_id = dash.current_cycle_id
    cycle_sql = f"AND t.cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    cur = await dash.db.conn.execute(
        f"""
        SELECT t.id, t.size_usdc, t.direction, t.mid_price AS entry_mid,
               t.entry_fill_price, t.decision_path,
               c.exit_mid_price, c.exit_fill_price,
               c.realized_pnl_usdc, c.close_reason,
               c.entry_fee_usdc, c.exit_fee_usdc
        FROM trades t
        JOIN trade_closures c ON c.trade_id = t.id
        WHERE t.is_tracer = 0 {cycle_sql}
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    rows = [dict(r) for r in rows]
    return rows, rows, rows  # same row shape works for all three callers


async def _current_mid_for_trade(dash, t: dict) -> float | None:
    """Best-effort current CLOB mid for an open trade. Returns None on
    miss; the wallet helpers treat None as entry_mid (unrealized PnL = 0)."""
    if dash.cfg is None:
        # No CLOB threaded → can't price open positions.
        return None
    # Cycle 24 (handoff E): refuse to mark dead markets as live. Polymarket's
    # /midpoint returns the frozen last-trade price for closed markets — accepting
    # that as 'current value' makes invested == current == fake-flat, hiding the
    # ~$15–17 of fees the stuck position will book at timeout.
    if t.get("market_closed_at") is not None or (
        t.get("market_active") is not None and int(t["market_active"]) != 1
    ):
        return None  # signals "unpriceable" to wallet helpers + display
    token_id = t.get("token_yes") if t["direction"] == "YES" else t.get("token_no")
    if not token_id:
        return None
    clob = getattr(dash, "_clob_for_dashboard", None)
    if clob is None:
        # Lazily build a CLOB client. Reuse across the page so we don't
        # spin up a new httpx.AsyncClient per open position.
        from app.clob import CLOBClient
        clob = CLOBClient(
            base_url=dash.cfg.clob.base_url,
            timeout_seconds=dash.cfg.clob.timeout_seconds,
        )
        dash._clob_for_dashboard = clob
    # Cycle 27: derive the mid from the live book instead of /midpoint —
    # the latter silently reports the last traded price on wide-spread
    # books, which is exactly the fiction the wallet-honesty work bans.
    book = await clob.get_top_of_book(token_id)
    if book is None:
        return None
    return (float(book[0]) + float(book[1])) / 2.0

FILENAME = "dashboard.txt"


async def render(dash) -> str:
    uptime = time.monotonic() - dash._t_boot
    # Cycle 3: filter live counts by current cycle so the dashboard
    # looks fresh per cycle. Old cycles' data stays in the DB for
    # chunker analysis but never bleeds into the live numbers.
    cycle_id = dash.current_cycle_id
    cycle_filter = f"AND cycle_id = {int(cycle_id)}" if cycle_id is not None else ""
    lines = [
        f"# irene dashboard ({_now().isoformat()})",
        f"uptime: {uptime:.0f}s    cycle_id: {cycle_id if cycle_id is not None else '?'}",
        "",
    ]
    # Cycle 4: timestamps are stored as ISO-8601 with 'T' + offset
    # ('2026-05-24T21:20:39.397799+00:00'), but `datetime('now',...)`
    # returns 'YYYY-MM-DD HH:MM:SS' (space separator). String comparison
    # always passes for T-formatted rows on the same date, so the
    # "(1h)" filter silently expands to "since midnight". Wrap both
    # sides with datetime() so the parser normalises them before compare.
    for sql, label in (
        (f"SELECT COUNT(*) FROM articles WHERE datetime(ingested_at) >= datetime('now','-1 hour') {cycle_filter}",
         "articles ingested (1h)"),
        (f"SELECT COUNT(*) FROM signals  WHERE datetime(created_at)  >= datetime('now','-1 hour') {cycle_filter}",
         "signals matched   (1h)"),
        (f"SELECT COUNT(*) FROM trades   WHERE datetime(decided_at)  >= datetime('now','-1 hour') {cycle_filter}",
         "trades decided    (1h)"),
        (f"SELECT COUNT(*) FROM decisions WHERE outcome='execute' AND "
         f"datetime(created_at) >= datetime('now','-1 hour') {cycle_filter}",
         "trades executed   (1h)"),
        (f"SELECT COUNT(*) FROM decisions WHERE outcome='reject'  AND "
         f"datetime(created_at) >= datetime('now','-1 hour') {cycle_filter}",
         "trades rejected   (1h)"),
    ):
        n = await scalar(dash.db, sql)
        lines.append(f"  {label}: {n}")

    # ── AI subsystem ────────────────────────────────────────────────────
    lines += ["", "AI subsystem:"]

    # Model + budget (from dash.cfg / dash.budget if threaded, else "?")
    model = "?"
    if dash.cfg is not None:
        try:
            model = dash.cfg.openrouter.direction_model
        except Exception:  # noqa: BLE001
            pass
    budget_state = "?"
    remaining = "?"
    if dash.budget is not None:
        try:
            budget_state = "PAUSED" if dash.budget.is_paused() else "polling"
            r = dash.budget.remaining_usd
            remaining = f"${r:.2f}" if r is not None else "(unknown)"
        except Exception:  # noqa: BLE001
            pass
    lines.append(f"  model: {model}    budget: {budget_state}    remaining: {remaining}")

    # 5-label distribution (1h) — v5 prompt taxonomy:
    #   Y=trade-yes  N=trade-no  P=pass  U=uncertain  R=mismatch  ?=parse-fail
    # Older v2/v3/v4 rows have label=NULL; we surface them as "(legacy)".
    # Cycle 4: cycle-scope the label distribution so cycle 20's R/U rate
    # doesn't bleed into cycle 25's headline metric.
    cur = await dash.db.conn.execute(
        f"SELECT label, COUNT(*) FROM predictions "
        f"WHERE datetime(created_at) >= datetime('now','-1 hour') {cycle_filter} "
        f"GROUP BY label"
    )
    label_dist = {r[0]: int(r[1]) for r in await cur.fetchall()}
    await cur.close()
    n_y = label_dist.get("Y", 0)
    n_n = label_dist.get("N", 0)
    n_p = label_dist.get("P", 0)
    n_u = label_dist.get("U", 0)
    n_r = label_dist.get("R", 0)
    n_q = label_dist.get("?", 0)
    n_legacy = label_dist.get(None, 0)
    n_parse_fail = await scalar(
        dash.db,
        f"SELECT COUNT(*) FROM llm_parse_failures "
        f"WHERE datetime(created_at) >= datetime('now','-1 hour') {cycle_filter}",
    )
    n_total = n_y + n_n + n_p + n_u + n_r + n_q + n_legacy
    pf_pct = 100.0 * n_parse_fail / max(n_total + n_parse_fail, 1)
    lines.append(
        f"  labels (1h): Y {n_y:>3d}  N {n_n:>3d}  P {n_p:>3d}  "
        f"U {n_u:>3d}  R {n_r:>3d}  ? {n_q:>3d}    parse_fail {n_parse_fail} ({pf_pct:.0f}%)"
    )
    if n_legacy:
        lines.append(f"  legacy (v<=4):    {n_legacy} (no label column)")

    # Latency + tokens + cache (pooled across models, 1h)
    # llm_calls has no cycle_id column; join to predictions (which does) to
    # scope the latency/token metrics to the current cycle. Use DISTINCT
    # because a single llm_call can have multiple prediction rows for the
    # cached-reparse path; we count each llm_call once.
    cycle_join_filter = (
        f"AND p.cycle_id = {int(dash.current_cycle_id)}"
        if dash.current_cycle_id is not None
        else ""
    )
    cur = await dash.db.conn.execute(
        f"""
        SELECT lc.latency_ms, lc.tokens_in, lc.tokens_out, lc.status_code
        FROM llm_calls lc
        WHERE datetime(lc.created_at) >= datetime('now','-1 hour')
          AND EXISTS (
            SELECT 1 FROM predictions p
            WHERE p.llm_call_id = lc.id {cycle_join_filter}
          )
        """
    )
    call_rows = await cur.fetchall()
    await cur.close()
    if call_rows:
        lat = _percentiles([float(r["latency_ms"]) for r in call_rows if r["latency_ms"] is not None])
        toks_in = sum(int(r["tokens_in"] or 0) for r in call_rows)
        toks_out = sum(int(r["tokens_out"] or 0) for r in call_rows)
        err = sum(1 for r in call_rows if not (200 <= (r["status_code"] or 0) < 300))
        err_pct = 100.0 * err / max(len(call_rows), 1)
        lines.append(
            f"  latency (ms):     p50 {lat['p50']:>4.0f}  "
            f"p95 {lat['p95']:>4.0f}  p99 {lat['p99']:>4.0f}    "
            f"calls {len(call_rows)}  err {err} ({err_pct:.1f}%)"
        )
        # cache hit %: ratio of predictions.cached=1 to total predictions in window
        n_cached = await scalar(
            dash.db,
            f"SELECT COUNT(*) FROM predictions WHERE cached = 1 "
            f"AND datetime(created_at) >= datetime('now','-1 hour') {cycle_filter}",
        )
        cache_pct = 100.0 * n_cached / max(n_total, 1)
        lines.append(
            f"  tokens (1h):      in {toks_in:>6d}   out {toks_out:>5d}    "
            f"cache_hit {cache_pct:.0f}%"
        )
    else:
        lines.append("  (no LLM calls in last hour)")

    lines.append("")
    samples = await end_to_end_latency_ms(dash.db, cycle_id=dash.current_cycle_id)
    p = _percentiles(samples)
    if p["count"]:
        lines += [
            "end-to-end latency (article.ingested → trade.decided), last 1h:",
            f"  p50={p['p50']:>8.0f} ms",
            f"  p95={p['p95']:>8.0f} ms",
            f"  p99={p['p99']:>8.0f} ms",
            f"  n  ={p['count']:>8d}",
        ]
    else:
        lines.append("end-to-end latency: no completed trades in the last hour.")

    # ── WALLET ──────────────────────────────────────────────────────────
    # Paper-trading wallet: cash + open-position value + realized PnL.
    # Accounting identity: equity == initial_balance + realized + unrealized.
    initial_balance = 1000.0
    if dash.cfg is not None:
        try:
            initial_balance = float(dash.cfg.trade.balance_usdc)
        except Exception:  # noqa: BLE001
            pass

    open_trades_raw = await _fetch_open_trades(dash)
    closed_trades, closed_pairs, closures = await _fetch_closed_pairs(dash)
    # Mark-to-market every open position via a fresh CLOB mid.
    open_with_mid: list[dict] = []
    for t in open_trades_raw:
        cur_mid = await _current_mid_for_trade(dash, t)
        open_with_mid.append({
            "direction": t["direction"],
            "entry_mid": float(t["entry_mid"]),
            # Cycle 27: fills-based cost basis; wallet helpers prefer it
            # over entry_mid when non-NULL (legacy rows keep NULL).
            "entry_fill_price": (
                float(t["entry_fill_price"]) if t.get("entry_fill_price") is not None else None
            ),
            "current_mid": cur_mid,
            "target_mid": (float(t["target_mid"]) if t.get("target_mid") is not None else None),
            "size_usdc": float(t["size_usdc"]),
            "decision_path": t["decision_path"],
            "market_id": t["market_id"],
            "decided_at": t["decided_at"],
            "question": t.get("question"),
            "id": t["id"],
            # Cycle 24 (handoff E): propagate market liveness for the
            # "unpriceable / frozen, market closed YYYY-MM-DD" annotation.
            "market_active": t.get("market_active"),
            "market_closed_at": t.get("market_closed_at"),
        })

    open_size = wallet.sum_open_size_invested(open_trades_raw)
    closed_size = wallet.sum_closed_size_invested(closed_trades)
    closed_exit_value = wallet.sum_closed_exit_value(closed_pairs)
    realized = wallet.sum_realized_pnl(closures)
    fees_paid = wallet.sum_fees_paid(closures)
    open_value = wallet.sum_open_positions_value(open_with_mid)
    unreal = wallet.sum_unrealized_pnl(open_with_mid)
    # Cycle 25: cash is real-money-like — fees deducted on close.
    cash = wallet.cash_balance(
        initial_balance, open_size, closed_exit_value, closed_size,
        fees_paid_total=fees_paid,
    )
    eq = wallet.equity(cash, open_value)
    ok, drift = wallet.accounting_identity_check(
        initial_balance, realized, unreal, eq, tol=0.05
    )

    n_open = len(open_with_mid)
    n_closed = len(closed_trades)
    n_pricable = sum(1 for t in open_with_mid if t["current_mid"] is not None)
    # Cycle 24 (handoff E): positions on dead markets (closed_at set OR
    # active != 1) get current_mid=None upstream. Surface that count so
    # operators don't read "invested == current" as healthy when it just
    # means "the market is dead and we can't price it."
    n_unpriceable = sum(
        1 for t in open_with_mid
        if t["current_mid"] is None and (
            t.get("market_closed_at") is not None
            or (t.get("market_active") is not None and int(t["market_active"]) != 1)
        )
    )
    unpriceable_tail = f" ({n_unpriceable} unpriceable)" if n_unpriceable else ""

    lines += [
        "",
        "WALLET (paper):",
        f"  initial:   ${initial_balance:>9.2f}",
        f"  cash:      ${cash:>9.2f}    "
        f"open positions ({n_open}, {n_pricable} priced{unpriceable_tail}): "
        f"invested ${open_size:>8.2f} → current ${open_value:>8.2f}",
        f"  realized:  {realized:>+9.2f}    unrealized: {unreal:>+9.2f}    "
        f"fees paid: ${fees_paid:>7.2f}    closed: {n_closed}",
        f"  EQUITY:    ${eq:>9.2f}    "
        f"identity {'OK' if ok else 'DRIFT'}{f' ({drift:+.2f})' if not ok else ''}",
    ]
    # Per-path attribution
    by_path_open = {}
    by_path_closed = {}
    for t in open_with_mid:
        by_path_open.setdefault(t["decision_path"] or "mvp", []).append(t)
    for c in closures:
        # closures don't carry decision_path; join via trades for the rollup.
        pass
    # Per-path closure rollup (cycle 3).
    by_path_closed_pnl: dict[str, list[float]] = {}
    for c in closures:
        path = (c.get("decision_path") or "mvp")
        by_path_closed_pnl.setdefault(path, []).append(float(c["realized_pnl_usdc"]))
    # Per-path attribution: open invested+current + closed n + realized PnL.
    paths = sorted(set(by_path_open) | set(by_path_closed_pnl))
    for path in paths:
        rows = by_path_open.get(path, [])
        inv = sum(r["size_usdc"] for r in rows)
        val = sum(
            wallet.position_value(
                r["direction"],
                r["entry_fill_price"] if r.get("entry_fill_price") is not None else r["entry_mid"],
                r["current_mid"] if r["current_mid"] is not None else (
                    r["entry_fill_price"] if r.get("entry_fill_price") is not None else r["entry_mid"]
                ),
                r["size_usdc"],
            )
            for r in rows
        )
        closed_pnls = by_path_closed_pnl.get(path, [])
        closed_n = len(closed_pnls)
        closed_realized = sum(closed_pnls)
        lines.append(
            f"    {path:<4} open {len(rows):>2d}  inv ${inv:>7.2f}  cur ${val:>7.2f}  "
            f"closed {closed_n:>3d}  realized {closed_realized:>+8.2f}"
        )

    # Recent open positions detail (5 newest)
    if open_with_mid:
        lines += ["", "open positions (5 newest):"]
        for t in open_with_mid[:5]:
            q = (t.get("question") or "(?)")[:50]
            cur_mid = t["current_mid"]
            # Cycle 24 (handoff E): if current_mid is None because the
            # market is closed/inactive, say so loudly. Anything else with
            # None is a transient CLOB miss → the old "no mid" placeholder.
            market_closed_at = t.get("market_closed_at")
            market_active = t.get("market_active")
            is_dead_market = market_closed_at is not None or (
                market_active is not None and int(market_active) != 1
            )
            if cur_mid is None:
                if is_dead_market:
                    closed_date = (market_closed_at or "")[:10] or "?"
                    priced = f"unpriceable (frozen, market closed {closed_date})"
                else:
                    priced = "  no mid "
                upnl_str = "    -   "
            else:
                basis = (
                    t["entry_fill_price"]
                    if t.get("entry_fill_price") is not None
                    else t["entry_mid"]
                )
                upnl_t = wallet.unrealized_pnl(
                    t["direction"], basis, cur_mid, t["size_usdc"]
                )
                priced = f"mid {cur_mid:>5.3f}"
                upnl_str = f"{upnl_t:>+7.3f}"
            decided = (t["decided_at"] or "")[11:19]
            tgt = t.get("target_mid")
            tgt_str = f"target {tgt:>5.3f}" if tgt is not None else "target  -   "
            lines.append(
                f"  [{decided}] {t['decision_path']:<3} {t['direction']:<3} "
                f"${t['size_usdc']:>5.2f} @ {t['entry_mid']:>5.3f} → {priced}  "
                f"{tgt_str}  upnl {upnl_str}  "
                f'"{q}"'
            )

    # Last 5 news articles seen (RSS, non-migrated)
    lines += ["", "last 5 articles ingested:"]
    cur = await dash.db.conn.execute(
        """
        SELECT ingested_at, source, title
        FROM articles
        WHERE source != 'seulgi_migration'
        ORDER BY ingested_at DESC
        LIMIT 5
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    if not rows:
        lines.append("  (none yet)")
    else:
        for r in rows:
            title = (r["title"] or "")[:84]
            lines.append(f"  {r['ingested_at'][11:19]}  [{r['source']:<4}] {title}")

    # Last 5 decisions — micro version of decisions.txt. Each shows the
    # article + market + a one-line gate-cascade summary so the operator
    # sees WHY without leaving this page.
    lines += ["", "last 5 decisions:"]
    cur = await dash.db.conn.execute(
        f"""
        SELECT d.created_at, d.outcome, d.reason, d.gate_history,
               COALESCE(t.decision_path, 'mvp') AS path,
               t.direction, t.size_usdc, t.edge,
               a.title    AS article,
               m.question AS market
        FROM decisions d
        LEFT JOIN signals  s ON s.id = d.signal_id
        LEFT JOIN trades   t ON t.decision_id = d.id
        LEFT JOIN articles a ON a.id = s.article_id
        LEFT JOIN markets  m ON m.condition_id = s.market_id
        WHERE d.is_tracer = 0
          {("AND d.cycle_id = " + str(int(cycle_id))) if cycle_id is not None else ""}
        ORDER BY d.created_at DESC
        LIMIT 5
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    if not rows:
        lines.append("  (none yet)")
    else:
        import json as _json
        for r in rows:
            ts = (r["created_at"] or "")[11:19]
            outcome = r["outcome"] or "?"
            path = r["path"] or "?"
            # Header line — outcome + reason/edge+size
            if outcome == "execute" and r["direction"]:
                head = f"{outcome} {r['direction']:<3} ${r['size_usdc']:.2f} edge={r['edge']:+.3f}"
            else:
                head = f"{outcome:<7} {r['reason'] or '?'}"
            lines.append(f"  [{ts}] {path:<3} {head}")
            # News + market that triggered it
            art = (r["article"] or "(missing)")[:60]
            mkt = (r["market"] or "(missing)")[:50]
            lines.append(f"        \"{art}\" → \"{mkt}\"")
            # One-line gate cascade: e.g. "sim 0.36 ✓ · fresh ✓ · dir NEUTRAL ✗"
            try:
                gates = _json.loads(r["gate_history"] or "[]")
            except (TypeError, ValueError):
                gates = []
            parts = []
            for g in gates:
                name = g.get("gate", "?")
                ok = g.get("outcome") == "ok"
                marker = "✓" if ok else "✗"
                short = {
                    "similarity_floor": "sim",
                    "freshness": "fresh",
                    "direction": "dir",
                    "edge": "edge",
                    "size": "size",
                }.get(name, name)
                v = g.get("value")
                if isinstance(v, float):
                    val = f"{v:.2f}"
                elif v is None:
                    val = ""
                else:
                    val = str(v)
                parts.append(f"{short} {val} {marker}".strip())
                if not ok:
                    break  # cascade stopped here
            if parts:
                lines.append(f"        " + " · ".join(parts))

    return "\n".join(lines) + "\n"
