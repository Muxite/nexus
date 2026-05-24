"""Operator landing page — `dashboard.txt`.

Everything an operator needs to know in one screen: uptime, 1h counts,
AI subsystem health, end-to-end latency, last 5 articles, last 5
decisions. The AI panel absorbed what was `llm_calls.txt` so the
prompt-quality signal (NEUTRAL rate, parse failures, cache hit) is
immediately visible on the landing page.
"""

from __future__ import annotations

import time

from ..db_helpers import end_to_end_latency_ms, scalar
from ..io import _now, _percentiles

FILENAME = "dashboard.txt"


async def render(dash) -> str:
    uptime = time.monotonic() - dash._t_boot
    lines = [
        f"# irene dashboard ({_now().isoformat()})",
        f"uptime: {uptime:.0f}s",
        "",
    ]
    for sql, label in (
        ("SELECT COUNT(*) FROM articles WHERE ingested_at >= datetime('now','-1 hour')",
         "articles ingested (1h)"),
        ("SELECT COUNT(*) FROM signals  WHERE created_at  >= datetime('now','-1 hour')",
         "signals matched   (1h)"),
        ("SELECT COUNT(*) FROM trades   WHERE decided_at  >= datetime('now','-1 hour')",
         "trades decided    (1h)"),
        ("SELECT COUNT(*) FROM decisions WHERE outcome='execute' AND "
         "created_at >= datetime('now','-1 hour')",
         "trades executed   (1h)"),
        ("SELECT COUNT(*) FROM decisions WHERE outcome='reject'  AND "
         "created_at >= datetime('now','-1 hour')",
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
    cur = await dash.db.conn.execute(
        "SELECT label, COUNT(*) FROM predictions "
        "WHERE created_at >= datetime('now','-1 hour') GROUP BY label"
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
        "SELECT COUNT(*) FROM llm_parse_failures "
        "WHERE created_at >= datetime('now','-1 hour')",
    )
    n_total = n_y + n_n + n_p + n_u + n_r + n_q + n_legacy
    pf_pct = 100.0 * n_parse_fail / max(n_total + n_parse_fail, 1)
    lines.append(
        f"  v5 labels  (1h): Y {n_y:>3d}  N {n_n:>3d}  P {n_p:>3d}  "
        f"U {n_u:>3d}  R {n_r:>3d}  ? {n_q:>3d}    parse_fail {n_parse_fail} ({pf_pct:.0f}%)"
    )
    if n_legacy:
        lines.append(f"  legacy (v<=4):    {n_legacy} (no label column)")

    # Latency + tokens + cache (pooled across models, 1h)
    cur = await dash.db.conn.execute(
        "SELECT latency_ms, tokens_in, tokens_out, status_code FROM llm_calls "
        "WHERE created_at >= datetime('now','-1 hour')"
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
            "SELECT COUNT(*) FROM predictions WHERE cached = 1 "
            "AND created_at >= datetime('now','-1 hour')",
        )
        cache_pct = 100.0 * n_cached / max(n_parsed, 1)
        lines.append(
            f"  tokens (1h):      in {toks_in:>6d}   out {toks_out:>5d}    "
            f"cache_hit {cache_pct:.0f}%"
        )
    else:
        lines.append("  (no LLM calls in last hour)")

    lines.append("")
    samples = await end_to_end_latency_ms(dash.db)
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
        """
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
