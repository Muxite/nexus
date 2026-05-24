"""DB write health tile.

Uses the synchronous in-memory snapshot so the render path never queues
behind a writer-bound PRAGMA. WAL frame counts come from the host-side
audit CLI (`db_health_audit.py`).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ..io import _ts

FILENAME = "db_health.txt"
TITLE = "DB HEALTH"


async def render(dash) -> str:
    now = datetime.now(timezone.utc)
    if dash._db is None:
        return f"# DB HEALTH — {_ts(now)}\n\n  (no DB attached)\n"
    health = dash._db.write_health_snapshot()

    lines: list[str] = [f"# DB HEALTH — {_ts(now)}", ""]
    bt = health.get("busy_timeout_ms")
    lines.append(f"  busy_timeout_ms : {bt if bt is not None else '?'}")
    wal_total = health.get("wal_total_frames")
    wal_done = health.get("wal_checkpointed_frames")
    wal_busy = health.get("wal_busy")
    if wal_total is not None:
        lines.append(
            f"  WAL frames      : {wal_total} total, {wal_done} checkpointed, "
            f"busy={wal_busy}"
        )
    if "wal_error" in health:
        lines.append(f"  WAL error       : {health['wal_error']}")
    lines.append("")

    lines.append("## Last successful insert per table")
    lines.append("")
    lines.append("| table             | last_insert (UTC)   | age      |")
    lines.append("|-------------------|---------------------|---------:|")
    last_insert_at = health.get("last_insert_at", {})
    for table in sorted(last_insert_at.keys()):
        ts_iso = last_insert_at[table]
        try:
            t = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
            age = (now - t).total_seconds()
            if age < 90:
                age_s = f"{age:>5.0f}s"
            elif age < 3600:
                age_s = f"{age/60:>4.1f}m"
            else:
                age_s = f"{age/3600:>4.1f}h"
            ts_display = ts_iso[:19]
        except Exception:  # noqa: BLE001
            age_s = "  ?  "
            ts_display = ts_iso[:19]
        lines.append(f"| {table:<17} | {ts_display:<19} | {age_s:>8} |")
    if not last_insert_at:
        lines.append("| (no successful inserts since boot)                          |")
    lines.append("")

    lines.append("## Write counters (ok / err since boot)")
    lines.append("")
    lines.append("| method                       | ok        | err     |")
    lines.append("|------------------------------|----------:|--------:|")
    counters = health.get("counters", {})
    for method in sorted(counters.keys()):
        c = counters[method]
        ok_n = c.get("ok", 0)
        err_n = c.get("err", 0)
        mark = " ⚠" if err_n > 0 else ""
        lines.append(
            f"| {method:<28} | {ok_n:>9} | {err_n:>7} |{mark}"
        )
    if not counters:
        lines.append("| (no write attempts yet)                                |")
    lines.append("")

    # A4C.2: telemetry queue tile.
    if dash._telemetry is not None:
        try:
            tm = dash._telemetry.metrics
            lines.append("## Telemetry queue")
            lines.append("")
            lines.append(
                "| depth | max | dropped_skip | dropped_decision | "
                "flush_batches | flush_errors |"
            )
            lines.append(
                "|------:|----:|-------------:|-----------------:|"
                "-------------:|-------------:|"
            )
            mark = " ⚠" if tm.get("flush_errors", 0) > 0 else ""
            lines.append(
                f"| {tm['depth']:>5} | {tm['max']:>3} | "
                f"{tm['dropped_skip']:>12} | {tm['dropped_decision']:>16} | "
                f"{tm['flush_batches']:>13} | {tm['flush_errors']:>12} |{mark}"
            )
            lines.append("")
        except Exception:  # noqa: BLE001
            pass

    # Phase 4B.1: per-direction calibration diagnostics tile.
    if dash._calibrator is not None:
        try:
            diag = dash._calibrator.diagnostics()
            rows = diag.get("directions", [])
            if rows:
                lines.append("## Calibration")
                lines.append("")
                lines.append(
                    "| direction | method   |   n | IC_before | "
                    "IC_after | Brier_before | Brier_after | fitted_at           |"
                )
                lines.append(
                    "|-----------|----------|----:|----------:|"
                    "---------:|-------------:|------------:|---------------------|"
                )
                for d in rows:
                    ts = (d.get("fitted_at") or "")[:19]
                    lines.append(
                        f"| {d['direction']:<9} | {d['method']:<8} | "
                        f"{d['n']:>3} | {d['ic_before']:+9.3f} | "
                        f"{d['ic_after']:+8.3f} | {d['brier_before']:>12.4f} | "
                        f"{d['brier_after']:>11.4f} | {ts:<19} |"
                    )
                lines.append("")
            else:
                lines.append("## Calibration")
                lines.append("")
                lines.append("  (identity fallback — closed-trade sample too small)")
                lines.append("")
        except Exception:  # noqa: BLE001
            pass

    # Phase 4E.1: composite scoring diagnostics tile.
    if dash._scorer is not None:
        try:
            diag = dash._scorer.diagnostics()
            models = diag.get("models", [])
            lines.append("## Composite Scoring")
            lines.append("")
            if models:
                lines.append(
                    "| direction |   n | train_AUC | test_AUC |"
                    "       IC | fitted_at           |"
                )
                lines.append(
                    "|-----------|----:|----------:|---------:|"
                    "---------:|---------------------|"
                )
                for m in models:
                    ts = (m.get("fitted_at") or "")[:19]
                    lines.append(
                        f"| {m['direction']:<9} | {m['n']:>3} | "
                        f"{m['train_auc']:>9.3f} | {m['test_auc']:>8.3f} | "
                        f"{m['ic']:+8.3f} | {ts:<19} |"
                    )
                lines.append(
                    f"  features : {diag.get('n_features', 0)}  "
                    "(see app/scoring.py:FEATURE_NAMES)"
                )
            else:
                lines.append("  (identity fallback — closed-trade sample too small)")
            lines.append("")
        except Exception:  # noqa: BLE001
            pass

    # Phase 4A.1: stage-0a capital-floor short-circuit counter.
    lines.append("## Ingress short-circuits (boot-cumulative)")
    lines.append("")
    lines.append(
        f"  capital_floor_skips : {dash._capital_floor_skips}  "
        "(headlines saved from embed/CLOB/DB)"
    )
    lines.append("")

    # Phase 4D.1 — spread+depth liquidity gate counters from in-memory metrics.
    try:
        import metrics as _m
        rows = _m.snapshot_recent()
        spread_skips = sum(r["count"] for r in rows
                           if r["name"] == "trader.spread_skips")
        depth_skips = sum(r["count"] for r in rows
                          if r["name"] == "trader.depth_skips")
        lines.append("## Liquidity gate (rolling ~5m) [Phase 4D.1]")
        lines.append("")
        lines.append(
            f"  spread_skips : {spread_skips}  "
            f"depth_skips : {depth_skips}  "
            "(replaces retired hour_gate)"
        )
        lines.append("")
    except Exception:  # noqa: BLE001
        pass

    # A4A.2: JIT pricing + IOC outcome tile.
    try:
        import metrics as _m
        jit_rows = [r for r in _m.snapshot_recent()
                    if r["name"].startswith("jit.")]
        if jit_rows:
            lines.append("## JIT pricing + IOC (rolling ~5m)")
            lines.append("")
            fills = sum(r["count"] for r in jit_rows
                        if r["name"] == "jit.ioc_fill")
            fails = sum(r["count"] for r in jit_rows
                        if r["name"] == "jit.ioc_fail")
            total = fills + fails
            fail_rate = (fails / total * 100.0) if total > 0 else 0.0
            delta = next((r for r in jit_rows
                          if r["name"] == "jit.delta_bps"), None)
            avg_delta = (delta["sum"] / delta["count"]
                         if delta and delta["count"] > 0 else 0.0)
            lines.append(
                f"  attempts : {total}  fills : {fills}  fails : {fails}"
                f"  fail_rate : {fail_rate:.1f}%"
            )
            lines.append(
                f"  avg_delta_bps : {avg_delta:+.2f}  "
                f"(snapshot → live drift)"
            )
            lines.append("")
    except Exception:  # noqa: BLE001
        pass

    # A4D.3 — position scaling tile suppressed when scaling has not fired.
    try:
        import metrics as _m
        ps_rows = [r for r in _m.snapshot_recent()
                   if r["name"].startswith("trader.position_scale")]
        if ps_rows:
            deltas = sum(r["count"] for r in ps_rows
                         if r["name"] == "trader.position_scale_delta_adds")
            at_cap = sum(r["count"] for r in ps_rows
                         if r["name"] == "trader.position_scale_at_cap")
            lines.append("## Position scaling (rolling ~5m)")
            lines.append("")
            lines.append(
                f"  delta_adds : {deltas}  at_cap : {at_cap}"
            )
            lines.append("")
    except Exception:  # noqa: BLE001
        pass

    # A4E.5 — counterfactual sampling: per-filter cf-trade counts (24h).
    try:
        cutoff = (now - timedelta(hours=24)).isoformat()
        async with dash._db._conn.execute(
            """
            SELECT COALESCE(cf_bypassed_filter, 'unknown') AS f,
                   COUNT(*) AS n_total,
                   SUM(CASE WHEN size_usdc > 0 AND skip_reason IS NULL
                            THEN 1 ELSE 0 END)                AS n_exec,
                   SUM(CASE WHEN realized_pnl IS NOT NULL
                            THEN 1 ELSE 0 END)                AS n_closed,
                   COALESCE(SUM(realized_pnl), 0)             AS sum_pnl
              FROM trades
             WHERE cf_bypassed_filter IS NOT NULL
               AND decided_at >= ?
             GROUP BY f
             ORDER BY n_total DESC
            """,
            (cutoff,),
        ) as cur:
            cf_rows = await cur.fetchall()
        if cf_rows:
            lines.append("## Counterfactual (24h, per-filter cf trades)")
            lines.append("")
            lines.append(
                "| filter              | n_total | n_exec | n_closed |  sum_pnl |"
            )
            lines.append(
                "|---------------------|--------:|-------:|---------:|---------:|"
            )
            for r in cf_rows:
                lines.append(
                    f"| {r['f']:<19} | {r['n_total']:>7} | "
                    f"{r['n_exec']:>6} | {r['n_closed']:>8} | "
                    f"{r['sum_pnl']:>+8.2f} |"
                )
            lines.append("")
    except Exception:  # noqa: BLE001
        pass

    # Cache section reads in-memory state; no extra DB query.
    cache = dash._heavy_cache
    last_refresh = cache.get("_last_refresh_at")
    if last_refresh is not None:
        cache_age = int((now - last_refresh).total_seconds())
        lines.append(
            f"## Dashboard heavy-query cache  (last refresh {cache_age}s ago)"
        )
    else:
        lines.append("## Dashboard heavy-query cache  (warming up)")
    lines.append("")
    for k in (
        "variant_stats", "decision_funnel", "confidence_buckets",
        "freshness_counts", "freshness_buckets", "scout_funnel",
    ):
        v = cache.get(k)
        status = "OK" if v is not None else "(empty)"
        lines.append(f"  {k:<22}: {status}")
    return "\n".join(lines) + "\n"
