"""Operator landing page — `dashboard.txt`.

This is the biggest single render. Each top-level section is a helper
that appends to a shared `lines` buffer; the entrypoint `render(dash)`
stitches them together. The split is mechanical (no behavior change) so
the output remains byte-equivalent with the pre-refactor version.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from ..io import _short, _ts

FILENAME = "dashboard.txt"
TITLE = "MAIN"


async def render(dash) -> str:
    now = datetime.now(timezone.utc)
    uptime = now - dash._started_at
    hours, rem = divmod(int(uptime.total_seconds()), 3600)
    mins = rem // 60

    last_news = dash._news[0] if dash._news else None
    last_trade = dash._executed[0] if dash._executed else None
    last_miss = dash._near_miss[0] if dash._near_miss else None
    last_skip = dash._declined[0] if dash._declined else None

    if dash._mode == "simulate":
        mode_label = "SIMULATE MODE — micro-bankroll, live capital tracking"
    elif dash._mode == "infinite" or dash._infinite_balance:
        mode_label = "INFINITE MODE — gain tracked vs staked"
    else:
        mode_label = "PAPER TRADING"

    lines = [
        "╔══════════════════════════════════════════════════════════════╗",
        "║                    SEULGI  DASHBOARD                        ║",
        "╚══════════════════════════════════════════════════════════════╝",
        "",
        f"  Updated : {_ts(now)}",
        f"  Uptime  : {hours}h {mins}m",
        "",
        f"── {mode_label} " + "─" * (66 - len(mode_label) - 5),
    ]

    per_engine = _per_engine_open_aggregates(dash)
    _render_balance_block(dash, lines, per_engine)
    _render_session_counters(dash, lines, per_engine)

    if dash._db is not None:
        _render_heavy_cache_blocks(dash, lines, now)

    _render_system_health(dash, lines)
    _render_watchers(dash, lines)
    _render_last_events(dash, lines, last_news, last_trade, last_miss, last_skip)
    _render_perf_metrics(lines)

    lines.append("")
    lines.append("  See positions_open.txt · trades_executed.txt · near_miss.txt")
    lines.append("      trades_declined.txt · news_seen.txt · activity_log.txt · profile.txt")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Per-engine open-position aggregates (cost basis + MTM)
# ---------------------------------------------------------------------------


def _per_engine_open_aggregates(dash) -> dict:
    """Derived strictly from currently-open positions.

    `_engine_staked` is cumulative-staked-ever and unsafe here — it never
    decrements on close, which produced the phantom "+$177 with 0 closes"
    headline in cycle 7.
    """
    per_engine_unreal: dict[str, float] = {}
    per_engine_open: dict[str, int] = {}
    per_engine_cost: dict[str, float] = {}
    per_engine_mtm: dict[str, float] = {}
    for p in dash._open_positions:
        eng = p.engine_name or "default"
        per_engine_unreal[eng] = per_engine_unreal.get(eng, 0.0) + p.unrealized_pnl
        per_engine_open[eng] = per_engine_open.get(eng, 0) + 1
        per_engine_cost[eng] = per_engine_cost.get(eng, 0.0) + p.size_usdc
        per_engine_mtm[eng] = per_engine_mtm.get(eng, 0.0) + (
            p.size_usdc + p.unrealized_pnl
        )
    return {
        "unreal": per_engine_unreal,
        "open": per_engine_open,
        "cost": per_engine_cost,
        "mtm": per_engine_mtm,
        "open_cost_total": sum(per_engine_cost.values()),
        "open_mtm_total": sum(per_engine_mtm.values()),
    }


def _engine_names_and_width(dash) -> tuple[list[str], int]:
    names = sorted(set(dash._engine_balances) | set(dash._engine_staked))
    name_w = max(14, *(len(n) for n in names)) if names else 14
    return names, name_w


# ---------------------------------------------------------------------------
# Balance block — three rendering modes
# ---------------------------------------------------------------------------


def _render_balance_block(dash, lines: list, per_engine: dict) -> None:
    if dash._mode == "simulate":
        _render_simulate(dash, lines, per_engine)
    elif dash._infinite_balance:
        _render_infinite(dash, lines, per_engine)
    else:
        _render_paper(dash, lines, per_engine)


def _render_simulate(dash, lines: list, per_engine: dict) -> None:
    """Honest accounting (post-bankroll-drift fix):
        MTM total = free cash + Σ (open position cost + unrealized)
        Net PnL   = realized (closed trades) + unrealized (open)
        Drift     = MTM_total - start - Net PnL. Nonzero drift = bug.
    """
    total_balance = (
        sum(dash._engine_balances.values()) if dash._engine_balances else 0.0
    )
    engine_names, name_w = _engine_names_and_width(dash)
    open_cost_total = per_engine["open_cost_total"]
    open_mtm_total = per_engine["open_mtm_total"]

    start = dash._simulate_total_usdc
    free_cash = total_balance
    mtm_total = free_cash + open_mtm_total
    unrealized = open_mtm_total - open_cost_total
    net_pnl = dash._total_realized_pnl + unrealized
    n_open = len(dash._open_positions)

    lines.append(f"  Starting bankroll       : ${start:>10.2f} USDC")
    lines.append(f"  Free cash               : ${free_cash:>10.2f} USDC")
    lines.append(
        f"  Open positions (cost)   : ${open_cost_total:>10.2f} USDC  (n={n_open})"
    )
    lines.append(f"  Open positions (MTM)    : ${open_mtm_total:>10.2f} USDC")
    mtm_delta = mtm_total - start
    md_sign = "+" if mtm_delta >= 0 else "-"
    lines.append(
        f"  Mark-to-market total    : ${mtm_total:>10.2f} USDC  "
        f"Δ vs start: {md_sign}${abs(mtm_delta):.2f}"
    )
    lines.append("")
    r_sign = "+" if dash._total_realized_pnl >= 0 else "-"
    lines.append(
        f"  Realized PnL (closed)   : {r_sign}${abs(dash._total_realized_pnl):>9.2f} USDC"
    )
    u_sign = "+" if unrealized >= 0 else "-"
    lines.append(
        f"  Unrealized PnL (open)   : {u_sign}${abs(unrealized):>9.2f} USDC"
    )
    n_sign = "+" if net_pnl >= 0 else "-"
    lines.append(
        f"  Net PnL (real+unreal)   : {n_sign}${abs(net_pnl):>9.2f} USDC  "
        f"← are we up?"
    )
    ai_status = "PAUSED (no free capital)" if dash._ai_paused else "ACTIVE"
    since = (
        f"  since {dash._ai_paused_since}"
        if (dash._ai_paused and dash._ai_paused_since)
        else ""
    )
    lines.append(f"  AI status               : {ai_status}{since}")

    drift = mtm_total - start - net_pnl
    if abs(drift) > 0.05 and start > 0:
        lines.append(
            f"  ⚠ ACCOUNTING DRIFT      : MTM-start-NetPnL = {drift:+.4f} USDC"
        )

    lines.append("")
    lines.append(
        f"  {'ENGINE':<{name_w}}  {'FREE':>10}  {'OPEN_COST':>10}  "
        f"{'OPEN_MTM':>10}  {'OPEN':>5}  {'UNREAL':>9}"
    )
    lines.append(
        "  " + "─" * (name_w + 2 + 10 + 2 + 10 + 2 + 10 + 2 + 5 + 2 + 9)
    )
    for eng_name in engine_names:
        free = dash._engine_balances.get(eng_name, 0.0)
        cost = per_engine["cost"].get(eng_name, 0.0)
        mtm = per_engine["mtm"].get(eng_name, 0.0)
        unreal = per_engine["unreal"].get(eng_name, 0.0)
        eng_open = per_engine["open"].get(eng_name, 0)
        lines.append(
            f"  {eng_name:<{name_w}}  {free:>10.2f}  {cost:>10.2f}  "
            f"{mtm:>10.2f}  {eng_open:>5d}  {unreal:>+9.2f}"
        )


def _render_infinite(dash, lines: list, per_engine: dict) -> None:
    total_balance = (
        sum(dash._engine_balances.values()) if dash._engine_balances else 0.0
    )
    total_staked = (
        sum(dash._engine_staked.values()) if dash._engine_staked else 0.0
    )
    engine_names, name_w = _engine_names_and_width(dash)

    tb_sign = "+" if total_balance >= 0 else "-"
    roi = (total_balance / total_staked * 100.0) if total_staked > 0 else 0.0
    lines.append(f"  Total realized gain : {tb_sign}${abs(total_balance):>10.2f} USDC")
    lines.append(f"  Total staked        : ${total_staked:>10.2f} USDC")
    lines.append(f"  ROI                 : {roi:>+6.2f}%")
    lines.append("")
    lines.append(
        f"  {'ENGINE':<{name_w}}  {'GAIN':>10}  {'STAKED':>10}  {'ROI':>8}  "
        f"{'OPEN':>5}  {'UNREALIZED':>11}"
    )
    lines.append("  " + "─" * (name_w + 2 + 10 + 2 + 10 + 2 + 8 + 2 + 5 + 2 + 11))
    for eng_name in engine_names:
        gain = dash._engine_balances.get(eng_name, 0.0)
        staked = dash._engine_staked.get(eng_name, 0.0)
        eng_roi = (gain / staked * 100.0) if staked > 0 else 0.0
        unreal = per_engine["unreal"].get(eng_name, 0.0)
        eng_open = per_engine["open"].get(eng_name, 0)
        lines.append(
            f"  {eng_name:<{name_w}}  {gain:>+10.2f}  {staked:>10.2f}  "
            f"{eng_roi:>+7.2f}%  {eng_open:>5d}  {unreal:>+11.2f}"
        )


def _render_paper(dash, lines: list, per_engine: dict) -> None:
    total_balance = (
        sum(dash._engine_balances.values()) if dash._engine_balances else 0.0
    )
    engine_names, name_w = _engine_names_and_width(dash)

    lines.append(f"  Total balance   : ${total_balance:>10.2f} USDC")
    lines.append("")
    lines.append(
        f"  {'ENGINE':<{name_w}}  {'BALANCE':>10}  {'OPEN':>5}  {'UNREALIZED':>11}"
    )
    lines.append("  " + "─" * (name_w + 2 + 10 + 2 + 5 + 2 + 11))
    for eng_name in engine_names:
        eng_bal = dash._engine_balances.get(eng_name, 0.0)
        unreal = per_engine["unreal"].get(eng_name, 0.0)
        eng_open = per_engine["open"].get(eng_name, 0)
        lines.append(
            f"  {eng_name:<{name_w}}  {eng_bal:>+10.2f}  {eng_open:>5d}  {unreal:>+11.2f}"
        )


# ---------------------------------------------------------------------------
# Session counters
# ---------------------------------------------------------------------------


def _render_session_counters(dash, lines: list, per_engine: dict) -> None:
    n_open_total = len(dash._open_positions)
    if dash._mode == "simulate":
        unrealized = per_engine["open_mtm_total"] - per_engine["open_cost_total"]
    else:
        unrealized = sum(p.unrealized_pnl for p in dash._open_positions)
    unreal_sign = "+" if unrealized >= 0 else "-"

    n_closed_session = len(dash._closed)
    session_realized = dash._total_realized_pnl
    sess_sign = "+" if session_realized >= 0 else ""
    lines += [
        "",
        f"  Open positions      : {n_open_total}",
        f"  Unrealized PnL      : {unreal_sign}${abs(unrealized):.2f}"
        "    (mark-to-market on open positions)",
        f"  Closes this session : {n_closed_session} / {sess_sign}${session_realized:.2f}"
        "   (since container start; lifetime gain shown above)",
        f"  Recent trades buf   : {len(dash._executed)} / {dash._executed.maxlen}"
        "    (in-memory ring)",
        f"  Trades declined     : {len(dash._declined)}",
        f"  Near misses         : {len(dash._near_miss)}",
    ]


# ---------------------------------------------------------------------------
# Heavy-cache derived blocks (variant leaderboard, funnel, freshness, conf)
# ---------------------------------------------------------------------------


def _render_heavy_cache_blocks(dash, lines: list, now: datetime) -> None:
    cache = dash._heavy_cache
    v_stats = cache.get("variant_stats") or []
    funnel = cache.get("decision_funnel")
    buckets = cache.get("confidence_buckets") or []
    fresh_counts = cache.get("freshness_counts") or []
    fresh_buckets = cache.get("freshness_buckets") or []
    last_refresh = cache.get("_last_refresh_at")
    if last_refresh is not None:
        age = int((now - last_refresh).total_seconds())
        cache_hint = f"  (5-min cache, refreshed {age}s ago)"
    else:
        cache_hint = "  (5-min cache, warming up…)"

    # E.7.1: filter to active variants only; variant_compare.txt keeps the
    # unfiltered table.
    if dash._active_variants:
        v_stats_view = [
            r for r in v_stats if (r.get("variant") or "") in dash._active_variants
        ]
    else:
        v_stats_view = list(v_stats)

    _render_variant_leaderboard(dash, lines, v_stats, v_stats_view, cache_hint)
    _render_decision_funnel(lines, funnel)
    _render_freshness_counts(lines, fresh_counts)
    _render_freshness_buckets(lines, fresh_buckets)
    _render_confidence_buckets(lines, buckets)


def _render_variant_leaderboard(
    dash, lines: list, v_stats: list, v_stats_view: list, cache_hint: str
) -> None:
    lines += ["", "── VARIANT LEADERBOARD (active engines, by ROI% on $100 pot) ────"]
    lines.append(cache_hint)
    if not v_stats_view:
        if dash._active_variants and v_stats:
            lines.append("  (no closed trades yet for the active engines)")
        else:
            lines.append("  (no executed trades yet)")
        return
    # M5: ROI on per-engine $100 pot. Each engine has its own pot;
    # sum_pnl / 100 IS its ROI (no cross-engine borrowing).
    now_for_last = datetime.now(timezone.utc)
    v_stats_view = sorted(
        v_stats_view,
        key=lambda r: (r.get("sum_pnl") or 0) / 100.0,
        reverse=True,
    )
    lines.append(
        f"  {'VARIANT':<34}  {'N':>4} {'HIT%':>5}  "
        f"{'PnL':>7} {'ROI%':>6}  {'HOLDmn':>6}  {'LASTtr':>7}"
    )
    lines.append("  " + "─" * 84)
    for r in v_stats_view[:14]:
        closed = (r.get("n_wins") or 0) + (r.get("n_losses") or 0)
        hit = (r["n_wins"] / closed * 100.0) if closed else 0.0
        pnl = r.get("sum_pnl") or 0.0
        roi_pot = (pnl / 100.0) * 100.0   # % on $100 pot
        avg_hold_min = (r.get("avg_hold_seconds") or 0) / 60.0
        last_at = r.get("last_trade_at")
        if last_at:
            try:
                t = datetime.fromisoformat(last_at.replace("Z", "+00:00"))
                mins = (now_for_last - t).total_seconds() / 60.0
                last_str = (
                    f"{mins:.0f}m" if mins < 90 else f"{mins / 60:.1f}h"
                )
            except Exception:  # noqa: BLE001
                last_str = "?"
        else:
            last_str = "—"
        lines.append(
            f"  {(r['variant'] or '?')[:34]:<34}  {r['n_trades']:>4} "
            f"{hit:>4.1f}%  {pnl:>+7.2f} {roi_pot:>+5.2f}%  "
            f"{avg_hold_min:>5.1f}m  {last_str:>7}"
        )
    lines.append("  Full table (incl. retired): variant_compare.txt")


def _render_decision_funnel(lines: list, funnel: Optional[dict]) -> None:
    lines += ["", "── DECISION FUNNEL (last 1h) ────────────────────────────────────"]
    if funnel is None or funnel["total"] == 0:
        lines.append("  (no decisions in last hour)")
        return
    total = funnel["total"]

    def _pct(n: int) -> str:
        return f"({n / total * 100.0:>4.1f}%)"

    lines.append(f"  decisions seen          : {total}")
    rows = [
        ("freshness skip       ",   funnel.get("failed_freshness", 0)),
        ("low_similarity bypass",   funnel.get("failed_low_sim", 0)),
        ("relevance gate dropped",  funnel.get("failed_relevance", 0)),
        ("pre-AI rule dropped",     funnel["pre_ai_dropped"]),
        ("AI returned NEUTRAL",     funnel["ai_neutral"]),
        ("conf below threshold",    funnel["failed_min_confidence"]),
        ("ensemble disagreement",   funnel["failed_agreement"]),
        ("post-AI rule dropped",    funnel["failed_post_ai_rule"]),
        ("rail / price unusable",   funnel["failed_rail"]),
        ("min-entry-mid blocked",   funnel.get("failed_min_entry", 0)),
        ("market too noisy",        funnel.get("failed_market_vol", 0)),
        ("edge too small",          funnel["failed_edge"]),
        ("position-cap blocked",    funnel["failed_position_guard"]),
        ("size/balance blocked",    funnel["failed_size"]),
        ("other skipped",           funnel["other_skipped"]),
        ("executed",                funnel["executed"]),
    ]
    # Pin the two cycle-9 buckets even when 0 so freshness + relevance gates
    # are visibly engaged. Others suppressed at 0 to keep the section scannable.
    always_show = {
        "freshness skip       ", "low_similarity bypass",
        "relevance gate dropped",
        "executed", "AI returned NEUTRAL",
    }
    for label, n in rows:
        if n == 0 and label not in always_show:
            continue
        lines.append(f"  → {label:<22}: {n:>4}  {_pct(n)}")


def _render_freshness_counts(lines: list, fresh_counts: list) -> None:
    lines += ["", "── FRESHNESS (last 1h headlines) ────────────────────────────────"]
    if not fresh_counts:
        lines.append("  (no labelled signals yet)")
        return
    total_f = sum(r["n"] for r in fresh_counts)
    for r in fresh_counts:
        pct = (r["n"] / total_f * 100.0) if total_f else 0.0
        lines.append(f"  {r['label']:<14} : {r['n']:>4}  ({pct:>4.1f}%)")


def _render_freshness_buckets(lines: list, fresh_buckets: list) -> None:
    lines += ["", "── FRESHNESS BUCKETS (last 24h) ─────────────────────────────────"]
    if not fresh_buckets:
        lines.append("  (no decisions in last 24h)")
        return
    lines.append(
        f"  {'LABEL':<14} {'EXEC':>5} {'WINS':>5} {'LOSS':>5}  "
        f"{'HIT%':>6}  {'AVG PnL':>8}  {'SUM PnL':>9}  {'SKIP':>5}"
    )
    lines.append("  " + "─" * 76)
    for b in fresh_buckets:
        closed = (b.get("n_wins") or 0) + (b.get("n_losses") or 0)
        hit_s = f"{(b['n_wins']/closed*100.0):>5.1f}%" if closed else "    —"
        avg_s = f"{b['avg_pnl']:>+8.3f}" if closed else "       —"
        sum_s = f"{b['sum_pnl']:>+9.2f}" if closed else "        —"
        lines.append(
            f"  {b['label']:<14} {b['n_executed']:>5} "
            f"{b['n_wins']:>5} {b['n_losses']:>5}  "
            f"{hit_s}  {avg_s}  {sum_s}  {b['n_skipped']:>5}"
        )


def _render_confidence_buckets(lines: list, buckets: list) -> None:
    lines += ["", "── CONFIDENCE BUCKETS (last 24h) ────────────────────────────────"]
    if not buckets:
        lines.append("  (no AI decisions in last 24h)")
        return
    lines.append(
        f"  {'CONF':<10} {'EXEC':>5} {'WINS':>5} {'LOSS':>5}  "
        f"{'HIT%':>6}  {'AVG PnL':>8}  {'SUM PnL':>9}  {'SKIP':>5}"
    )
    lines.append("  " + "─" * 76)
    for b in buckets:
        closed = (b.get("n_wins") or 0) + (b.get("n_losses") or 0)
        hit_s = f"{(b['n_wins']/closed*100.0):>5.1f}%" if closed else "    —"
        avg_s = f"{b['avg_pnl']:>+8.3f}" if closed else "       —"
        sum_s = f"{b['sum_pnl']:>+9.2f}" if closed else "        —"
        lines.append(
            f"  {b['bucket']:<10} {b['n_executed']:>5} "
            f"{b['n_wins']:>5} {b['n_losses']:>5}  "
            f"{hit_s}  {avg_s}  {sum_s}  {b['n_skipped']:>5}"
        )


# ---------------------------------------------------------------------------
# System health + watchers + last events + perf
# ---------------------------------------------------------------------------


def _render_system_health(dash, lines: list) -> None:
    lines += [
        "",
        "── SYSTEM HEALTH ───────────────────────────────────────────────",
    ]
    sh = dash._system_health
    scraper_glyph = "✓" if sh.scraper_reachable else "✗"
    if sh.scraper_reachable:
        scraper_summary = (
            f"reachable  (last OK {_ts(sh.scraper_last_ok_at)}"
            + (
                f", latency {sh.scraper_latency_ms:.0f}ms)"
                if sh.scraper_latency_ms is not None else ")"
            )
        )
    else:
        scraper_summary = f"UNREACHABLE — {sh.scraper_last_error or 'unknown'}"
    lines.append(f"  {scraper_glyph} scraper      : {scraper_summary}")
    lines.append(
        f"    AI key      : {'✓ present' if sh.ai_key_present else '✗ MISSING'}"
    )
    if sh.ai_total_decisions_1h > 0 and sh.ai_neutral_rate_1h is not None:
        warn = "  ⚠ signal starved" if sh.ai_neutral_rate_1h >= 0.85 else ""
        lines.append(
            f"    AI 1h       : {sh.ai_neutral_rate_1h*100:>5.1f}% NEUTRAL "
            f"(n={sh.ai_total_decisions_1h}){warn}"
        )
    if sh.db_last_insert_at is not None:
        lines.append(f"    DB last write : {_ts(sh.db_last_insert_at)}")
    lines.append("")


def _render_watchers(dash, lines: list) -> None:
    lines.append("── WATCHERS ────────────────────────────────────────────────────")
    sh = dash._system_health
    if not sh.scraper_reachable and dash._watcher_statuses:
        lines.append("  ⚠ scraper unreachable — every watcher below will report failures")
    for url, ws in dash._watcher_statuses.items():
        status = "DEAD" if ws.is_unreachable else f"fail={ws.failures}"
        since = _ts(ws.last_headline_at) if ws.last_headline_at else "never"
        tail = f"  err={ws.last_error[:40]}" if ws.last_error else ""
        lines.append(
            f"  {'✓' if not ws.is_unreachable else '✗'} {_short(url):<30} "
            f"last={since}  {status}{tail}"
        )
    if not dash._watcher_statuses:
        lines.append("  (no watchers registered yet)")
    lines.append("")


def _render_last_events(
    dash, lines: list, last_news, last_trade, last_miss, last_skip
) -> None:
    lines.append("── LAST EVENTS ─────────────────────────────────────────────────")
    if last_news:
        lines.append(
            f"  Last news    : [{_ts(last_news.detected_at)}] "
            f"{last_news.headline[:80]}"
        )
        lines.append(f"               : {last_news.site_url}")
    if last_trade:
        lines.append(
            f"  Last trade   : [{_ts(last_trade.decided_at)}] {last_trade.direction} "
            f"${last_trade.size_usdc:.0f} @ {last_trade.mid_price:.3f}  "
            f"{last_trade.question[:60]}"
        )
    if last_miss:
        lines.append(
            f"  Last near miss: [{_ts(last_miss.decided_at)}] {last_miss.direction} "
            f"edge={last_miss.edge:+.3f}  {last_miss.question[:60]}"
        )
    if last_skip:
        lines.append(
            f"  Last declined : [{_ts(last_skip.decided_at)}] {last_skip.skip_reason}  "
            f"{last_skip.question[:60]}"
        )


def _render_perf_metrics(lines: list) -> None:
    try:
        import metrics as _m
        rows = _m.snapshot_recent()
        if not rows:
            return
        lines.append("")
        lines.append("── PERF METRICS (rolling, ~5m in-memory) ───────────────────────")
        lines.append(
            f"  {'NAME':<28} {'TAGS':<22} {'N':>6}  "
            f"{'p50':>6}  {'p95':>6}  {'p99':>6}  {'MAX':>6}"
        )
        lines.append("  " + "─" * 84)
        for r in rows[:15]:
            tags_str = (r["tags"] or "").replace('"', "")[:22]
            if r["sum"] > 0:  # timer
                lines.append(
                    f"  {r['name'][:28]:<28} {tags_str:<22} {r['count']:>6}  "
                    f"{r['p50']:>5.0f}ms  {r['p95']:>5.0f}ms  "
                    f"{r['p99']:>5.0f}ms  {r['max']:>5.0f}ms"
                )
            else:  # counter
                lines.append(
                    f"  {r['name'][:28]:<28} {tags_str:<22} {r['count']:>6}  "
                    f"{'—':>6}  {'—':>6}  {'—':>6}  {'—':>6}"
                )
    except Exception:  # noqa: BLE001
        pass
