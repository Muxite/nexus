"""
Plain-text dashboard file (Kazuha-style).

Overwrites ``dashboard.txt`` on a timer so operators can ``cat`` a mounted
volume path without using the HTTP API.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from irene_shared.db import IreneDB
from irene_shared.redis_client import IreneRedis

logger = logging.getLogger(__name__)

_W = 88  # total line width


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

def _money(v: float) -> str:
    sign = "-" if v < 0 else "+"
    return f"{sign}${abs(v):,.2f}"


def _money_plain(v: float) -> str:
    """No sign prefix — for absolute values like balance."""
    return f"${abs(v):,.2f}"


def _pct(v: float) -> str:
    return f"{v:+.2f}%"


def _ts(v: Any, short: bool = False) -> str:
    if v is None:
        return "—"
    s = v.isoformat() if isinstance(v, datetime) else str(v)
    if len(s) >= 19:
        return s[11:19] + " UTC" if short else s[:19] + " UTC"
    return s


def _age(dt: Any) -> str:
    """Human-readable age since dt."""
    if dt is None:
        return "—"
    now = datetime.now(timezone.utc)
    if not hasattr(dt, "tzinfo") or dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = now - dt
    total = int(delta.total_seconds())
    if total < 0:
        return "0s"
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _bar(value: float, max_value: float, width: int = 20, char: str = "█") -> str:
    if max_value <= 0:
        return "·" * width
    filled = int(round((value / max_value) * width))
    filled = max(0, min(filled, width))
    return char * filled + "·" * (width - filled)


def _rule(title: str = "", char: str = "─") -> str:
    if not title:
        return char * _W
    inner = f" {title} "
    side = (_W - len(inner)) // 2
    return char * side + inner + char * (_W - side - len(inner))


def _col(label: str, value: str, w: int = 42) -> str:
    """Left-pad label, right-pad value to fixed total width."""
    entry = f"  {label:<28}{value}"
    return entry[:w].ljust(w)


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------

def _section_equity(stats: dict, starting_balance: float, mode: str) -> list[str]:
    realized = float(stats.get("realized_pnl_total") or 0)
    realized_24 = float(stats.get("realized_pnl_24h") or 0)
    unrealized = float(stats.get("unrealized_pnl_total") or 0)
    exposure = float(stats.get("open_exposure_usdc") or 0)
    net_pnl = realized + unrealized
    sb = float(starting_balance)
    equity = sb + net_pnl
    ret_pct = (net_pnl / sb * 100.0) if sb > 0 else 0.0
    mode_tag = f"[{mode.upper()}]"

    return [
        _rule("EQUITY & P&L"),
        f"  Starting balance: {_money_plain(sb):<12}  Mode: {mode_tag:<8}  "
        f"Implied equity: {_money_plain(equity)}",
        f"  Net P&L (realized + unrealized):  {_money(net_pnl):<14}  vs start: {_pct(ret_pct)}",
        f"  Realized all time:  {_money(realized):<16}  Realized 24h: {_money(realized_24)}",
        f"  Unrealized (open):  {_money(unrealized):<16}  Exposure:     {_money_plain(exposure)}",
    ]


def _section_scorecard(stats: dict) -> list[str]:
    wins = int(stats.get("closed_wins") or 0)
    losses = int(stats.get("closed_losses") or 0)
    be = int(stats.get("closed_breakeven") or 0)
    decided = wins + losses
    win_rate = (wins / decided * 100.0) if decided > 0 else None
    win_rate_str = f"{win_rate:.1f}%" if win_rate is not None else "—"

    executed = int(stats.get("trades_executed_total") or 0)
    skipped = int(stats.get("trades_skipped_total") or 0)
    closed_total = int(stats.get("trades_closed_total") or 0)
    closed_24h = int(stats.get("trades_closed_24h") or 0)
    open_exec = int(stats.get("trades_open_executed") or 0)
    open_pos = int(stats.get("open_positions") or 0)

    bar_width = 30
    total_decided = wins + losses + be
    win_bar = _bar(wins, total_decided, bar_width, "W") if total_decided else "·" * bar_width

    return [
        _rule("SCORECARD"),
        f"  Executed: {executed:<6}  Skipped: {skipped:<6}  Open trades: {open_exec:<6}  Positions: {open_pos}",
        f"  Closed:   {closed_total:<6} total    {closed_24h:<6} in last 24h",
        f"  Wins / Losses / Flat:  {wins} / {losses} / {be}   "
        f"Win rate: {win_rate_str} (excl. flat)",
        f"  [{win_bar}]  {wins}W {losses}L {be}flat",
    ]


def _section_pipeline(stats: dict) -> list[str]:
    return [
        _rule("PIPELINE"),
        f"  Active markets:              {stats['active_markets']:>6}",
        f"  Signals ingested:            {stats['signals_total']:>6}   "
        f"  mapped: {stats.get('signals_mapped_total', 0)}",
        f"  Signals ingested (24h):      {stats['signals_ingested_24h']:>6}",
        f"  Mapped signals:              {stats.get('mapped_signals_total', 0):>6}   "
        f"  dispatched: {stats.get('mapped_signals_dispatched', 0)}",
        f"  Mapped signals (24h):        {stats.get('mapped_signals_24h', 0):>6}",
    ]


def _section_services(last_seen: dict, metrics: dict) -> list[str]:
    lines = [_rule("SERVICES")]
    for svc in ["scanner", "feeder", "trader"]:
        ts = last_seen.get(svc)
        dot = "●" if ts else "○"
        ts_str = ts if ts else "never seen"
        line = f"  {dot} {svc:<10} {ts_str}"
        m = metrics.get(svc, {})
        if m:
            ops = [k for k in m.keys() if not k.startswith("_")][:3]
            summary = "  ".join(
                f"{op}: {m[op].get('mean_ms', 0):.0f}ms" for op in ops if "mean_ms" in m.get(op, {})
            )
            if summary:
                line += f"   [{summary}]"
        lines.append(line)

    # Watched pages (reported by feeder via Redis profile)
    feeder_metrics = metrics.get("feeder", {})
    watched = feeder_metrics.get("_watched_pages", {})
    if watched:
        lines.append("")
        lines.append(f"  {'WATCHED PAGES':}")
        for url, status in watched.items():
            dot = "●" if status == "running" else "○"
            short = url.replace("https://", "").replace("http://", "").rstrip("/")
            lines.append(f"    {dot} {short}")

    return lines


def _section_positions(positions: list[dict]) -> list[str]:
    if not positions:
        return [_rule(f"OPEN POSITIONS (0)"), "  — none —"]
    lines = [_rule(f"OPEN POSITIONS ({len(positions)})")]
    hdr = f"  {'#':<4} {'Dir':<5} {'Size':>8}  {'Entry':>6}  {'Last':>6}  {'Unreal.':>9}  {'Age':<10}  Market"
    lines.append(hdr)
    lines.append("  " + "─" * (_W - 2))
    for i, p in enumerate(positions[:20], 1):
        direction = str(p.get("direction", "?"))
        size = float(p.get("size_usdc") or 0)
        entry = float(p.get("entry_price") or 0)
        last = float(p.get("last_price") or entry)
        unreal = float(p.get("unrealized_pnl") or 0)
        cid = str(p.get("condition_id", ""))[:20]
        age_str = _age(p.get("opened_at"))
        unreal_str = _money(unreal)
        lines.append(
            f"  {i:<4} {direction:<5} {_money_plain(size):>8}  {entry:>6.3f}  {last:>6.3f}"
            f"  {unreal_str:>9}  {age_str:<10}  {cid}"
        )
    if len(positions) > 20:
        lines.append(f"  … and {len(positions) - 20} more")
    return lines


def _section_recent_trades(trades: list[dict]) -> list[str]:
    if not trades:
        return [_rule("RECENT CLOSED TRADES (last 15)"), "  — none —"]
    closed = [t for t in trades if t.get("closed_at") is not None][:15]
    lines = [_rule(f"RECENT CLOSED TRADES (last {len(closed)})")]
    hdr = f"  {'#':<4} {'Dir':<5} {'Size':>8}  {'Entry':>6}  {'Exit':>6}  {'P&L':>9}  {'Outcome':<8}  {'Closed':<12}  Reason"
    lines.append(hdr)
    lines.append("  " + "─" * (_W - 2))
    for i, t in enumerate(closed, 1):
        direction = str(t.get("direction", "?"))
        size = float(t.get("fill_size_usdc") or t.get("kelly_size_usdc") or 0)
        entry = float(t.get("fill_price") or t.get("mid_price") or 0)
        exit_p = float(t.get("exit_price") or 0)
        pnl = float(t.get("realized_pnl") or 0)
        outcome = str(t.get("outcome") or "—")
        closed_ts = _ts(t.get("closed_at"), short=True)
        reason = str(t.get("exit_reason") or "—")[:12]
        lines.append(
            f"  {i:<4} {direction:<5} {_money_plain(size):>8}  {entry:>6.3f}  {exit_p:>6.3f}"
            f"  {_money(pnl):>9}  {outcome:<8}  {closed_ts:<12}  {reason}"
        )
    return lines


def _section_skip_reasons(trades: list[dict]) -> list[str]:
    from collections import Counter
    skipped = [t for t in trades if not t.get("executed") and t.get("skip_reason")]
    if not skipped:
        return []
    counts: Counter = Counter(t["skip_reason"] for t in skipped)
    lines = [_rule("SKIP REASONS (recent)")]
    for reason, count in counts.most_common(10):
        bar = _bar(count, counts.most_common(1)[0][1], 20)
        lines.append(f"  {count:>5}  [{bar}]  {reason}")
    return lines


def _section_calibration(brier: list[dict]) -> list[str]:
    if not brier:
        return [_rule("CALIBRATION (Brier scores)"), "  — no resolved trades yet —"]
    lines = [_rule("CALIBRATION (Brier scores — lower is better, random=0.25)")]
    hdr = f"  {'Source':<16} {'Samples':>8}  {'Mean Brier':>12}  {'First':<20}  {'Last':<20}"
    lines.append(hdr)
    lines.append("  " + "─" * (_W - 2))
    for row in brier:
        src = str(row.get("signal_source", "?"))[:15]
        n = int(row.get("sample_count") or 0)
        mb = float(row.get("mean_brier") or 0)
        bar = _bar(mb, 0.25, 12)  # 0.25 = random baseline
        lines.append(
            f"  {src:<16} {n:>8}  {mb:>10.4f}  [{bar}]"
            f"  {_ts(row.get('first_at')):<20}  {_ts(row.get('last_at')):<20}"
        )
    return lines


def _section_timeline(stats: dict) -> list[str]:
    return [
        _rule("TIMELINE"),
        f"  First signal ingested:   {_ts(stats.get('first_signal_ingested_at'))}",
        f"  First trade decided:     {_ts(stats.get('first_trade_decided_at'))}",
        f"  Last trade closed:       {_ts(stats.get('last_trade_closed_at'))}",
    ]


# ---------------------------------------------------------------------------
# Top-level formatter
# ---------------------------------------------------------------------------

def format_dashboard_text(
    stats: dict[str, Any],
    last_seen: dict[str, str | None],
    positions: list[dict[str, Any]],
    recent_trades: list[dict[str, Any]],
    brier: list[dict[str, Any]],
    metrics: dict[str, Any],
    *,
    starting_balance_usdc: float,
    mode: str,
) -> str:
    gen: datetime = stats["generated_at"]
    ts = gen.isoformat()
    ts_short = ts[:19] + " UTC" if len(ts) >= 19 else ts

    sections: list[list[str]] = [
        ["═" * _W, f"  IRENE DASHBOARD   {ts_short}", "═" * _W],
        _section_equity(stats, starting_balance_usdc, mode),
        _section_scorecard(stats),
        _section_pipeline(stats),
        _section_services(last_seen, metrics),
        _section_positions(positions),
        _section_recent_trades(recent_trades),
        _section_calibration(brier),
        _section_timeline(stats),
        ["═" * _W, "  tail -f irene/logs/*.log   |   cat irene/data/dashboard.txt", "═" * _W],
    ]

    skip = _section_skip_reasons(recent_trades)
    if skip:
        sections.insert(-2, skip)  # before timeline

    lines: list[str] = []
    for sec in sections:
        lines.extend(sec)
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

async def write_dashboard_text_file(
    db: IreneDB,
    redis: IreneRedis,
    path: Path,
    *,
    starting_balance_usdc: float,
    mode: str,
) -> None:
    stats = await db.get_dashboard_stats()
    positions = await db.get_open_positions()
    recent_trades = await db.get_recent_trades(limit=50)
    brier = await db.get_brier_summary()

    last_seen = {
        "scanner": await redis.get_last_seen("scanner"),
        "feeder": await redis.get_last_seen("feeder"),
        "trader": await redis.get_last_seen("trader"),
    }
    metrics: dict[str, Any] = {}
    for svc in ["scanner", "feeder", "trader"]:
        profile = await redis.get_profile(svc)
        if profile:
            metrics[svc] = profile

    text = format_dashboard_text(
        stats,
        last_seen,
        positions,
        recent_trades,
        brier,
        metrics,
        starting_balance_usdc=starting_balance_usdc,
        mode=mode,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)
    logger.debug("Wrote text dashboard to %s", path)
