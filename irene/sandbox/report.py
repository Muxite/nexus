"""Shared markdown rendering — per-row blocks + summary tables.

Used by show_set, run_strategy, verdict_template, compare_runs.
"""

from __future__ import annotations

from collections import Counter
from typing import Iterable, Optional  # noqa: F401

from irene.sandbox.loader import SandboxRow


def fmt_float(v, fmt="{:.3f}") -> str:
    return fmt.format(v) if isinstance(v, (int, float)) else "?"


def fmt_money(v) -> str:
    return f"${v:,.0f}" if isinstance(v, (int, float)) else "?"


def resolved_outcome(row: SandboxRow) -> Optional[str]:
    """'YES' / 'NO' that the market resolved to, or None if not yet
    resolved. Derived from `resolved_yes_payout` (~1 = YES, ~0 = NO)."""
    p = row.resolved_yes_payout
    if p is None:
        return None
    return "YES" if p >= 0.5 else "NO"


def _resolution_line(row: SandboxRow, decision: Optional[dict]) -> Optional[str]:
    """Ground-truth resolution + (when a directional decision exists) a
    correctness mark — 'did we get it right' for this row."""
    outcome = resolved_outcome(row)
    if outcome is None:
        return "- **Resolution:** pending"
    line = f"- **Resolution:** {outcome} (yes_payout {fmt_float(row.resolved_yes_payout)})"
    direction = (decision or {}).get("direction")
    if direction in ("YES", "NO"):
        mark = "✓ correct" if direction == outcome else "✗ wrong"
        line += f" · predicted {direction} → {mark}"
    return line


def render_row_block(n: int, row: SandboxRow, *, decision: Optional[dict] = None,
                     trade: Optional[dict] = None) -> str:
    art = (row.article_title or "<missing>").strip()
    src = row.article_source or "?"
    url = row.article_url or ""
    q = row.market_question.strip() or "<missing>"
    slug_or_id = row.market_condition_id
    end = row.market_end_date or "?"
    mid = fmt_float(row.market_mid_t0)
    vol = fmt_money(row.market_volume_24h)
    hte = fmt_float(row.hours_to_expiry, "{:.1f}h")
    parts = [
        f"## [{n}] exploration_id={row.exploration_id}  ·  cycle `{row.cycle_name or '?'}`",
        f"t0: {row.t0_iso}  ·  orig_conf {fmt_float(row.orig_llm_confidence)}  "
        f"·  freshness {row.orig_freshness or '?'}  ·  status {row.capture_status}",
        f"- **Article:** \"{art}\" — *{src}*  ",
        f"  {url}",
        f"- **Market:** \"{q}\" — `{slug_or_id}` · ends {end} · mid {mid} · vol {vol} · {hte} to expiry",
        f"- **Orig reason:** {(row.orig_llm_reason or '').strip() or '<none>'}",
    ]
    if decision is not None:
        parts.append(
            f"- **Decision:** related={decision.get('related')} · "
            f"direction={decision.get('direction') or '-'} · "
            f"conf {fmt_float(decision.get('confidence'))} · "
            f"reason: {(decision.get('reason') or '').strip()[:200]}"
        )
        if decision.get("parse_error"):
            parts.append(f"  - parse_error: `{decision['parse_error']}`")
    res = _resolution_line(row, decision)
    if res:
        parts.append(res)
    if trade is not None:
        if trade.get("skip_reason"):
            parts.append(f"- **Trade:** skipped ({trade['skip_reason']})")
        else:
            parts.append(
                f"- **Trade:** {trade['direction']} @ {fmt_float(trade.get('entry_price'))} "
                f"→ {fmt_float(trade.get('exit_price'))}  "
                f"({trade.get('entry_offset_s')}s → {trade.get('exit_offset_s')}s)  "
                f"PnL **{fmt_float(trade.get('pnl'), '${:+.2f}')}**"
            )
    return "\n".join(parts) + "\n"


def summary_counts(rows: Iterable[SandboxRow]) -> dict:
    rows = list(rows)
    confs = [r.orig_llm_confidence for r in rows if r.orig_llm_confidence is not None]
    return {
        "n": len(rows),
        "by_cycle": dict(Counter((r.cycle_name or "?") for r in rows)),
        "by_status": dict(Counter(r.capture_status for r in rows)),
        "by_source": dict(Counter((r.article_source or "?") for r in rows)),
        "by_tier": dict(Counter((r.article_tier or "?") for r in rows)),
        "by_freshness": dict(Counter((r.orig_freshness or "unknown") for r in rows)),
        "judged": sum(1 for r in rows if r.orig_llm_confidence is not None),
        "unjudged": sum(1 for r in rows if r.orig_llm_confidence is None
                        and (r.orig_llm_reason or "") == "gate_skipped"),
        "conf_min": min(confs) if confs else None,
        "conf_max": max(confs) if confs else None,
        "conf_ge_0_7": sum(1 for c in confs if c >= 0.7),
        "conf_ge_0_9": sum(1 for c in confs if c >= 0.9),
        "resolved": sum(1 for r in rows if r.resolved_yes_payout is not None),
        "resolved_yes": sum(1 for r in rows if resolved_outcome(r) == "YES"),
        "resolved_no": sum(1 for r in rows if resolved_outcome(r) == "NO"),
        "unresolved": sum(1 for r in rows if r.resolved_yes_payout is None),
    }


def render_summary(name: str, s: dict) -> str:
    def kv(d):
        return ", ".join(f"{k}: {v}" for k, v in sorted(d.items(), key=lambda x: -x[1]))
    lines = [
        f"# {name}",
        f"- **n:** {s['n']}  (judged {s.get('judged', '?')}, "
        f"unjudged {s.get('unjudged', '?')})",
        f"- **by cycle:** {kv(s.get('by_cycle', {}))}",
        f"- **by status:** {kv(s['by_status'])}",
        f"- **by source:** {kv(s['by_source'])}",
        f"- **by freshness:** {kv(s['by_freshness'])}",
        f"- **conf range:** {fmt_float(s['conf_min'])} – {fmt_float(s['conf_max'])}  "
        f"·  ≥0.7: {s['conf_ge_0_7']}  ·  ≥0.9: {s['conf_ge_0_9']}",
        f"- **resolution:** resolved {s.get('resolved', 0)} "
        f"(YES {s.get('resolved_yes', 0)} / NO {s.get('resolved_no', 0)})  "
        f"·  pending {s.get('unresolved', 0)}",
        "",
    ]
    return "\n".join(lines)


def render_pnl_summary(trades: list[dict], *, bankroll_usdc: Optional[float] = None) -> str:
    executed = [t for t in trades if not t.get("skip_reason")]
    skipped = [t for t in trades if t.get("skip_reason")]
    pnl_vals = [t["pnl"] for t in executed if t.get("pnl") is not None]
    total = sum(pnl_vals) if pnl_vals else 0.0
    wins = sum(1 for p in pnl_vals if p > 0)
    losses = sum(1 for p in pnl_vals if p < 0)
    win_rate = (wins / len(pnl_vals) * 100) if pnl_vals else 0.0
    skip_counts = Counter(t.get("skip_reason") or "" for t in skipped)
    lines = [
        "## PnL summary",
        f"- **Trades:** {len(executed)} executed · {len(skipped)} skipped",
        f"- **Total PnL:** {fmt_float(total, '${:+.2f}')}  "
        f"·  wins {wins} / losses {losses}  ·  win-rate {win_rate:.1f}%",
    ]
    if pnl_vals:
        lines.append(f"- **Mean PnL/trade:** {fmt_float(sum(pnl_vals)/len(pnl_vals), '${:+.2f}')}")

    has_kelly = any(t.get("raw_kelly_f") is not None for t in trades)
    if has_kelly:
        deployed = sum(
            (t.get("entry_price") or 0.0) * (t.get("notional") or 0.0)
            for t in executed
        )
        roi_dep = (total / deployed * 100) if deployed > 0 else 0.0
        edges = [t.get("edge") for t in executed if t.get("edge") is not None]
        avg_edge = sum(edges) / len(edges) if edges else 0.0
        avg_notional = sum(t["notional"] for t in executed) / len(executed) if executed else 0.0
        lines.append(f"- **Capital deployed:** {fmt_float(deployed, '${:,.2f}')}")
        lines.append(f"- **ROI on deployed:** {roi_dep:+.2f}%")
        if bankroll_usdc:
            lines.append(f"- **ROI on bankroll** (${bankroll_usdc:,.0f}): "
                         f"{(total / bankroll_usdc * 100):+.2f}%")
        lines.append(f"- **Avg edge (executed):** {avg_edge:+.3f}  "
                     f"·  **Avg notional:** ${avg_notional:,.2f}")

    if skip_counts:
        lines.append("")
        lines.append("### Skip-reason histogram")
        for k, v in sorted(skip_counts.items(), key=lambda x: -x[1]):
            lines.append(f"- `{k or 'unknown'}`: {v}")

    if has_kelly:
        lines.append("")
        lines.append("### PnL by edge bucket (executed trades only)")
        edge_buckets = [
            ("<0.05", 0.0, 0.05),
            ("0.05–0.15", 0.05, 0.15),
            ("0.15–0.30", 0.15, 0.30),
            (">=0.30", 0.30, 10.0),
        ]
        lines.append("| bucket | n | total PnL | win-rate | avg notional |")
        lines.append("|---|---:|---:|---:|---:|")
        for label, lo, hi in edge_buckets:
            bucket = [
                t for t in executed
                if t.get("edge") is not None and lo <= t["edge"] < hi
            ]
            n = len(bucket)
            if not n:
                lines.append(f"| {label} | 0 | — | — | — |")
                continue
            tot = sum(t["pnl"] for t in bucket if t.get("pnl") is not None)
            w = sum(1 for t in bucket if (t.get("pnl") or 0) > 0)
            wr = (w / n * 100) if n else 0.0
            an = sum(t["notional"] for t in bucket) / n
            lines.append(
                f"| {label} | {n} | {fmt_float(tot, '${:+.2f}')} | "
                f"{wr:.1f}% | ${an:,.2f} |"
            )
    return "\n".join(lines) + "\n"
