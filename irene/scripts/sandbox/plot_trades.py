"""Plot per-trade odds time-series for a sandbox run.

For each executed trade in a run's trades.csv, draw two panels from
`odds_observations`: a full-history view (≈ -24h..+1h, to spot flat/dead
markets and historical volatility) and a zoomed ±1h view with the bid/ask
band and entry/exit markers. Panels are colored green/red by PnL sign.
Also emits a sibling `trades.txt` (or `<out>.txt`) with the same per-trade
content as plain text, including a full-history and a ±1h unicode sparkline.

    python -m irene.scripts.sandbox.plot_trades --run <run_id>
    python -m irene.scripts.sandbox.plot_trades --run <run_a> <run_b> ... --out combined.pdf
    python -m irene.scripts.sandbox.plot_trades --run <run_id> --max-trades 16
    python -m irene.scripts.sandbox.plot_trades --run <run_id> --no-pdf   # txt only
    python -m irene.scripts.sandbox.plot_trades --run <run_id> --no-txt   # pdf only

Default: 2 trades per page (2 panels each), multi-page PDF, all executed
trades. Each --run becomes a section in the PDF, preceded by a title page.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from typing import Optional

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from irene.sandbox import features, runio
from irene.sandbox.paths import DB_PATH, RUNS_DIR


SPARK_CHARS = "▁▂▃▄▅▆▇█"
SPARK_WIDTH = 60
# Each trade renders two panels: a full-history view (≈ -24h..+1h, to spot
# flat/dead markets and historical volatility) and a zoomed view bounded to
# this window around news (legible entry/exit action).
ZOOM_WINDOW_S = 3600
TRADES_PER_PAGE = 2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", nargs="+", required=True,
                    help="One or more run_ids (each becomes a section in the PDF)")
    ap.add_argument("--out", default=None, help="output PDF path (txt path derived by swapping suffix)")
    ap.add_argument("--max-trades", type=int, default=0,
                    help="Cap trades per run (largest |PnL| first). 0 = no cap.")
    ap.add_argument("--no-pdf", action="store_true", help="Skip PDF emission")
    ap.add_argument("--no-txt", action="store_true", help="Skip TXT emission")
    args = ap.parse_args()

    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row

    if args.out:
        pdf_path = args.out
    elif len(args.run) == 1:
        pdf_path = os.path.join(RUNS_DIR, args.run[0], "trades.pdf")
    else:
        pdf_path = os.path.join(RUNS_DIR, "trades_combined.pdf")
    base, _ = os.path.splitext(pdf_path)
    txt_path = base + ".txt"

    # Pre-resolve per-run data so we can render PDF and TXT in lockstep.
    sections: list[dict] = []
    for run_id in args.run:
        run_dir = os.path.join(RUNS_DIR, run_id)
        try:
            manifest = json.load(open(os.path.join(run_dir, "manifest.json")))
        except FileNotFoundError:
            print(f"skip {run_id}: no manifest")
            continue
        trades = runio.load_trades(run_dir)
        decisions = runio.load_decisions(run_dir)
        by_eid = {d["exploration_id"]: d for d in decisions}

        executed = [t for t in trades if not t.get("skip_reason") and t.get("pnl") is not None]
        executed.sort(key=lambda t: (0 if t["pnl"] > 0 else 1, -abs(t["pnl"])))
        capped = bool(args.max_trades and len(trades) > args.max_trades)
        if args.max_trades and len(executed) > args.max_trades:
            sampled = sorted(executed, key=lambda t: -abs(t["pnl"]))[:args.max_trades]
            executed = sorted(sampled,
                              key=lambda t: (0 if t["pnl"] > 0 else 1, -abs(t["pnl"])))

        sections.append({
            "run_id": run_id,
            "manifest": manifest,
            "trades_total": len(trades),
            "executed": executed,
            "by_eid": by_eid,
            "capped": capped,
        })

    if not sections:
        print("no sections to render")
        return

    n_per = TRADES_PER_PAGE
    total_pages = 0

    if not args.no_pdf:
        with PdfPages(pdf_path) as pdf:
            for sec in sections:
                _write_section_cover(pdf, sec["run_id"], sec["manifest"],
                                     len(sec["executed"]), capped=sec["capped"])
                executed = sec["executed"]
                n_pages = (len(executed) + n_per - 1) // n_per if executed else 0
                for page_start in range(0, len(executed), n_per):
                    page = executed[page_start:page_start + n_per]
                    # One trade per row: [full-history | zoomed ±1h].
                    fig, axes = plt.subplots(n_per, 2, figsize=(11, 8.5))
                    axes = axes.reshape(n_per, 2)
                    for r in range(n_per):
                        if r < len(page):
                            trade = page[r]
                            dec = sec["by_eid"].get(trade["exploration_id"], {})
                            _plot_full(axes[r][0], con, trade, dec)
                            _plot_zoom(axes[r][1], con, trade, dec)
                        else:
                            axes[r][0].axis("off")
                            axes[r][1].axis("off")
                    fig.suptitle(
                        f"{sec['manifest'].get('strategy')} × {sec['manifest'].get('set')}   "
                        f"·   page {page_start // n_per + 1}/{n_pages}   "
                        f"·   left: full history   right: ±1h zoom",
                        fontsize=9, y=0.995,
                    )
                    fig.tight_layout(rect=[0, 0, 1, 0.96], h_pad=3.0)
                    pdf.savefig(fig)
                    plt.close(fig)
                    total_pages += 1
        print(f"wrote {pdf_path}  ({len(sections)} runs, {total_pages} trade pages)")

    if not args.no_txt:
        with open(txt_path, "w") as f:
            for i, sec in enumerate(sections):
                if i:
                    f.write("\n" + "=" * 78 + "\n\n")
                f.write(_section_text(con, sec))
        print(f"wrote {txt_path}")


def _write_section_cover(pdf, run_id, manifest, n_executed, capped):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    lines = _cover_lines(run_id, manifest, n_executed, capped)
    ax.text(0.5, 0.55, "\n".join(lines), ha="center", va="center",
            fontsize=12, family="monospace")
    pdf.savefig(fig)
    plt.close(fig)


def _cover_lines(run_id, manifest, n_executed, capped):
    bt = manifest.get("backtest", {})
    return [
        f"{manifest.get('strategy', '?')}  ×  {manifest.get('set', '?')}",
        "",
        f"run_id: {run_id}",
        f"strategy version: {manifest.get('strategy_version', '?')}",
        f"model: {manifest.get('model', '?')}",
        f"rows in set: {manifest.get('n_rows', '?')}",
        f"executed trades plotted: {n_executed}"
        + ("   (capped — largest |PnL| first)" if capped else ""),
        "",
        f"sizing: {bt.get('sizing', '?')}   "
        f"exit_horizon_s: {bt.get('exit_horizon_seconds', '?')}   "
        f"min_edge: {bt.get('min_edge', '?')}",
    ]


def _entry_exit_yes(trade: dict) -> tuple:
    """Entry/exit offsets and prices back-projected into YES-mid space for
    plotting (pnl.py stores them in the bet's price space)."""
    direction = trade["direction"]
    ent_px = trade.get("entry_price")
    exit_px = trade.get("exit_price")
    if direction == "NO":
        ent_yes = (1.0 - ent_px) if ent_px is not None else None
        exit_yes = (1.0 - exit_px) if exit_px is not None else None
    else:
        ent_yes, exit_yes = ent_px, exit_px
    return trade.get("entry_offset_s"), ent_yes, trade.get("exit_offset_s"), exit_yes


def _bezel(ax, color: str) -> None:
    for spine in ax.spines.values():
        spine.set_edgecolor(color)
        spine.set_linewidth(1.2)


def _plot_full(ax, con, trade: dict, decision: dict) -> None:
    """Left panel: the full captured history (≈ -24h..+1h). A flat horizontal
    line here = a dead market we should never have traded."""
    eid = trade["exploration_id"]
    pnl = trade["pnl"]
    color = "#2ca02c" if pnl > 0 else "#d62728"
    ticks = _load_ticks(con, eid)
    if not ticks:
        ax.text(0.5, 0.5, f"no ticks for eid {eid}", ha="center", va="center")
        ax.axis("off")
        return

    xs = [r["offset_seconds"] for r in ticks]
    ys_mid = [r["mid"] for r in ticks]
    ax.axvspan(min(xs), 0, color="#f0f0f0", alpha=0.6, zorder=0)
    ax.axvline(0, color="#444", lw=0.8, linestyle="--", alpha=0.7, zorder=1)
    ax.plot([x / 3600.0 for x in xs], ys_mid, color="#1f77b4", lw=1.0, zorder=2)
    _bezel(ax, color)
    ax.set_ylim(-0.02, 1.02)
    ax.set_xlabel("hours from news (t0)", fontsize=8)
    ax.set_ylabel("YES mid", fontsize=8)
    ax.tick_params(axis="both", labelsize=7)
    ax.grid(True, alpha=0.25, linewidth=0.4)

    meta = _row_meta(con, eid)
    f = features.compute(con, eid, direction=trade.get("direction", "") or "")
    vol = meta.get("volume_usdc")
    stats = (f"hist vol(pre)={_n(f.pre_stdev)}  ·  vol=${_n(vol, money=True)}  "
             f"·  mid_t0={_n(f.mid_t0)}")
    article = (meta.get("article_title") or "?")[:74]
    market = (meta.get("market_question") or "?")[:74]
    head = f"eid={eid}  ·  {trade['direction']}  ·  PnL ${pnl:+.2f}"
    ax.set_title(head, fontsize=8.5, color=color, loc="left", pad=4)
    ax.text(0.01, -0.34, stats, transform=ax.transAxes, fontsize=7,
            family="monospace", va="top", color="#333")
    ax.text(0.01, -0.45, f"news:   {article}", transform=ax.transAxes,
            fontsize=7, family="monospace", va="top")
    ax.text(0.01, -0.56, f"market: {market}", transform=ax.transAxes,
            fontsize=7, family="monospace", va="top")


def _plot_zoom(ax, con, trade: dict, decision: dict) -> None:
    """Right panel: bounded to ±ZOOM_WINDOW_S so the entry/exit action and
    the bid/ask band are legible instead of squished by 24h of history."""
    eid = trade["exploration_id"]
    pnl = trade["pnl"]
    color = "#2ca02c" if pnl > 0 else "#d62728"
    ticks = [r for r in _load_ticks(con, eid)
             if -ZOOM_WINDOW_S <= r["offset_seconds"] <= ZOOM_WINDOW_S]
    if not ticks:
        ax.text(0.5, 0.5, "no ticks in ±1h", ha="center", va="center")
        ax.axis("off")
        return

    xs = [r["offset_seconds"] for r in ticks]
    ys_mid = [r["mid"] for r in ticks]
    ys_bid = [r["best_bid"] for r in ticks]
    ys_ask = [r["best_ask"] for r in ticks]
    ax.axvspan(min(xs), 0, color="#f0f0f0", alpha=0.6, zorder=0)
    ax.axvline(0, color="#444", lw=0.8, linestyle="--", alpha=0.7, zorder=1)

    band = [(x, b, a) for x, b, a in zip(xs, ys_bid, ys_ask)
            if b is not None and a is not None]
    if band:
        bx, bb, ba = zip(*band)
        ax.fill_between(bx, bb, ba, color="#bbbbbb", alpha=0.30,
                        step="post", linewidth=0, zorder=1)
    ax.plot(xs, ys_mid, color="#1f77b4", lw=1.1, zorder=2)

    ent_off, ent_yes, exit_off, exit_yes = _entry_exit_yes(trade)
    if ent_off is not None and ent_yes is not None and abs(ent_off) <= ZOOM_WINDOW_S:
        ax.scatter([ent_off], [ent_yes], marker="^", s=70, color=color,
                   edgecolor="black", linewidth=0.5, zorder=5)
    if exit_off is not None and exit_yes is not None and abs(exit_off) <= ZOOM_WINDOW_S:
        ax.scatter([exit_off], [exit_yes], marker="v", s=70, color=color,
                   edgecolor="black", linewidth=0.5, zorder=5)

    _bezel(ax, color)
    ax.set_ylim(-0.02, 1.02)
    ax.set_xlim(-ZOOM_WINDOW_S - 20, ZOOM_WINDOW_S + 20)
    ax.set_xlabel("offset_s from news (t0)", fontsize=8)
    ax.tick_params(axis="both", labelsize=7)
    ax.grid(True, alpha=0.25, linewidth=0.4)

    impact = ""
    chain = decision.get("chain") or {}
    if "classify" in chain:
        cls = chain["classify"] or {}
        impact = f"  ·  impact={cls.get('impact')}"
    p_yes = decision.get("p_yes")
    p_yes_str = (f"  ·  p_yes={p_yes:.2f}"
                 if isinstance(p_yes, (int, float)) and p_yes >= 0 else "")
    exit_reason = trade.get("exit_reason") or ""
    rstr = f"  ·  exit={exit_reason}" if exit_reason else ""
    ax.set_title(f"±1h zoom{impact}{p_yes_str}{rstr}", fontsize=8.5,
                 color=color, loc="left", pad=4)


def _n(v, money=False) -> str:
    if not isinstance(v, (int, float)):
        return "n/a"
    return f"{v:,.0f}" if money else f"{v:.3f}"


def _load_ticks(con, eid: int) -> list[sqlite3.Row]:
    return list(con.execute(
        "SELECT offset_seconds, mid, best_bid, best_ask FROM odds_observations "
        "WHERE exploration_id = ? AND mid IS NOT NULL "
        "ORDER BY offset_seconds",
        (eid,),
    ))


def _row_meta(con, eid: int) -> dict:
    r = con.execute(
        "SELECT a.title AS article_title, m.question AS market_question, "
        "m.volume_usdc AS volume_usdc "
        "FROM explorations e "
        "LEFT JOIN articles a ON a.id = e.article_id "
        "LEFT JOIN markets m ON m.condition_id = e.market_condition_id "
        "WHERE e.id = ?",
        (eid,),
    ).fetchone()
    return dict(r) if r else {}


def _section_text(con, sec: dict) -> str:
    manifest = sec["manifest"]
    executed = sec["executed"]
    parts: list[str] = []
    parts.extend(_cover_lines(sec["run_id"], manifest, len(executed), sec["capped"]))
    parts.append("")
    parts.append(f"total trades in run: {sec['trades_total']}   executed: {len(executed)}")
    parts.append("")
    if not executed:
        parts.append("(no executed trades to render)")
        return "\n".join(parts) + "\n"

    for i, trade in enumerate(executed, 1):
        decision = sec["by_eid"].get(trade["exploration_id"], {})
        parts.append(_trade_text_block(con, trade, decision, i, len(executed)))
    return "\n".join(parts) + "\n"


def _trade_text_block(con, trade: dict, decision: dict, n: int, total: int) -> str:
    eid = trade["exploration_id"]
    direction = trade["direction"] or "?"
    pnl = trade["pnl"]
    is_win = pnl > 0
    verdict = "WIN " if is_win else "LOSS"

    ent_off = trade.get("entry_offset_s")
    ent_px = trade.get("entry_price")
    exit_off = trade.get("exit_offset_s")
    exit_px = trade.get("exit_price")
    edge = trade.get("edge")
    notional = trade.get("notional")
    p_yes = decision.get("p_yes")

    impact_str = ""
    chain = decision.get("chain") or {}
    if "classify" in chain:
        cls = chain["classify"] or {}
        impact_str = f"  ·  impact={cls.get('impact')} ({cls.get('direction')})"
    p_yes_str = (f"  ·  p_yes={p_yes:.2f}"
                 if isinstance(p_yes, (int, float)) and p_yes >= 0 else "")

    meta = _row_meta(con, eid)
    article = (meta.get("article_title") or "?").strip()
    market = (meta.get("market_question") or "?").strip()

    ticks = _load_ticks(con, eid)
    zoom_ticks = [r for r in ticks
                  if -ZOOM_WINDOW_S <= r["offset_seconds"] <= ZOOM_WINDOW_S]
    full_spark, full_axis, flo, fhi = _build_sparkline(ticks, trade, width=SPARK_WIDTH)
    zoom_spark, zoom_axis, zlo, zhi = _build_sparkline(zoom_ticks, trade, width=SPARK_WIDTH)
    n_ticks = len(ticks)
    if ticks:
        t_lo, t_hi = ticks[0]["offset_seconds"], ticks[-1]["offset_seconds"]
        full_range = (f"full  YES mid {flo:.3f}..{fhi:.3f}   "
                      f"t={t_lo / 3600:+.1f}h..{t_hi / 3600:+.1f}h   ticks={n_ticks}")
    else:
        full_range = "(no ticks)"
    zoom_range = (f"±1h   YES mid {zlo:.3f}..{zhi:.3f}   ticks={len(zoom_ticks)}"
                  if zoom_ticks else "±1h   (no ticks)")

    def fmt_off(v): return f"{v:+d}s" if isinstance(v, int) else "?"
    def fmt_px(v): return f"{v:.3f}" if isinstance(v, (int, float)) else "?"
    def fmt_edge(v): return f"{v:+.3f}" if isinstance(v, (int, float)) else "?"
    def fmt_money(v): return f"${v:,.2f}" if isinstance(v, (int, float)) else "?"

    lines = [
        f"--- [{n}/{total}]  eid={eid}  ·  {direction}  ·  PnL ${pnl:+.2f}  ·  {verdict}{impact_str}{p_yes_str}",
        f"     entry: {fmt_px(ent_px)} @ {fmt_off(ent_off)}    "
        f"exit: {fmt_px(exit_px)} @ {fmt_off(exit_off)}    "
        f"edge: {fmt_edge(edge)}    notional: {fmt_money(notional)}",
        f"     news:    {article[:140]}",
        f"     market:  {market[:140]}",
        f"     {full_range}",
        f"     |{full_spark}|",
        f"     |{full_axis}|   (full history; 0=t0, E=entry, X=exit)",
        f"     {zoom_range}",
        f"     |{zoom_spark}|",
        f"     |{zoom_axis}|   (±1h zoom; 0=t0, E=entry, X=exit)",
    ]
    return "\n".join(lines) + "\n"


def _build_sparkline(ticks, trade, *, width: int) -> tuple[str, str, float, float]:
    if not ticks:
        return "", "", 0.0, 1.0
    xs = [r["offset_seconds"] for r in ticks]
    ys = [r["mid"] for r in ticks]
    x_lo, x_hi = min(xs), max(xs)
    binned = _bin_series(xs, ys, x_lo, x_hi, width)
    # Forward-fill empty bins so the line stays continuous.
    filled: list[float] = []
    last: Optional[float] = None
    for v in binned:
        if v is None:
            filled.append(last if last is not None else (ys[0] if ys else 0.0))
        else:
            filled.append(v)
            last = v
    lo, hi = min(filled), max(filled)
    spark = _sparkline(filled, lo=lo, hi=hi)

    axis = list("·" * width)

    def col(t: float) -> int:
        if x_hi == x_lo:
            return 0
        return min(width - 1, max(0, int((t - x_lo) / (x_hi - x_lo) * (width - 1))))

    if x_lo <= 0 <= x_hi:
        axis[col(0)] = "0"
    ent_off = trade.get("entry_offset_s")
    exit_off = trade.get("exit_offset_s")
    if isinstance(ent_off, int) and x_lo <= ent_off <= x_hi:
        axis[col(ent_off)] = "E"
    if isinstance(exit_off, int) and x_lo <= exit_off <= x_hi:
        axis[col(exit_off)] = "X"
    return spark, "".join(axis), lo, hi


def _bin_series(xs, ys, x_lo, x_hi, n_bins: int) -> list[Optional[float]]:
    bins: list[list[float]] = [[] for _ in range(n_bins)]
    if x_hi == x_lo:
        return [ys[0] if ys else None] + [None] * (n_bins - 1)
    span = x_hi - x_lo
    for x, y in zip(xs, ys):
        if y is None:
            continue
        b = int((x - x_lo) / span * n_bins)
        if b >= n_bins:
            b = n_bins - 1
        bins[b].append(y)
    return [sum(b) / len(b) if b else None for b in bins]


def _sparkline(values, *, lo: float, hi: float) -> str:
    if not values:
        return ""
    if hi <= lo:
        return SPARK_CHARS[0] * len(values)
    n_chars = len(SPARK_CHARS) - 1
    out = []
    for v in values:
        idx = int((v - lo) / (hi - lo) * n_chars + 0.5)
        idx = max(0, min(n_chars, idx))
        out.append(SPARK_CHARS[idx])
    return "".join(out)


if __name__ == "__main__":
    main()
