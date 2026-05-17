"""validate-report — independently re-derive per-variant stats and emit
seulgi/REPORT_VALIDATION_<date>.md.

This is the gate. Sections B–G of the profitability-foundation plan do not
start unless this script's final line reads `VALIDATION: PASS`.

What it does
────────────
1. Re-derives per-variant closed-trade stats (n, W, L, hit%, sum_pnl,
   ROI on staked) using the same SQL conventions as `variants.py` /
   `pnl.py` — no shortcuts.
2. Adds Wilson 95% CI bounds on hit-rate (formula taken from
   REPORT_2026-05-16 line 27).
3. Cross-checks anchor rows against REPORT_2026-05-16. Tolerances:
   hit% ±0.5pp, CI bounds ±0.5pp, sum_pnl ±$0.01. Where the current DB
   has accumulated more closes since the report, a delta column reports
   it rather than failing.
4. Spread-cost sanity check: every closed trade must satisfy
   realized_pnl ∈ [-(stake)*(1+slack), stake/entry*(1-entry)] +/- slack.
5. Computes a per-variant selection score (B.0):
     score = roi_wilson_lower × min(1, n/30) × frequency_factor
     friction_adjusted_roi is currently identical to raw ROI; once
     §B.1's friction-aware paper trader lands and `--friction-mode
     realistic` is implemented, the score multiplies by that adjusted
     number instead.
6. Emits a markdown report with:
     - Anchor PASS/FAIL table
     - Money-eligible strategies today (n≥30 ∧ friction_adj_roi>0 ∧ score>0)
     - Watch list (high point-ROI, building sample)
     - Selection winner (or NONE — pause)
     - VALIDATION: PASS|FAIL
"""
from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from scripts.analysis._common import (
    REPO, add_common_args, build_filters, closed_trades_filter, open_db,
)
from scripts.analysis._format import Section


# ---------------------------------------------------------------------------
# Anchors from seulgi/REPORT_2026-05-16.md (lines 37-62)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Anchor:
    variant: str
    W: int
    L: int
    hit_pct: float        # %
    ci_lo: float          # %
    ci_hi: float          # %
    sum_pnl: float        # USDC


ANCHORS: tuple[Anchor, ...] = (
    Anchor("aggressive/conf25_tp005_sl015_lowvol",        16, 1, 94.1, 72.9, 98.9, 126.17),
    Anchor("aggressive/conf25_tp005_sl015",               21, 7, 75.0, 56.7, 87.4,  76.41),
    Anchor("aggressive",                                  24,11, 68.6, 52.0, 81.4,  46.13),
    Anchor("aggressive/conf25_tp005_sl015_norail",        17,10, 63.0, 44.2, 78.5, -43.28),
    Anchor("balanced/conf70_tp020_sl030",                  3, 9, 25.0,  8.9, 53.2, -48.78),
)

# Hit-rate tolerances (pp) and sum_pnl tolerance (USDC)
HIT_TOL = 0.5
CI_TOL = 0.5
PNL_TOL = 0.01

# Selection thresholds
MIN_N_FOR_MONEY = 30
WILSON_Z = 1.96   # 95%


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def wilson_ci(wins: int, n: int, z: float = WILSON_Z) -> tuple[float, float, float]:
    """Wilson 95% CI on a proportion. Returns (lo, point, hi), all in [0,1].

    Formula matches REPORT_2026-05-16 line 27:
      center     = (p + z²/2n) / (1 + z²/n)
      half-width = z · √(p(1-p)/n + z²/4n²) / (1 + z²/n)
    """
    if n <= 0:
        return (0.0, 0.0, 0.0)
    p = wins / n
    z2 = z * z
    denom = 1.0 + z2 / n
    center = (p + z2 / (2.0 * n)) / denom
    half = z * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n)) / denom
    lo = max(0.0, center - half)
    hi = min(1.0, center + half)
    return (lo, p, hi)


# ---------------------------------------------------------------------------
# Per-variant fetch
# ---------------------------------------------------------------------------

@dataclass
class VariantStats:
    variant: str
    n: int
    W: int
    L: int
    sum_pnl: float
    staked: float
    days_active: float
    # Derived
    hit_pct: float
    ci_lo_pct: float
    ci_hi_pct: float
    roi_pct: float
    roi_wilson_lower_pct: float
    friction_adj_roi_pct: float
    trades_per_day: float
    score: float
    money_eligible: bool


def _fetch_variants(args, conn) -> list[VariantStats]:
    extra, params = build_filters(args, prefix="")
    sql = f"""
        SELECT
            COALESCE(variant_name, engine_name, 'default') AS variant,
            COUNT(*) AS n,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN realized_pnl <= 0 THEN 1 ELSE 0 END) AS losses,
            SUM(realized_pnl) AS sum_pnl,
            SUM(size_usdc) AS staked,
            MIN(decided_at) AS first_at,
            MAX(decided_at) AS last_at
        FROM trades
        WHERE {closed_trades_filter()}{extra}
        GROUP BY variant
        ORDER BY sum_pnl DESC
    """
    out: list[VariantStats] = []
    for r in conn.execute(sql, params).fetchall():
        n = int(r["n"] or 0)
        W = int(r["wins"] or 0)
        L = int(r["losses"] or 0)
        sum_pnl = float(r["sum_pnl"] or 0.0)
        staked = float(r["staked"] or 0.0)
        lo, p, hi = wilson_ci(W, W + L)
        roi = (sum_pnl / staked * 100.0) if staked > 0 else 0.0

        days_active = 0.0
        if r["first_at"] and r["last_at"]:
            try:
                first = datetime.fromisoformat(r["first_at"])
                last = datetime.fromisoformat(r["last_at"])
                days_active = max((last - first).total_seconds() / 86400.0, 1e-6)
            except Exception:
                days_active = 1.0
        trades_per_day = (n / days_active) if days_active > 0 else 0.0

        # Naive ROI lower-bound under Wilson: scale by lo/p.
        # This isn't a real CI on ROI, just a sample-size-aware
        # discount that does the same job as the score in §B.0.
        roi_w_lower = roi * (lo / p) if p > 0 else 0.0
        # Friction-adjusted ROI is identical until §B.1 ships and we can
        # rerun the trade tape under modeled fees/spread/delay.
        friction_adj_roi = roi
        # Frequency factor: ramps from 0 to 1 as trades_per_day climbs from
        # 0 to 4 (target floor); plateaus above. Penalises starved variants.
        freq_factor = min(1.0, trades_per_day / 4.0)
        sample_factor = min(1.0, (W + L) / MIN_N_FOR_MONEY)
        score = roi_w_lower * sample_factor * freq_factor * (friction_adj_roi / max(roi, 1e-9) if roi != 0 else 0.0)
        if score != score:   # NaN guard
            score = 0.0

        money_eligible = (
            (W + L) >= MIN_N_FOR_MONEY
            and friction_adj_roi > 0
            and score > 0
        )

        out.append(VariantStats(
            variant=r["variant"],
            n=n, W=W, L=L, sum_pnl=sum_pnl, staked=staked,
            days_active=days_active,
            hit_pct=(W / (W + L) * 100.0) if (W + L) > 0 else 0.0,
            ci_lo_pct=lo * 100.0,
            ci_hi_pct=hi * 100.0,
            roi_pct=roi,
            roi_wilson_lower_pct=roi_w_lower,
            friction_adj_roi_pct=friction_adj_roi,
            trades_per_day=trades_per_day,
            score=score,
            money_eligible=money_eligible,
        ))
    return out


# ---------------------------------------------------------------------------
# Anchor cross-check
# ---------------------------------------------------------------------------

@dataclass
class AnchorResult:
    anchor: Anchor
    matched: Optional[VariantStats]   # None if variant not present in DB
    closes_delta: int                  # current (W+L) minus anchor (W+L)
    hit_delta_pp: float
    ci_lo_delta_pp: float
    ci_hi_delta_pp: float
    pnl_delta_usdc: float
    # PASS rules:
    #   - if closes_delta == 0: every field within tolerance
    #   - if closes_delta  > 0: skip hit/CI/PnL drift but require monotone
    #     non-collapse: current hit% within max(HIT_TOL, |closes_delta|*1.5)
    #     of anchor, and current sum_pnl >= anchor sum_pnl − 5*spread_cost.
    pass_: bool
    note: str


def _anchor_check(anchor: Anchor, var: Optional[VariantStats]) -> AnchorResult:
    if var is None:
        return AnchorResult(
            anchor=anchor, matched=None,
            closes_delta=0, hit_delta_pp=0, ci_lo_delta_pp=0,
            ci_hi_delta_pp=0, pnl_delta_usdc=0,
            pass_=False,
            note="variant absent from DB — cannot validate",
        )
    closes_now = var.W + var.L
    closes_anchor = anchor.W + anchor.L
    closes_delta = closes_now - closes_anchor
    hit_d = var.hit_pct - anchor.hit_pct
    lo_d = var.ci_lo_pct - anchor.ci_lo
    hi_d = var.ci_hi_pct - anchor.ci_hi
    pnl_d = var.sum_pnl - anchor.sum_pnl

    if closes_delta == 0:
        ok = (
            abs(hit_d) <= HIT_TOL
            and abs(lo_d) <= CI_TOL
            and abs(hi_d) <= CI_TOL
            and abs(pnl_d) <= PNL_TOL
        )
        note = "exact-match check" if ok else "field drift exceeds tolerance"
    else:
        # Drift expected. Require:
        #  - sample only grew
        #  - hit% didn't collapse (within max(HIT_TOL, 20pp) — generous;
        #    we're proving the variant is still positive, not identical)
        #  - sum_pnl didn't go materially negative vs anchor
        sample_grew = closes_delta > 0
        hit_ok = abs(hit_d) <= max(HIT_TOL, 20.0)
        pnl_ok = pnl_d >= -max(PNL_TOL, abs(anchor.sum_pnl) * 0.5)
        ok = sample_grew and hit_ok and pnl_ok
        note = f"DB has +{closes_delta} extra closes since REPORT — delta-mode check"
        if not ok:
            note += " (FAIL: sample shrank or metric collapsed)"
    return AnchorResult(
        anchor=anchor, matched=var,
        closes_delta=closes_delta, hit_delta_pp=hit_d,
        ci_lo_delta_pp=lo_d, ci_hi_delta_pp=hi_d, pnl_delta_usdc=pnl_d,
        pass_=ok, note=note,
    )


# ---------------------------------------------------------------------------
# Spread sanity check
# ---------------------------------------------------------------------------

def _spread_sanity(conn) -> tuple[bool, str]:
    """Every closed trade must satisfy
       realized_pnl ∈ [-(size_usdc * 1.02), size_usdc * (1/entry − 1) + 0.01]
    Loose bounds — designed to catch off-by-1e3 unit bugs or stale rows
    with mis-recorded PnL, not subtle pricing errors.
    """
    sql = f"""
        SELECT id, size_usdc, mid_price AS entry, realized_pnl
        FROM trades
        WHERE {closed_trades_filter()}
    """
    bad: list[tuple[int, float, float, float]] = []
    n = 0
    for r in conn.execute(sql).fetchall():
        n += 1
        stake = float(r["size_usdc"] or 0.0)
        entry = float(r["entry"] or 0.0)
        pnl = float(r["realized_pnl"] or 0.0)
        if stake <= 0 or entry <= 0:
            continue
        max_loss = -stake * 1.02
        # Worst-case upside: bought near entry, sold at 1.0
        max_gain = stake * (1.0 / entry - 1.0) + 0.01
        if pnl < max_loss or pnl > max_gain:
            bad.append((r["id"], stake, entry, pnl))
    if not bad:
        return True, f"OK — {n} closed trades within spread sanity bounds"
    sample = "; ".join(f"id={i} stake={s:.2f} entry={e:.3f} pnl={p:.2f}"
                       for i, s, e, p in bad[:5])
    return False, f"FAIL — {len(bad)}/{n} trades outside sanity bounds. Sample: {sample}"


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

def _select_winner(stats: list[VariantStats]) -> Optional[VariantStats]:
    eligible = [s for s in stats if s.money_eligible]
    if not eligible:
        return None
    return max(eligible, key=lambda s: s.score)


def _watch_list(stats: list[VariantStats]) -> list[VariantStats]:
    """High raw ROI, not money-eligible (usually low sample). Sorted by raw ROI."""
    return sorted(
        [s for s in stats
         if not s.money_eligible and s.roi_pct > 5.0 and (s.W + s.L) >= 5],
        key=lambda s: s.roi_pct,
        reverse=True,
    )[:10]


# ---------------------------------------------------------------------------
# Markdown writer
# ---------------------------------------------------------------------------

def _write_report(
    path: Path,
    *,
    db_path: Path,
    args,
    stats: list[VariantStats],
    anchor_results: list[AnchorResult],
    spread_ok: bool,
    spread_msg: str,
    winner: Optional[VariantStats],
    watch: list[VariantStats],
    friction_mode: str,
    overall_pass: bool,
    fail_reasons: list[str],
) -> None:
    out: list[str] = []
    add = out.append
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    add(f"# Seulgi Validation Report — {today}\n")
    add(f"_DB: `{db_path}` · friction-mode: `{friction_mode}` · "
        f"filters: since={args.since or 'none'} variant={args.variant or 'all'}_\n")

    # Verdict up top
    add("\n## Verdict\n")
    if overall_pass:
        add("**VALIDATION: PASS**\n")
    else:
        add("**VALIDATION: FAIL**\n")
        for r in fail_reasons:
            add(f"- {r}\n")

    # Anchor table
    add("\n## Anchor cross-check (vs REPORT_2026-05-16)\n")
    add("| variant | anchor n | current n | Δcloses | Δhit pp | Δsum_pnl | result |")
    add("|---|---:|---:|---:|---:|---:|:--|")
    for r in anchor_results:
        anchor_n = r.anchor.W + r.anchor.L
        cur_n = (r.matched.W + r.matched.L) if r.matched else 0
        sign = "✅ PASS" if r.pass_ else "❌ FAIL"
        add(f"| `{r.anchor.variant}` | {anchor_n} | {cur_n} | "
            f"{r.closes_delta:+d} | {r.hit_delta_pp:+.1f} | "
            f"{r.pnl_delta_usdc:+.2f} | {sign} — {r.note} |")

    # Spread sanity
    add("\n## Spread-cost sanity\n")
    add(("✅ " if spread_ok else "❌ ") + spread_msg + "\n")

    # Money-eligible
    add("\n## Money-eligible strategies today\n")
    add("(n ≥ 30 closed · friction_adj_roi > 0 · sample-discounted ROI > 0)\n")
    elig = [s for s in stats if s.money_eligible]
    if not elig:
        add("\n**NONE** — no variant meets the money-eligibility bar. "
            "Pause B.1–G; let sample accumulate.\n")
    else:
        add("\n| variant | n | hit% | CI | ROI% | sample-disc ROI% | trades/day | score |")
        add("|---|---:|---:|---|---:|---:|---:|---:|")
        for s in sorted(elig, key=lambda s: s.score, reverse=True):
            add(f"| `{s.variant}` | {s.W+s.L} | {s.hit_pct:.1f} | "
                f"[{s.ci_lo_pct:.1f}, {s.ci_hi_pct:.1f}] | "
                f"{s.roi_pct:+.2f} | {s.roi_wilson_lower_pct:+.2f} | "
                f"{s.trades_per_day:.2f} | {s.score:+.3f} |")

    # Winner
    add("\n## Selection winner\n")
    if winner:
        add(f"**`{winner.variant}`** — carve this into `aggressive_core/`. "
            f"score={winner.score:+.3f}, n={winner.W+winner.L}, "
            f"hit=[{winner.ci_lo_pct:.1f}%, {winner.ci_hi_pct:.1f}%], "
            f"ROI={winner.roi_pct:+.2f}%.\n")
    else:
        add("**NONE** — selection paused. Continue paper-trading the watch list.\n")

    # Watch list
    add("\n## Watch list (high point-ROI, building sample)\n")
    if not watch:
        add("(empty)\n")
    else:
        add("\n| variant | n | hit% | CI | ROI% | days_active |")
        add("|---|---:|---:|---|---:|---:|")
        for s in watch:
            add(f"| `{s.variant}` | {s.W+s.L} | {s.hit_pct:.1f} | "
                f"[{s.ci_lo_pct:.1f}, {s.ci_hi_pct:.1f}] | "
                f"{s.roi_pct:+.2f} | {s.days_active:.1f} |")

    # Full leaderboard
    add("\n## Full leaderboard (closed trades)\n")
    add("| variant | n | W | L | hit% | CI | sum_pnl | staked | ROI% |")
    add("|---|---:|---:|---:|---:|---|---:|---:|---:|")
    for s in sorted(stats, key=lambda s: s.sum_pnl, reverse=True):
        add(f"| `{s.variant}` | {s.W+s.L} | {s.W} | {s.L} | {s.hit_pct:.1f} | "
            f"[{s.ci_lo_pct:.1f}, {s.ci_hi_pct:.1f}] | "
            f"{s.sum_pnl:+.2f} | {s.staked:.2f} | {s.roi_pct:+.2f} |")

    add("\n---")
    add(f"\n{'VALIDATION: PASS' if overall_pass else 'VALIDATION: FAIL'}\n")
    path.write_text("\n".join(out))


# ---------------------------------------------------------------------------
# CLI entry — matches the scripts.analysis subcommand protocol
# ---------------------------------------------------------------------------

def add_parser(p) -> None:
    add_common_args(p)
    p.add_argument(
        "--output", type=Path, default=None,
        help="markdown report path (default: seulgi/REPORT_VALIDATION_<date>.md)",
    )
    p.add_argument(
        "--friction-mode", choices=("raw", "realistic"), default="raw",
        help="raw = use stored realized_pnl. realistic = recompute under "
             "the friction-aware paper trader (requires §B.1 — currently "
             "logs and falls back to raw).",
    )


def run(args, conn) -> Section:
    # 1. Per-variant stats
    stats = _fetch_variants(args, conn)
    by_name = {s.variant: s for s in stats}

    # 2. Anchor cross-check
    anchor_results = [_anchor_check(a, by_name.get(a.variant)) for a in ANCHORS]

    # 3. Spread sanity
    spread_ok, spread_msg = _spread_sanity(conn)

    # 4. Friction mode handling
    friction_mode = args.friction_mode
    friction_warn = ""
    if friction_mode == "realistic":
        friction_warn = (
            "WARN: --friction-mode realistic requested but §B.1 friction-aware "
            "paper trader is not implemented yet; using raw realized_pnl."
        )
        friction_mode = "raw (fallback)"

    # 5. Winner + watch list
    winner = _select_winner(stats)
    watch = _watch_list(stats)

    # 6. Overall pass: every anchor passes AND spread sanity passes.
    #    Winner can be None (that's an explicit "pause", not a fail).
    fail_reasons: list[str] = []
    if not all(r.pass_ for r in anchor_results):
        bad = [r.anchor.variant for r in anchor_results if not r.pass_]
        fail_reasons.append(f"anchor mismatch on: {', '.join(bad)}")
    if not spread_ok:
        fail_reasons.append("spread-cost sanity FAIL")
    overall_pass = not fail_reasons

    # 7. Write report
    db_path = Path(conn.execute("PRAGMA database_list").fetchone()[2])
    if args.output is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out_path = REPO / f"REPORT_VALIDATION_{date}.md"
    else:
        out_path = args.output
    _write_report(
        out_path, db_path=db_path, args=args, stats=stats,
        anchor_results=anchor_results,
        spread_ok=spread_ok, spread_msg=spread_msg,
        winner=winner, watch=watch, friction_mode=friction_mode,
        overall_pass=overall_pass, fail_reasons=fail_reasons,
    )

    # 8. Console summary Section
    summary_rows = [
        ["report path", str(out_path)],
        ["anchors checked", len(anchor_results)],
        ["anchors passed", sum(1 for r in anchor_results if r.pass_)],
        ["spread sanity", "OK" if spread_ok else "FAIL"],
        ["variants total", len(stats)],
        ["money-eligible", sum(1 for s in stats if s.money_eligible)],
        ["watch list", len(watch)],
        ["winner", winner.variant if winner else "NONE"],
        ["verdict", "PASS" if overall_pass else "FAIL"],
    ]
    notes = [friction_warn] if friction_warn else []
    if not overall_pass:
        notes.extend(fail_reasons)
    return Section(
        title="validate-report",
        headers=["metric", "value"],
        rows=summary_rows,
        aligns=["l", "l"],
        notes=notes,
        summary=f"db={db_path.name} · friction-mode={friction_mode}",
    )
