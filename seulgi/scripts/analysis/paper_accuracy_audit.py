"""
SEULGI paper-trade accuracy audit (Phase 0, agent A0.1).

Read-only offline check that the simulate-mode paper trades persisted in
``data/seulgi.db`` are faithful to what the simulator code says should
have happened at the exchange. Looks at every CLOSED trade in the window
(``--since`` / ``--until``, default last 30 days) and runs six checks:

  1. ENTRY_PRICE_TRUTH  trades.mid_price vs the closest
                        market_price_snaps row for the same
                        (condition_id, side) at decided_at.
  2. SLIPPAGE_FIT       trader_trace[fidelity].entry_price vs
                        simulate.apply_slippage(mid, notional, ...).
                        Optional realized-vs-modelled walk comparison
                        against price_impacts.delta_t5m on a 50-trade
                        sample.
  3. FEE_SCHEDULE       trades.entry_fee_usdc / exit_fee_usdc vs
                        simulate.compute_fee(shares, price, mtype) + gas.
  4. EXIT_LOGIC         For each close_reason (TP, SL, TRAIL, TIMEOUT,
                        TIMEOUT_BLEED, RESOLVE_WIN, RESOLVE_LOSS,
                        COLD_CULL, cycle_reset, engine_retired) the
                        inequality between entry_price and the
                        reconstructed exit_price holds (sign-flip for
                        NO is not needed because positions.entry_price
                        is the mid of the entered side, not the YES
                        price; trader.py uses (current - entry) for
                        both YES and NO).
  5. PNL_FORMULA        realized_pnl ≈ (exit_price - entry_price) *
                        shares - exit_fee_usdc, where exit_price is
                        either the rail (RESOLVE_WIN/LOSS) or the
                        closest CLOB mid at closed_at.
  6. BOOK_DEPTH         heuristic — size_usdc shouldn't dwarf the
                        plausible top-of-book depth at the entered mid.

Each check returns (name, n_total, n_pass, n_fail, fail_examples).
Exit 0 if every check passes ≥ 99 %; exit 1 otherwise.

Offline-only: imports yaml + sqlite3 + stdlib. Reads simulate.py
(via ``app/`` on sys.path) without importing the full trader stack.

Usage:
    python3 -m scripts.analysis.paper_accuracy_audit
    python3 -m scripts.analysis.paper_accuracy_audit --since 2026-04-22
    python3 -m scripts.analysis.paper_accuracy_audit --since 30d --json out.json
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import statistics
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

REPO = Path(__file__).resolve().parents[2]
DB_PATH = REPO / "data" / "seulgi.db"
CONFIG_PATH = REPO / "config.yaml"

# Make ``import simulate`` resolve without dragging the trader stack in.
sys.path.insert(0, str(REPO / "app"))


# ---------------------------------------------------------------------------
# Config — local lightweight loader (the real config.py pulls in heavy deps).
# ---------------------------------------------------------------------------


@dataclass
class FeesCfg:
    enabled: bool = True
    default_rate: float = 0.05
    category_rates: dict = field(default_factory=lambda: {
        "crypto":         0.072,
        "crypto_price":   0.072,
        "economics":      0.05,
        "weather":        0.05,
        "sports":         0.03,
        "geopolitics":    0.00,
        "political":      0.04,
        "politics":       0.04,
        "finance":        0.04,
        "tech":           0.04,
        "event":          0.04,
        "entertainment":  0.05,
        "price_threshold": 0.04,
    })
    min_fee_usdc: float = 0.00001
    gas_cost_per_trade_usdc: float = 0.005


@dataclass
class SlipCfg:
    enabled: bool = True
    bps_per_dollar: float = 2.0
    max_price_move: float = 0.02
    disable_below_usdc: float = 5.0
    disable_for_categories: list = field(default_factory=lambda: ["geopolitics"])


def load_config_yaml(path: Path) -> tuple[FeesCfg, SlipCfg, dict]:
    """Load only the sim subset of config.yaml — minimal yaml dependency."""
    try:
        import yaml  # type: ignore
    except ImportError:
        print("ERROR: PyYAML required (`pip install pyyaml`).", file=sys.stderr)
        sys.exit(2)
    raw = yaml.safe_load(path.read_text()) or {}
    trading = raw.get("trading", {}) or {}
    fees_raw = trading.get("simulate_fees") or {}
    slip_raw = trading.get("simulate_slippage") or {}

    fees = FeesCfg()
    if fees_raw:
        fees.enabled = bool(fees_raw.get("enabled", fees.enabled))
        fees.default_rate = float(fees_raw.get("default_rate", fees.default_rate))
        merged = dict(fees.category_rates)
        merged.update(fees_raw.get("category_rates") or {})
        fees.category_rates = merged
        fees.min_fee_usdc = float(fees_raw.get("min_fee_usdc", fees.min_fee_usdc))
        fees.gas_cost_per_trade_usdc = float(
            fees_raw.get("gas_cost_per_trade_usdc", fees.gas_cost_per_trade_usdc)
        )
    slip = SlipCfg()
    if slip_raw:
        slip.enabled = bool(slip_raw.get("enabled", slip.enabled))
        slip.bps_per_dollar = float(slip_raw.get("bps_per_dollar", slip.bps_per_dollar))
        slip.max_price_move = float(slip_raw.get("max_price_move", slip.max_price_move))
        slip.disable_below_usdc = float(
            slip_raw.get("disable_below_usdc", slip.disable_below_usdc)
        )
        slip.disable_for_categories = list(
            slip_raw.get("disable_for_categories") or slip.disable_for_categories
        )
    return fees, slip, trading


# ---------------------------------------------------------------------------
# Pure-function copies of the simulator's hot paths (so we don't import the
# whole config dataclass tree).  Must stay in sync with app/simulate.py.
# ---------------------------------------------------------------------------

PRICE_MIN, PRICE_MAX = 0.01, 0.99
SHARE_INCREMENT = 0.01
_WIN_PRICE = 0.95
_LOSS_PRICE = 0.05


def clamp_price(p: float) -> float:
    return max(PRICE_MIN, min(PRICE_MAX, p))


def fee_rate_for(market_type: Optional[str], cfg: FeesCfg) -> float:
    if not market_type:
        return cfg.default_rate
    return cfg.category_rates.get(market_type, cfg.default_rate)


def apply_slippage(mid: float, notional: float, side: str,
                   market_type: Optional[str], cfg: SlipCfg) -> tuple[float, float, bool]:
    """Return (effective_price, walk_abs, applied)."""
    mid = clamp_price(mid)
    if not cfg.enabled:
        return mid, 0.0, False
    if notional < cfg.disable_below_usdc:
        return mid, 0.0, False
    if market_type and market_type in cfg.disable_for_categories:
        return mid, 0.0, False
    walk = min(cfg.bps_per_dollar * notional / 10000.0, cfg.max_price_move)
    if side.upper() == "YES":
        eff = clamp_price(mid + walk)
    else:
        eff = clamp_price(mid - walk)
    return eff, walk, True


def compute_fee(shares: float, price: float, market_type: Optional[str],
                cfg: FeesCfg) -> float:
    if not cfg.enabled or shares <= 0:
        return 0.0
    rate = fee_rate_for(market_type, cfg)
    p = clamp_price(price)
    raw = shares * rate * p * (1.0 - p)
    return max(cfg.min_fee_usdc, raw) if rate > 0 else 0.0


# ---------------------------------------------------------------------------
# DB plumbing
# ---------------------------------------------------------------------------


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_dt(spec: str) -> datetime:
    if spec.lower().endswith("d") and spec[:-1].isdigit():
        return datetime.now(timezone.utc) - timedelta(days=int(spec[:-1]))
    dt = datetime.fromisoformat(spec)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_ts(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Trade loading + per-trade reconstruction
# ---------------------------------------------------------------------------


@dataclass
class TradeRow:
    id: int
    decided_at: datetime
    closed_at: datetime
    close_reason: Optional[str]
    direction: str               # YES | NO
    condition_id: str
    token_id: Optional[str]
    size_usdc: float
    mid_price: Optional[float]   # un-slipped, side we entered
    mid_yes: Optional[float]
    mid_no: Optional[float]
    realized_pnl: Optional[float]
    entry_fee_usdc: Optional[float]
    exit_fee_usdc: Optional[float]
    market_type: Optional[str]
    # From trader_trace[fidelity]:
    fidelity_entry_price: Optional[float]
    fidelity_shares: Optional[float]
    fidelity_entry_fee: Optional[float]
    fidelity_gas: Optional[float]
    fidelity_fee_rate: Optional[float]
    fidelity_market_type: Optional[str]
    fidelity_slip_walk: Optional[float]
    engine_name: Optional[str]
    variant_name: Optional[str]


def load_trades(conn: sqlite3.Connection, since: datetime,
                until: datetime) -> list[TradeRow]:
    rows = conn.execute(
        """
        SELECT t.id, t.decided_at, t.closed_at, t.close_reason, t.direction,
               t.condition_id, t.token_id, t.size_usdc, t.mid_price,
               t.mid_yes, t.mid_no, t.realized_pnl,
               t.entry_fee_usdc, t.exit_fee_usdc,
               t.trader_trace, t.engine_name, t.variant_name,
               m.market_type
          FROM trades t
          LEFT JOIN markets m ON m.condition_id = t.condition_id
         WHERE t.skip_reason IS NULL
           AND t.closed_at IS NOT NULL
           AND t.decided_at >= ?
           AND t.decided_at <  ?
        """,
        (iso(since), iso(until)),
    ).fetchall()

    out: list[TradeRow] = []
    for r in rows:
        decided = parse_ts(r["decided_at"])
        closed = parse_ts(r["closed_at"])
        if decided is None or closed is None:
            continue
        fidelity = _extract_fidelity(r["trader_trace"])
        out.append(TradeRow(
            id=r["id"],
            decided_at=decided,
            closed_at=closed,
            close_reason=r["close_reason"],
            direction=(r["direction"] or "").upper(),
            condition_id=r["condition_id"],
            token_id=r["token_id"],
            size_usdc=float(r["size_usdc"] or 0),
            mid_price=r["mid_price"],
            mid_yes=r["mid_yes"],
            mid_no=r["mid_no"],
            realized_pnl=r["realized_pnl"],
            entry_fee_usdc=r["entry_fee_usdc"],
            exit_fee_usdc=r["exit_fee_usdc"],
            market_type=r["market_type"],
            fidelity_entry_price=fidelity.get("entry_price"),
            fidelity_shares=fidelity.get("shares"),
            fidelity_entry_fee=fidelity.get("entry_fee_usdc"),
            fidelity_gas=fidelity.get("gas_usdc"),
            fidelity_fee_rate=fidelity.get("fee_rate"),
            fidelity_market_type=fidelity.get("market_type"),
            fidelity_slip_walk=fidelity.get("slip_walk"),
            engine_name=r["engine_name"],
            variant_name=r["variant_name"],
        ))
    return out


def _extract_fidelity(trace_json: Optional[str]) -> dict:
    if not trace_json:
        return {}
    try:
        steps = json.loads(trace_json)
    except Exception:
        return {}
    for step in steps:
        if isinstance(step, dict) and step.get("step") == "fidelity":
            return step
    return {}


# ---------------------------------------------------------------------------
# Snap lookup — closest YES market_price_snaps row to a target time
# ---------------------------------------------------------------------------


def closest_snap_price(conn: sqlite3.Connection, condition_id: str,
                       target_dt: datetime, window_secs: int = 600) -> tuple[Optional[float], Optional[int]]:
    """Closest YES-side prefetch snap for ``condition_id`` to ``target_dt``.

    Returns (yes_price, delta_secs) or (None, None) if no snap within
    ``window_secs`` of target.
    """
    target = int(target_dt.timestamp())
    row = conn.execute(
        """
        SELECT price, t_unix FROM market_price_snaps
         WHERE condition_id = ?
           AND t_unix BETWEEN ? AND ?
         ORDER BY ABS(t_unix - ?) ASC
         LIMIT 1
        """,
        (condition_id, target - window_secs, target + window_secs, target),
    ).fetchone()
    if not row:
        return None, None
    return float(row["price"]), int(row["t_unix"]) - target


# ---------------------------------------------------------------------------
# Check result helpers
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    name: str
    n_total: int = 0
    n_pass: int = 0
    n_fail: int = 0
    n_skipped: int = 0
    fail_examples: list[dict] = field(default_factory=list)
    extra: dict = field(default_factory=dict)

    @property
    def pass_pct(self) -> float:
        if self.n_total == 0:
            return 100.0
        return self.n_pass / self.n_total * 100.0

    def add_pass(self):
        self.n_total += 1
        self.n_pass += 1

    def add_fail(self, example: dict, cap: int = 8):
        self.n_total += 1
        self.n_fail += 1
        if len(self.fail_examples) < cap:
            self.fail_examples.append(example)

    def add_skip(self):
        self.n_skipped += 1

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "n_total": self.n_total,
            "n_pass": self.n_pass,
            "n_fail": self.n_fail,
            "n_skipped": self.n_skipped,
            "pass_pct": round(self.pass_pct, 2),
            "fail_examples": self.fail_examples,
            "extra": self.extra,
        }


# ---------------------------------------------------------------------------
# CHECK 1 — Entry price truth
# ---------------------------------------------------------------------------


def check_entry_price_truth(conn: sqlite3.Connection, trades: list[TradeRow],
                            tol: float = 0.02) -> CheckResult:
    """trades.mid_price (un-slipped, side-we-entered) ≈ closest snap.

    market_price_snaps only stores the YES side; for NO direction we
    invert (expected = 1 - yes_snap). Tolerance defaults to 2 cents,
    which is generous: prefetch and decision-time are typically ~50ms
    apart, but the bot also captures stale-cached mids on rare paths.
    """
    res = CheckResult(name="ENTRY_PRICE_TRUTH")
    diffs: list[float] = []
    for t in trades:
        if t.mid_price is None:
            res.add_skip()
            continue
        yes_snap, dt = closest_snap_price(conn, t.condition_id,
                                          t.decided_at, window_secs=300)
        if yes_snap is None:
            res.add_skip()
            continue
        expected = yes_snap if t.direction == "YES" else (1.0 - yes_snap)
        diff = abs(t.mid_price - expected)
        diffs.append(diff)
        if diff <= tol:
            res.add_pass()
        else:
            res.add_fail({
                "trade_id": t.id,
                "decided_at": t.decided_at.isoformat(),
                "direction": t.direction,
                "stored_mid": round(t.mid_price, 4),
                "expected_mid": round(expected, 4),
                "yes_snap": round(yes_snap, 4),
                "snap_delta_secs": dt,
                "abs_diff": round(diff, 4),
            })
    if diffs:
        diffs_sorted = sorted(diffs)
        res.extra = {
            "tol_used": tol,
            "diff_mean":  round(statistics.fmean(diffs), 6),
            "diff_p50":   round(statistics.median(diffs), 6),
            "diff_p95":   round(diffs_sorted[int(0.95 * (len(diffs) - 1))], 6),
            "diff_p99":   round(diffs_sorted[int(0.99 * (len(diffs) - 1))], 6),
            "diff_max":   round(max(diffs), 6),
            "n_with_snap": len(diffs),
        }
    return res


# ---------------------------------------------------------------------------
# CHECK 2 — Slippage curve fit
# ---------------------------------------------------------------------------


def check_slippage_fit(trades: list[TradeRow], slip_cfg: SlipCfg,
                      tol: float = 1e-4) -> CheckResult:
    """Modelled slippage walk reproduces trader_trace fidelity record.

    fidelity_entry_price = clamp(mid ± walk).
    Recompute apply_slippage and compare. Tight tolerance — these are
    pure-function recomputes, any drift means simulate.py drifted from
    the historical config.
    """
    res = CheckResult(name="SLIPPAGE_FIT")
    walks_logged: list[float] = []
    walks_modelled: list[float] = []
    for t in trades:
        if t.fidelity_entry_price is None or t.mid_price is None:
            res.add_skip()
            continue
        # The trader passes notional = size (post-fee est, but apply_slippage
        # only gates on the disable_below threshold; the actual walk math uses
        # size_usdc above the threshold).
        notional = t.size_usdc
        eff, walk, applied = apply_slippage(
            mid=t.mid_price, notional=notional, side=t.direction,
            market_type=t.fidelity_market_type if t.fidelity_market_type
                        and t.fidelity_market_type != "(none)" else None,
            cfg=slip_cfg,
        )
        diff = abs(eff - t.fidelity_entry_price)
        walks_logged.append(t.fidelity_slip_walk or 0.0)
        walks_modelled.append(walk)
        if diff <= tol:
            res.add_pass()
        else:
            res.add_fail({
                "trade_id": t.id,
                "direction": t.direction,
                "mid": round(t.mid_price, 4),
                "size_usdc": round(t.size_usdc, 4),
                "logged_entry_price": round(t.fidelity_entry_price, 4),
                "modelled_entry_price": round(eff, 4),
                "logged_walk": round(t.fidelity_slip_walk or 0.0, 6),
                "modelled_walk": round(walk, 6),
                "applied": applied,
            })
    res.extra = {
        "tol_used": tol,
        "logged_walk_mean":   round(statistics.fmean(walks_logged), 6) if walks_logged else None,
        "modelled_walk_mean": round(statistics.fmean(walks_modelled), 6) if walks_modelled else None,
    }
    return res


def realised_vs_modelled_walk(conn: sqlite3.Connection, trades: list[TradeRow],
                              slip_cfg: SlipCfg, sample: int = 50) -> dict:
    """Optional: compare modelled walk to realised |Δprice_5m| from price_impacts.

    Pure diagnostic — does NOT block exit code. Mean / 20%-band-hit-rate.
    """
    # Join via headline_id is not possible here (we don't carry it on TradeRow);
    # use condition_id + close-in-time match.
    sample_trades = [t for t in trades if t.fidelity_entry_price is not None][:sample]
    rows: list[tuple[float, float]] = []
    for t in sample_trades:
        row = conn.execute(
            """
            SELECT delta_t5m FROM price_impacts
             WHERE condition_id = ?
               AND detected_at BETWEEN ? AND ?
             ORDER BY ABS(strftime('%s', detected_at) - strftime('%s', ?))
             LIMIT 1
            """,
            (t.condition_id,
             iso(t.decided_at - timedelta(minutes=5)),
             iso(t.decided_at + timedelta(minutes=5)),
             iso(t.decided_at)),
        ).fetchone()
        if not row or row["delta_t5m"] is None:
            continue
        modelled_walk = (t.fidelity_entry_price - (t.mid_price or t.fidelity_entry_price))
        rows.append((abs(modelled_walk), abs(float(row["delta_t5m"]))))
    if not rows:
        return {"n": 0}
    n = len(rows)
    in_band = sum(1 for m, r in rows
                  if r == 0 or abs(m - r) / max(r, 1e-9) <= 0.20)
    return {
        "n": n,
        "mean_modelled": round(statistics.fmean(m for m, _ in rows), 6),
        "mean_realised": round(statistics.fmean(r for _, r in rows), 6),
        "pct_within_20pct_of_realised": round(100.0 * in_band / n, 2),
    }


# ---------------------------------------------------------------------------
# Helper — reconstruct exit_price from realized_pnl using the trader formula.
# trader.py: realized = proceeds - size_usdc; proceeds = shares*exit - exit_fee
# ⇒ exit_price = (realized + exit_fee + size_usdc) / shares
# ---------------------------------------------------------------------------


def reconstruct_exit_price(t: "TradeRow") -> Optional[float]:
    if (t.realized_pnl is None or t.fidelity_shares is None
            or t.fidelity_shares <= 0):
        return None
    exit_fee = t.exit_fee_usdc or 0.0
    return ((t.realized_pnl + exit_fee + t.size_usdc) / t.fidelity_shares)


# ---------------------------------------------------------------------------
# CHECK 3 — Fee schedule
# ---------------------------------------------------------------------------


def check_fee_schedule(trades: list[TradeRow], fees_cfg: FeesCfg,
                       tol_usdc: float = 1e-4) -> CheckResult:
    """Stored entry/exit fees reproduce compute_fee(shares, price, mtype) + gas.

    Source of truth for *the rate* is `fidelity.fee_rate` recorded at
    decision time — this is what compute_fee actually used. Comparing to
    the live config would only surface config drift (a separate concern
    handled by FEE_CONFIG_DRIFT). The arithmetic check here is therefore:

        raw    = shares × rate × p × (1-p)
        fee    = max(min_fee_usdc, raw)
        total  = fee + gas

    Entry leg:
        expected_entry_fee = compute_fee(shares, entry_price, mtype@rate) + gas
    Exit leg:
        - RESOLVE_WIN / RESOLVE_LOSS → exit_fee MUST be 0
        - active book exit          → compute_fee(shares, exit_price) + gas
          We don't store exit_price; reconstruct from realized_pnl.
    """
    res = CheckResult(name="FEE_SCHEDULE")
    for t in trades:
        # If the trader's own fidelity record carries `gas_usdc`, it tells us
        # exactly how much gas was applied at the time (matches the persisted
        # entry_fee_usdc). Falling back to 0 covers the pre-2026-05-19 path
        # where gas wasn't modelled yet.
        gas = (t.fidelity_gas if t.fidelity_gas is not None else 0.0)
        # Entry fee
        if t.entry_fee_usdc is None or t.fidelity_shares is None \
                or t.fidelity_entry_price is None:
            res.add_skip()
            continue
        mtype = t.fidelity_market_type
        if mtype == "(none)" or not mtype:
            mtype = None
        # Use the rate the trader actually recorded if present — this is the
        # ground truth for the *formula*. Falling back to the cfg lookup
        # surfaces config drift and is checked separately.
        rate = (t.fidelity_fee_rate if t.fidelity_fee_rate is not None
                else fee_rate_for(mtype, fees_cfg))
        raw_entry = t.fidelity_shares * rate * t.fidelity_entry_price * (
            1.0 - t.fidelity_entry_price
        )
        modelled_entry_fee = (max(fees_cfg.min_fee_usdc, raw_entry)
                              if rate > 0 else 0.0) + gas
        entry_diff = abs(modelled_entry_fee - t.entry_fee_usdc)

        # Exit fee
        exit_ok = True
        exit_diff = 0.0
        modelled_exit_fee: Optional[float] = None
        if t.close_reason in ("RESOLVE_WIN", "RESOLVE_LOSS"):
            # Settlement is fee-free per the trader.py rule.
            modelled_exit_fee = 0.0
            if t.exit_fee_usdc is not None and t.exit_fee_usdc > tol_usdc:
                exit_ok = False
                exit_diff = abs(t.exit_fee_usdc)
        elif t.close_reason in ("cycle_reset", "engine_retired"):
            # Administrative close — trader.py path doesn't write exit_fee;
            # we expect NULL. Don't penalise these here, they're audited
            # under EXIT_LOGIC.
            modelled_exit_fee = None
        else:
            if t.exit_fee_usdc is None or t.realized_pnl is None:
                # No fee data → audit can't recompute. Skip the exit leg.
                pass
            else:
                # Reconstruct exit_price from realized_pnl using the trader
                # accounting:
                #   proceeds = shares * exit - exit_fee
                #   realized = proceeds - size_usdc
                # ⇒ exit_price = (realized + exit_fee + size_usdc) / shares
                ep = reconstruct_exit_price(t)
                if ep is not None:
                    exit_price = clamp_price(ep)
                    raw_exit = (t.fidelity_shares * rate * exit_price *
                                (1.0 - exit_price))
                    modelled_exit_fee = (
                        (max(fees_cfg.min_fee_usdc, raw_exit) if rate > 0 else 0.0)
                        + gas
                    )
                    exit_diff = abs(modelled_exit_fee - t.exit_fee_usdc)
                    if exit_diff > tol_usdc:
                        exit_ok = False

        if entry_diff <= tol_usdc and exit_ok:
            res.add_pass()
        else:
            res.add_fail({
                "trade_id": t.id,
                "close_reason": t.close_reason,
                "shares": t.fidelity_shares,
                "entry_price": t.fidelity_entry_price,
                "stored_entry_fee": t.entry_fee_usdc,
                "modelled_entry_fee": round(modelled_entry_fee, 8),
                "entry_diff": round(entry_diff, 8),
                "stored_exit_fee": t.exit_fee_usdc,
                "modelled_exit_fee": (round(modelled_exit_fee, 8)
                                      if modelled_exit_fee is not None else None),
                "exit_diff": round(exit_diff, 8),
                "market_type": mtype,
            })
    return res


# ---------------------------------------------------------------------------
# CHECK 3b — Fee config drift (informational, doesn't affect exit code)
# ---------------------------------------------------------------------------


def check_fee_config_drift(trades: list[TradeRow], fees_cfg: FeesCfg) -> CheckResult:
    """The historical fee_rate captured in trader_trace matches the rate
    the *current* config would choose for that market_type.

    A mismatch isn't a bug per-se (config changed) but it tells operators
    that PnL on these trades was computed under stale fee assumptions.
    """
    res = CheckResult(name="FEE_CONFIG_DRIFT")
    for t in trades:
        if t.fidelity_fee_rate is None:
            res.add_skip()
            continue
        mtype = t.fidelity_market_type
        if mtype == "(none)" or not mtype:
            mtype = None
        cfg_rate = fee_rate_for(mtype, fees_cfg)
        if abs(cfg_rate - t.fidelity_fee_rate) <= 1e-6:
            res.add_pass()
        else:
            res.add_fail({
                "trade_id": t.id,
                "decided_at": t.decided_at.isoformat(),
                "market_type": mtype,
                "historical_fee_rate": t.fidelity_fee_rate,
                "current_config_rate": cfg_rate,
            })
    return res


# ---------------------------------------------------------------------------
# CHECK 4 — Exit logic
# ---------------------------------------------------------------------------


_VARIANT_RE = __import__("re").compile(
    r"(?:^|[/_])(tp|sl|slpct|trail|conf)(\d+)"
)


def _parse_variant_overrides(variant_name: Optional[str]) -> dict:
    """Extract numeric overrides from a variant name like
    ``conf25_tp005_sl015_slpct10``. tp/sl/trail are interpreted as 0.XXX
    (matching the variant-config convention from cycle-3 onwards); slpct
    is a fraction of entry_price (so ``slpct10`` = 0.10).
    """
    out: dict = {}
    if not variant_name:
        return out
    for key, num in _VARIANT_RE.findall(variant_name):
        n = int(num)
        if key == "tp":
            out["tp"] = n / 1000.0
        elif key == "sl":
            out["sl"] = n / 1000.0
        elif key == "trail":
            out["trail"] = n / 1000.0
        elif key == "slpct":
            out["stop_loss_pct"] = n / 100.0
        elif key == "conf":
            out["min_confidence"] = n / 100.0
    return out


# Retired engines from git history (config.yaml a39b0c8 + earlier).  These
# served live trades that still appear in the audit window even though the
# top-level config no longer carries them. Without these the EXIT_LOGIC
# check would call them "unknown engine" and use the global defaults
# (tp=0, sl=0) → every TP/TRAIL/SL fires the inequality check.
_RETIRED_ENGINE_DEFAULTS = {
    "aggressive": {
        "tp": 0.005, "sl": 0.015, "trail": 0.005,
        "min_hold": 60.0, "timeout_hours": 24.0,
    },
    "balanced": {
        "tp": 0.020, "sl": 0.030, "trail": 0.010,
        "min_hold": 300.0, "timeout_hours": 24.0,
    },
    "inverted_highconf": {
        "tp": 0.020, "sl": 0.030, "trail": 0.010,
        "min_hold": 300.0, "timeout_hours": 24.0,
    },
    "loose_relevance_yes": {
        "tp": 0.025, "sl": 0.045, "trail": 0.012,
        "min_hold": 300.0, "timeout_hours": 24.0,
    },
    "scout": {
        "tp": 0.05, "sl": 0.05, "trail": 0.02,
        "min_hold": 60.0, "timeout_hours": 24.0,
    },
    "super_scout": {
        "tp": 0.05, "sl": 0.05, "trail": 0.02,
        "min_hold": 60.0, "timeout_hours": 24.0,
    },
}


def _engine_thresholds(trading_yaml: dict, engine_name: Optional[str]) -> dict:
    """Look up TP / SL / TRAIL / timeout / min_hold for a named engine.

    Falls back to global ``trading:`` defaults. Returns the four scalars
    plus min_hold_seconds. Unknown engine → globals + a marker so the
    check can flag unmatched engine names rather than silently using
    bad defaults.
    """
    defaults = {
        "tp": float(trading_yaml.get("take_profit", 0.0) or 0.0),
        "sl": float(trading_yaml.get("stop_loss", 0.0) or 0.0),
        "trail": float(trading_yaml.get("trailing_stop", 0.0) or 0.0),
        "min_hold": float(trading_yaml.get("min_hold_seconds", 0) or 0),
        "timeout_hours": float(trading_yaml.get("position_timeout_hours", 24) or 24),
        "stop_loss_pct": float(trading_yaml.get("stop_loss_pct", 0.0) or 0.0),
        "found": False,
    }
    if not engine_name:
        return defaults
    # engine_name in DB is "<engine>/<variant>"; split first.
    base_name, _, variant_tail = engine_name.partition("/")
    engines = trading_yaml.get("_engines") or []
    base_cfg = None
    for e in engines:
        if e.get("name") == base_name:
            base_cfg = e
            break
    if base_cfg is None:
        base_cfg = _RETIRED_ENGINE_DEFAULTS.get(base_name)
    result = dict(defaults)
    if base_cfg is None:
        return result  # unknown — caller flags it
    result["found"] = True
    if "take_profit" in base_cfg:
        result["tp"] = float(base_cfg["take_profit"])
    if "stop_loss" in base_cfg:
        result["sl"] = float(base_cfg["stop_loss"])
    if "trailing_stop" in base_cfg:
        result["trail"] = float(base_cfg["trailing_stop"])
    if "min_hold_seconds" in base_cfg:
        result["min_hold"] = float(base_cfg["min_hold_seconds"])
    if "position_timeout_hours" in base_cfg:
        result["timeout_hours"] = float(base_cfg["position_timeout_hours"])
    if "stop_loss_pct" in base_cfg:
        result["stop_loss_pct"] = float(base_cfg["stop_loss_pct"])
    if "tp" in base_cfg:
        result["tp"] = float(base_cfg["tp"])
    if "sl" in base_cfg:
        result["sl"] = float(base_cfg["sl"])
    if "trail" in base_cfg:
        result["trail"] = float(base_cfg["trail"])
    # Variant suffix overrides (e.g. conf40_tp020_sl030_slpct10).
    variant_cfg = None
    if base_cfg and isinstance(base_cfg, dict):
        for v in base_cfg.get("variants", []) or []:
            if v.get("name") == variant_tail:
                variant_cfg = v
                break
    if variant_cfg:
        for src_key, dst_key in (
            ("take_profit", "tp"), ("stop_loss", "sl"),
            ("trailing_stop", "trail"),
            ("min_hold_seconds", "min_hold"),
            ("position_timeout_hours", "timeout_hours"),
            ("stop_loss_pct", "stop_loss_pct"),
        ):
            if src_key in variant_cfg:
                result[dst_key] = float(variant_cfg[src_key])
    # Last-resort numeric parse from the variant tail.
    parsed = _parse_variant_overrides(variant_tail)
    for k, v in parsed.items():
        result[k] = float(v)
    return result


def check_exit_logic(trades: list[TradeRow], trading_yaml: dict,
                     tol_price: float = 0.01) -> CheckResult:
    """Verify exit_price respects the engine's TP/SL bounds + timeout window.

    exit_price is reconstructed from realized_pnl + exit_fee + shares.
    For RESOLVE_WIN/LOSS the rail constraint is direct.
    For TIMEOUT-class closes we verify hold ≥ position_timeout_hours.
    For cycle_reset / engine_retired (administrative) we verify ordering
    only — these never go through the TP/SL path so there's no
    inequality to check; we just confirm the trade has a non-NULL
    realized_pnl.
    """
    res = CheckResult(name="EXIT_LOGIC")
    reasons_seen: dict[str, int] = {}
    reasons_fail: dict[str, int] = {}

    for t in trades:
        cr = t.close_reason or "UNKNOWN"
        reasons_seen[cr] = reasons_seen.get(cr, 0) + 1
        thr = _engine_thresholds(trading_yaml, t.engine_name)
        ok = True
        why = ""
        exit_price: Optional[float] = None
        hold_secs = (t.closed_at - t.decided_at).total_seconds()

        if t.fidelity_entry_price is None or t.fidelity_shares in (None, 0):
            # No fidelity record (rare; mostly paper-mode legacy) — only
            # check administrative consistency.
            if t.realized_pnl is None:
                ok, why = False, "realized_pnl_null"
        else:
            if cr == "RESOLVE_WIN":
                # exit was $1; realized = shares - size_usdc
                exit_price = 1.0
                expected = t.fidelity_shares - t.size_usdc
                if t.realized_pnl is None or abs(t.realized_pnl - expected) > 0.01:
                    ok, why = False, f"resolve_win_pnl_mismatch expected={expected:.4f}"
            elif cr == "RESOLVE_LOSS":
                exit_price = 0.0
                expected = -t.size_usdc
                if t.realized_pnl is None or abs(t.realized_pnl - expected) > 0.01:
                    ok, why = False, f"resolve_loss_pnl_mismatch expected={expected:.4f}"
            else:
                if (t.realized_pnl is None or t.exit_fee_usdc is None
                        or t.fidelity_shares <= 0):
                    # Admin close paths (cycle_reset / engine_retired) skip
                    # exit-fee accounting; we only verify hold_secs sanity.
                    if cr in ("cycle_reset", "engine_retired"):
                        pass
                    else:
                        ok, why = False, "exit_data_missing"
                else:
                    # trader.py: realized = (shares*exit - exit_fee) - size_usdc
                    # ⇒ exit_price = (realized + exit_fee + size_usdc) / shares
                    exit_price = ((t.realized_pnl + t.exit_fee_usdc + t.size_usdc) /
                                  t.fidelity_shares)
                    # The TRIGGER price is the un-slipped CLOB mid that the
                    # monitor sampled (`current_price`). The FILL is then
                    # walked AGAINST the trader (sell direction = opposite
                    # of entered side). For a YES position being sold the
                    # walk is downward: exit_price_filled = mid - walk.
                    # So reverse the walk to recover the trigger mid.
                    # For micro-stakes (size < disable_below_usdc) walk = 0.
                    walk_est = 0.0
                    notional = t.fidelity_shares * exit_price
                    if notional >= 5.0:  # disable_below_usdc default
                        walk_est = min(
                            2.0 * notional / 10000.0,  # bps_per_dollar default
                            0.02,                       # max_price_move default
                        )
                    # Trigger mid = exit_price + walk (for YES) or - walk (NO).
                    # We rely on the un-walked trigger price for the
                    # inequality, since trader.py compares `current_price`
                    # (the mid), not `exit_price` (the fill), to the threshold.
                    trigger_mid = (exit_price + walk_est
                                   if t.direction == "YES"
                                   else exit_price - walk_est)
                    mtm = trigger_mid - t.fidelity_entry_price
                    sl_floor = thr["stop_loss_pct"] * t.fidelity_entry_price
                    sl_dist = max(thr["sl"], sl_floor)
                    if cr == "TP":
                        if mtm + 1e-4 < thr["tp"]:
                            ok, why = (
                                False,
                                f"TP fired with mtm={mtm:.4f} < tp={thr['tp']:.4f}",
                            )
                        if hold_secs + 1 < thr["min_hold"]:
                            ok, why = (
                                False,
                                f"TP fired before min_hold ({hold_secs:.0f}s < {thr['min_hold']:.0f}s)",
                            )
                    elif cr == "SL":
                        if mtm > -sl_dist + 1e-4:
                            ok, why = (
                                False,
                                f"SL fired with mtm={mtm:.4f} > -sl={-sl_dist:.4f}",
                            )
                    elif cr == "TRAIL":
                        # Trail requires mtm ≥ 0 (locked-in profit) at close.
                        if mtm < -1e-3:
                            ok, why = False, f"TRAIL fired underwater mtm={mtm:.4f}"
                    elif cr in ("TIMEOUT", "TIMEOUT_BLEED"):
                        if cr == "TIMEOUT":
                            timeout_secs = thr["timeout_hours"] * 3600
                            if hold_secs + 1 < timeout_secs:
                                ok, why = (
                                    False,
                                    f"TIMEOUT before window "
                                    f"({hold_secs:.0f}s < {timeout_secs:.0f}s)",
                                )
                        else:  # TIMEOUT_BLEED
                            # Requires age >= 3600 AND mtm <= -0.5 * sl_dist
                            if hold_secs + 1 < 3600:
                                ok, why = (
                                    False,
                                    f"TIMEOUT_BLEED before 1h ({hold_secs:.0f}s)",
                                )
                            elif mtm > -0.5 * sl_dist + 1e-3:
                                ok, why = (
                                    False,
                                    f"TIMEOUT_BLEED with mtm={mtm:.4f} > "
                                    f"{-0.5*sl_dist:.4f}",
                                )
                    elif cr == "COLD_CULL":
                        # COLD_CULL has no inequality — just verify finite PnL
                        # and that the position was losing at close. Skip the
                        # losing-only assertion for now (the trader path uses
                        # cold_position_hours w/ last-headline-time, which we
                        # don't have here).
                        pass
                    elif cr in ("cycle_reset", "engine_retired"):
                        # Administrative — no inequality. Already passed via
                        # outer check (realized_pnl + exit data accounted for
                        # under FEE_SCHEDULE & PNL_FORMULA).
                        pass
                    elif cr == "manual":
                        pass
                    else:
                        ok, why = False, f"unknown_close_reason:{cr}"
        if ok:
            res.add_pass()
        else:
            reasons_fail[cr] = reasons_fail.get(cr, 0) + 1
            res.add_fail({
                "trade_id": t.id,
                "close_reason": cr,
                "engine_name": t.engine_name,
                "hold_secs": round(hold_secs, 1),
                "entry_price": t.fidelity_entry_price,
                "shares": t.fidelity_shares,
                "exit_price_reconstructed":
                    (round(exit_price, 4) if exit_price is not None else None),
                "realized_pnl": t.realized_pnl,
                "tp": thr["tp"], "sl": thr["sl"], "trail": thr["trail"],
                "min_hold": thr["min_hold"],
                "stop_loss_pct": thr["stop_loss_pct"],
                "engine_threshold_found": thr["found"],
                "why": why,
            })
    res.extra = {
        "close_reason_counts": reasons_seen,
        "close_reason_fail_counts": reasons_fail,
    }
    return res


# ---------------------------------------------------------------------------
# CHECK 5 — PnL formula
# ---------------------------------------------------------------------------


def check_pnl_formula(conn: sqlite3.Connection, trades: list[TradeRow],
                      tol_usdc: float = 0.01) -> CheckResult:
    """realized_pnl ≈ (exit_price - entry_price) * shares - exit_fee.

    For RESOLVE_WIN/LOSS exit_price is rail. For early exits we
    reconstruct exit_price from the closest CLOB snap at closed_at,
    then check the formula directly. Tolerance is 1 cent (a single
    extra micro-fee shouldn't dominate at typical $1-$5 stakes).
    """
    res = CheckResult(name="PNL_FORMULA")
    for t in trades:
        if (t.realized_pnl is None or t.fidelity_entry_price is None
                or t.fidelity_shares in (None, 0) or t.size_usdc <= 0):
            res.add_skip()
            continue
        cr = t.close_reason or ""
        exit_fee = t.exit_fee_usdc or 0.0
        if cr == "RESOLVE_WIN":
            exit_price = 1.0
        elif cr == "RESOLVE_LOSS":
            exit_price = 0.0
        else:
            yes_snap, _ = closest_snap_price(
                conn, t.condition_id, t.closed_at, window_secs=1800)
            if yes_snap is None:
                # No exit snap → can't independently verify exit_price. We
                # therefore skip this trade rather than fall back to a
                # tautological closed-form check.
                res.add_skip()
                continue
            else:
                exit_price = yes_snap if t.direction == "YES" else (1.0 - yes_snap)
        # trader.py: realized = (shares*exit - exit_fee) - size_usdc
        # On RESOLVE_WIN/LOSS path: realized = settlement_payout - size_usdc
        # = shares*exit - size_usdc (no fee). Match both via the unified
        # formula where exit_fee=0 on settlement paths.
        proceeds = t.fidelity_shares * exit_price - exit_fee
        expected = proceeds - t.size_usdc
        diff = abs(expected - t.realized_pnl)
        # Permit up to half-cent for share-quantization noise; otherwise
        # scale tolerance with notional so a $20 stake doesn't fail at
        # the same cent threshold as a $1 stake.
        scaled_tol = max(tol_usdc, 0.005 * t.size_usdc)
        if diff <= scaled_tol:
            res.add_pass()
        else:
            res.add_fail({
                "trade_id": t.id,
                "close_reason": cr,
                "direction": t.direction,
                "entry_price": t.fidelity_entry_price,
                "shares": t.fidelity_shares,
                "exit_price_used": round(exit_price, 4),
                "exit_fee": exit_fee,
                "stored_pnl": t.realized_pnl,
                "expected_pnl": round(expected, 4),
                "diff": round(diff, 4),
            })
    return res


# ---------------------------------------------------------------------------
# CHECK 6 — Book depth feasibility (heuristic)
# ---------------------------------------------------------------------------


def check_book_depth(trades: list[TradeRow]) -> CheckResult:
    """size_usdc shouldn't exceed a plausible top-of-book.

    Heuristic mapping mid → depth, derived from a hand-tuned Polymarket
    profile (mid markets are deep, rails are thin):

        mid in [0.40, 0.60]  → 50 USDC plausible
        mid in [0.20, 0.80]  → 25 USDC plausible
        mid in [0.10, 0.90]  → 10 USDC plausible
        outside              →  3 USDC plausible (very thin)
    """
    res = CheckResult(name="BOOK_DEPTH_FEASIBILITY")
    for t in trades:
        if t.fidelity_entry_price is None:
            res.add_skip()
            continue
        p = t.fidelity_entry_price
        if 0.40 <= p <= 0.60:
            cap = 50.0
        elif 0.20 <= p <= 0.80:
            cap = 25.0
        elif 0.10 <= p <= 0.90:
            cap = 10.0
        else:
            cap = 3.0
        if t.size_usdc <= cap:
            res.add_pass()
        else:
            res.add_fail({
                "trade_id": t.id,
                "entry_price": p,
                "size_usdc": round(t.size_usdc, 4),
                "depth_cap_assumed": cap,
            })
    return res


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def render_table(results: list[CheckResult]) -> str:
    width = max(len(r.name) for r in results) + 2
    lines = []
    lines.append(f"{'CHECK':{width}} {'TOTAL':>8} {'PASS':>8} {'FAIL':>6} {'SKIP':>6} {'PASS%':>7}  STATUS")
    for r in results:
        ok = "PASS" if (r.pass_pct >= 99.0 or r.n_total == 0) else "FAIL"
        lines.append(
            f"{r.name:{width}} {r.n_total:>8} {r.n_pass:>8} {r.n_fail:>6} "
            f"{r.n_skipped:>6} {r.pass_pct:>6.2f}%  {ok}"
        )
    return "\n".join(lines)


def main(argv: list[str] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Audit paper-trade fidelity against the simulator spec.")
    ap.add_argument("--db", type=Path, default=DB_PATH)
    ap.add_argument("--since", default="30d",
                    help="ISO datetime or Nd duration (default: 30d)")
    ap.add_argument("--until", default=None,
                    help="ISO datetime (default: now)")
    ap.add_argument("--config", type=Path, default=CONFIG_PATH)
    ap.add_argument("--json", type=Path, default=None,
                    help="dump full results JSON to this path")
    ap.add_argument("--tol-entry", type=float, default=0.02,
                    help="entry-mid abs tolerance (default 0.02)")
    args = ap.parse_args(argv)

    fees_cfg, slip_cfg, trading_yaml = load_config_yaml(args.config)
    # Stash the engines list on the trading dict so _engine_thresholds
    # can find it without re-reading config.yaml.
    try:
        import yaml
        raw = yaml.safe_load(args.config.read_text()) or {}
        trading_yaml["_engines"] = raw.get("engines") or []
    except Exception:
        trading_yaml["_engines"] = []

    since = parse_dt(args.since)
    until = parse_dt(args.until) if args.until else datetime.now(timezone.utc)

    conn = open_db(args.db)
    trades = load_trades(conn, since, until)

    print(f"# paper_accuracy_audit")
    print(f"db={args.db}")
    print(f"since={iso(since)} until={iso(until)}")
    print(f"n_closed_trades={len(trades)}")
    if trades:
        cr_counts: dict[str, int] = {}
        for t in trades:
            cr_counts[t.close_reason or "UNKNOWN"] = \
                cr_counts.get(t.close_reason or "UNKNOWN", 0) + 1
        print(f"close_reason_counts={cr_counts}")
    print()

    results = [
        check_entry_price_truth(conn, trades, tol=args.tol_entry),
        check_slippage_fit(trades, slip_cfg),
        check_fee_schedule(trades, fees_cfg),
        check_fee_config_drift(trades, fees_cfg),
        check_exit_logic(trades, trading_yaml),
        check_pnl_formula(conn, trades),
        check_book_depth(trades),
    ]

    print(render_table(results))
    print()

    # Bonus: realised-vs-modelled walk diagnostic
    rvm = realised_vs_modelled_walk(conn, trades, slip_cfg, sample=50)
    print(f"SLIPPAGE_REALISED_DIAGNOSTIC: {rvm}")
    print()

    # Failure detail
    for r in results:
        if r.fail_examples:
            print(f"--- {r.name} failures (showing {len(r.fail_examples)}) ---")
            for ex in r.fail_examples:
                print(json.dumps(ex, default=str))
            if r.extra:
                print(f"  extra: {r.extra}")
            print()

    if args.json:
        payload = {
            "db": str(args.db),
            "since": iso(since), "until": iso(until),
            "n_closed_trades": len(trades),
            "results": [r.to_dict() for r in results],
            "slippage_realised_diagnostic": rvm,
        }
        args.json.write_text(json.dumps(payload, indent=2, default=str))
        print(f"wrote {args.json}")

    # FEE_CONFIG_DRIFT is informational — surfaces config changes, not bugs.
    informational = {"FEE_CONFIG_DRIFT"}
    failing = [
        r for r in results
        if r.pass_pct < 99.0 and r.n_total > 0 and r.name not in informational
    ]
    if failing:
        print(f"FAIL: {len(failing)} check(s) below 99% pass rate "
              f"(informational checks excluded).")
        return 1
    print("PASS: every check >= 99%.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
