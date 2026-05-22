#!/usr/bin/env python3
"""backtest.py — offline PnL replay for seulgi (Phase 2 A2.3+A2.4).

Walks historical decisions in time order, re-runs `trader._evaluate_pure`
+ `_close_pure` against a (possibly different) config, and joins exit
prices from `market_price_snaps`. Spends zero LLM tokens.

Oracle: `decision_inputs` if populated (Phase 5), else `engine_results`
(direction + confidence + p_hat are the oracle; ~95% replayable per A0.12).
Source DB is read-only (`?mode=ro&immutable=1`).

Output `<out_dir>/runs/<run_id>/`:
    manifest.json         — metadata + coverage + per-engine totals
    trades.{parquet,csv,jsonl} — per-trade decision + exit prices + PnL
    skips.csv             — sampled skip rows (capped at 5000)
    summary.md            — per-engine summary + top winners/losers

CLI: python3 -m seulgi.scripts.replay.backtest \\
        --since 2026-05-19 --until 2026-05-22 \\
        --variant-config seulgi/config.yaml \\
        --engines balanced/conf40_tp025_sl045 --out-dir /tmp/bt
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sqlite3
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Resolve seulgi/app imports whether invoked via `-m seulgi.scripts.replay.backtest`
# (cwd=jennie) or `-m scripts.replay.backtest` (cwd=seulgi).
_HERE = os.path.dirname(os.path.abspath(__file__))
_SEULGI = os.path.normpath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, os.path.join(_SEULGI, "app"))

from ai import ClassifyResult, Direction  # noqa: E402
from config import load_config  # noqa: E402
from markets import MarketMatch  # noqa: E402
from trader import (  # noqa: E402
    EntryOutcome,
    _close_pure,
    _evaluate_pure,
    _EntryCtx,
)

logger = logging.getLogger("backtest")

_HORIZONS_SEC = (300, 900, 1800, 3600, 14400)   # 5m / 15m / 30m / 1h / 4h
_HORIZON_LABELS = ("5m", "15m", "30m", "60m", "240m")


@dataclass
class _Decision:
    """One historical decision pulled from the oracle."""
    engine_name: str
    headline_id: str
    condition_id: str
    question: str
    direction: str
    confidence: float
    p_hat: float
    mid_price: Optional[float]
    similarity: float
    decided_at: str
    decided_unix: int
    token_yes: Optional[str]
    token_no: Optional[str]
    market_type: Optional[str]


@dataclass
class _TradeRow:
    engine_name: str
    headline_id: str
    condition_id: str
    decided_at: str
    direction: str
    confidence: float
    p_alpha: float
    mid_yes: Optional[float]
    mid_no: Optional[float]
    similarity: float
    size_usdc: float
    shares: float
    entry_price: float
    entry_fee: float
    exit_prices: dict = field(default_factory=dict)
    simulated_pnl: dict = field(default_factory=dict)
    close_reason_inferred: Optional[str] = None
    action: str = "TRADE"
    pnl: float = 0.0                # diff_variants.py field


def _open_ro(path: str) -> sqlite3.Connection:
    """Read-only SQLite (`mode=ro&immutable=1` on read-only files)."""
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    uri = (f"file:{path}?mode=ro&immutable=1" if not os.access(path, os.W_OK)
           else f"file:{path}?mode=ro")
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    return con


def _hash_db(path: str) -> str:
    """Cheap fingerprint: size + mtime (full hash too slow on 9GB live DB)."""
    try:
        st = os.stat(path)
        return f"size={st.st_size}:mtime={int(st.st_mtime)}"
    except OSError:
        return "unknown"


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=_SEULGI,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:  # noqa: BLE001
        return "unknown"


def _choose_oracle(con: sqlite3.Connection, since: str, until: str) -> str:
    """Decide between `decision_inputs` (preferred) and `engine_results`."""
    try:
        n_di = con.execute(
            "SELECT COUNT(*) FROM decision_inputs "
            "WHERE decided_at >= ? AND decided_at < ?",
            (since, until),
        ).fetchone()[0]
    except sqlite3.OperationalError:
        n_di = 0
    return "decision_inputs" if n_di > 0 else "engine_results"


def _load_market_index(con: sqlite3.Connection) -> dict:
    """condition_id -> {question, token_yes, token_no, market_type}."""
    rows = con.execute(
        "SELECT condition_id, question, token_yes, token_no, market_type FROM markets"
    ).fetchall()
    return {
        r["condition_id"]: {
            "question": r["question"] or "",
            "token_yes": r["token_yes"],
            "token_no": r["token_no"],
            "market_type": r["market_type"],
        }
        for r in rows
    }


def _load_freshness_for_headlines(con: sqlite3.Connection, hids: set) -> dict:
    if not hids:
        return {}
    # SQLite has a parameter limit; chunk if needed.
    out: dict = {}
    hids_list = list(hids)
    for i in range(0, len(hids_list), 500):
        chunk = hids_list[i:i + 500]
        qs = ",".join("?" * len(chunk))
        rows = con.execute(
            f"SELECT headline_id, freshness_label, nearest_age_secs "
            f"FROM signals WHERE headline_id IN ({qs})",
            chunk,
        ).fetchall()
        for r in rows:
            out[r["headline_id"]] = {
                "label": r["freshness_label"],
                "age": r["nearest_age_secs"],
            }
    return out


def _parse_decided(s: str) -> int:
    """ISO decided_at -> unix seconds. Tolerates trailing Z + ±HH:MM."""
    try:
        # Python 3.11+ handles "Z"; we pre-strip for older just in case.
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:  # noqa: BLE001
        return 0


def _walk_decisions_from_engine_results(
    con: sqlite3.Connection, since: str, until: str,
    engine_filter: Optional[set], max_headlines: Optional[int],
    market_index: dict, only_directional: bool = True,
):
    """Yield _Decision rows ordered by (decided_at, engine_name).

    `only_directional=True` (default) skips rows with `direction='NEUTRAL'`
    — those are pre-AI gate drops (low_similarity / pre-AI rules) for
    which the oracle has no signal. With `False` the full corpus is
    walked (useful for debugging the funnel)."""
    dir_filter = "AND er.direction != 'NEUTRAL'" if only_directional else ""
    # Pull at most `max_headlines` distinct headline_ids first (in time
    # order) — this caps the fast-dev run cost. The ORDER BY ensures
    # determinism.
    if max_headlines is not None and max_headlines > 0:
        hid_rows = con.execute(
            f"""
            SELECT headline_id, MIN(decided_at) AS first_decided
              FROM engine_results er
             WHERE er.decided_at >= ? AND er.decided_at < ?
               AND er.headline_id IS NOT NULL
               {dir_filter}
          GROUP BY headline_id
          ORDER BY first_decided ASC
             LIMIT ?
            """,
            (since, until, max_headlines),
        ).fetchall()
        hids = {r["headline_id"] for r in hid_rows}
        if not hids:
            return
        hid_filter_sql = f"AND er.headline_id IN ({','.join('?' * len(hids))})"
        params: list = [since, until, *hids]
    else:
        hid_filter_sql = ""
        params = [since, until]

    rows = con.execute(
        f"""
        SELECT er.engine_name, er.headline_id, er.condition_id, er.question,
               er.direction, er.confidence, er.p_hat, er.mid_price,
               er.similarity, er.edge, er.size_usdc, er.executed, er.decided_at
          FROM engine_results er
         WHERE er.decided_at >= ? AND er.decided_at < ?
           {dir_filter}
           {hid_filter_sql}
      ORDER BY er.decided_at ASC, er.engine_name ASC, er.id ASC
        """,
        params,
    )

    for r in rows:
        eng = r["engine_name"] or ""
        if engine_filter and eng not in engine_filter:
            continue
        cid = r["condition_id"]
        mkt = market_index.get(cid) or {}
        yield _Decision(
            engine_name=eng,
            headline_id=r["headline_id"] or "",
            condition_id=cid,
            question=r["question"] or mkt.get("question", ""),
            direction=r["direction"] or "NEUTRAL",
            confidence=float(r["confidence"] or 0.0),
            p_hat=float(r["p_hat"] or 0.0),
            mid_price=float(r["mid_price"]) if r["mid_price"] is not None else None,
            similarity=float(r["similarity"] or 0.0),
            decided_at=r["decided_at"],
            decided_unix=_parse_decided(r["decided_at"]),
            token_yes=mkt.get("token_yes"),
            token_no=mkt.get("token_no"),
            market_type=mkt.get("market_type"),
        )


class _PriceTape:
    """In-memory price tape per (condition_id, side). Lazy-loaded; binary
    search finds the nearest snap within ε seconds. Falls back to the
    complement side (1 - mid) when only one side has snaps."""

    def __init__(self, con: sqlite3.Connection, epsilon_s: int = 600):
        self._con = con
        self._cache: dict = {}
        self._epsilon = epsilon_s

    def _ensure(self, condition_id: str, side: str) -> list:
        key = (condition_id, side)
        v = self._cache.get(key)
        if v is not None:
            return v
        rows = self._con.execute(
            "SELECT t_unix, price FROM market_price_snaps "
            "WHERE condition_id = ? AND side = ? ORDER BY t_unix ASC",
            (condition_id, side),
        ).fetchall()
        v = [(int(r["t_unix"]), float(r["price"])) for r in rows]
        self._cache[key] = v
        return v

    def nearest(self, condition_id: str, side: str,
                target_unix: int) -> Optional[float]:
        side = side.lower()
        tape = self._ensure(condition_id, side)
        if not tape:
            other = "no" if side == "yes" else "yes"
            tape_o = self._ensure(condition_id, other)
            if not tape_o:
                return None
            v = self._binary_nearest(tape_o, target_unix)
            return None if v is None else max(0.01, min(0.99, 1.0 - v))
        return self._binary_nearest(tape, target_unix)

    def _binary_nearest(self, tape: list, target_unix: int) -> Optional[float]:
        lo, hi = 0, len(tape)
        while lo < hi:
            mid = (lo + hi) // 2
            if tape[mid][0] < target_unix:
                lo = mid + 1
            else:
                hi = mid
        best_t, best_p = None, None
        for i in (lo - 1, lo):
            if 0 <= i < len(tape):
                t, p = tape[i]
                if best_t is None or abs(t - target_unix) < abs(best_t - target_unix):
                    best_t, best_p = t, p
        if best_t is None or abs(best_t - target_unix) > self._epsilon:
            return None
        return best_p


def _build_engine_cfg(base_cfg, engine_name: str):
    """Reproduce engine.py:_merge_trading_cfg + _merge_ai_cfg for one
    `engine/variant` name. Falls back to base when the name is not in
    `cfg.engines`."""
    from dataclasses import replace
    parent_name, _, variant_name = engine_name.partition("/")
    base_trading = base_cfg.trading
    base_ai = base_cfg.ai
    parent = next((e for e in (base_cfg.engines or [])
                   if e.name == parent_name), None)
    if parent is None:
        return base_trading, base_ai
    overridable = (
        "balance_usdc", "max_position_usdc", "min_trade_usdc", "min_edge",
        "edge_increment", "position_timeout_hours", "max_open_positions",
        "take_profit", "stop_loss", "trailing_stop", "min_hold_seconds",
        "rail_skip_threshold", "entry_rail_skip_threshold",
        "invert_high_conf", "invert_conf_threshold", "spread_cost_bps",
        "stop_loss_pct", "min_entry_yes_mid", "max_market_vol_30m",
        "direction_filter", "freshness_filter", "mid_yes_min", "mid_yes_max",
    )
    trading = base_trading
    ai = base_ai
    for fld in overridable:
        v = getattr(parent, fld, None)
        if v is not None:
            trading = replace(trading, **{fld: v})
    if parent.min_confidence is not None:
        ai = replace(ai, min_confidence=parent.min_confidence)
    if variant_name:
        variant = next((v for v in (parent.variants or [])
                        if v.name == variant_name), None)
        if variant is not None:
            for fld in overridable:
                v = getattr(variant, fld, None)
                if v is not None:
                    trading = replace(trading, **{fld: v})
            if variant.min_confidence is not None:
                ai = replace(ai, min_confidence=variant.min_confidence)
    return trading, ai


def _to_classify_result(d: _Decision) -> ClassifyResult:
    """Synthesise ClassifyResult from engine_results oracle. `confidence` is
    treated as `p_alpha` (ai.py docstring: alias since cycle-7)."""
    try:
        direction = Direction(d.direction)
    except ValueError:
        direction = Direction.NEUTRAL
    return ClassifyResult(
        direction=direction, confidence=d.confidence, reason="replay",
        tokens_used=0, p_alpha=d.confidence, agreement=1.0,
        votes={direction.value: 1},
    )


def _to_market_match(d: _Decision) -> MarketMatch:
    """engine_results only stored the entered-side mid; derive the complement
    via `1 - mid` (Polymarket binary)."""
    if d.mid_price is None:
        mid_yes = mid_no = None
    elif d.direction == "NO":
        mid_no = d.mid_price
        mid_yes = max(0.01, min(0.99, 1.0 - d.mid_price))
    else:
        mid_yes = d.mid_price
        mid_no = max(0.01, min(0.99, 1.0 - d.mid_price))
    return MarketMatch(
        condition_id=d.condition_id, question=d.question,
        token_yes=d.token_yes, token_no=d.token_no,
        similarity=d.similarity, mid_yes=mid_yes, mid_no=mid_no,
        market_type=d.market_type,
    )


def _walk_horizons(
    pos: dict, cfg, decided_unix: int, tape: _PriceTape, condition_id: str,
    side: str,
) -> tuple[dict, dict, Optional[str]]:
    """For one open position, look up nearest snap at each horizon and run
    `_close_pure`. Returns (exit_prices, pnl_at, first_close_reason)."""
    exit_prices: dict = {}
    pnl_at: dict = {}
    first_close: Optional[str] = None
    for label, dt in zip(_HORIZON_LABELS, _HORIZONS_SEC):
        target_unix = decided_unix + dt
        price = tape.nearest(condition_id, side, target_unix)
        exit_prices[label] = price
        if price is None:
            pnl_at[label] = None
            continue
        now_dt = datetime.fromtimestamp(target_unix, tz=timezone.utc)
        outcome = _close_pure(pos, current_price=price, cfg=cfg, now=now_dt)
        if outcome.action == "CLOSE":
            pnl_at[label] = outcome.realized_pnl
            if first_close is None:
                first_close = outcome.close_reason
        else:
            shares = pos.get("shares") or (pos["size_usdc"] / pos["entry_price"])
            mtm_gross = (price - pos["entry_price"]) * shares
            pnl_at[label] = mtm_gross - max(0.0, pos["size_usdc"]
                                            - shares * pos["entry_price"])
    return exit_prices, pnl_at, first_close


def _new_state(cfg, eng: str) -> tuple[dict, dict]:
    trading, ai = _build_engine_cfg(cfg, eng)
    st = {"trading": trading, "ai": ai, "open": [], "unique": set(),
          "bal": float(trading.balance_usdc)}
    rec = {"trade_n": 0, "skip_n": 0, "skip_reasons": {},
           "n_close_by_reason": {},
           "pnl_sum_at": {l: 0.0 for l in _HORIZON_LABELS},
           "pnl_count_at": {l: 0 for l in _HORIZON_LABELS}}
    return st, rec


def _record_skip(rec: dict, skips: list, d: _Decision, reason: str,
                 short: Optional[str] = None) -> None:
    rec["skip_n"] += 1
    short = short or reason.split(":")[0]
    rec["skip_reasons"][short] = rec["skip_reasons"].get(short, 0) + 1
    if len(skips) < 5000:
        skips.append({"headline_id": d.headline_id, "engine": d.engine_name,
                      "condition_id": d.condition_id, "action": short,
                      "skip_reason": reason, "size": 0.0, "pnl": 0.0,
                      "decided_at": d.decided_at})


def _simulate(cfg, decisions, market_index: dict, tape: _PriceTape,
              freshness: dict, log_every: int = 5000,
              ) -> tuple[list, list, dict]:
    """Walk decisions in time order; re-evaluate per engine; emit a
    _TradeRow on TRADE. Per-engine open-position / duplicate state is
    tracked across the run."""
    per_engine_state: dict = {}
    per_engine_results: dict = {}
    trades: list = []
    skips: list = []
    n_walked = 0
    n_no_price_entry = 0

    for d in decisions:
        n_walked += 1
        if n_walked % log_every == 0:
            logger.info("[backtest] walked=%d trades=%d", n_walked, len(trades))
        st = per_engine_state.get(d.engine_name)
        if st is None:
            st, rec_new = _new_state(cfg, d.engine_name)
            per_engine_state[d.engine_name] = st
            per_engine_results[d.engine_name] = rec_new
        trading, ai_cfg = st["trading"], st["ai"]
        # GC expired open positions (so duplicate_position doesn't lock).
        st["open"] = [p for p in st["open"]
                      if (d.decided_unix - p["_opened_unix"])
                      < trading.position_timeout_hours * 3600]
        st["unique"] = {p["condition_id"] for p in st["open"]}

        match = _to_market_match(d)
        result = _to_classify_result(d)
        ctx = _EntryCtx(
            cfg=trading, ai_cfg=ai_cfg,
            engine_name=d.engine_name,
            parent_engine=d.engine_name.split("/")[0],
            open_positions=len(st["open"]),
            unique_markets=len(st["unique"]),
            duplicate_position=d.condition_id in st["unique"],
            balance=st["bal"], cluster_factor=1.0,
            rules_post_ai_reason=None,
            freshness_label=(freshness.get(d.headline_id) or {}).get("label"),
        )
        outcome: EntryOutcome = _evaluate_pure(match, result, ctx)
        rec = per_engine_results[d.engine_name]
        if outcome.action == "SKIP":
            _record_skip(rec, skips, d, outcome.skip_reason or "unknown")
            continue

        # TRADE.
        size = float(outcome.size_usdc or 0.0)
        entry_price = float(outcome.entry_price
                            or match.mid_yes or match.mid_no or 0.5)
        shares = float(outcome.shares
                       or (size / entry_price if entry_price > 0 else 0.0))
        entry_fee = float(outcome.entry_fee_usdc or 0.0)
        side = "yes" if outcome.direction == "YES" else "no"
        first_target = d.decided_unix + _HORIZONS_SEC[0]
        if tape.nearest(d.condition_id, side, first_target) is None:
            n_no_price_entry += 1
            _record_skip(rec, skips, d, "no_price_for_exit",
                         short="no_price_for_exit")
            continue

        pos = {
            "condition_id": d.condition_id, "direction": outcome.direction,
            "entry_price": entry_price, "size_usdc": size, "shares": shares,
            "high_water_price": None, "market_type": d.market_type,
            "opened_at": d.decided_at, "_opened_unix": d.decided_unix,
        }
        exits, pnl_at, first_close = _walk_horizons(
            pos, trading, d.decided_unix, tape, d.condition_id, side,
        )

        # Final PnL = first close PnL else longest-horizon MTM.
        final_pnl = 0.0
        order = _HORIZON_LABELS if first_close else reversed(_HORIZON_LABELS)
        for lbl in order:
            p = pnl_at.get(lbl)
            if p is not None:
                final_pnl = p
                break

        rec["trade_n"] += 1
        if first_close:
            rec["n_close_by_reason"][first_close] = (
                rec["n_close_by_reason"].get(first_close, 0) + 1
            )
        for lbl in _HORIZON_LABELS:
            p = pnl_at.get(lbl)
            if p is not None:
                rec["pnl_sum_at"][lbl] += p
                rec["pnl_count_at"][lbl] += 1
        st["open"].append(pos)
        st["unique"].add(d.condition_id)
        st["bal"] = max(0.0, st["bal"] - size)
        trades.append(_TradeRow(
            engine_name=d.engine_name, headline_id=d.headline_id,
            condition_id=d.condition_id, decided_at=d.decided_at,
            direction=outcome.direction or "", confidence=d.confidence,
            p_alpha=result.p_alpha, mid_yes=match.mid_yes, mid_no=match.mid_no,
            similarity=d.similarity, size_usdc=size, shares=shares,
            entry_price=entry_price, entry_fee=entry_fee,
            exit_prices=exits, simulated_pnl=pnl_at,
            close_reason_inferred=first_close, action="TRADE", pnl=final_pnl,
        ))

    stats = {"n_walked": n_walked, "n_trades": len(trades),
             "n_skips": len(skips), "n_no_price_entry": n_no_price_entry,
             "per_engine": per_engine_results}
    return trades, skips, stats


def _trade_dict(t: _TradeRow) -> dict:
    """Flat dict for parquet/csv/jsonl serialisation."""
    d = {
        "engine_name": t.engine_name, "engine": t.engine_name,
        "headline_id": t.headline_id, "condition_id": t.condition_id,
        "decided_at": t.decided_at, "direction": t.direction,
        "confidence": t.confidence, "p_alpha": t.p_alpha,
        "mid_yes": t.mid_yes, "mid_no": t.mid_no,
        "similarity": t.similarity,
        "size_usdc": t.size_usdc, "shares": t.shares,
        "entry_price": t.entry_price, "entry_fee": t.entry_fee,
        "close_reason_inferred": t.close_reason_inferred,
        "action": t.action, "size": t.size_usdc, "pnl": t.pnl,
    }
    for lbl in _HORIZON_LABELS:
        d[f"exit_price_at_{lbl}"] = t.exit_prices.get(lbl)
        d[f"simulated_pnl_{lbl}"] = t.simulated_pnl.get(lbl)
    return d


def _write_trades(trades: list, out_dir: Path) -> str:
    """Try parquet; fall back to csv. Always also dump jsonl for the
    sibling diff_variants.py consumer."""
    rows = [_trade_dict(t) for t in trades]
    # JSONL (always emitted — diff_variants.py reads jsonl preferentially).
    jsonl_path = out_dir / "trades.jsonl"
    with jsonl_path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, default=str))
            f.write("\n")
    # CSV (sorted columns so diffs stay stable).
    csv_path = out_dir / "trades.csv"
    if rows:
        cols = list(rows[0].keys())
    else:
        cols = ["engine_name", "headline_id", "condition_id", "decided_at",
                "action", "size", "pnl"]
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    # Parquet (best-effort).
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
        if rows:
            table = pa.Table.from_pylist(rows)
            pq.write_table(table, str(out_dir / "trades.parquet"))
            return "parquet+csv+jsonl"
    except Exception:  # noqa: BLE001
        pass
    return "csv+jsonl"


def _write_skips(skips: list, out_dir: Path) -> None:
    if not skips:
        return
    cols = list(skips[0].keys())
    with (out_dir / "skips.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in skips:
            w.writerow(r)


def _write_summary(stats: dict, manifest: dict, trades: list, out_dir: Path) -> None:
    """Per-engine summary + top winners/losers + quantile distribution."""
    L: list[str] = ["# Backtest summary", "",
                    f"- run_id: {manifest['run_id']}",
                    f"- window: {manifest['window']['since']} → {manifest['window']['until']}",
                    f"- oracle: {manifest['oracle_strategy']} ({manifest['oracle_coverage_pct']:.1f}%)",
                    f"- decisions walked: {stats['n_walked']}",
                    f"- trades simulated: {stats['n_trades']}",
                    f"- skips (sampled): {stats['n_skips']}",
                    f"- skipped trades (no exit price): {stats['n_no_price_entry']}",
                    f"- run_seconds: {manifest['run_seconds']:.2f}", ""]
    L.append("## Per-engine PnL (longest horizon with coverage)")
    L.append("")
    L.append("| engine | trades | skips | horizon | sum_pnl | avg_pnl | top_skip |")
    L.append("|--------|-------:|------:|--------:|--------:|--------:|---------:|")
    for eng, rec in sorted(stats["per_engine"].items()):
        best_lbl = next((lbl for lbl in reversed(_HORIZON_LABELS)
                         if rec["pnl_count_at"].get(lbl, 0) > 0), "5m")
        sum_p = rec["pnl_sum_at"].get(best_lbl, 0.0)
        cnt_p = rec["pnl_count_at"].get(best_lbl, 0)
        avg_p = sum_p / cnt_p if cnt_p else 0.0
        top_skip = (max(rec["skip_reasons"].items(), key=lambda x: x[1])[0]
                    if rec["skip_reasons"] else "")
        L.append(f"| {eng} | {rec['trade_n']} | {rec['skip_n']} | {best_lbl} "
                 f"| {sum_p:+.4f} | {avg_p:+.4f} | {top_skip} |")
    L.append("")
    sorted_trades = sorted(trades, key=lambda t: t.pnl)
    if sorted_trades:
        def _fmt(t):
            return (f"- {t.engine_name} {t.condition_id[:10]} pnl={t.pnl:+.4f} "
                    f"dir={t.direction} close={t.close_reason_inferred or 'HOLD'}")
        L += ["## Top 10 losers (by final PnL)", ""]
        L += [_fmt(t) for t in sorted_trades[:10]]
        L += ["", "## Top 10 winners", ""]
        L += [_fmt(t) for t in sorted_trades[-10:][::-1]]
        L.append("")
        pnls = [t.pnl for t in trades]
        if len(pnls) >= 5:
            qs = statistics.quantiles(pnls, n=10)
            L += ["## PnL deciles", ""]
            L += [f"- D{i}: {q:+.4f}" for i, q in enumerate(qs, 1)]
            L += [f"- mean: {statistics.mean(pnls):+.4f}",
                  f"- stdev: {statistics.pstdev(pnls):+.4f}", ""]
    (out_dir / "summary.md").write_text("\n".join(L))


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--since", required=True,
                   help="ISO date — decisions with decided_at >= this.")
    p.add_argument("--until", required=True,
                   help="ISO date — decisions with decided_at < this.")
    p.add_argument("--variant-config", default=None,
                   help="Path to a variant config.yaml. Defaults to the prod one.")
    p.add_argument("--engines", default="",
                   help="Comma-separated allowlist of engine names. Empty = all.")
    p.add_argument("--out-dir", required=True,
                   help="Output directory (`runs/<run_id>/` is created under here).")
    p.add_argument("--source-db", default=None,
                   help="Path to source DB. Default: seulgi/data/seulgi.db.")
    p.add_argument("--max-headlines", type=int, default=None,
                   help="Cap on distinct headline_ids walked (dev runs).")
    p.add_argument("--epsilon-secs", type=int, default=600,
                   help="Tolerance window for nearest exit price.")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--calibration", action="store_true",
                   help="Enable calibration overlay (TODO: Phase 4B.1).")
    p.add_argument("--include-neutral", action="store_true",
                   help="Also walk rows with direction='NEUTRAL' (pre-AI "
                   "skips). Off by default — those have no LLM oracle.")
    return p.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.calibration:
        raise SystemExit(
            "--calibration is not yet implemented (Phase 4B.1 not shipped); "
            "remove the flag to run the uncalibrated backtest."
        )

    source_db = args.source_db or os.path.join(_SEULGI, "data", "seulgi.db")
    variant_cfg_path = args.variant_config or os.path.join(_SEULGI, "config.yaml")
    engine_filter = (set(s.strip() for s in args.engines.split(",") if s.strip())
                     if args.engines else None)

    run_id = (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
              + "_" + uuid.uuid4().hex[:6])
    out_root = Path(args.out_dir) / "runs" / run_id
    out_root.mkdir(parents=True, exist_ok=True)

    cfg = load_config(variant_cfg_path)
    con = _open_ro(source_db)

    started = time.time()
    oracle = _choose_oracle(con, args.since, args.until)
    logger.info("[backtest] oracle=%s window=%s..%s", oracle, args.since, args.until)
    market_index = _load_market_index(con)
    logger.info("[backtest] loaded %d markets", len(market_index))

    if oracle == "decision_inputs":
        logger.warning(
            "[backtest] decision_inputs oracle not yet implemented "
            "(A2.1 wiring pending); falling back to engine_results."
        )
        oracle = "engine_results"

    decisions = list(_walk_decisions_from_engine_results(
        con, args.since, args.until, engine_filter, args.max_headlines,
        market_index, only_directional=not args.include_neutral,
    ))
    n_walked = len(decisions)
    if n_walked == 0:
        raise SystemExit(
            f"oracle '{oracle}' has zero rows in [{args.since}, {args.until}) — "
            f"hard-failing rather than silently exiting without trades."
        )
    # Per-engine oracle coverage = decisions where direction is non-NEUTRAL.
    n_directional = sum(1 for d in decisions if d.direction != "NEUTRAL")
    coverage = (n_directional / n_walked * 100.0) if n_walked else 0.0
    hids = {d.headline_id for d in decisions if d.headline_id}
    freshness = _load_freshness_for_headlines(con, hids)
    logger.info("[backtest] walking %d decisions across %d headlines",
                n_walked, len(hids))

    tape = _PriceTape(con, epsilon_s=args.epsilon_secs)
    trades, skips, stats = _simulate(cfg, decisions, market_index, tape, freshness)

    n_skipped_by_reason: dict = {}
    for rec in stats["per_engine"].values():
        for r, n in rec["skip_reasons"].items():
            n_skipped_by_reason[r] = n_skipped_by_reason.get(r, 0) + n
    # Per-engine PnL: use the longest horizon with non-zero coverage, else 5m.
    pnl_per_engine: dict = {}
    for eng, rec in stats["per_engine"].items():
        best_lbl = "5m"
        for lbl in _HORIZON_LABELS:
            if rec["pnl_count_at"].get(lbl, 0) > 0:
                best_lbl = lbl
        pnl_per_engine[eng] = {
            "horizon": best_lbl,
            "sum_pnl": round(rec["pnl_sum_at"].get(best_lbl, 0.0), 6),
            "n_trades_at_horizon": rec["pnl_count_at"].get(best_lbl, 0),
            "n_trades_total": rec["trade_n"],
        }

    run_seconds = time.time() - started
    manifest = {
        "run_id": run_id,
        "git_sha": _git_sha(),
        "started_at": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        "source_db_path": source_db,
        "source_db_hash": _hash_db(source_db),
        "window": {"since": args.since, "until": args.until},
        "variant_config_path": variant_cfg_path,
        "oracle_strategy": oracle,
        "oracle_coverage_pct": coverage,
        "n_headlines_walked": len(hids),
        "n_decisions_walked": n_walked,
        "n_trades_simulated": stats["n_trades"],
        "n_skipped": {
            "by_reason": dict(sorted(n_skipped_by_reason.items(),
                                     key=lambda x: -x[1])[:30]),
            "total": sum(n_skipped_by_reason.values()),
            "no_price_entry": stats["n_no_price_entry"],
        },
        "total_pnl_per_engine": pnl_per_engine,
        "engines_filter": sorted(engine_filter) if engine_filter else None,
        "max_headlines": args.max_headlines,
        "epsilon_secs": args.epsilon_secs,
        "run_seconds": run_seconds,
        "calibration": False,
    }

    fmt = _write_trades(trades, out_root)
    manifest["trade_format"] = fmt
    (out_root / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
    _write_skips(skips, out_root)
    _write_summary(stats, manifest, trades, out_root)
    logger.info("[backtest] done: trades=%d skips=%d secs=%.2f out=%s",
                stats["n_trades"], stats["n_skips"], run_seconds, out_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
