"""Run a strategy against a set, simulate PnL, write run artefacts.

    # Baseline — no LLM call, derive direction from mid<0.5
    python -m irene.scripts.sandbox.run_strategy \
        --set all_complete --strategy v1-baseline --use-orig

    # Real LLM, cap at 10 calls during smoke
    python -m irene.scripts.sandbox.run_strategy \
        --set smoke10 --strategy v2-direction-with-skip --max-calls 10

    # Kelly-sized LLM backtest
    python -m irene.scripts.sandbox.run_strategy \
        --set explorer0_all --strategy v3-kelly-pyes --max-calls 300

    # Re-run cached only (no network)
    python -m irene.scripts.sandbox.run_strategy \
        --set smoke10 --strategy v3-kelly-pyes --dry-run
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import shlex
import sys
import uuid
from dataclasses import asdict

from irene.capture_schedule import CAPTURE_POST_SECS
from irene.sandbox import chain, features, llm, loader, pnl, sets, strategies
from irene.sandbox.paths import RUNS_DIR, ensure_data_dirs
from irene.sandbox.report import (
    render_pnl_summary, render_row_block, render_summary, summary_counts,
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", required=True, dest="set_name")
    ap.add_argument("--strategy", required=True, dest="strategy_name")
    ap.add_argument("--use-orig", action="store_true",
                    help="Skip LLM. Use orig llm_related and derive direction from mid_t0.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Only use cached LLM responses; abort on cache miss.")
    ap.add_argument("--max-calls", type=int, default=50)
    args = ap.parse_args()

    ensure_data_dirs()
    s = sets.load(args.set_name)
    strat = strategies.load(args.strategy_name)
    con = loader.connect_readonly()
    rows = loader.load_rows(con, s.exploration_ids)

    if args.use_orig:
        unjudged = sum(
            1 for r in rows
            if r.orig_llm_confidence is None and (r.orig_llm_reason or "") == "gate_skipped"
        )
        if unjudged:
            sys.stderr.write(
                f"--use-orig requires LLM-judged rows; this set has {unjudged} "
                f"unjudged (capture-mode) rows out of {len(rows)}. "
                f"Use a real strategy (e.g. v3-kelly-pyes) to evaluate them, "
                f"or filter the set with `e.llm_prompt_hash IS NOT NULL`.\n"
            )
            sys.exit(1)
    set_id_sha = hashlib.sha1(
        ",".join(str(i) for i in sorted(s.exploration_ids)).encode()
    ).hexdigest()

    run_id = _new_run_id(args.strategy_name, args.set_name)
    run_dir = os.path.join(RUNS_DIR, run_id)
    os.makedirs(run_dir, exist_ok=True)

    bt = strat.backtest
    sizing_mode = str(bt.get("sizing", "flat")).lower()
    flat_notional = float(bt.get("notional_usdc", 100.0))
    fee_pct = float(bt.get("fee_pct", 0.02))
    # Default to the canonical last checkpoint from the shared capture
    # schedule (irene/capture_schedule.py) so a strategy that omits an
    # explicit horizon exits at the same point the explorer captures to.
    # Strategies may still set a different exit_horizon_seconds for
    # research (e.g. the t-series fast horizons).
    exit_horizon = int(bt.get("exit_horizon_seconds", CAPTURE_POST_SECS))
    use_fallback = bt.get("fallback_entry") == "market_mid_t0"
    skip_no_book = bool(bt.get("skip_when_no_book", True))
    bankroll = float(bt.get("bankroll_usdc", 1000.0))
    kelly_fraction = float(bt.get("kelly_fraction", 0.25))
    max_position_pct = float(bt.get("max_position_pct", 0.10))
    min_size_usdc = float(bt.get("min_size_usdc", 1.0))
    min_edge = float(bt.get("min_edge", 0.02))
    exit_config = bt.get("exit") or None

    decisions_path = os.path.join(run_dir, "decisions.jsonl")
    trades_path = os.path.join(run_dir, "trades.csv")
    report_path = os.path.join(run_dir, "report.md")
    manifest_path = os.path.join(run_dir, "manifest.json")

    calls_made = 0
    cache_hits = 0
    decisions_out = []
    trades_out = []

    for i, row in enumerate(rows, 1):
        if strat.pretrade:
            gate = _pretrade_skip(con, row, strat.pretrade)
            if gate:
                trades_out.append(_skip_row(row, gate, 0.0))
                continue

        if args.use_orig:
            decision = _orig_decision(row)
            decision_source = "orig"
        elif strat.kind == "deterministic_momentum":
            decision = _momentum_decision(con, row, strat.kind_config)
            decision_source = "momentum"
        elif strat.steps:
            # Multi-step chain.
            cr = chain.run_chain(
                strat, row,
                max_calls_remaining=max(0, args.max_calls - calls_made),
                dry_run=args.dry_run,
                log_progress=False,
            )
            calls_made += cr.calls_made
            cache_hits += cr.cache_hits
            final_val = cr.resolve(strat.final_field) if strat.final_field else None
            decision = {
                "related": final_val is not None,
                "direction": "",
                "confidence": 0.0,
                "reason": "",
                "p_yes": float(final_val) if isinstance(final_val, (int, float)) else -1.0,
                "parse_error": "" if final_val is not None else (
                    "; ".join(f"{k}:{v}" for k, v in cr.errors.items())
                    or f"final_field_unresolved:{strat.final_field}"
                ),
                "prompt_sha": "",
                "chain": cr.steps,
                "chain_errors": cr.errors,
            }
            decision_source = "chain"
        else:
            user = strategies.render_user_prompt(strat, row)
            sha = llm.prompt_sha(
                system=strat.system_prompt, user=user,
                model=strat.model, strategy_version=strat.version,
            )
            cached = llm.load_cached(sha)
            if cached is None:
                if args.dry_run:
                    decision = _empty_decision("dry_run_miss", prompt_sha=sha)
                    decisions_out.append({"exploration_id": row.exploration_id,
                                          "source": "miss", **decision})
                    trades_out.append(_skip_row(row, "dry_run_miss", 0.0))
                    continue
                if calls_made >= args.max_calls:
                    decision = _empty_decision("max_calls_reached", prompt_sha=sha)
                    decisions_out.append({"exploration_id": row.exploration_id,
                                          "source": "limit", **decision})
                    trades_out.append(_skip_row(row, "max_calls_reached", 0.0))
                    continue
                result = llm.call(
                    system=strat.system_prompt, user=user,
                    model=strat.model, strategy_version=strat.version,
                    max_tokens=strat.max_output_tokens,
                )
                calls_made += 1
            else:
                result = cached
                cache_hits += 1
            decision_obj = strategies.parse_response(
                result.content,
                require_direction=strat.require_direction,
                require_p_yes=strat.require_p_yes,
            )
            decision = asdict(decision_obj)
            decision["prompt_sha"] = result.prompt_sha
            decision["latency_ms"] = result.latency_ms
            decision["cached"] = result.cached
            decision_source = "cache" if result.cached else "live"

        decision["exploration_id"] = row.exploration_id
        decision["source"] = decision_source
        decisions_out.append(decision)

        # ----- decide direction -----
        if strat.derive_direction_from_p_yes:
            p_yes = decision.get("p_yes", -1.0)
            if p_yes is None or p_yes < 0:
                trades_out.append(_skip_row(row, "missing_p_yes", 0.0))
                continue
            direction = strategies.derive_direction(p_yes, row.market_mid_t0, min_edge)
        else:
            direction = decision.get("direction") or ""

        if direction not in ("YES", "NO"):
            reason = "no_edge" if strat.derive_direction_from_p_yes else (
                "not_related" if not decision.get("related") else "no_direction"
            )
            trades_out.append(_skip_row(row, reason, 0.0))
            continue

        # ----- size the position -----
        if sizing_mode == "kelly":
            entry = pnl.entry_quote(con, row.exploration_id, direction)
            if entry is None:
                if row.market_mid_t0 is not None and use_fallback:
                    entry_price_for_sizing = (
                        row.market_mid_t0 if direction == "YES" else 1.0 - row.market_mid_t0
                    )
                else:
                    trades_out.append(_skip_row(row, "no_entry_quote", 0.0))
                    continue
            else:
                entry_price_for_sizing = entry[0]

            notional, raw_kelly_f, sk = pnl.kelly_size(
                p_yes=decision.get("p_yes", -1.0),
                entry_price=entry_price_for_sizing,
                direction=direction,
                bankroll=bankroll,
                kelly_fraction=kelly_fraction,
                max_position_pct=max_position_pct,
                min_size_usdc=min_size_usdc,
                min_edge=min_edge,
            )
            if sk:
                t = _skip_row(row, sk, 0.0)
                t["direction"] = direction
                t["raw_kelly_f"] = raw_kelly_f
                t["p_yes"] = decision.get("p_yes")
                trades_out.append(t)
                continue
        else:
            notional = flat_notional
            raw_kelly_f = None

        trade = pnl.simulate(
            con,
            exploration_id=row.exploration_id,
            direction=direction,
            notional=notional,
            fee_pct=fee_pct,
            exit_horizon_seconds=exit_horizon,
            fallback_entry_mid_t0=row.market_mid_t0 if use_fallback else None,
            skip_when_no_book=skip_no_book,
            exit_config=exit_config,
        )
        td = _trade_to_dict(trade)
        if sizing_mode == "kelly":
            td["raw_kelly_f"] = raw_kelly_f
            td["p_yes"] = decision.get("p_yes")
            if trade.entry_price is not None:
                td["edge"] = (
                    decision.get("p_yes", 0.0) - trade.entry_price
                    if direction == "YES"
                    else (1.0 - decision.get("p_yes", 1.0)) - trade.entry_price
                )
        trades_out.append(td)

    # decisions.jsonl
    with open(decisions_path, "w") as f:
        for d in decisions_out:
            f.write(json.dumps(d) + "\n")

    # trades.csv (Kelly columns appended; flat-mode runs leave them empty)
    trade_cols = [
        "exploration_id", "direction", "notional",
        "entry_offset_s", "entry_price", "exit_offset_s", "exit_price",
        "fee", "pnl", "skip_reason", "raw_kelly_f", "edge", "p_yes",
        "exit_reason", "exit_spread_cost",
    ]
    with open(trades_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=trade_cols, extrasaction="ignore")
        w.writeheader()
        for t in trades_out:
            w.writerow(t)

    # manifest.json
    manifest = {
        "run_id": run_id,
        "set": args.set_name,
        "set_id_sha": set_id_sha,
        "set_size": len(rows),
        "strategy": args.strategy_name,
        "strategy_version": strat.version,
        "model": strat.model,
        "use_orig": args.use_orig,
        "dry_run": args.dry_run,
        "max_calls": args.max_calls,
        "calls_made": calls_made,
        "cache_hits": cache_hits,
        "n_rows": len(rows),
        "backtest": {
            "sizing": sizing_mode,
            "notional_usdc": flat_notional,
            "fee_pct": fee_pct,
            "exit_horizon_seconds": exit_horizon,
            "fallback_entry_mid_t0": use_fallback,
            "skip_when_no_book": skip_no_book,
            "bankroll_usdc": bankroll,
            "kelly_fraction": kelly_fraction,
            "max_position_pct": max_position_pct,
            "min_size_usdc": min_size_usdc,
            "min_edge": min_edge,
            "exit": exit_config,
        },
        "created_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reproduce_cmd": _reproduce_cmd(args),
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    # report.md
    sum_block = render_summary(f"Run {run_id}", summary_counts(rows))
    pnl_block = render_pnl_summary(
        trades_out,
        bankroll_usdc=bankroll if sizing_mode == "kelly" else None,
    )
    detail_blocks = []
    by_id = {d["exploration_id"]: d for d in decisions_out}
    trade_by_id = {t["exploration_id"]: t for t in trades_out}
    for i, row in enumerate(rows, 1):
        detail_blocks.append(render_row_block(
            i, row,
            decision=by_id.get(row.exploration_id),
            trade=trade_by_id.get(row.exploration_id),
        ))
    with open(report_path, "w") as f:
        f.write(sum_block)
        f.write(f"_strategy: {args.strategy_name} ({strat.version})  ·  ")
        f.write(f"sizing: {sizing_mode}  ·  ")
        f.write(f"calls: {calls_made}  ·  cache hits: {cache_hits}  ·  ")
        f.write(f"use_orig: {args.use_orig}_\n\n")
        f.write(pnl_block + "\n---\n\n")
        for block in detail_blocks:
            f.write(block + "\n")

    repro = manifest["reproduce_cmd"]
    print(f"run_id     = {run_id}")
    print(f"rows       = {len(rows)}  (set sha {set_id_sha[:10]})")
    print(f"calls made = {calls_made}  (cache hits {cache_hits})")
    print(f"sizing     = {sizing_mode}")
    print(f"report     = {report_path}")
    print(f"reproduce  : {repro}")


def _pretrade_skip(con, row, cfg: dict) -> str:
    """Second-stage gate. Return a skip_reason ('pretrade:<why>') if the row
    fails any configured predicate, else '' to let it through. Cheap checks
    (columns) run before the feature computation that walks odds history."""
    if cfg.get("require_llm_related") and not row.orig_llm_related:
        return "pretrade:not_related"

    min_vol = cfg.get("min_volume_usdc")
    if min_vol is not None and (row.market_volume_24h is None
                                or row.market_volume_24h < float(min_vol)):
        return "pretrade:low_volume"

    band = cfg.get("mid_band")
    if band:
        lo, hi = float(band[0]), float(band[1])
        if row.market_mid_t0 is None or not (lo <= row.market_mid_t0 <= hi):
            return "pretrade:extreme_price"

    max_spread = cfg.get("max_entry_spread")
    if max_spread is not None and (row.market_spread_t0 is None
                                   or row.market_spread_t0 > float(max_spread)):
        return "pretrade:wide_spread"

    min_pre_stdev = cfg.get("min_pre_stdev")
    if min_pre_stdev is not None:
        f = features.compute(con, row.exploration_id)
        if f.pre_stdev is None or f.pre_stdev < float(min_pre_stdev):
            return "pretrade:flat_history"

    return ""


def _momentum_decision(con, row, cfg: dict) -> dict:
    """Deterministic momentum: compare first live mid to mid at >=window_s.
    No LLM call; direction picked from observed price drift."""
    window_s = int(cfg.get("window_s", 60))
    threshold = float(cfg.get("threshold", 0.005))
    first = con.execute(
        "SELECT mid, offset_seconds AS off FROM odds_observations "
        "WHERE exploration_id = ? AND source = 'live' "
        "AND offset_seconds >= 0 AND mid IS NOT NULL "
        "ORDER BY offset_seconds ASC LIMIT 1",
        (row.exploration_id,),
    ).fetchone()
    late = con.execute(
        "SELECT mid, offset_seconds AS off FROM odds_observations "
        "WHERE exploration_id = ? AND source = 'live' "
        "AND offset_seconds >= ? AND mid IS NOT NULL "
        "ORDER BY offset_seconds ASC LIMIT 1",
        (row.exploration_id, window_s),
    ).fetchone()
    if first is None or late is None:
        return {
            "related": False, "direction": "", "confidence": 0.0,
            "reason": "no_momentum_window", "p_yes": -1.0,
            "parse_error": "no_momentum_window", "prompt_sha": "",
        }
    invert = bool(cfg.get("invert", False))
    delta = float(late["mid"]) - float(first["mid"])
    if abs(delta) < threshold:
        return {
            "related": False, "direction": "", "confidence": 0.0,
            "reason": f"below_threshold {delta:+.4f}",
            "p_yes": -1.0, "parse_error": "", "prompt_sha": "",
        }
    rising = delta > 0
    if invert:
        rising = not rising
    direction = "YES" if rising else "NO"
    return {
        "related": True, "direction": direction,
        "confidence": min(1.0, abs(delta) / 0.05),
        "reason": f"momentum {delta:+.4f} over {window_s}s",
        "p_yes": -1.0, "parse_error": "", "prompt_sha": "",
    }


def _orig_decision(row) -> dict:
    direction = ""
    if row.orig_llm_related and row.market_mid_t0 is not None:
        direction = "YES" if row.market_mid_t0 < 0.5 else "NO"
    return {
        "related": bool(row.orig_llm_related),
        "direction": direction,
        "confidence": row.orig_llm_confidence or 0.0,
        "reason": row.orig_llm_reason or "",
        "p_yes": -1.0,
        "parse_error": "",
        "prompt_sha": "",
    }


def _empty_decision(parse_error: str, *, prompt_sha: str = "") -> dict:
    return {
        "related": False, "direction": "", "confidence": 0.0,
        "reason": "", "p_yes": -1.0, "parse_error": parse_error,
        "prompt_sha": prompt_sha,
    }


def _skip_row(row, reason: str, notional: float) -> dict:
    return {
        "exploration_id": row.exploration_id,
        "direction": "", "notional": notional,
        "entry_offset_s": None, "entry_price": None,
        "exit_offset_s": None, "exit_price": None,
        "fee": 0.0, "pnl": None,
        "skip_reason": reason,
        "raw_kelly_f": None, "edge": None, "p_yes": None,
    }


def _trade_to_dict(t) -> dict:
    return {
        "exploration_id": t.exploration_id,
        "direction": t.direction,
        "notional": t.notional,
        "entry_offset_s": t.entry_offset_s,
        "entry_price": t.entry_price,
        "exit_offset_s": t.exit_offset_s,
        "exit_price": t.exit_price,
        "fee": t.fee,
        "pnl": t.pnl,
        "skip_reason": t.skip_reason,
        "raw_kelly_f": t.raw_kelly_f,
        "edge": t.edge,
        "p_yes": t.p_yes,
        "exit_reason": t.exit_reason,
        "exit_spread_cost": t.exit_spread_cost,
    }


def _new_run_id(strategy: str, set_name: str) -> str:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}_{strategy}_{set_name}_{uuid.uuid4().hex[:6]}"


def _reproduce_cmd(args) -> str:
    parts = ["python3", "-m", "irene.scripts.sandbox.run_strategy",
             "--set", args.set_name, "--strategy", args.strategy_name,
             "--max-calls", str(args.max_calls)]
    if args.use_orig:
        parts.append("--use-orig")
    if args.dry_run:
        parts.append("--dry-run")
    return " ".join(shlex.quote(p) for p in parts)


if __name__ == "__main__":
    main()
