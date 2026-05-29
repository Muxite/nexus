"""Shared loaders for sandbox run artifacts.

Single source of truth for parsing trades.csv / decisions.jsonl /
manifest.json from a run directory. All inspector / plot scripts
import these instead of re-implementing the CSV parsing.
"""

from __future__ import annotations

import csv
import json
import os
from typing import Optional


def run_dir(runs_root: str, run_id: str) -> str:
    return os.path.join(runs_root, run_id)


def load_manifest(run_dir: str) -> dict:
    with open(os.path.join(run_dir, "manifest.json")) as f:
        return json.load(f)


def load_trades(run_dir: str) -> list[dict]:
    out = []
    with open(os.path.join(run_dir, "trades.csv")) as f:
        for r in csv.DictReader(f):
            for k in ("entry_offset_s", "exit_offset_s"):
                r[k] = int(r[k]) if r.get(k) not in ("", None) else None
            for k in ("notional", "entry_price", "exit_price", "fee", "pnl",
                      "raw_kelly_f", "edge", "p_yes"):
                r[k] = _f(r.get(k))
            r["exploration_id"] = int(r["exploration_id"])
            out.append(r)
    return out


def load_decisions(run_dir: str) -> list[dict]:
    out = []
    path = os.path.join(run_dir, "decisions.jsonl")
    if not os.path.exists(path):
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def executed(trades: list[dict]) -> list[dict]:
    return [t for t in trades if not t.get("skip_reason") and t.get("pnl") is not None]


def _f(v) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
