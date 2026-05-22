#!/usr/bin/env python3
"""diff_variants — per-headline diff between two backtest runs (A2.5).

Consumes two `runs/<run_id>/` dirs (manifest.json + trades.parquet|csv|jsonl)
produced by sibling A2.3/A2.4 `backtest.py`; emits a Markdown diff of
the top |Δpnl| rows + per-engine aggregates. Read-only; the live DB is
opened RO via ?mode=ro only when supplied (joins historical realized_pnl).

Usage:
    python3 -m seulgi.scripts.replay.diff_variants <run_a> <run_b> \\
        [--out diff.md] [--db seulgi/data/seulgi.db] [--top 50]

Row schema (any serialisation): headline_id, engine, action, size, pnl
where action is "TRADE" or the skip_reason string.
"""
from __future__ import annotations

import argparse, csv, json, os, sqlite3, sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


@dataclass
class Row:
    headline_id: str
    engine: str
    action: str       # "TRADE" or skip_reason
    size: float
    pnl: float


def _coerce(d: dict) -> Optional[Row]:
    hid = d.get("headline_id") or d.get("hid")
    eng = d.get("engine") or d.get("engine_name")
    if not hid or not eng:
        return None
    action = d.get("action")
    if action is None:
        if d.get("executed") in (1, True, "1", "true", "True"):
            action = "TRADE"
        else:
            action = str(d.get("skip_reason") or "UNKNOWN")
    def _f(k1, k2):
        try:
            return float(d.get(k1, d.get(k2, 0.0)) or 0.0)
        except (TypeError, ValueError):
            return 0.0
    return Row(str(hid), str(eng), str(action),
               _f("size", "size_usdc"), _f("pnl", "realized_pnl"))


def _iter_rows(run_dir: Path) -> Iterable[Row]:
    """Try parquet → csv → jsonl; first hit wins."""
    p = run_dir / "trades.parquet"
    if p.exists():
        try:
            import pyarrow.parquet as pq  # type: ignore
            for d in pq.read_table(str(p)).to_pylist():
                r = _coerce(d)
                if r: yield r
            return
        except ImportError:
            print(f"[warn] {p} found but pyarrow missing — falling back",
                  file=sys.stderr)
    p = run_dir / "trades.csv"
    if p.exists():
        with p.open() as f:
            for d in csv.DictReader(f):
                r = _coerce(d)
                if r: yield r
        return
    p = run_dir / "trades.jsonl"
    if p.exists():
        with p.open() as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    r = _coerce(json.loads(line))
                except json.JSONDecodeError:
                    continue
                if r: yield r
        return
    raise FileNotFoundError(f"no trades.{{parquet,csv,jsonl}} in {run_dir}")


def _load_run(run_dir: Path) -> tuple[dict[tuple[str, str], Row], dict]:
    rows = {(r.headline_id, r.engine): r for r in _iter_rows(run_dir)}
    p = run_dir / "manifest.json"
    manifest: dict = {}
    if p.exists():
        try:
            manifest = json.loads(p.read_text())
        except json.JSONDecodeError:
            pass
    return rows, manifest


def _load_historical(
    db_path: Optional[str], keys: Iterable[tuple[str, str]],
) -> dict[tuple[str, str], float]:
    """realized_pnl per (headline_id, engine_name) for executed trades only."""
    if not db_path or not os.path.exists(db_path):
        return {}
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        return {}
    out: dict[tuple[str, str], float] = {}
    try:
        kl = list(keys)
        for i in range(0, len(kl), 400):              # respect SQLite var-cap
            sub = kl[i:i + 400]
            hids = sorted({k[0] for k in sub})
            qm = ",".join("?" * len(hids))
            for hid, eng, pnl in con.execute(
                f"""SELECT headline_id, engine_name, realized_pnl
                       FROM trades
                      WHERE skip_reason IS NULL
                        AND realized_pnl IS NOT NULL
                        AND headline_id IN ({qm})""", hids,
            ).fetchall():
                if hid and eng and pnl is not None:
                    out[(hid, eng)] = float(pnl)
    finally:
        con.close()
    return out


def _aggregate(rows_a, rows_b) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = defaultdict(
        lambda: {"a_better": 0, "b_better": 0, "tie": 0,
                 "both_skipped": 0, "n": 0}
    )
    for k in set(rows_a) | set(rows_b):
        a, b = rows_a.get(k), rows_b.get(k)
        bucket = out[(a or b).engine]
        bucket["n"] += 1
        a_t = a is not None and a.action == "TRADE"
        b_t = b is not None and b.action == "TRADE"
        if not a_t and not b_t:
            bucket["both_skipped"] += 1
            continue
        ap, bp = (a.pnl if a else 0.0), (b.pnl if b else 0.0)
        if abs(ap - bp) < 1e-9:
            bucket["tie"] += 1
        elif ap > bp:
            bucket["a_better"] += 1
        else:
            bucket["b_better"] += 1
    return out


def _pnl_totals(rows: dict[tuple[str, str], Row]) -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    for r in rows.values():
        out[r.engine] += r.pnl
    return out


def _fmt(r: Optional[Row]) -> tuple[str, str, str]:
    if r is None:
        return ("MISSING", "-", "-")
    return (r.action, f"{r.size:.2f}", f"{r.pnl:+.3f}")


def render(rows_a, manifest_a, rows_b, manifest_b, historical, top_n: int) -> str:
    common = set(rows_a) & set(rows_b)
    deltas = sorted(common,
                    key=lambda k: abs(rows_a[k].pnl - rows_b[k].pnl),
                    reverse=True)[:top_n]
    def _name(m): return m.get("variant") or m.get("name") or m.get("run_id") or "?"
    def _win(m):
        w = m.get("window")
        if isinstance(w, dict): return f"{w.get('since', '?')}..{w.get('until', '?')}"
        return str(w) if w else f"{m.get('since', '?')}..{m.get('until', '?')}"
    name_a, name_b = _name(manifest_a), _name(manifest_b)
    win_a, win_b = _win(manifest_a), _win(manifest_b)

    L = [
        f"# Backtest diff — `{name_a}` vs `{name_b}`", "",
        f"- variant A: `{name_a}` (window: {win_a})",
        f"- variant B: `{name_b}` (window: {win_b})",
        f"- A row count: {len(rows_a)}",
        f"- B row count: {len(rows_b)}",
        f"- common (headline,engine): {len(common)}",
        f"- showing top {len(deltas)} by |Δpnl|", "",
        "## Top per-headline deltas", "",
        "| headline_id | engine | A_action | A_size | A_pnl | "
        "B_action | B_size | B_pnl | delta | historical_outcome |",
        "|---|---|---|---:|---:|---|---:|---:|---:|---:|",
    ]
    for k in deltas:
        a, b = rows_a[k], rows_b[k]
        aa, asz, ap = _fmt(a)
        ba, bsz, bp = _fmt(b)
        h = historical.get(k)
        L.append(f"| {k[0]} | {k[1]} | {aa} | {asz} | {ap} | "
                 f"{ba} | {bsz} | {bp} | {a.pnl - b.pnl:+.3f} | "
                 f"{f'{h:+.3f}' if h is not None else '-'} |")
    L += ["", "## Aggregate per engine", "",
          "| engine | n | a_better | b_better | tie | both_skipped |",
          "|---|---:|---:|---:|---:|---:|"]
    for eng, b in sorted(_aggregate(rows_a, rows_b).items()):
        L.append(f"| {eng} | {b['n']} | {b['a_better']} | {b['b_better']} | "
                 f"{b['tie']} | {b['both_skipped']} |")
    L += ["", "## Sanity: total PnL per engine", "",
          "| engine | A_total | B_total | delta |",
          "|---|---:|---:|---:|"]
    pa, pb = _pnl_totals(rows_a), _pnl_totals(rows_b)
    for eng in sorted(set(pa) | set(pb)):
        ta, tb = pa.get(eng, 0.0), pb.get(eng, 0.0)
        L.append(f"| {eng} | {ta:+.3f} | {tb:+.3f} | {ta - tb:+.3f} |")
    L.append("")
    return "\n".join(L)


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("run_a", help="First backtest run dir.")
    p.add_argument("run_b", help="Second backtest run dir.")
    p.add_argument("--out", default=None, help="Markdown out path. Default: stdout.")
    p.add_argument("--db", default=None,
                   help="Live SQLite DB for historical_outcome join. "
                        "Default: $SEULGI_DB or seulgi/data/seulgi.db.")
    p.add_argument("--top", type=int, default=50,
                   help="Top |Δpnl| rows to show. Default 50.")
    args = p.parse_args(argv)
    rows_a, manifest_a = _load_run(Path(args.run_a))
    rows_b, manifest_b = _load_run(Path(args.run_b))
    db_path = args.db or os.environ.get("SEULGI_DB")
    if db_path is None:
        d = Path("seulgi/data/seulgi.db")
        if d.exists(): db_path = str(d)
    historical = _load_historical(db_path, set(rows_a) & set(rows_b))
    out = render(rows_a, manifest_a, rows_b, manifest_b, historical, args.top)
    if args.out:
        Path(args.out).write_text(out)
        print(f"[diff_variants] report → {args.out}", file=sys.stderr)
    else:
        print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
