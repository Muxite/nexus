"""`python -m scripts.analysis` — read-only DB inspection CLI.

Each subcommand answers one focused question and prints one table.
Pipe with `>` to capture; use `--format md` for github-flavoured
markdown to paste into a report being authored by hand.

Run `python -m scripts.analysis --help` for the list.
"""
from __future__ import annotations

import argparse
import importlib
import sys
from typing import Iterable

# Subcommand module names. Each module exposes `add_parser(sub)` and
# `run(args, conn) -> Section`. Listed in alphabetical command order so
# `--help` reads consistently.
_SUBCOMMANDS: tuple[str, ...] = (
    "close_reasons",
    "conf_buckets",
    "decision_card",
    "direction",
    "drag",
    "entry_buckets",
    "freshness",
    "funnel",
    "missed",
    "pnl",
    "sample_failures",
    "sources",
    "validate_report",
    "variants",
    "vol_buckets",
)


def _load(name: str):
    return importlib.import_module(f"scripts.analysis.{name}")


def main(argv: Iterable[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m scripts.analysis",
        description=(
            "Read-only analysis CLI for seulgi.db. Each subcommand prints "
            "one focused table; compose them at the shell."
        ),
    )
    sub = p.add_subparsers(dest="cmd", required=True, metavar="COMMAND")
    modules = {}
    for name in _SUBCOMMANDS:
        try:
            mod = _load(name)
        except Exception as exc:  # pragma: no cover — diagnostic
            print(f"!! failed to load subcommand {name!r}: {exc}", file=sys.stderr)
            continue
        modules[name.replace("_", "-")] = mod
        # Use CLI-style hyphen names ("close-reasons" not "close_reasons").
        sub_p = sub.add_parser(name.replace("_", "-"), help=mod.__doc__ or name)
        mod.add_parser(sub_p)

    args = p.parse_args(argv)

    mod = modules.get(args.cmd)
    if mod is None:
        p.error(f"unknown command: {args.cmd}")
        return 2

    from scripts.analysis._common import open_db
    from scripts.analysis._format import render

    conn = open_db(getattr(args, "db", None))
    try:
        section = mod.run(args, conn)
    finally:
        conn.close()

    fmt = getattr(args, "format", "text")
    render(section, fmt=fmt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
