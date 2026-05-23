"""`python -m scripts.analysis <subcommand>` dispatcher.

Each subcommand is a module in this package that exposes:
    add_parser(p: argparse.ArgumentParser) -> None
    run(args, conn: sqlite3.Connection) -> Section

Pattern from seulgi/scripts/analysis. Same shape, fewer subcommands —
P3+ adds more as the analyst tier grows.
"""

from __future__ import annotations

import argparse
import sys

from scripts.analysis import _common
from scripts.analysis import _format

from scripts.analysis import funnel, latency, decision_card, impact

_SUBCOMMANDS = {
    "funnel":         funnel,
    "latency":        latency,
    "decision":       decision_card,
    "decision_card":  decision_card,
    "impact":         impact,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.analysis",
        description="irene analysis subcommands — operator-facing slices of "
                    "the DB. See `<subcommand> --help` for per-command flags.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name, mod in _SUBCOMMANDS.items():
        sp = sub.add_parser(name, help=mod.__doc__.splitlines()[0].rstrip())
        mod.add_parser(sp)
    args = parser.parse_args(argv)
    mod = _SUBCOMMANDS[args.cmd]
    try:
        conn = _common.open_db(args.db)
    except FileNotFoundError as exc:
        print(f"analysis: ERROR {exc}", file=sys.stderr)
        return 2
    try:
        section = mod.run(args, conn)
    finally:
        conn.close()
    sys.stdout.write(_format.render(section, args.format))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
