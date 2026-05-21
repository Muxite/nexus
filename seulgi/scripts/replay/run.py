#!/usr/bin/env python3
"""
Replay harness CLI.

Usage:
    python3 seulgi/scripts/replay/run.py \\
        --src-db seulgi/data/seulgi.db \\
        --test-db /tmp/replay_$(date +%s).db \\
        --signals 100 --concurrency 4 \\
        --report /tmp/replay_report.md

Exit code 0 = zero row-ID gaps. Anything else = bug; see report.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from harness import ReplayHarness


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--src-db", required=True,
                   help="Source DB with historical signals + trades (RO).")
    p.add_argument("--test-db", required=True,
                   help="Test DB path. WILL BE OVERWRITTEN.")
    p.add_argument("--signals", type=int, default=100,
                   help="Number of historical headlines to replay.")
    p.add_argument("--since", default=None,
                   help="Only replay signals with detected_at >= this ISO date.")
    p.add_argument("--concurrency", type=int, default=1,
                   help="Concurrent runner.process() calls. 1=sequential.")
    p.add_argument("--rate-multiplier", type=float, default=0.0,
                   help="Replay at original cadence × this. 0=as-fast-as-possible.")
    p.add_argument("--n-engines-cap", type=int, default=None,
                   help="Cap engines for faster runs. Default: all 15.")
    p.add_argument("--report", default=None,
                   help="Write Markdown report to this path. Always prints to stdout.")
    p.add_argument("--log-level", default="WARNING",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--keep-test-db", action="store_true",
                   help="Do NOT delete the test DB after the run.")
    p.add_argument("--config", default=None,
                   help="Path to config.yaml. Defaults to SEULGI_CONFIG env or 'config.yaml'.")
    return p.parse_args()


async def _main(args: argparse.Namespace) -> int:
    harness = ReplayHarness(
        src_db_path=args.src_db,
        test_db_path=args.test_db,
        n_engines_cap=args.n_engines_cap,
        log_level=getattr(logging, args.log_level),
        config_path=args.config,
    )
    try:
        await harness.setup()
        triples = harness.load_headlines(n=args.signals, since=args.since)
        if not triples:
            print("ERROR: no historical headlines matched the criteria", file=sys.stderr)
            return 2
        print(f"[replay] loaded {len(triples)} headlines from {args.src_db}",
              file=sys.stderr)

        result = await harness.run(
            triples,
            concurrency=args.concurrency,
            rate_multiplier=args.rate_multiplier,
        )
    finally:
        await harness.close()

    report = ReplayHarness.format_report(result)
    print(report)
    if args.report:
        with open(args.report, "w") as f:
            f.write(report)
        print(f"\n[replay] report → {args.report}", file=sys.stderr)

    if not args.keep_test_db:
        for suffix in ("", "-wal", "-shm"):
            p = args.test_db + suffix
            if os.path.exists(p):
                try:
                    os.unlink(p)
                except OSError:
                    pass

    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(_main(_parse_args())))
