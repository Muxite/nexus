"""
SEULGI WAL checkpoint utility.

Forces a WAL checkpoint (TRUNCATE by default) and reports the result.
Use this when the seulgi container is STOPPED to reclaim disk space
from a runaway `-wal` file before restarting.

Modes:
  PASSIVE  — safe with a live writer; checkpoints what it can without
             blocking. Doesn't shrink the WAL file on disk.
  FULL     — waits for the writer lock; checkpoints completely. Will
             block if a writer is active.
  RESTART  — like FULL but also restarts the WAL log.
  TRUNCATE — like RESTART but also truncates the WAL file on disk back
             to zero bytes (default; only use when container is STOPPED).

Usage
─────
    # Container running → safe, no truncation:
    python3 seulgi/scripts/analysis/wal_checkpoint.py --mode PASSIVE

    # Container stopped → free the 400+ MB WAL:
    python3 seulgi/scripts/analysis/wal_checkpoint.py --mode TRUNCATE

Returns (busy, total_frames, checkpointed_frames). busy=0 means success.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys


VALID_MODES = ("PASSIVE", "FULL", "RESTART", "TRUNCATE")


def main() -> int:
    p = argparse.ArgumentParser(description="Force WAL checkpoint on seulgi.db")
    p.add_argument("--db", default="seulgi/data/seulgi.db", help="path to seulgi.db")
    p.add_argument(
        "--mode", default="TRUNCATE", choices=VALID_MODES,
        help="checkpoint mode (default: TRUNCATE — only safe when container is stopped)",
    )
    p.add_argument("--timeout", type=float, default=30.0, help="busy_timeout (sec)")
    args = p.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: db not found at {args.db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(args.db, timeout=args.timeout, isolation_level=None)
    try:
        conn.execute(f"PRAGMA busy_timeout={int(args.timeout * 1000)}")
        row = conn.execute(f"PRAGMA wal_checkpoint({args.mode})").fetchone()
        busy, total, done = row
        print(f"wal_checkpoint({args.mode}): busy={busy} total_frames={total} checkpointed_frames={done}")
        # Print resulting file sizes for the operator.
        for suffix in ("", "-wal", "-shm"):
            path = args.db + suffix
            if os.path.exists(path):
                size = os.path.getsize(path)
                print(f"  {os.path.basename(path):<20} {size:>14,} bytes")
        return 0 if busy == 0 else 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
