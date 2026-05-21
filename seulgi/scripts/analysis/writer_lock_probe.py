"""
SEULGI writer-lock probe.

Times how long the sqlite writer takes to accept a tiny INSERT into the
`metrics_minutely` table. Use this from the host to measure whether the
B-WRITES suspect (writer-thread starvation) is still happening, or to
prove that a fix unblocked the lock.

The probe uses `metrics_minutely` because:
- it has a composite PK with `ON CONFLICT(...) DO UPDATE` so we don't
  accumulate junk rows on repeated runs;
- it has no FK to any other table — minimum interference;
- it's small (the table the host actually owns observability for).

Usage
─────
    python3 seulgi/scripts/analysis/writer_lock_probe.py
    python3 seulgi/scripts/analysis/writer_lock_probe.py --samples 30
    python3 seulgi/scripts/analysis/writer_lock_probe.py --db PATH --interval 1.0

Output: per-sample wall-clock (ms), final p50/p95/p99 summary.

Safe to run while the live container is writing — uses
`busy_timeout=15000` so each probe waits up to 15 s before erroring out.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import statistics
import sys
import time
from datetime import datetime, timezone


def _now_bucket() -> str:
    """Minute-bucket ISO timestamp — matches the metrics.py convention."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:00+00:00")


def probe_once(db_path: str) -> float:
    """Return wall-clock ms for one INSERT-or-update on metrics_minutely.

    Uses `ON CONFLICT(bucket_ts, name, tags) DO UPDATE` so re-runs in the
    same minute are idempotent. Raises if the writer doesn't accept the
    write within busy_timeout.
    """
    conn = sqlite3.connect(db_path, timeout=20)
    try:
        conn.execute("PRAGMA busy_timeout=15000")
        t0 = time.monotonic()
        conn.execute(
            """
            INSERT INTO metrics_minutely
                (bucket_ts, name, tags, count, sum, min, max, p50, p95, p99)
            VALUES (?, ?, ?, ?, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
            ON CONFLICT(bucket_ts, name, tags) DO UPDATE SET
                count = count + excluded.count
            """,
            (_now_bucket(), "host.writer_lock_probe", "", 1),
        )
        conn.commit()
        ms = (time.monotonic() - t0) * 1000.0
    finally:
        conn.close()
    return ms


def main() -> int:
    p = argparse.ArgumentParser(description="Seulgi writer-lock probe")
    p.add_argument("--db", default="seulgi/data/seulgi.db", help="path to seulgi.db")
    p.add_argument("--samples", type=int, default=10, help="number of probes")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between probes")
    args = p.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: db not found at {args.db}", file=sys.stderr)
        return 2

    samples: list[float] = []
    errors: list[tuple[int, str]] = []
    for i in range(args.samples):
        try:
            ms = probe_once(args.db)
            samples.append(ms)
            print(f"  sample {i+1:>3}/{args.samples}: {ms:>8.1f} ms", flush=True)
        except Exception as exc:  # noqa: BLE001
            errors.append((i + 1, repr(exc)))
            print(f"  sample {i+1:>3}/{args.samples}: ERROR {exc!r}", flush=True)
        if i + 1 < args.samples:
            time.sleep(args.interval)

    if not samples:
        print("\nNo successful samples.", file=sys.stderr)
        return 2

    samples.sort()
    n = len(samples)
    p50 = samples[n // 2]
    p95 = samples[min(int(0.95 * n), n - 1)]
    p99 = samples[min(int(0.99 * n), n - 1)]
    print(
        f"\n{n} samples ok / {len(errors)} errors  "
        f"min={samples[0]:.1f}  p50={p50:.1f}  p95={p95:.1f}  p99={p99:.1f}  "
        f"max={samples[-1]:.1f}  mean={statistics.mean(samples):.1f} ms"
    )
    # Non-zero exit if p95 > 1 s — writer is contended.
    return 0 if p95 < 1000.0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
