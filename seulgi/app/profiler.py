"""
Ultra-lightweight span profiler.  Pure stdlib, zero external deps.

Enabled by default (dev mode). Set PROFILE=0 to disable completely —
all spans become no-ops with zero hot-path cost.

Usage
-----
    from profiler import prof          # seulgi singleton

    with prof.span("markets/search"):
        ...

    @prof.traced("ai/classify")
    async def classify(self, ...): ...

Output file (atomic write every flush_interval seconds):
    ── seulgi  2026-04-19 12:34:56 ──────────────────────────────────────
      ai/classify          n=  142  avg= 890.1ms  p50= 780.3  p95=1800.4  p99=3200.1
      markets/search       n=  142  avg=   2.1ms  p50=   1.8  p95=   4.2  p99=  12.3
      watcher/fetch        n= 1203  avg= 312.4ms  p50= 280.1  p95= 890.2  p99=2100.0
"""

import asyncio
import collections
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

_ENABLED: bool = os.getenv("PROFILE", "1") != "0"


class Profiler:
    def __init__(
        self,
        service: str,
        output_path: Optional[str] = None,
        maxlen: int = 2000,
        flush_interval: float = 10.0,
    ) -> None:
        self.service = service
        self._path = output_path
        self._buf: dict[str, collections.deque] = collections.defaultdict(
            lambda: collections.deque(maxlen=maxlen)
        )
        self._lock = threading.Lock()

        if _ENABLED and output_path:
            t = threading.Thread(
                target=self._flush_loop, args=(flush_interval,),
                daemon=True, name=f"profiler-{service}",
            )
            t.start()

    @contextmanager
    def span(self, name: str):
        """Measure wall-clock time of enclosed block. No-op when PROFILE=0."""
        if not _ENABLED:
            yield
            return
        t0 = time.perf_counter()
        try:
            yield
        finally:
            ms = (time.perf_counter() - t0) * 1000.0
            with self._lock:
                self._buf[name].append(ms)

    def traced(self, name: str):
        """Decorator wrapping a sync or async function in a span."""
        def decorator(fn):
            if asyncio.iscoroutinefunction(fn):
                async def _w(*a, **kw):
                    with self.span(name):
                        return await fn(*a, **kw)
                _w.__name__ = fn.__name__
                return _w
            else:
                def _w(*a, **kw):
                    with self.span(name):
                        return fn(*a, **kw)
                _w.__name__ = fn.__name__
                return _w
        return decorator

    def snapshot(self) -> dict[str, dict]:
        """Thread-safe copy of current stats."""
        with self._lock:
            out = {}
            for key, dq in self._buf.items():
                s = sorted(dq)
                n = len(s)
                if n == 0:
                    continue
                out[key] = {
                    "n":   n,
                    "avg": sum(s) / n,
                    "p50": s[n // 2],
                    "p95": s[min(int(n * 0.95), n - 1)],
                    "p99": s[min(int(n * 0.99), n - 1)],
                }
        return out

    def _flush_loop(self, interval: float) -> None:
        while True:
            time.sleep(interval)
            self._write()

    def _write(self) -> None:
        snap = self.snapshot()
        if not snap or not self._path:
            return
        ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        pad = "─" * max(0, 52 - len(self.service))
        lines = [f"── {self.service}  {ts} {pad}"]
        for key in sorted(snap):
            s = snap[key]
            lines.append(
                f"  {key:<38} n={s['n']:>5}"
                f"  avg={s['avg']:>8.1f}ms"
                f"  p50={s['p50']:>8.1f}"
                f"  p95={s['p95']:>8.1f}"
                f"  p99={s['p99']:>8.1f}"
            )
        try:
            tmp = self._path + ".tmp"
            with open(tmp, "w") as f:
                f.write("\n".join(lines) + "\n")
            os.replace(tmp, self._path)
        except Exception:
            pass


# ── Seulgi singleton ──────────────────────────────────────────────────────────
_dashboard_dir = os.getenv("SEULGI_DASHBOARD_DIR", "/data/dashboard")
prof = Profiler("seulgi", os.path.join(_dashboard_dir, "profile.txt"))
