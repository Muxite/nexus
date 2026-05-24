"""Irene Dashboard class: periodic snapshot writer.

Per-page rendering lives under `app.dashboard.pages.*`; per-plot rendering
under `app.dashboard.plots.*`. The class itself owns the run loop, the
plot cadence, and the data_dir handle.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from app.db import DB

from .io import _atomic_write
from .registry import PAGES

logger = logging.getLogger(__name__)


@dataclass
class Dashboard:
    data_dir: Path
    db: DB
    interval_secs: int = 30
    plot_interval_secs: int = 300   # PNGs are expensive; refresh every 5 min
    # Optional handles so pages can render AI/budget state. Old call sites
    # that don't pass these (tests, P1 wiring) keep working — pages must
    # check for None.
    budget: Optional[object] = None
    cfg: Optional[object] = None
    _task: Optional[asyncio.Task] = None
    _plot_task: Optional[asyncio.Task] = None
    _stopped: bool = False
    _t_boot: float = field(default_factory=time.monotonic, init=False)

    def __post_init__(self) -> None:
        self.data_dir = Path(self.data_dir)
        self._t_boot = time.monotonic()

    async def run(self) -> None:
        # Lazy import: plotting is optional and pulls matplotlib.
        self._plot_task = asyncio.create_task(self._plot_loop())
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — one bad tick can't kill the loop
                logger.exception("dashboard tick failed; continuing")
            await asyncio.sleep(self.interval_secs)

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        for task in (self._task, self._plot_task):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._task = None
        self._plot_task = None

    async def tick(self) -> None:
        """One text-snapshot pass. Each page is independent — failure on
        any one logs WARNING and leaves its file alone."""
        for page in PAGES:
            try:
                body = await page.render(self)
                if body:
                    _atomic_write(self.data_dir / page.FILENAME, body)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "dashboard page %r failed; leaving file alone",
                    page.FILENAME,
                )

    async def _plot_loop(self) -> None:
        """Regenerate PNG plots every plot_interval_secs.

        Plots are best-effort: a missing matplotlib (or a single failing
        plot) does not affect the text-file tier.
        """
        try:
            from .plots.registry import PLOTS
        except Exception:  # noqa: BLE001
            logger.warning("dashboard plots unavailable; matplotlib missing?")
            return
        while True:
            try:
                await self.plot_tick(PLOTS)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("dashboard plot pass failed; continuing")
            try:
                await asyncio.sleep(self.plot_interval_secs)
            except asyncio.CancelledError:
                raise

    async def plot_tick(self, plots) -> None:
        """One PNG-snapshot pass. Runs each plot in an executor (matplotlib
        is sync + CPU-bound) so the asyncio loop stays responsive.
        """
        loop = asyncio.get_running_loop()
        from .io import atomic_write_png
        plots_dir = self.data_dir / "plots"
        plots_dir.mkdir(parents=True, exist_ok=True)
        for plot in plots:
            try:
                png = await plot.render(self, loop=loop)
                if png:
                    atomic_write_png(plots_dir / plot.FILENAME, png)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "dashboard plot %r failed; leaving file alone",
                    plot.FILENAME,
                )
