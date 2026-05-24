"""File/format helpers shared by every page and plot module.

All writers go through `_write` (text) or `atomic_write_png` (binary) so
readers never observe a half-written file.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

from .events import TradeEvent

logger = logging.getLogger(__name__)


def _ts(dt) -> str:
    if dt is None:
        return "—"
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except (ValueError, TypeError):
            return dt[:19]
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _short(url: str) -> str:
    """Strip scheme and www from a URL for compact display."""
    return url.replace("https://", "").replace("http://", "").replace("www.", "")


def _header(title: str) -> str:
    bar = "─" * 66
    return f"\n{bar}\n  {title}\n{bar}\n"


def _trade_block(lines: list[str], ev: TradeEvent) -> None:
    edge_str = f"{ev.edge:+.4f}" if ev.mid_price is not None else "n/a"
    lines.append(
        f"  [{_ts(ev.decided_at)}]  {ev.direction:<4}  conf={ev.confidence:.0%}  "
        f"sim={ev.similarity:.2f}  edge={edge_str}"
    )
    if ev.skip_reason is None:
        lines.append(f"    EXECUTED  ${ev.size_usdc:.2f} @ {ev.mid_price:.3f}")
    else:
        lines.append(f"    SKIPPED   reason={ev.skip_reason}")
    lines.append(f"    Headline : {ev.headline[:95]}")
    lines.append(f"    Question : {ev.question[:95]}")
    lines.append(f"    Market   : {ev.condition_id[:40]}")
    lines.append("")


def _write(path: str, content: str) -> None:
    """Atomic text write via rename."""
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, path)
    except Exception as exc:
        logger.warning("[dash] failed to write %s: %s", path, exc)
        try:
            os.unlink(tmp)
        except OSError:
            pass


def atomic_write_png(path: str, data: bytes) -> None:
    """Atomic binary write via rename — for PNG plots."""
    tmp = path + ".tmp"
    try:
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    except Exception as exc:
        logger.warning("[dash] failed to write %s: %s", path, exc)
        try:
            os.unlink(tmp)
        except OSError:
            pass
