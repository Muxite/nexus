"""Shared matplotlib setup + figure-to-bytes helper.

All plot modules import from here so the Agg backend is set once and
every figure gets the same dark style. Use `figure_to_png(fig)` to
serialise a finished figure — it closes the figure to avoid the slow
matplotlib memory leak on long-running processes.
"""

from __future__ import annotations

import io

import matplotlib

# Must be set before pyplot is imported. Agg = headless, no GUI.
matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402

# Dark palette for terminal-style operator consoles. Cheap and readable.
plt.rcParams.update({
    "figure.facecolor":  "#111418",
    "axes.facecolor":    "#111418",
    "axes.edgecolor":    "#888888",
    "axes.labelcolor":   "#dddddd",
    "axes.titlecolor":   "#ffffff",
    "xtick.color":       "#bbbbbb",
    "ytick.color":       "#bbbbbb",
    "text.color":        "#dddddd",
    "grid.color":        "#333333",
    "axes.grid":         True,
    "grid.linestyle":    ":",
    "grid.alpha":        0.6,
    "figure.dpi":        100,
    "savefig.dpi":       100,
    "savefig.bbox":      "tight",
    "font.size":         9,
    "axes.titlesize":    11,
})


def figure_to_png(fig) -> bytes:
    """Serialise `fig` to PNG bytes and close it. Safe to call from an
    executor thread (matplotlib is GIL-bound but Agg is thread-safe per
    figure)."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()


__all__ = ["plt", "figure_to_png"]
