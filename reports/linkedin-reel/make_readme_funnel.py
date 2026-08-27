#!/usr/bin/env python3
"""True-to-scale decision funnel for the repo README (docs/images/decision-funnel.png).

The prior version (and the first LinkedIn-deck revamp) used a sqrt-mapped bar
width with a hardcoded minimum, so the three smallest stages rendered at
nearly identical widths despite spanning a >100x range. This version plots
every bar on a plain linear scale, exactly proportional to its count, with no
floor and no transform. The smallest stages render as slivers on purpose:
that's the actual point of a funnel this steep. Value labels sit outside each
bar so a bar can shrink arbitrarily thin without losing its number.

Uses the README's own current figures (176,605 articles in / 1,475 trades
out), not the live-DB LinkedIn-deck snapshot -- keep these two in sync only
if the README's numbers are refreshed at the same time.
"""
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["text.parse_math"] = False
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(os.path.dirname(os.path.dirname(HERE)), "docs", "images",
                    "decision-funnel.png")

BG = "#0d1117"
FG = "#f0f3f7"
DIM = "#b7c0cc"
BLUE = "#58a6ff"
PURPLE = "#bc8cff"
CYAN = "#39c5cf"
AMBER = "#e8b34d"
GREEN = "#3fb950"
BORDER = "#30363d"
MONO = "DejaVu Sans Mono"
PX = 1080
DPI = 100


def style_axes(ax):
    ax.set_facecolor(BG)
    for s in ax.spines.values():
        s.set_color(BORDER)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(colors=DIM, labelsize=14)


def main():
    levels = [
        ("articles ingested",               176605, BLUE,   "236 feeds · RSS+HTML"),
        ("article→market signals",          137554, BLUE,   "SBERT cosine top-K"),
        ("gate decisions evaluated",        426481, PURPLE, "each signal re-scored per gate"),
        ("- direction neutral",             260341, CYAN,   "166,140 cut · LLM Y/N/U"),
        ("- market inactive / expiry",       36627, CYAN,   "223,714 cut"),
        ("- freshness / entity / imbalance", 11881, AMBER,  "24,746 cut"),
        ("- price-sanity / edge / other",     1475, GREEN,  "executed: 0.35% of decisions"),
    ]
    labels = [l[0] for l in levels]
    cnts = np.array([l[1] for l in levels], dtype=float)
    cols = [l[2] for l in levels]
    subs = [l[3] for l in levels]

    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)
    fig.text(0.5, 0.965, "THE DECISION FUNNEL", fontsize=28, fontfamily=MONO,
              color=FG, ha="center", fontweight="bold")
    fig.text(0.5, 0.925, "176,605 articles in  →  1,475 paper trades out  ·  every "
              "rejection audited", fontsize=13, fontfamily=MONO, color=DIM,
              ha="center")

    ax = fig.add_axes([0.40, 0.09, 0.50, 0.74])
    style_axes(ax)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.set_yticks([])
    ax.tick_params(left=False, bottom=False, labelbottom=False, labelleft=False)

    n = len(levels)
    y = np.arange(n)[::-1]

    # plain linear scale: bar length is exactly proportional to count, no
    # transform, no minimum-width floor. The smallest bars are meant to look
    # tiny -- that IS the finding.
    ax.set_xlim(0, cnts.max() * 1.05)
    bars = ax.barh(y, cnts, height=0.6, color=cols, edgecolor=BG,
                    linewidth=0.5, zorder=3)

    ax.set_ylim(-0.6, n - 0.4)
    yaxis_t = ax.get_yaxis_transform()
    for yi, lab, sub in zip(y, labels, subs):
        ax.text(-0.05, yi + 0.02, lab, fontsize=11.5, fontfamily=MONO, color=FG,
                ha="right", va="center", fontweight="bold", zorder=4,
                transform=yaxis_t)
        ax.text(-0.05, yi - 0.30, sub, fontsize=9.5, fontfamily=MONO, color=DIM,
                ha="right", va="center", zorder=4, transform=yaxis_t)

    for b, c in zip(bars, cnts):
        x_end = b.get_width() + cnts.max() * 0.012
        ax.text(x_end, b.get_y() + b.get_height() / 2, f"{int(c):,}",
                fontsize=13, fontfamily=MONO, color=FG, ha="left", va="center",
                fontweight="bold", zorder=4)

    fig.text(0.5, 0.04,
              "426,481 gate decisions · 11 ordered gates · 0 silent skips\n"
              "every rejected candidate persists its full per-gate decision chain\n"
              "bar length is exactly proportional to count, no floor, no scale transform",
              fontsize=11.5, fontfamily=MONO, color=DIM, ha="center")

    fig.savefig(OUT, dpi=DPI, facecolor=BG)
    plt.close(fig)
    print("wrote", OUT)


if __name__ == "__main__":
    main()
