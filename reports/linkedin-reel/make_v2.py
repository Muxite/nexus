#!/usr/bin/env python3
"""Second-pass revamp of the lean 2026-08 LinkedIn deck.

Owner feedback on the first lean set (reports/linkedin-reel-2026-08/):
  - cover (01): drop the markets/spend stats, keep only graph-size numbers
    (nodes/edges) + a title; kill the cyan label color, white text only,
    higher-contrast scrim for legibility.
  - numbers grid (02) and architecture (03): cut entirely.
  - funnel (now 02): the sqrt-width bars hit a hardcoded 108px floor, so the
    three smallest stages (42,824 / 13,550 / 1,833) rendered at nearly
    identical widths despite spanning a 23x range. Rebuilt as a real
    matplotlib barh with a sqrt function-scale x-axis (no floor, no fudge)
    and square (non-rounded) bars.
  - calibration (now 03): already a real matplotlib bar() with square
    corners; carried over with only a title/footer trim.
  - ending (06): cut entirely.
  - also emits 00_dkg_full.png, a full (non-sparse-background) knowledge-graph
    render, as an alternate cover candidate to compare against 01_cover.png.

Everything here is additive — reads from make_reel.py's/make_cover.py's own
output artifacts, doesn't touch either file.
"""
import os
import shutil
import numpy as np
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["text.parse_math"] = False
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.patches import Rectangle

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(os.path.dirname(HERE), "linkedin-reel-2026-08-v2")
os.makedirs(OUT_DIR, exist_ok=True)

BG = "#0d1117"
FG = "#f0f3f7"
DIM = "#b7c0cc"
AMBER = "#e8b34d"
GREEN = "#3fb950"
BLUE = "#58a6ff"
RED = "#f85149"
PURPLE = "#bc8cff"
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
    for lbl in ax.get_xticklabels() + ax.get_yticklabels():
        lbl.set_fontfamily(MONO)


# =============================================================================
# 00 -- full DKG visualization, offered as an alternate cover
# =============================================================================
def make_full_dkg():
    src = os.path.join(HERE, "09_dkg_dark_radial_live.png")
    dst = os.path.join(OUT_DIR, "00_dkg_full_alt_cover.png")
    shutil.copyfile(src, dst)
    print("wrote", dst, "(copy of 09_dkg_dark_radial_live.png, for comparison against 01_cover.png)")


# =============================================================================
# 01 -- cover: title + graph-size numbers only, white text, no blue/cyan
# =============================================================================
def _text_scrim(px=PX, x0=0, x1=760, y0=70, y1=760, max_alpha=0.72,
                 x_feather=170, y_feather=150):
    xs = np.arange(px)
    ys = np.arange(px)
    X, Y = np.meshgrid(xs, ys)
    fade_start, fade_end = x1 - x_feather, x1 + x_feather
    fx = np.clip((fade_end - X) / (fade_end - fade_start), 0.0, 1.0)
    fx[X < x0] = 1.0
    fy_top = np.clip((Y - (y0 - y_feather)) / y_feather, 0.0, 1.0)
    fy_bot = np.clip(((y1 + y_feather) - Y) / y_feather, 0.0, 1.0)
    fy = fy_top * fy_bot
    alpha = max_alpha * fx * fy
    rgba = np.zeros((px, px, 4))
    rgba[..., 3] = alpha
    return rgba


def make_cover():
    img = mpimg.imread(os.path.join(HERE, "09_dkg_sparse_bg.png"))
    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor("#05070a")
    ax = fig.add_axes([0, 0, 1, 1])
    ax.imshow(img, extent=[0, PX, PX, 0], aspect="auto", zorder=0)
    ax.set_xlim(0, PX)
    ax.set_ylim(PX, 0)
    ax.axis("off")

    ax.imshow(_text_scrim(), extent=[0, PX, PX, 0], zorder=2)

    # title
    ax.text(70, 118, "NEXUS · IRENE", fontsize=30, fontfamily=MONO,
            color=FG, fontweight="bold", ha="left", va="center", zorder=5)
    ax.text(70, 158, "a news-driven prediction-market research platform",
            fontsize=14, fontfamily=MONO, color=DIM, ha="left", va="center",
            zorder=5)

    # graph-size numbers only -- no markets/spend stats, all white
    stats = [
        ("122,145", "knowledge-graph nodes"),
        ("168,842", "knowledge-graph edges"),
        ("88,516", "facts linked across the graph"),
    ]
    y = 330
    for num, lab in stats:
        ax.text(70, y, num, fontsize=56, fontfamily=MONO, color=FG,
                fontweight="bold", ha="left", va="center", zorder=5)
        ax.text(70, y + 60, lab, fontsize=15, fontfamily=MONO, color=FG,
                ha="left", va="center", zorder=5, alpha=0.85)
        y += 132

    hook = "We built a machine to find an edge in prediction markets."
    hook2 = "It found the truth instead."
    ax.text(70, 850, hook, fontsize=18, fontfamily=MONO, color=FG,
            fontweight="bold", ha="left", va="center", zorder=5)
    ax.text(70, 884, hook2, fontsize=18, fontfamily=MONO, color=FG,
            fontweight="bold", ha="left", va="center", zorder=5)

    out = os.path.join(OUT_DIR, "01_cover.png")
    fig.savefig(out, dpi=DPI, facecolor="#05070a")
    plt.close(fig)
    print("wrote", out)


# =============================================================================
# 02 -- decision funnel: accurate sqrt-scale barh, hard square edges
# =============================================================================
def make_funnel():
    levels = [
        ("articles ingested",                234966, BLUE,   "237 feeds · RSS+HTML"),
        ("article→market signals",           164360, BLUE,   "SBERT cosine top-K"),
        ("gate decisions evaluated",         552571, PURPLE, "each signal re-scored per gate"),
        ("− direction neutral",              337590, "#39c5cf", "214,981 cut · LLM Y/N/U"),
        ("− market inactive / expiry",        42824, "#39c5cf", "294,766 cut"),
        ("− freshness / entity / imbalance",  13550, AMBER,  "29,274 cut"),
        ("− price-sanity / edge / other",      1833, GREEN,  "executed: 0.33% of decisions"),
    ]
    labels = [l[0] for l in levels]
    cnts = np.array([l[1] for l in levels], dtype=float)
    cols = [l[2] for l in levels]
    subs = [l[3] for l in levels]

    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)
    fig.text(0.5, 0.955, "THE DECISION FUNNEL", fontsize=28, fontfamily=MONO,
              color=FG, ha="center", fontweight="bold")
    fig.text(0.5, 0.915, "234,966 articles in  →  1,833 paper trades out  ·  every "
              "rejection audited", fontsize=13.5, fontfamily=MONO, color=DIM,
              ha="center")

    ax = fig.add_axes([0.40, 0.10, 0.52, 0.72])
    style_axes(ax)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.set_yticks([])
    ax.tick_params(left=False, bottom=False, labelbottom=False, labelleft=False)

    n = len(levels)
    y = np.arange(n)[::-1]  # top row = first stage

    # true sqrt scale, no artificial minimum-width clamp
    ax.set_xscale("function", functions=(
        lambda x: np.sqrt(np.clip(x, 0, None)),
        lambda x: np.square(x),
    ))
    ax.set_xlim(0, cnts.max() * 1.05)

    bars = ax.barh(y, cnts, height=0.6, color=cols, edgecolor=BG,
                    linewidth=0.5, zorder=3)

    ax.set_ylim(-0.6, n - 0.4)
    yaxis_t = ax.get_yaxis_transform()  # x: axes fraction, y: data coords
    for yi, lab, sub in zip(y, labels, subs):
        ax.text(-0.06, yi + 0.02, lab, fontsize=11.5, fontfamily=MONO, color=FG,
                ha="right", va="center", fontweight="bold", zorder=4,
                transform=yaxis_t)
        ax.text(-0.06, yi - 0.30, sub, fontsize=9.5, fontfamily=MONO, color=DIM,
                ha="right", va="center", zorder=4, transform=yaxis_t)

    for b, c in zip(bars, cnts):
        # sqrt-scale-aware offset so the label clears the bar by a constant
        # visual gap regardless of the bar's (non-linear) plotted width
        x_end = np.sqrt(b.get_width()) + np.sqrt(cnts.max()) * 0.018
        ax.text(x_end ** 2, b.get_y() + b.get_height() / 2,
                f"{int(c):,}", fontsize=13, fontfamily=MONO, color=FG,
                ha="left", va="center", fontweight="bold", zorder=4)

    fig.text(0.5, 0.045,
              "552,571 gate decisions · 11 ordered gates · 0 silent skips\n"
              "every rejected candidate persists its full per-gate decision chain\n"
              "bar length ∝ √(count) — every stage plotted to true relative scale, no floor",
              fontsize=12, fontfamily=MONO, color=DIM, ha="center")

    out = os.path.join(OUT_DIR, "02_funnel.png")
    fig.savefig(out, dpi=DPI, facecolor=BG)
    plt.close(fig)
    print("wrote", out)


# =============================================================================
# 03 -- calibration: already a real matplotlib bar(), square corners
# =============================================================================
def make_calibration():
    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)

    fig.text(0.5, 0.93, "THE MODEL'S CONFIDENCE WAS A LIE", fontsize=25,
              fontfamily=MONO, color=FG, ha="center", fontweight="bold")
    fig.text(0.5, 0.875, "measured, then calibrated: SEULGI engine, n=501 closed decisions",
              fontsize=14, fontfamily=MONO, color=DIM, ha="center")

    ax1 = fig.add_axes([0.10, 0.17, 0.37, 0.56])
    style_axes(ax1)
    vals = [0.35, 0.17]
    ax1.bar([0, 1], vals, width=0.5, color=[RED, GREEN], zorder=3,
            edgecolor=BG, linewidth=0.5)
    ax1.errorbar(1, 0.195, yerr=0.025, color=FG, capsize=6, lw=1.6, zorder=4)
    ax1.set_xticks([0, 1])
    ax1.set_xticklabels(["raw", "calibrated"], fontsize=14, fontfamily=MONO, color=FG)
    ax1.set_ylim(0, 0.42)
    ax1.grid(axis="y", color=BORDER, lw=0.7, zorder=0)
    ax1.set_title("Brier score  (lower = better)", fontsize=15,
                  fontfamily=MONO, color=FG, pad=14)
    ax1.text(0, 0.362, "~0.35", fontsize=17, fontfamily=MONO, color=FG,
              ha="center", fontweight="bold")
    ax1.text(1, 0.232, "0.17-0.22", fontsize=15, fontfamily=MONO, color=FG,
              ha="center", fontweight="bold")
    ax1.text(0.5, 0.30, "≈ halved", fontsize=20, fontfamily=MONO, color=AMBER,
              ha="center", fontweight="bold")

    ax2 = fig.add_axes([0.59, 0.17, 0.37, 0.56])
    style_axes(ax2)
    vals = [-0.19, 0.12, 0.16]
    cols = [RED, GREEN, GREEN]
    bars = ax2.bar([0, 1, 2], vals, width=0.5, color=cols, zorder=3,
                    edgecolor=BG, linewidth=0.5)
    ax2.set_xticks([0, 1, 2])
    ax2.set_xticklabels(["raw", "Platt", "isotonic"], fontsize=13,
                        fontfamily=MONO, color=FG)
    ax2.set_ylim(-0.26, 0.26)
    ax2.axhline(0, color=FG, lw=1.1)
    ax2.grid(axis="y", color=BORDER, lw=0.7, zorder=0)
    ax2.set_title("IC(confidence, outcome)", fontsize=15,
                  fontfamily=MONO, color=FG, pad=14)
    for b, v in zip(bars, vals):
        off = 0.015 if v > 0 else -0.035
        ax2.text(b.get_x() + b.get_width() / 2, v + off, f"{v:+.2f}",
                  fontsize=16, fontfamily=MONO, color=FG, ha="center",
                  fontweight="bold")
    ax2.text(0, 0.12, "anti-\ninformative", fontsize=12, fontfamily=MONO,
              color=RED, ha="center")

    fig.text(0.5, 0.065,
              "raw confidence correlated NEGATIVELY with outcomes.\n"
              "Platt + isotonic regression per direction flipped the sign.",
              fontsize=14, fontfamily=MONO, color=DIM, ha="center")

    out = os.path.join(OUT_DIR, "03_calibration.png")
    fig.savefig(out, dpi=DPI, facecolor=BG)
    plt.close(fig)
    print("wrote", out)


if __name__ == "__main__":
    make_full_dkg()
    make_cover()
    make_funnel()
    make_calibration()
    print("\nfinal set in", OUT_DIR)
