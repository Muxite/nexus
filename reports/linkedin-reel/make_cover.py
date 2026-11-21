#!/usr/bin/env python3
"""Cover slide for the lean 2026-08 LinkedIn deck.

Composites the sparse DKG background (09_dkg_sparse_bg.png, Task 3) with
large headline stats + a hook line. Matches make_reel.py's palette/mono-font
aesthetic so it sits naturally as slide 1 of reports/linkedin-reel-2026-08/.

The hook line here is a PLACEHOLDER for layout-judging purposes only — final
LinkedIn copy is written separately.
"""
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["text.parse_math"] = False
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(os.path.dirname(HERE), "linkedin-reel-2026-08")

BG_IMG = os.path.join(HERE, "09_dkg_sparse_bg.png")

FG = "#f0f3f7"
DIM = "#b7c0cc"
AMBER = "#e8b34d"
CYAN = "#8fe4ea"
MONO = "DejaVu Sans Mono"
PX = 1080
DPI = 100


def _text_scrim(px=PX, x0=0, x1=760, y0=70, y1=1020, max_alpha=0.62,
                 x_feather=170, y_feather=130):
    """Soft-edged dark scrim over the text column so the headline stats and
    hook line stay legible against the graph background wherever a dense
    node cluster happens to sit behind them, while the graph stays fully
    visible in the untouched surrounding area (right side, far top/bottom).
    """
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


def main():
    img = mpimg.imread(BG_IMG)

    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor("#05070a")
    ax = fig.add_axes([0, 0, 1, 1])
    ax.imshow(img, extent=[0, PX, PX, 0], aspect="auto", zorder=0)
    ax.set_xlim(0, PX)
    ax.set_ylim(PX, 0)
    ax.axis("off")

    scrim = _text_scrim()
    ax.imshow(scrim, extent=[0, PX, PX, 0], zorder=2)

    # kicker
    ax.text(70, 108, "NEXUS · IRENE", fontsize=17, fontfamily=MONO,
            color=AMBER, fontweight="bold", ha="left", va="center", zorder=5)
    ax.text(70, 140, "a news-driven prediction-market research platform",
            fontsize=13, fontfamily=MONO, color=DIM, ha="left", va="center",
            zorder=5)

    # headline stats -- boldest, verified, non-trading numbers
    stats = [
        ("230,854", "markets tracked  ·  21x gen-1's universe"),
        ("122,145", "knowledge-graph nodes  ·  168,842 edges"),
        ("$128", "total LLM spend  ·  434k calls, fully audited"),
    ]
    y = 330
    for num, lab in stats:
        ax.text(70, y, num, fontsize=54, fontfamily=MONO, color=FG,
                fontweight="bold", ha="left", va="center", zorder=5)
        ax.text(70, y + 58, lab, fontsize=14.5, fontfamily=MONO, color=CYAN,
                ha="left", va="center", zorder=5)
        y += 128

    # hook line (placeholder copy -- final wording handled separately)
    hook = "We built a machine to find an edge in prediction markets."
    hook2 = "It found the truth instead."
    ax.text(70, 860, hook, fontsize=17, fontfamily=MONO, color=FG,
            fontweight="bold", ha="left", va="center", zorder=5)
    ax.text(70, 892, hook2, fontsize=17, fontfamily=MONO, color=FG,
            fontweight="bold", ha="left", va="center", zorder=5)

    ax.text(70, 990, "234,966 articles  →  552,571 gate decisions  →  "
            "an engineering result, not a trading one", fontsize=12.5,
            fontfamily=MONO, color=DIM, ha="left", va="center", zorder=5)

    out = os.path.join(OUT_DIR, "01_cover.png")
    fig.savefig(out, dpi=DPI, facecolor="#05070a")
    plt.close(fig)
    print("wrote", out)


if __name__ == "__main__":
    main()
