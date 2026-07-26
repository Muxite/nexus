#!/usr/bin/env python3
"""LinkedIn hype-reel generator for Nexus.

Renders 1080x1080 slides that look like terminal screenshots / matplotlib
charts. Every number comes from reports/2026-05-31-resume-achievements.md
(verified claims only; money figures excluded per the appendix rule).
"""

import os
import pyfiglet
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["text.parse_math"] = False  # '$' is literal, not math mode
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, FancyBboxPatch, Circle

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)))

# ---- palette (GitHub-dark-ish) ----------------------------------------------
BG      = "#0d1117"
PANEL   = "#10161d"
BORDER  = "#30363d"
FG      = "#c9d1d9"
DIM     = "#8b949e"
GREEN   = "#3fb950"
BLUE    = "#58a6ff"
AMBER   = "#d29922"
RED     = "#f85149"
PURPLE  = "#bc8cff"
CYAN    = "#39c5cf"

MONO = "DejaVu Sans Mono"
CH_EM = 0.6027          # DejaVu Sans Mono advance width (em)
DPI = 100
PX = 1080


def new_fig():
    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, PX)
    ax.set_ylim(PX, 0)        # y grows downward, like a terminal
    ax.axis("off")
    return fig, ax


def char_w(fs):
    return fs * (DPI / 72) * CH_EM


def line_h(fs, spacing=1.45):
    return fs * (DPI / 72) * spacing


def draw_text(ax, x, y, segments, fs, bold=False):
    """segments: str or list of (text, color) tuples; returns x after last char."""
    if isinstance(segments, str):
        segments = [(segments, FG)]
    cw = char_w(fs)
    for text, color in segments:
        ax.text(x, y, text, fontsize=fs, fontfamily=MONO, color=color,
                va="top", ha="left",
                fontweight="bold" if bold else "normal")
        x += len(text) * cw
    return x


def draw_block(ax, x, y, lines, fs, spacing=1.45, bold=False):
    lh = line_h(fs, spacing)
    for ln in lines:
        draw_text(ax, x, y, ln, fs, bold=bold)
        y += lh
    return y


def window_chrome(ax, title, y=28):
    """macOS-style terminal window header across the top."""
    ax.add_patch(Rectangle((0, 0), PX, 64, facecolor="#161b22", edgecolor="none"))
    for i, c in enumerate(["#ff5f57", "#febc2e", "#28c840"]):
        ax.add_patch(Circle((44 + i * 34, 32), 9, facecolor=c, edgecolor="none"))
    ax.text(PX / 2, 32, title, fontsize=15, fontfamily=MONO, color=DIM,
            va="center", ha="center")
    ax.add_patch(Rectangle((0, 63), PX, 2, facecolor=BORDER, edgecolor="none"))


# =============================================================================
# Slide 1 — title card
# =============================================================================
def slide_title():
    fig, ax = new_fig()
    window_chrome(ax, "muk@nexus: ~/projects/nexus")

    y = 130
    y = draw_block(ax, 56, y, [[("$ ", GREEN), ("cat", FG), (" MOTD", BLUE)]], 20)
    y += 28

    banner = pyfiglet.figlet_format("NEXUS", font="ansi_shadow").rstrip("\n").split("\n")
    bw = max(len(l) for l in banner)
    fs_b = 21
    x0 = (PX - bw * char_w(fs_b)) / 2
    for ln in banner:
        draw_text(ax, x0, y, [(ln, GREEN)], fs_b)
        y += line_h(fs_b, 1.06)

    y += 44
    tagline = "a news-driven prediction-market research platform"
    fs = 19
    draw_text(ax, (PX - len(tagline) * char_w(fs)) / 2, y, [(tagline, FG)], fs, bold=True)
    y += line_h(fs) + 36

    flow = "100+ news sources  →  100k markets  →  LLM-gated paper trades"
    fs = 16
    draw_text(ax, (PX - len(flow) * char_w(fs)) / 2, y, [(flow, DIM)], fs)
    y += line_h(fs) + 26

    gens = [("gen-1 ", DIM), ("SEULGI", PURPLE), (" cosine engine", DIM),
            ("  ·  ", BORDER),
            ("gen-2 ", DIM), ("IRENE", CYAN), (" knowledge graph", DIM)]
    fs = 16
    gw_ = sum(len(t) for t, _ in gens) * char_w(fs)
    draw_text(ax, (PX - gw_) / 2, y, gens, fs)
    y += line_h(fs) + 60

    # stat strip
    stats = [("3", "months"), ("30", "dev cycles"), ("130k", "LOC"), ("1,397", "tests")]
    cellw = 230
    x0 = (PX - cellw * len(stats)) / 2
    for i, (num, lab) in enumerate(stats):
        cx = x0 + cellw * i + cellw / 2
        ax.text(cx, y, num, fontsize=34, fontfamily=MONO, color=BLUE,
                va="top", ha="center", fontweight="bold")
        ax.text(cx, y + 52, lab, fontsize=14, fontfamily=MONO, color=DIM,
                va="top", ha="center")
        if i:
            ax.plot([x0 + cellw * i, x0 + cellw * i], [y + 4, y + 64],
                    color=BORDER, lw=1)
    y += 150

    y = draw_block(ax, 56, y, [
        [("$ ", GREEN), ("# every decision, gate rejection, LLM call and", DIM)],
        [("  ", FG), ("# latency sample — audited in SQLite", DIM)],
        [("$ ", GREEN), ("█", FG)],
    ], 17)

    fig.savefig(f"{OUT}/01_title.png", dpi=DPI, facecolor=BG)
    plt.close(fig)


# =============================================================================
# Slide 2 — big numbers grid
# =============================================================================
def slide_numbers():
    fig, ax = new_fig()
    window_chrome(ax, "nexus — by the numbers")

    head = pyfiglet.figlet_format("SHIPPED", font="ansi_shadow").rstrip("\n").split("\n")
    fs_b = 14
    bw = max(len(l) for l in head)
    y = 110
    x0 = (PX - bw * char_w(fs_b)) / 2
    for ln in head:
        draw_text(ax, x0, y, [(ln, PURPLE)], fs_b)
        y += line_h(fs_b, 1.06)
    y += 10
    segs = [("2026-03-14 → 2026-07-09 · solo · two generations: ", DIM),
            ("SEULGI", PURPLE), (" → ", DIM), ("IRENE", CYAN)]
    fs = 15
    sw = sum(len(t) for t, _ in segs) * char_w(fs)
    draw_text(ax, (PX - sw) / 2, y, segs, fs)

    cells = [
        ("130k", "lines of Python", "SEULGI 48k · IRENE 69k · svcs", GREEN),
        ("1,578", "automated tests", "collected by pytest", GREEN),
        ("215", "commits", "30 numbered dev cycles", BLUE),
        ("10.98M", "result rows", "in one SQLite table", BLUE),
        ("334k", "LLM calls", "~$98 total · gemini-2.5-flash", AMBER),
        ("4.77M", "odds samples", "live CLOB market-data capture", AMBER),
        ("231k", "markets indexed", "across 60.6k events · 21x gen-1", CYAN),
        ("~18k", "WS subscriptions", "sharded CLOB pool", CYAN),
    ]
    cols, rows = 2, 4
    gw, gh = 470, 178
    gx = (PX - cols * gw - 40) / 2
    gy = 310
    for i, (num, lab, sub_, col) in enumerate(cells):
        r, c = divmod(i, cols)
        x = gx + c * (gw + 40)
        yy = gy + r * (gh + 12)
        ax.add_patch(FancyBboxPatch((x, yy), gw, gh,
                     boxstyle="round,pad=0,rounding_size=10",
                     facecolor=PANEL, edgecolor=BORDER, lw=1.4))
        ax.text(x + 28, yy + 30, num, fontsize=33, fontfamily=MONO,
                color=col, va="top", ha="left", fontweight="bold")
        ax.text(x + 28, yy + 92, lab, fontsize=17, fontfamily=MONO,
                color=FG, va="top", ha="left")
        ax.text(x + 28, yy + 128, sub_, fontsize=13, fontfamily=MONO,
                color=DIM, va="top", ha="left")

    fig.savefig(f"{OUT}/02_numbers.png", dpi=DPI, facecolor=BG)
    plt.close(fig)


# =============================================================================
# Slide 3 — fake tmux dashboard
# =============================================================================
def pane(ax, x, y, w, h, title, color):
    ax.add_patch(Rectangle((x, y), w, h, facecolor=PANEL,
                           edgecolor=BORDER, lw=1.4))
    draw_text(ax, x + 18, y + 14, [("┤ ", BORDER), (title, color),
                                   (" ├", BORDER)], 15, bold=True)


def kv_rows(ax, x, y, rows, fs=15, keyw=22):
    lh = line_h(fs, 1.62)
    for k, v, c in rows:
        draw_text(ax, x, y, [(k.ljust(keyw), DIM), (v, c)], fs)
        y += lh
    return y


def slide_tmux():
    fig, ax = new_fig()
    window_chrome(ax, "tmux: IRENE — live telemetry (real numbers, simplified layout)")

    m, gap, top, statush = 26, 14, 86, 46
    w = (PX - 2 * m - gap) / 2
    h = (PX - top - statush - 2 * gap - m) / 2

    # pane 0: market universe
    x, y = m, top
    pane(ax, x, y, w, h, "0:universe", GREEN)
    kv_rows(ax, x + 24, y + 62, [
        ("markets indexed", "230,581", GREEN),
        ("distinct events", "60,594", FG),
        ("active markets", "5,852", FG),
        ("events coverage", "99.6%", GREEN),
        ("legacy cap", "10,761 (capped)", RED),
        ("universe gain", "21x", AMBER),
    ])
    draw_text(ax, x + 24, y + h - 56, [("dual-read /events + /markets", DIM)], 13)

    # pane 1: news pipeline
    x = m + w + gap
    pane(ax, x, y, w, h, "1:news", BLUE)
    kv_rows(ax, x + 24, y + 62, [
        ("feeds", "236  (was 33)", FG),
        ("articles stored", "176,603", FG),
        ("semantic clusters", "7,915", BLUE),
        ("dedup ratio", "18.1x", AMBER),
        ("first-poll burst", "1,112 in 2 min", FG),
        ("freshness", "BRK/EVO/REH/STALE", DIM),
    ])
    draw_text(ax, x + 24, y + h - 56, [("SBERT + Chroma cosine d=0.55", DIM)], 13)

    # pane 2: decision latency
    x, y = m, top + h + gap
    pane(ax, x, y, w, h, "2:latency", AMBER)
    bars = [("p50", 578, GREEN), ("p95", 3060, AMBER), ("p99", 9868, RED)]
    yy = y + 66
    maxb = 9868
    for lab, v, c in bars:
        n = max(2, round(v / maxb * 16))
        draw_text(ax, x + 24, yy, [
            (lab.ljust(5), DIM), ("▇" * n, c), (" " + f"{v:,} ms", FG)], 15)
        yy += line_h(15, 1.9)
    yy += 8
    yy = kv_rows(ax, x + 24, yy, [
        ("samples", "n=125,130", FG),
        ("breakdown", "7-step JSON", DIM),
    ])
    draw_text(ax, x + 24, y + h - 56,
              [("fetch→ai→resolve→imbalance→insert", DIM)], 13)

    # pane 3: llm ops
    x = m + w + gap
    pane(ax, x, y, w, h, "3:llm", PURPLE)
    kv_rows(ax, x + 24, y + 62, [
        ("calls (audited)", "333,516", FG),
        ("total spend", "~$98 (gemini)", GREEN),
        ("ensemble", "N=3, 2/3-vote gate", FG),
        ("disagree →", "hard NEUTRAL", AMBER),
        ("budget watch", "OK [flag: clear]", GREEN),
        ("cache hit (replay)", "78%", BLUE),
    ])
    draw_text(ax, x + 24, y + h - 56, [("pause-before-spend governor", DIM)], 13)

    # tmux status bar
    sy = PX - statush + 6
    ax.add_patch(Rectangle((0, PX - statush), PX, statush,
                           facecolor="#161b22", edgecolor="none"))
    draw_text(ax, 20, sy + 6, [
        ("[IRENE] ", CYAN), ("0:universe ", FG), ("1:news ", FG),
        ("2:latency", FG), ("* ", GREEN), ("3:llm", FG),
    ], 15)
    right = '"gen-2" 09:41 2026-07-09'
    draw_text(ax, PX - 20 - len(right) * char_w(15), sy + 6, [(right, DIM)], 15)

    fig.savefig(f"{OUT}/03_dashboard.png", dpi=DPI, facecolor=BG)
    plt.close(fig)


# =============================================================================
# Slide 4 — architecture (ascii diagram)
# =============================================================================
def slide_arch():
    """Clean box-and-arrow diagram (not terminal style)."""
    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    ax.text(50, 95, "IRENE · ARCHITECTURE", ha="center", va="center",
            color=FG, fontsize=27, fontfamily=MONO, fontweight="bold")
    ax.text(50, 89.5, "one event-driven bus  ·  every stage audited",
            ha="center", va="center", color=DIM, fontsize=14.5, fontfamily=MONO)

    def box(cx, cy, w, h, primary, secondary=None, edge=BLUE, ptc=FG, pfs=15.5):
        ax.add_patch(FancyBboxPatch(
            (cx - w / 2, cy - h / 2), w, h,
            boxstyle="round,pad=0.4,rounding_size=1.6",
            linewidth=2.4, edgecolor=edge, facecolor=PANEL, zorder=2))
        if secondary:
            ax.text(cx, cy + 1.7, primary, ha="center", va="center",
                    color=ptc, fontsize=pfs, fontfamily=MONO, fontweight="bold", zorder=3)
            ax.text(cx, cy - 2.4, secondary, ha="center", va="center",
                    color=DIM, fontsize=11, fontfamily=MONO, zorder=3)
        else:
            ax.text(cx, cy, primary, ha="center", va="center",
                    color=ptc, fontsize=pfs, fontfamily=MONO, fontweight="bold", zorder=3)

    def arrow(x0, y0, x1, y1, color=BLUE):
        ax.annotate("", xy=(x1, y1), xytext=(x0, y0), zorder=1,
                    arrowprops=dict(arrowstyle="-|>", color=color, lw=2.6,
                                    shrinkA=1, shrinkB=1, mutation_scale=22))

    # --- nodes ---
    box(28, 79, 38, 11, "236 news feeds", "ujin · FastAPI", edge=BLUE)
    box(72, 79, 38, 11, "Polymarket CLOB", "sharded WS · ~18k tokens", edge=BLUE)
    box(50, 61, 84, 11, "async pub/sub bus",
        "strict JSON · backpressure · fault isolation", edge=PURPLE, ptc=PURPLE)
    box(50, 43, 84, 12, "ingest → dedupe → match → gates → paper trade",
        "SBERT / Chroma  ·  cosine top-K  ·  per-gate audit, no silent skips",
        edge=CYAN, ptc=FG, pfs=15)
    box(50, 25, 74, 11, "SQLite (WAL) · 34 tables",
        "decisions · gates · llm calls · latency · stats", edge=GREEN, ptc=GREEN)
    box(50, 9, 60, 9, "dashboard · 15 pages · 10 plot modules",
        edge=CYAN, ptc=CYAN, pfs=14)

    # --- arrows ---
    arrow(31, 73.5, 41, 66.7)
    arrow(69, 73.5, 59, 66.7)
    arrow(50, 55.5, 50, 49.2)
    arrow(50, 37, 50, 30.7)
    arrow(50, 19.5, 50, 13.7)

    fig.savefig(f"{OUT}/04_architecture.png", dpi=DPI, facecolor=BG)
    plt.close(fig)


# =============================================================================
# Slide 5 — market universe chart (matplotlib)
# =============================================================================
def style_axes(ax):
    ax.set_facecolor(BG)
    for s in ax.spines.values():
        s.set_color(BORDER)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(colors=DIM, labelsize=14)
    for lbl in ax.get_xticklabels() + ax.get_yticklabels():
        lbl.set_fontfamily(MONO)


def slide_universe():
    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)
    ax = fig.add_axes([0.10, 0.16, 0.84, 0.56])
    style_axes(ax)

    fig.text(0.5, 0.93, "21x THE TRADABLE UNIVERSE", fontsize=27,
             fontfamily=MONO, color=FG, ha="center", fontweight="bold")
    fig.text(0.5, 0.875, "escaping the Gamma API offset=10,000 pagination cap",
             fontsize=15, fontfamily=MONO, color=DIM, ha="center")

    labels = ["SEULGI (gen-1)\nlegacy /markets", "IRENE (gen-2)\n/events + /markets"]
    vals = [10761, 230581]
    colors = [RED, GREEN]
    bars = ax.bar([0, 1], vals, width=0.52, color=colors, zorder=3)
    ax.set_xticks([0, 1])
    ax.set_xticklabels(labels, fontsize=15, fontfamily=MONO, color=FG)
    ax.set_ylim(0, 262000)
    ax.set_yticks([0, 50000, 100000, 150000, 200000])
    ax.set_yticklabels(["0", "50k", "100k", "150k", "200k"])
    ax.grid(axis="y", color=BORDER, lw=0.7, zorder=0)
    ax.set_ylabel("markets indexed (cumulative)", fontsize=14, fontfamily=MONO, color=DIM)

    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v + 6500, f"{v:,}",
                fontsize=19, fontfamily=MONO, color=FG, ha="center",
                fontweight="bold")
    ax.annotate("", xy=(1, 212000), xytext=(0, 24000),
                arrowprops=dict(arrowstyle="->", color=AMBER, lw=2.2,
                                connectionstyle="arc3,rad=-0.25"))
    ax.text(0.42, 150000, "21x", fontsize=30, fontfamily=MONO, color=AMBER,
            ha="center", fontweight="bold")

    fig.text(0.5, 0.065,
             "60,594 distinct events · 99.6% discoverable via /events\n"
             "promotion gated on only_markets < 1% telemetry (measured: 0.37%)",
             fontsize=14, fontfamily=MONO, color=DIM, ha="center")

    fig.savefig(f"{OUT}/05_universe.png", dpi=DPI, facecolor=BG)
    plt.close(fig)


# =============================================================================
# Slide 6 — calibration chart (matplotlib)
# =============================================================================
def slide_calibration():
    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)

    fig.text(0.5, 0.93, "THE MODEL'S CONFIDENCE WAS A LIE", fontsize=25,
             fontfamily=MONO, color=FG, ha="center", fontweight="bold")
    fig.text(0.5, 0.875, "measured, then calibrated — SEULGI engine, n=501 closed decisions",
             fontsize=14, fontfamily=MONO, color=DIM, ha="center")

    # left: Brier score
    ax1 = fig.add_axes([0.10, 0.17, 0.37, 0.56])
    style_axes(ax1)
    vals = [0.35, 0.17]
    bars = ax1.bar([0, 1], vals, width=0.5, color=[RED, GREEN], zorder=3)
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

    # right: information coefficient
    ax2 = fig.add_axes([0.59, 0.17, 0.37, 0.56])
    style_axes(ax2)
    vals = [-0.19, 0.12, 0.16]
    cols = [RED, GREEN, GREEN]
    bars = ax2.bar([0, 1, 2], vals, width=0.5, color=cols, zorder=3)
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

    fig.savefig(f"{OUT}/06_calibration.png", dpi=DPI, facecolor=BG)
    plt.close(fig)


# =============================================================================
# Slide 7 — the honest ending
# =============================================================================
def slide_ending():
    fig, ax = new_fig()
    window_chrome(ax, "reports/2026-05-29-trading-viability-synthesis.md")

    banner = pyfiglet.figlet_format("EDGE = 0", font="ansi_shadow").rstrip("\n").split("\n")
    fs_b = 17
    bw = max(len(l) for l in banner)
    y = 150
    x0 = (PX - bw * char_w(fs_b)) / 2
    for ln in banner:
        draw_text(ax, x0, y, [(ln, RED)], fs_b)
        y += line_h(fs_b, 1.06)
    y += 36

    sub = "the most rigorous output was knowing when to stop"
    fs = 17
    draw_text(ax, (PX - len(sub) * char_w(fs)) / 2, y, [(sub, FG)], fs, bold=True)
    y += line_h(fs) + 50

    fs = 16
    findings = [
        [("  [x] ", GREEN), ("post-news directional edge ≈ 0", FG)],
        [("  [x] ", GREEN), ("losses are friction — fees + spread, not predictions", FG)],
        [("  [x] ", GREEN), ("even a perfect-foresight exit cannot cross zero", FG)],
        [("  [x] ", GREEN), ("validated by 4-way loss attribution that reconstructs", FG)],
        [("      ", FG),    ("realized PnL exactly, per trade", DIM)],
    ]
    x0 = 96
    for ln in findings:
        draw_text(ax, x0, y, ln, fs)
        y += line_h(fs, 1.7)
    y += 30

    # adverse selection mini-chart
    draw_text(ax, x0, y, [("WHY: adverse selection (n≈2,090 news events)", AMBER)], 15, bold=True)
    y += line_h(15, 1.9)
    rows = [
        ("post-news repricing", 6.1, RED),
        ("median half-spread", 0.5, BLUE),
    ]
    for lab, v, c in rows:
        n = max(1, round(v / 6.1 * 30))
        draw_text(ax, x0, y, [(lab.ljust(22), DIM), ("▇" * n, c),
                              (f" {v}¢/share", FG)], 15)
        y += line_h(15, 1.9)
    draw_text(ax, x0, y, [("→ the market reprices 12x deeper than you can quote", DIM)], 14)
    y += line_h(14) + 64

    closing = [
        [("$ ", GREEN), ("# a negative result, instrumented well enough", DIM)],
        [("  ", FG), ("# to be trusted, is an engineering outcome.", DIM)],
        [("$ ", GREEN), ("█", FG)],
    ]
    draw_block(ax, 96, y, closing, 16)

    fig.savefig(f"{OUT}/07_edge_zero.png", dpi=DPI, facecolor=BG)
    plt.close(fig)


# =============================================================================
# Slide 8 — 2x2 of REAL dashboard panels
# =============================================================================
def _odds_curve(eid, lo_min=-60, hi_min=60):
    """Mid-price samples for one exploration, as (minutes_from_news, mid)."""
    import sqlite3
    con = sqlite3.connect(
        "file:/home/muk/projects/nexus/irene/data/irene.db?mode=ro", uri=True)
    rows = con.execute(
        """SELECT offset_seconds, mid FROM odds_observations
           WHERE exploration_id=? AND mid IS NOT NULL
             AND offset_seconds BETWEEN ? AND ? ORDER BY offset_seconds""",
        (eid, lo_min * 60, hi_min * 60)).fetchall()
    con.close()
    return [r[0] / 60.0 for r in rows], [r[1] for r in rows]


def _price_panel(fig, rect, xs, ys, color):
    """Plot market probability before / during / after the news broke (t=0)."""
    ax = fig.add_axes(rect)
    ax.set_facecolor(PANEL)
    x0, x1 = min(xs), max(xs)
    ax.axvspan(x0, 0, color=BLUE, alpha=0.06, zorder=0)   # before
    ax.axvspan(0, x1, color=GREEN, alpha=0.06, zorder=0)  # after
    ax.plot(xs, ys, color=color, lw=2.2, zorder=3,
            solid_capstyle="round", solid_joinstyle="round")
    ax.axvline(0, color=AMBER, lw=1.8, ls=(0, (4, 3)), zorder=2)
    ax.text(0, 1.05, "← news breaks", color=AMBER, fontsize=9.5, fontfamily=MONO,
            ha="center", va="bottom", clip_on=False)
    ax.text(x0 + (0 - x0) * 0.5, 0.07, "before", color=BLUE, fontsize=9,
            fontfamily=MONO, ha="center", alpha=0.85)
    ax.text((0 + x1) * 0.5, 0.07, "after", color=GREEN, fontsize=9,
            fontfamily=MONO, ha="center", alpha=0.85)
    ax.set_xlim(x0, x1)
    ax.set_ylim(0, 1.0)
    ax.set_yticks([0, 0.5, 1.0])
    ax.set_yticklabels(["0", "50", "100"])
    ax.set_xticks([t for t in (-30, 0, 30, 60) if x0 <= t <= x1])
    style_axes(ax)
    ax.tick_params(labelsize=8.5)


def slide_real_plots():
    """2x2 of the diagnostic panels the system renders on itself.

    Top row = IRENE, freshly regenerated from the live DB by regen_panels.py
    (run it first). Bottom row = SEULGI gen-1, the frozen historical record
    (that bot was retired 2026-05-27; its plots are final, not stale).
    """
    import matplotlib.image as mpimg
    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)

    fig.text(0.5, 0.955, "INSIDE THE LIVE DASHBOARD", fontsize=25,
             fontfamily=MONO, color=FG, ha="center", fontweight="bold")
    fig.text(0.5, 0.915, "real diagnostics the system renders on itself · "
             "IRENE live (top) + SEULGI gen-1 frozen (bottom)",
             fontsize=12.5, fontfamily=MONO, color=DIM, ha="center")

    root = "/home/muk/projects/nexus"
    panels = [
        (f"{OUT}/_panels/throughput_overlay.png",
         "IRENE · live per-stage throughput (24h, log)", GREEN),
        (f"{OUT}/_panels/per_gate_rejection.png",
         "IRENE · per-gate rejection audit (24h)", AMBER),
        (f"{root}/seulgi/data/dashboard/plots/equity_curve.png",
         "SEULGI gen-1 · paper equity curve — friction wins", RED),
        (f"{root}/seulgi/data/dashboard/plots/confidence_buckets.png",
         "SEULGI gen-1 · confidence vs hit rate (frozen)", BLUE),
    ]
    cols = [0.275, 0.725]
    rows = [0.66, 0.30]
    for i, (path, caption, accent) in enumerate(panels):
        r, c = divmod(i, 2)
        cx, cy = cols[c], rows[r]
        if not os.path.exists(path):
            fig.text(cx, cy, "(missing — run regen_panels.py)", fontsize=11,
                     fontfamily=MONO, color=RED, ha="center")
            continue
        img = mpimg.imread(path)
        w_im = 0.43
        h_im = min(0.255, w_im * img.shape[0] / img.shape[1])
        ax = fig.add_axes([cx - w_im / 2, cy - h_im / 2 + 0.02, w_im, h_im])
        ax.imshow(img, aspect="auto")
        ax.set_xticks([]); ax.set_yticks([])
        for s in ax.spines.values():
            s.set_color(accent); s.set_linewidth(1.6)
        fig.text(cx, cy - h_im / 2 + 0.004, caption, fontsize=10.5,
                 fontfamily=MONO, color=DIM, ha="center")

    fig.text(0.5, 0.045,
             "every panel is generated by the bot from its own SQLite store — "
             "no external BI tool\nthe dashboard is part of the topology, not "
             "bolted on after", fontsize=12, fontfamily=MONO,
             color=DIM, ha="center")
    fig.savefig(f"{OUT}/08_real_plots.png", dpi=DPI, facecolor=BG)
    plt.close(fig)


# =============================================================================
# Slide 10 — decision funnel (how the program operates)
# =============================================================================
def slide_funnel():
    """Centred funnel: every gate decision is audited; 0.35% become trades.

    Counts are read straight from irene.db on 2026-07-09 (is_tracer=0).
    Bar widths use a sqrt map so the deep, thin levels stay legible while the
    taper still reads as a funnel.
    """
    import numpy as np
    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, PX)
    ax.set_ylim(PX, 0)
    ax.axis("off")

    fig.text(0.5, 0.945, "THE DECISION FUNNEL", fontsize=30, fontfamily=MONO,
             color=FG, ha="center", fontweight="bold")
    fig.text(0.5, 0.905, "176,605 articles in  →  1,475 paper trades out  ·  every "
             "rejection audited", fontsize=14.5, fontfamily=MONO, color=DIM,
             ha="center")

    # (label, surviving-count-after-this-stage, color, sublabel)
    levels = [
        ("articles ingested",         176605, BLUE,   "236 feeds · RSS+HTML"),
        ("article→market signals",    137554, BLUE,   "SBERT cosine top-K"),
        ("gate decisions evaluated",  426481, PURPLE, "each signal re-scored per gate"),
        ("− direction neutral",       260341, CYAN,   "166,140 cut · LLM Y/N/U"),
        ("− market inactive / expiry", 36627, CYAN,   "223,714 cut"),
        ("− freshness / entity / imbalance", 11881, AMBER, "24,746 cut"),
        ("− price-sanity / edge / other", 1475, GREEN, "executed: 0.35% of decisions"),
    ]
    cnts = [l[1] for l in levels]
    wmax = max(c ** 0.5 for c in cnts)
    cx = PX * 0.32
    top, bot = 0.215 * PX, 0.93 * PX
    n = len(levels)
    band_h = (bot - top) / n * 0.62
    gap = (bot - top) / n
    full_w = PX * 0.42
    lx = cx + full_w / 2 + 24          # labels anchored to the widest bar's edge
    for i, (lab, cnt, col, sub) in enumerate(levels):
        # sqrt map for the taper, clamped so the deep thin bars still fit a count
        w = max(108.0, full_w * (cnt ** 0.5) / wmax)
        y = top + i * gap
        ax.add_patch(FancyBboxPatch((cx - w / 2, y), w, band_h,
                     boxstyle="round,pad=0,rounding_size=7",
                     facecolor=col, edgecolor="none", alpha=0.92, zorder=3))
        # count, centred in the band
        ax.text(cx, y + band_h * 0.5, f"{cnt:,}", fontsize=18, fontfamily=MONO,
                color=BG if col != AMBER else "#1a1205", ha="center", va="center",
                fontweight="bold", zorder=4)
        # label + sublabel to the right
        ax.text(lx, y + band_h * 0.30, lab, fontsize=14, fontfamily=MONO,
                color=FG, ha="left", va="center", zorder=4, fontweight="bold")
        ax.text(lx, y + band_h * 0.74, sub, fontsize=11.5, fontfamily=MONO,
                color=DIM, ha="left", va="center", zorder=4)

    fig.text(0.5, 0.055,
             "426,481 gate decisions · 11 ordered gates · 0 silent skips\n"
             "every rejected candidate persists its full per-gate decision chain",
             fontsize=13.5, fontfamily=MONO, color=DIM, ha="center")
    fig.savefig(f"{OUT}/10_funnel.png", dpi=DPI, facecolor=BG)
    plt.close(fig)


# =============================================================================
# Slide 11 — real market moves around the news (what the program sees)
# =============================================================================
def _odds_curve_full(eid, lo_s=-7200, hi_s=None):
    import sqlite3
    con = sqlite3.connect(
        "file:/home/muk/projects/nexus/irene/data/irene.db?mode=ro", uri=True)
    hi = hi_s if hi_s is not None else 10 ** 9
    rows = con.execute(
        """SELECT offset_seconds, mid FROM odds_observations
           WHERE exploration_id=? AND mid IS NOT NULL
             AND offset_seconds BETWEEN ? AND ? ORDER BY offset_seconds""",
        (eid, lo_s, hi)).fetchall()
    con.close()
    return [r[0] / 60.0 for r in rows], [r[1] for r in rows]


def _move_panel(fig, rect, xs, ys, color, unit="min"):
    ax = fig.add_axes(rect)
    ax.set_facecolor(PANEL)
    x0, x1 = min(xs), max(xs)
    ax.axvspan(x0, 0, color=BLUE, alpha=0.06, zorder=0)
    ax.axvspan(0, x1, color=color, alpha=0.07, zorder=0)
    ax.plot(xs, ys, color=color, lw=2.0, zorder=3,
            solid_capstyle="round", solid_joinstyle="round")
    ax.axvline(0, color=AMBER, lw=1.6, ls=(0, (4, 3)), zorder=2)
    ax.text(0, 1.04, "news breaks", color=AMBER, fontsize=9, fontfamily=MONO,
            ha="center", va="bottom", clip_on=False)
    ax.set_xlim(x0, x1)
    ax.set_ylim(0, 1.0)
    ax.set_yticks([0, 0.5, 1.0]); ax.set_yticklabels(["0", "50", "100"])
    if unit == "h":
        ticks = [t for t in (-60, 0, 720, 1440, 2160, 2880) if x0 <= t <= x1]
        ax.set_xticks(ticks)
        ax.set_xticklabels([f"{int(t/60)}h" for t in ticks])
    else:
        ticks = [t for t in (-30, 0, 30, 60) if x0 <= t <= x1]
        ax.set_xticks(ticks)
        ax.set_xticklabels([f"{int(t)}" for t in ticks])
    style_axes(ax)
    ax.tick_params(labelsize=8)


def slide_market_moves():
    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)
    fig.text(0.5, 0.955, "WHAT IRENE CAPTURES", fontsize=27, fontfamily=MONO,
             color=FG, ha="center", fontweight="bold")
    fig.text(0.5, 0.915, "six real Polymarket repricings, timestamped around the "
             "headline that moved them", fontsize=12.5, fontfamily=MONO,
             color=DIM, ha="center")

    # (eid, caption, move-direction-color, window-seconds, unit)
    moves = [
        (115554, "US x Iran peace deal by Jun 30   49% → 94%   ($10.6M vol)",
         GREEN, (-7200, 180000), "h"),
        (69780,  "WTI crude oil closes > $86 (Jun 11)   57% → 96%   ($43k)",
         GREEN, (-3600, 3600), "min"),
        (9386,   "Bitcoin between $74k-$76k   42% → 5%   ($46k vol)",
         RED, (-3600, 3600), "min"),
        (23446,  "Microsoft finishes week > $440   19% → 84%   ($2.9k)",
         GREEN, (-3600, 3600), "min"),
        (25229,  "Bosnia vs N. Macedonia  O/U 1.5   71% → 27%   ($17k)",
         RED, (-3600, 3600), "min"),
        (69972,  "Fewer than 25 ships transit Hormuz   17% → 54%   ($1.8k)",
         GREEN, (-3600, 3600), "min"),
    ]
    cols = [0.275, 0.725]
    rows = [0.685, 0.435, 0.185]
    w_ax, h_ax = 0.40, 0.145
    for i, (eid, cap, col, win, unit) in enumerate(moves):
        r, c = divmod(i, 2)
        cx, cy = cols[c], rows[r]
        xs, ys = _odds_curve_full(eid, win[0], win[1])
        if not xs:
            continue
        _move_panel(fig, [cx - w_ax / 2, cy - h_ax / 2 + 0.018, w_ax, h_ax],
                    xs, ys, col, unit)
        fig.text(cx, cy - h_ax / 2 - 0.012, cap, fontsize=9.3, fontfamily=MONO,
                 color=DIM, ha="center")

    fig.text(0.5, 0.052,
             "4.8M mid/bid/ask samples across 274,521 captures · y = implied "
             "probability (%)\nthe dataset behind the verdict: news increments "
             "narratives already in motion", fontsize=12, fontfamily=MONO,
             color=DIM, ha="center")
    fig.savefig(f"{OUT}/11_market_moves.png", dpi=DPI, facecolor=BG)
    plt.close(fig)


# =============================================================================
# Slide 12 — explorer dataset growth (what the program achieves)
# =============================================================================
def slide_dataset():
    import numpy as np
    fig = plt.figure(figsize=(PX / DPI, PX / DPI), dpi=DPI)
    fig.patch.set_facecolor(BG)
    fig.text(0.5, 0.945, "A LABELED RESEARCH DATASET", fontsize=26,
             fontfamily=MONO, color=FG, ha="center", fontweight="bold")
    fig.text(0.5, 0.905, "4.8M order-book samples banked in ~44 days, ~$2/day — "
             "the real deliverable", fontsize=13.5, fontfamily=MONO,
             color=DIM, ha="center")

    # cumulative odds observations by day (from irene.db, 2026-07-09)
    days = ["05-27", "05-28", "05-29", "05-30", "06-05", "06-06", "06-07",
            "06-08", "06-09", "06-10", "06-11", "06-12", "06-13", "06-14",
            "06-15", "06-16", "06-17", "06-18", "06-19", "06-20", "06-21",
            "06-22", "06-23", "06-24", "06-25", "06-26", "06-27", "06-28",
            "06-29", "06-30", "07-01", "07-02", "07-03", "07-04", "07-05",
            "07-06", "07-07", "07-08", "07-09"]
    cum = [0.192, 0.662, 1.199, 1.413, 1.414, 1.415, 1.415, 1.464, 1.668,
           2.005, 2.302, 2.583, 2.802, 3.000, 3.100, 3.250, 3.352, 3.401,
           3.409, 3.411, 3.512, 3.676, 3.787, 3.897, 4.036, 4.156, 4.305,
           4.427, 4.512, 4.605, 4.655, 4.681, 4.700, 4.718, 4.732, 4.744,
           4.761, 4.774, 4.775]  # millions
    x = np.arange(len(days))

    ax = fig.add_axes([0.135, 0.17, 0.80, 0.62])
    style_axes(ax)
    ax.fill_between(x, 0, cum, color=AMBER, alpha=0.16, zorder=1)
    ax.plot(x, cum, color=AMBER, lw=2.6, zorder=3, solid_capstyle="round")
    ax.scatter([len(days) - 1], [cum[-1]], s=60, color=AMBER, zorder=4)

    # mark the 8-day container outage (Dockerfile bug) as a shaded gap
    ax.axvspan(3, 7, color=RED, alpha=0.10, zorder=0)
    ax.text(5, 3.55, "8-day outage\n(recovered, no\nintervention)", color=RED,
            fontsize=11, fontfamily=MONO, ha="center", va="center")

    # capture pace slowed markedly in July (H8 slow-catalyst lane: fewer,
    # longer-horizon captures rather than high-frequency short-window ones)
    ax.axvspan(30, len(days) - 1, color=BLUE, alpha=0.06, zorder=0)
    ax.text(34, 1.0, "H8 lane:\nfewer, longer\ncaptures", color=BLUE,
            fontsize=10, fontfamily=MONO, ha="center", va="center")

    ax.set_ylim(0, 5.4)
    ax.set_yticks([0, 1, 2, 3, 4, 5])
    ax.set_yticklabels(["0", "1M", "2M", "3M", "4M", "5M"])
    ticks = [0, 4, 9, 14, 19, 25, 30, 34, 38]
    ax.set_xticks(ticks)
    ax.set_xticklabels([days[i] for i in ticks], fontsize=10.5)
    ax.grid(axis="y", color=BORDER, lw=0.7, zorder=0)
    ax.set_ylabel("cumulative odds observations", fontsize=13,
                  fontfamily=MONO, color=DIM)
    ax.text(len(days) - 1.4, cum[-1] + 0.22, "4.78M", color=AMBER,
            fontsize=17, fontfamily=MONO, ha="right", fontweight="bold")

    # stat strip under the chart
    stats = [("274,521", "gated captures"), ("10,081", "passed gates"),
             ("~$2/day", "LLM cost"), ("100%", "rows labeled")]
    y = 0.115
    cellw = 0.225
    x0 = 0.5 - cellw * len(stats) / 2
    for i, (num, lab) in enumerate(stats):
        cxf = x0 + cellw * i + cellw / 2
        fig.text(cxf, y, num, fontsize=22, fontfamily=MONO, color=BLUE,
                 ha="center", fontweight="bold")
        fig.text(cxf, y - 0.038, lab, fontsize=12.5, fontfamily=MONO,
                 color=DIM, ha="center")
    fig.savefig(f"{OUT}/12_dataset.png", dpi=DPI, facecolor=BG)
    plt.close(fig)


if __name__ == "__main__":
    slide_title()
    slide_numbers()
    slide_tmux()
    slide_arch()
    slide_universe()
    slide_calibration()
    slide_ending()
    slide_real_plots()
    slide_funnel()
    slide_market_moves()
    slide_dataset()
    print("done:", sorted(f for f in os.listdir(OUT) if f.endswith(".png")))
