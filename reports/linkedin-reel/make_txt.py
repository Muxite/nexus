#!/usr/bin/env python3
"""Plain-text versions of the terminal slides — screenshot them in VS Code.

Box alignment is generated, not hand-drawn, so every line is exact.
Numbers from reports/2026-05-31-resume-achievements.md (verified only).
"""

import os
import pyfiglet

OUT = os.path.dirname(os.path.abspath(__file__))


def banner(text):
    lines = pyfiglet.figlet_format(text, font="ansi_shadow").split("\n")
    return [l for l in lines if l.strip()]


def center_block(lines, width):
    return [(" " * max(0, (width - len(l)) // 2)) + l for l in lines]


def save(name, lines, width):
    body = "\n".join(l.rstrip() for l in lines) + "\n"
    with open(os.path.join(OUT, name), "w") as f:
        f.write(body)
    print(f"{name:24s} {max(len(l) for l in lines):3d} wide  {len(lines):3d} lines")


W = 66

# =============================================================================
# 00 — main hero: trimmed title + dense status board
# =============================================================================
def txt_main():
    iw, ih = 34, 8
    tot = (iw + 2) * 2

    p0 = make_pane("0:news", [
        ("articles", "153,848"),
        ("avg rate", "~2k/day"),
        ("sources", "~120 (was 33)"),
        ("dedup ratio", "12.7x"),
    ], "SBERT 384-dim · 4-label fresh", iw, ih)
    p1 = make_pane("1:universe", [
        ("markets indexed", "100,084"),
        ("distinct events", "22,343"),
        ("events coverage", "99.1%"),
        ("vs gen-1", "9.3x"),
    ], "dual-read /events + /markets", iw, ih)
    p2 = make_pane("2:engine", [
        ("decisions", "62,490"),
        ("llm calls", "76,274"),
        ("parse failures", "0.26%"),
        ("latency p50/p99", "1.5s / 13.8s"),
    ], "every reject: full gate chain", iw, ih)
    p3 = make_pane("3:ledger", [
        ("rows", "10.98M"),
        ("db size", "8.6 GB"),
        ("pnl (paper)", "-$124.88"),
        ("roi (paper)", "-12.49%"),
    ], "edge ≈ 0 · friction-bound", iw, ih)

    L = ["$ jennie status --all"]
    L += center_block(banner("JENNIE"), tot)
    L += center_block(["news → 100k markets → LLM paper trades · SEULGI gen-1 / IRENE gen-2"], tot)
    L.append("")
    for a, b in zip(p0, p1):
        L.append(a + b)
    for a, b in zip(p2, p3):
        L.append(a + b)
    status_l = "[IRENE] 0:news 1:universe 2:engine 3:ledger"
    status_r = "126.9k LOC · 1,397 tests"
    L.append(status_l + " " * (tot - len(status_l) - len(status_r)) + status_r)
    save("00_main.txt", L, tot)


# =============================================================================
# 01 — title
# =============================================================================
def txt_title():
    L = []
    L.append("$ cat MOTD")
    L.append("")
    L += center_block(banner("JENNIE"), W)
    L.append("")
    L += center_block(["a news-driven prediction-market research platform"], W)
    L.append("")
    L += center_block(["100+ news sources → 100k markets → LLM-gated paper trades"], W)
    L += center_block(["gen-1 SEULGI cosine engine · gen-2 IRENE knowledge graph"], W)
    L.append("")
    cells = [("2.5", "months"), ("26", "dev cycles"), ("127k", "LOC"), ("1,397", "tests")]
    cw = 13
    top = "┌" + "┬".join(["─" * cw] * 4) + "┐"
    mid = "│" + "│".join(c[0].center(cw) for c in cells) + "│"
    lab = "│" + "│".join(c[1].center(cw) for c in cells) + "│"
    bot = "└" + "┴".join(["─" * cw] * 4) + "┘"
    L += center_block([top, mid, lab, bot], W)
    L.append("")
    L.append("$ # every decision, gate rejection, LLM call and latency")
    L.append("  # sample — audited in SQLite")
    L.append("$ █")
    save("01_title.txt", L, W)


# =============================================================================
# 02 — numbers
# =============================================================================
def txt_numbers():
    L = []
    L += center_block(banner("SHIPPED"), W)
    L.append("")
    L += center_block(["2026-03-14 → 2026-05-29 · two generations: SEULGI → IRENE"], W)
    L.append("")
    cells = [
        ("126.9k", "lines of Python", "SEULGI 48k · IRENE 65k"),
        ("1,397", "automated tests", "collected by pytest"),
        ("209", "commits", "26 numbered dev cycles"),
        ("10.98M", "result rows", "in one SQLite table"),
        ("76,274", "LLM calls", "0.26% parse-failure rate"),
        ("~18k", "WS subscriptions", "sharded CLOB pool"),
        ("32", "strategy variants", "A/B-tested in production"),
        ("8.6 GB", "production DB", "WAL-mode SQLite, 34 tables"),
    ]
    cw = 30
    grid = ["┌" + "─" * cw + "┬" + "─" * cw + "┐"]
    for r in range(0, len(cells), 2):
        a, b = cells[r], cells[r + 1]
        for i in range(3):
            pa = ("  " + a[i]).ljust(cw)
            pb = ("  " + b[i]).ljust(cw)
            grid.append("│" + pa + "│" + pb + "│")
        if r + 2 < len(cells):
            grid.append("├" + "─" * cw + "┼" + "─" * cw + "┤")
    grid.append("└" + "─" * cw + "┴" + "─" * cw + "┘")
    L += center_block(grid, W)
    save("02_numbers.txt", L, W)


# =============================================================================
# 03 — tmux dashboard
# =============================================================================
def make_pane(title, rows, footer, iw, ih):
    """rows: list of (key, val); returns list of strings, width iw+2."""
    out = ["┌─ " + title + " " + "─" * (iw - len(title) - 3) + "┐"]
    body = []
    for k, v in rows:
        pad = iw - 2 - len(k) - len(v)
        body.append("│ " + k + " " * max(1, pad) + v + " │")
    while len(body) < ih - 2:
        body.append("│" + " " * iw + "│")
    # footer on last body line
    body[-1] = "│ " + footer.ljust(iw - 2) + " │"
    out += body
    out.append("└" + "─" * iw + "┘")
    return out


def txt_tmux():
    iw, ih = 34, 11
    p0 = make_pane("0:gates", [
        ("stage 1", "market_open"),
        ("stage 2", "dkg_threshold"),
        ("stage 3", "llm_batch"),
        ("every reject", "full audit row"),
        ("silent skips", "0"),
        ("migration", "+2,940 / -179"),
    ], "configurable gate pipeline", iw, ih)
    p1 = make_pane("1:verify", [
        ("tests collected", "1,397"),
        ("irene / seulgi", "775 / 509"),
        ("replay suite", "0 row-id gaps"),
        ("tracer templates", "15"),
        ("invariant", "tracer≠trade"),
        ("test containers", "ephemeral"),
    ], "replay prod DB read-only", iw, ih)
    p2 = make_pane("2:latency", [
        ("p50 ▇", "1,506 ms"),
        ("p95 ▇▇▇▇", "7,732 ms"),
        ("p99 ▇▇▇▇▇▇▇▇", "13,837 ms"),
        ("samples", "n=190"),
        ("breakdown", "7-step JSON"),
        ("", ""),
    ], "fetch→ai→resolve→insert", iw, ih)
    p3 = make_pane("3:llm", [
        ("calls (audited)", "76,274"),
        ("parse failures", "0.26% / 2.93%"),
        ("ensemble", "N=3, 2/3 vote"),
        ("disagree →", "hard NEUTRAL"),
        ("budget watch", "OK [flag clear]"),
        ("cache hit (replay)", "78%"),
    ], "pause-before-spend governor", iw, ih)

    L = ["# tmux: IRENE — live telemetry (real numbers, simplified layout)", ""]
    for a, b in zip(p0, p1):
        L.append(a + b)
    for a, b in zip(p2, p3):
        L.append(a + b)
    total = (iw + 2) * 2
    status_l = "[IRENE] 0:gates 1:verify 2:latency* 3:llm"
    status_r = '"gen-2" 09:41'
    L.append("")
    L.append(status_l + " " * (total - len(status_l) - len(status_r)) + status_r)
    save("03_dashboard.txt", L, total)


# =============================================================================
# 04 — architecture
# =============================================================================
def txt_arch():
    Wb = 48
    bus_top = "╔" + " async pub/sub bus ".center(Wb, "═") + "╗"
    bus_mid = "║" + " strict JSON · backpressure · fault isolation".ljust(Wb) + "║"
    bus_bot = "╚═╤══════╤═══════╤════════╤════════╤═════════════╝"
    L = [
        "ONE BUS, EVERY STAGE AUDITED",
        "IRENE (gen-2) · event-driven · contract-first pub/sub",
        "",
        "~120 news sources              Polymarket CLOB",
        "(scraper-v2, FastAPI)          (sharded WS pool, ~18k tokens)",
        "      │                              │",
        "      ▼                              ▼",
        bus_top,
        bus_mid,
        bus_bot,
        "  │      │       │        │        │",
        "  ▼      ▼       ▼        ▼        ▼",
        "ingest→dedupe→ match → gates → paper trade",
        "        SBERT    cosine   market_open",
        "        Chroma   top-K    →dkg_threshold",
        "                        →llm_batch",
        "                          │",
        "            every reject ─┘ full per-gate",
        "            audit row — no silent skips",
        "                   │",
        "                   ▼",
        "┌────────────────────────────────────────────────┐",
        "│ SQLite (WAL) — 34 tables, atomic batch writes  │",
        "│ decisions · gates · llm calls · latency · stats│",
        "└───────────────────────┬────────────────────────┘",
        "                        ▼",
        "      dashboard — 15 pages · 10 plot modules",
    ]
    L = ["  " + l for l in L]
    save("04_architecture.txt", L, 66)


# =============================================================================
# 07 — edge = 0
# =============================================================================
def txt_edge():
    L = []
    L.append("# reports/2026-05-29-trading-viability-synthesis.md")
    L.append("")
    L += center_block(banner("EDGE = 0"), W)
    L.append("")
    L += center_block(["the most rigorous output was knowing when to stop"], W)
    L.append("")
    L += [
        "  [x] post-news directional edge ≈ 0",
        "  [x] losses are friction — fees + spread, not predictions",
        "  [x] even a perfect-foresight exit cannot cross zero",
        "  [x] validated by 4-way loss attribution that reconstructs",
        "      realized PnL exactly, per trade",
        "",
        "  WHY: adverse selection (n≈2,090 news events)",
        "  post-news repricing  ▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 6.1¢",
        "  median half-spread   ▇▇ 0.5¢",
        "  → the market reprices 12x deeper than you can quote",
        "",
        "$ # a negative result, instrumented well enough",
        "  # to be trusted, is an engineering outcome.",
        "$ █",
    ]
    save("07_edge_zero.txt", L, W)


if __name__ == "__main__":
    txt_main()
    # txt_title() retired — 00_main covers banner + tagline + stats
    txt_numbers()
    txt_tmux()
    txt_arch()
    txt_edge()
