#!/usr/bin/env python3
"""Visualizations of irene's Dynamic Knowledge Graph (DKG).

Reads irene.db read-only and renders 2048x2048 px graphs in two layouts:

  radial   inner disk  = events, middle ring = entities (sectors by type),
           outer ring  = markets.  Edges bundle toward the center.
  linear   three stacked bands (events / entities / markets); types form
           contiguous x-stripes.  Edges bundle toward the entity baseline.

Two themes only (the survivors of the earlier sweep):
  DARK   typed colors on near-black, bold.            -> 09_dkg_dark_*.png
  INK    dark lines on warm paper, monochrome.        -> 09_dkg_ink_*.png

Geometry for each layout is computed once; each VARIANT re-renders it with a
theme + boldness. Text is deliberately sparse: title, one subtitle line, a
compact legend, a single footer stat line, two anchor stats. The graph carries
the rest.
"""

import math
import os
import sqlite3
from collections import defaultdict

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection

DB = "/home/muk/projects/nexus/irene/data/irene.db"
HERE = os.path.dirname(os.path.abspath(__file__))

PX = 2048
DPI = 200
FIG_IN = PX / DPI          # 10.24 in
FS = FIG_IN / 40.0         # font scale relative to the old 40in renders

MONO = "DejaVu Sans Mono"

# ---- curated headline numbers (universe-scale, not just what's in this DB) ---
SUBTITLE = "news  →  230,448 markets  →  live LLM paper trades"

# ---- themes -----------------------------------------------------------------
# DARK: typed colors, bold, near-black. Refreshed palette — COUNTRY moved to a
# pink so it no longer collides with EVENT's coral, market amber separated from
# the ECON yellow.
DARK = dict(
    name="dark", bg="#0a0e16", fg="#e6edf3", dim="#7d8590", ring="#1b2230",
    event="#ffffff", market="#f59e0b", mono=None,
    types={
        "PERSON": "#4fa8ff", "ORG": "#b08cff", "COMPANY": "#3ad07a",
        "EVENT": "#ff6b6b", "ASSET": "#2dd4bf", "ECON_INDICATOR": "#facc15",
        "COUNTRY": "#f472b6",
    },
)
# INK: monochrome dark lines on warm paper. The "light mode with dark lines"
# look that read well. No type hues — single ink color throughout.
INK = dict(
    name="ink", bg="#f3efe6", fg="#1a1d22", dim="#6b6256", ring="#d8d0c0",
    event="#1a1d22", market="#1a1d22", mono="#15181d",
    types={k: "#15181d" for k in DARK["types"]},
)

# ---- variants: 2 themes x 2 layouts x 2 boldness = 8 ------------------------
DEFAULTS = dict(theme=DARK, layout="radial", lw=1.0, alpha=1.0,
                node=1.0, glow=True, glow_scale=1.0, note="")
# No glow/bloom on any variant. Boldness comes from line weight; points stay
# small so nodes and connections carry equal emphasis.
VARIANTS = [
    dict(name="dark_radial",      theme=DARK, layout="radial", lw=1.5, alpha=1.10, node=1.0,  glow=False, note="dark · radial · bold"),
    dict(name="dark_radial_max",  theme=DARK, layout="radial", lw=2.4, alpha=1.50, node=1.15, glow=False, note="dark · radial · boldest"),
    dict(name="dark_linear",      theme=DARK, layout="linear", lw=1.5, alpha=1.10, node=1.0,  glow=False, note="dark · linear · bold"),
    dict(name="dark_linear_max",  theme=DARK, layout="linear", lw=2.4, alpha=1.50, node=1.15, glow=False, note="dark · linear · boldest"),
    dict(name="ink_radial",       theme=INK,  layout="radial", lw=1.3, alpha=1.05, node=1.0,  glow=False, note="ink · radial"),
    dict(name="ink_radial_max",   theme=INK,  layout="radial", lw=2.1, alpha=1.40, node=1.1,  glow=False, note="ink · radial · bold"),
    dict(name="ink_linear",       theme=INK,  layout="linear", lw=1.3, alpha=1.05, node=1.0,  glow=False, note="ink · linear"),
    dict(name="ink_linear_max",   theme=INK,  layout="linear", lw=2.1, alpha=1.40, node=1.1,  glow=False, note="ink · linear · bold"),
]

# ---- experimental ring-order variants ----------------------------------------
# Same dark/radial/bold rendering as dark_radial, but with which data type
# occupies which ring reshuffled. `order` is (outer, mid, inner); layout_radial
# only cares about the radius band each type gets, so the angle-placement
# logic (entity sectors -> event angle from its entities -> market angle from
# its event) is untouched — just the physical ring each layer sits in changes.
ORDER_VARIANTS = [
    dict(name="ord_mkt_news_ent", theme=DARK, layout="radial",
         order=("market", "event", "entity"), lw=1.5, alpha=1.10, node=1.0,
         glow=False, note="dark · radial · outer=markets, mid=news, inner=entities"),
    dict(name="ord_news_ent_mkt", theme=DARK, layout="radial",
         order=("event", "entity", "market"), lw=1.5, alpha=1.10, node=1.0,
         glow=False, note="dark · radial · outer=news, mid=entities, inner=markets"),
]

# Market edges (mk) render at a FIXED "boldest"-equivalent weight, deliberately
# ignoring the variant's own lw_scale/alpha_scale — if it scaled WITH the
# variant, the boldest variant would compound both multipliers and wash the
# outer band out into a solid glow. Inner edges (ee/ve/vv) keep scaling with
# the variant as before (mult=1.0, i.e. unchanged, "normal" bold/boldest
# behavior), so only the market band is pinned to the bold end.
KIND_STYLE = {
    "mk": dict(fixed_lw=2.0, fixed_alpha=1.40),
    "ve": dict(mult_lw=1.0, mult_alpha=1.0),
    "vv": dict(mult_lw=1.0, mult_alpha=1.0),
    "ee": dict(mult_lw=1.0, mult_alpha=1.0),
}

# node base sizes (points^2) tuned for 2048px — small, so points don't dominate
NODE_K = 0.095

rng = np.random.default_rng(26)  # fixed so variants share geometry


# ---- geometry helpers -------------------------------------------------------
def circmean(angles):
    s, c = np.sin(angles).sum(), np.cos(angles).sum()
    return math.atan2(s, c) % (2 * math.pi)


def bezier_radial(p0, p1, pull, n=14):
    """Quadratic bezier, control = midpoint pulled toward the origin."""
    mid = (p0 + p1) / 2.0
    ctrl = mid * pull
    t = np.linspace(0.0, 1.0, n)[:, None]
    return (1 - t) ** 2 * p0 + 2 * t * (1 - t) * ctrl + t ** 2 * p1


def bezier_linear(p0, p1, sag, n=14):
    """Quadratic bezier, control pulled toward the y=0 (entity) baseline."""
    mid = (p0 + p1) / 2.0
    ctrl = np.array([mid[0], mid[1] * (1.0 - sag)])
    t = np.linspace(0.0, 1.0, n)[:, None]
    return (1 - t) ** 2 * p0 + 2 * t * (1 - t) * ctrl + t ** 2 * p1


def polylines_to_segments(lines):
    segs = [np.stack([ln[:-1], ln[1:]], axis=1) for ln in lines]
    return np.concatenate(segs) if segs else np.empty((0, 2, 2))


# ---- load + degrees ---------------------------------------------------------
def load_graph():
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    entities = con.execute("SELECT id, entity_type FROM entities").fetchall()
    facts = con.execute(
        "SELECT subj_entity_id, obj_entity_id, event_id FROM facts").fetchall()
    events = con.execute("SELECT id, support_count FROM events").fetchall()
    eedges = con.execute(
        "SELECT from_event_id, to_event_id FROM event_edges").fetchall()
    mlinks = con.execute(
        """SELECT market_condition_id, event_id, MAX(final_score)
           FROM market_links GROUP BY market_condition_id""").fetchall()
    n_links_total = con.execute("SELECT COUNT(*) FROM market_links").fetchone()[0]
    con.close()

    ent_deg, ev_deg = defaultdict(int), defaultdict(int)
    for s, o, ev in facts:
        if s is not None:
            ent_deg[s] += 1
        if o is not None:
            ent_deg[o] += 1
        if ev is not None:
            ev_deg[ev] += 1
    for a, b in eedges:
        ev_deg[a] += 1
        ev_deg[b] += 1
    for m, ev, sc in mlinks:
        ev_deg[ev] += 1

    by_type = defaultdict(list)
    for eid, etype in entities:
        by_type[etype].append(eid)
    type_order = sorted(by_type, key=lambda t: -len(by_type[t]))

    mk_score = {m: (sc or 0.5) for m, ev, sc in mlinks}

    return dict(
        entities=entities, facts=facts, events=events, eedges=eedges,
        mlinks=mlinks, ent_type=dict(entities), by_type=by_type,
        type_order=type_order, ent_deg=ent_deg, ev_deg=ev_deg,
        mk_score=mk_score, n_ent=len(entities), n_links_total=n_links_total,
    )


# ---- radial layout ----------------------------------------------------------
# Bands pushed outward (circle nearly fills the frame) and kept thin, so the
# wide gaps between rings let the bundled connections read as strongly as the
# nodes. Tight gaussian std keeps few items on the hard band edges.
#
# Which data type sits in which ring is configurable (see `order` below) —
# the three band radii are generic slots, not tied to a data type by name.
BAND_INNER = (0.07, 0.26)
BAND_MID = (0.66, 0.82)
BAND_OUTER = (1.10, 1.26)
DEFAULT_ORDER = ("market", "entity", "event")  # outer, mid, inner


def _gauss_r(mid, std, lo, hi):
    """Gaussian radius about mid, clipped to a generous margin around the
    nominal band — items cloud toward the band centre and thin out gradually,
    with the clip only there to stop rare tail draws from crossing into a
    neighboring ring, not to enforce a hard edge. Wide std + wide margin is
    what makes each ring read as an organic cloud instead of a disc with a
    fixed radius cutoff."""
    return float(np.clip(rng.normal(mid, std), lo, hi))


def layout_radial(G, order=DEFAULT_ORDER):
    """order: (outer_type, mid_type, inner_type), each in {market, entity, event}."""
    bands = dict(zip(order, (BAND_OUTER, BAND_MID, BAND_INNER)))
    ENT_LO, ENT_HI = bands["entity"]
    EV_LO, EV_HI = bands["event"]
    MK_LO, MK_HI = bands["market"]

    ent_deg, ev_deg = G["ent_deg"], G["ev_deg"]
    by_type, type_order, n_ent = G["by_type"], G["type_order"], G["n_ent"]
    gap = 0.05  # wider sector gaps -> less dense, clearer type stripes
    ent_mid, ent_w = (ENT_LO + ENT_HI) / 2, ENT_HI - ENT_LO

    ent_angle, ent_r = {}, {}
    cursor = -math.pi / 2
    span_avail = 2 * math.pi - gap * len(type_order)
    for etype in type_order:
        ids = list(by_type[etype])
        span = span_avail * len(ids) / n_ent
        ids.sort(key=lambda i: ent_deg.get(i, 0))
        ent_order = ids[::2] + ids[1::2][::-1]  # heavy nodes mid-sector
        for k, eid in enumerate(ent_order):
            ent_angle[eid] = cursor + span * (k + 0.5) / len(ent_order) \
                + rng.normal(0, span * 0.012)
            # heavy nodes pulled a little inward; everyone scattered gaussian
            d = ent_deg.get(eid, 0)
            shift = -0.28 * ent_w * min(1.0, math.log1p(d) / 6.0)
            ent_r[eid] = _gauss_r(ent_mid + shift, ent_w * 0.46,
                                  max(0.0, ENT_LO - 0.40 * ent_w), ENT_HI + 0.40 * ent_w)
        cursor += span + gap

    ev_ent = defaultdict(list)
    for s, o, ev in G["facts"]:
        if ev is None:
            continue
        if s in ent_angle:
            ev_ent[ev].append(ent_angle[s])
        if o is not None and o in ent_angle:
            ev_ent[ev].append(ent_angle[o])

    ev_mid, ev_w = (EV_LO + EV_HI) / 2, EV_HI - EV_LO
    ev_angle, ev_r = {}, {}
    for evid, support in G["events"]:
        angs = ev_ent.get(evid)
        # Wide jitter around the circular mean — a tiny std here collapses
        # events into thin angular streaks that match each entity sector's
        # center exactly, so the news band reads as a few sharp spokes
        # instead of a cloud of individually visible points. This keeps the
        # loose correlation to "which sector this event's entities live in"
        # (so the bridge coloring still makes sense) without the hard streak.
        a = (circmean(np.array(angs)) + rng.normal(0, 0.16)) if angs \
            else rng.uniform(0, 2 * math.pi)
        ev_angle[evid] = a
        d = ev_deg.get(evid, 0) + (support or 0)
        shift = -0.30 * ev_w * min(1.0, math.log1p(d) / 5.0)
        ev_r[evid] = _gauss_r(ev_mid + shift, ev_w * 0.42,
                              max(0.01, EV_LO - 0.40 * ev_w), EV_HI + 0.40 * ev_w)

    mk_mid = (MK_LO + MK_HI) / 2
    mk_w = MK_HI - MK_LO
    mk_angle, mk_r = {}, {}
    for m, ev, sc in G["mlinks"]:
        mk_angle[m] = ev_angle.get(ev, rng.uniform(0, 2 * math.pi)) \
            + rng.normal(0, 0.03)
        mk_r[m] = _gauss_r(mk_mid, mk_w * 0.46, max(0.01, MK_LO - 0.40 * mk_w),
                           MK_HI + 0.40 * mk_w)

    def xy(a, r):
        return np.array([r * math.cos(a), r * math.sin(a)])

    return dict(
        ent_xy={i: xy(ent_angle[i], ent_r[i]) for i in ent_angle},
        ev_xy={i: xy(ev_angle[i], ev_r[i]) for i in ev_angle},
        mk_xy={m: xy(mk_angle[m], mk_r[m]) for m in mk_angle},
        guides=[np.mean([EV_LO, EV_HI]),
                np.mean([ENT_LO, ENT_HI]),
                np.mean([MK_LO, MK_HI])],
    )


# ---- linear layout ----------------------------------------------------------
X_LO, X_HI = -1.32, 1.32
EV_Y, ENT_Y, MK_Y = -0.96, 0.0, 0.96


def layout_linear(G):
    ent_deg, ev_deg = G["ent_deg"], G["ev_deg"]
    by_type, type_order, n_ent = G["by_type"], G["type_order"], G["n_ent"]
    gap = 0.05
    width = (X_HI - X_LO)

    ent_x, ent_y = {}, {}
    cursor = X_LO
    span_avail = width - gap * (len(type_order) - 1)
    for etype in type_order:
        ids = list(by_type[etype])
        span = span_avail * len(ids) / n_ent
        ids.sort(key=lambda i: ent_deg.get(i, 0))
        order = ids[::2] + ids[1::2][::-1]  # heavy nodes mid-stripe
        for k, eid in enumerate(order):
            ent_x[eid] = cursor + span * (k + 0.5) / len(order) \
                + rng.normal(0, span * 0.015)
            # thin gaussian band about the baseline
            ent_y[eid] = ENT_Y + rng.normal(0, 0.085)
        cursor += span + gap

    ev_ent = defaultdict(list)
    for s, o, ev in G["facts"]:
        if ev is None:
            continue
        if s in ent_x:
            ev_ent[ev].append(ent_x[s])
        if o is not None and o in ent_x:
            ev_ent[ev].append(ent_x[o])

    ev_x, ev_y = {}, {}
    for evid, support in G["events"]:
        xs = ev_ent.get(evid)
        ev_x[evid] = (np.mean(xs) + rng.normal(0, 0.02)) if xs \
            else rng.uniform(X_LO, X_HI)
        ev_y[evid] = EV_Y + rng.normal(0, 0.085)

    mk_x, mk_y = {}, {}
    for m, ev, sc in G["mlinks"]:
        mk_x[m] = ev_x.get(ev, rng.uniform(X_LO, X_HI)) + rng.normal(0, 0.025)
        mk_y[m] = MK_Y + rng.normal(0, 0.085)

    return dict(
        ent_xy={i: np.array([ent_x[i], ent_y[i]]) for i in ent_x},
        ev_xy={i: np.array([ev_x[i], ev_y[i]]) for i in ev_x},
        mk_xy={m: np.array([mk_x[m], mk_y[m]]) for m in mk_x},
        guides=[EV_Y, ENT_Y, MK_Y],
    )


# ---- edges ------------------------------------------------------------------
def build_edges(G, P, theme, layout):
    """(color, base_alpha, base_lw) -> [polyline]."""
    ent_xy, ev_xy, mk_xy = P["ent_xy"], P["ev_xy"], P["mk_xy"]
    ent_type, tcol, mono = G["ent_type"], theme["types"], theme["mono"]
    groups = defaultdict(list)
    n_edges_total = 0

    def col(c):
        return mono if mono else c

    # Mono (ink) themes render every group in the same single ink color, so
    # there's no hue separation between overlapping groups the way DARK's
    # typed colors give it — the same edge density reads noticeably bolder,
    # so mono keeps a smaller fraction of an already-subsampled set.
    mono_keep_scale = 0.6 if mono else 1.0

    def keep(*ids, pct):
        """Deterministic subsample (same edges every render, no RNG state
        shared with layout). At this edge density, even alpha=0.01 saturates
        to a solid wedge once a few thousand near-coincident lines stack in
        the same few pixels — so the density itself has to come down, not
        just the opacity. `pct` in [0, 100]."""
        return (hash(ids) % 1000) < pct * mono_keep_scale * 10

    # Bundling pulls are kept well away from a single collapse point (the
    # literal origin for radial, the y=0 baseline for linear). With 100k+
    # edges, anything that funnels every curve through the same few pixels
    # stacks up to a solid blob no matter how faint each individual line is —
    # so f_ee/f_vv (the two groups with the tightest pulls) are loosened the
    # most; they still read as "bundled toward the core", just over a wider
    # hub instead of a point.
    if layout == "radial":
        def f_ee(p0, p1):  # entity-entity — kept local to the ring, see below
            return bezier_radial(p0, p1, 0.78)
        def f_ve(p0, p1):  # event-entity: near-straight radial spoke
            return bezier_radial(p0, p1, 0.90)
        def f_vv(p0, p1):  # event-event
            return bezier_radial(p0, p1, 0.50)
        def f_mk(p0, p1):  # event-market
            return bezier_radial(p0, p1, 0.72)
    else:
        def f_ee(p0, p1):
            return bezier_linear(p0, p1, 0.03)
        def f_ve(p0, p1):  # straight riser, news band -> entity band
            return bezier_linear(p0, p1, 0.10)
        def f_vv(p0, p1):
            return bezier_linear(p0, p1, 0.09)
        def f_mk(p0, p1):
            return bezier_linear(p0, p1, 0.45)

    # group key = (kind, color, alpha, lw, zorder). `kind` lets render() apply
    # a different boldness multiplier per edge type (see KIND_STYLE) — market
    # edges read closer to the "boldest" variant's weight, entity/event edges
    # stay closer to "bold", regardless of which overall variant is rendered.
    #
    # ee and ve used to share the same per-type color and a similar reach
    # toward center, so they blended into one indistinguishable cone — you
    # couldn't tell "this entity connects to news" from "these two entities
    # connect to each other." Now they're deliberately split: ve keeps the
    # vivid per-type color and is the one thing that reaches all the way from
    # the core to the ring (the "bridge", brighter and heavier, lifted above
    # the rest). ee is recolored to a flat neutral dim tone and its bundling
    # pull keeps it hugging the entity ring rather than reaching inward — so
    # it reads as quiet local texture instead of competing with the bridge
    # for the same color and the same visual space. Base alphas are
    # deliberately low — these get multiplied by tens of thousands of
    # overlapping lines right where the bundles converge, so anything much
    # higher saturates to a solid core regardless of the per-line value.
    for s, o, ev in G["facts"]:
        if s in ent_xy and o is not None and o in ent_xy:
            n_edges_total += 1
            if not keep(s, o, pct=14):
                continue
            groups[("ee", col(theme["dim"]), 0.026, 0.22, 1)].append(
                f_ee(ent_xy[s], ent_xy[o]))
    for s, o, ev in G["facts"]:
        if ev in ev_xy and s in ent_xy:
            n_edges_total += 1
            if not keep(ev, s, pct=24):
                continue
            c = col(tcol.get(ent_type.get(s), theme["dim"]))
            groups[("ve", c, 0.085, 0.42, 2)].append(f_ve(ev_xy[ev], ent_xy[s]))
    for a, b in G["eedges"]:
        if a in ev_xy and b in ev_xy:
            n_edges_total += 1
            if not keep(a, b, pct=22):
                continue
            groups[("vv", col(tcol["ORG"]), 0.05, 0.26, 1)].append(f_vv(ev_xy[a], ev_xy[b]))
    # Markets vastly outnumber events (~230k markets -> ~60k events), so every
    # market's edge funnels into the same handful of event positions — the
    # linear layout's market band in particular has no angular spread to
    # break that up, so it needs a lower keep rate than the other groups.
    mk_a, mk_w = (0.028, 0.20) if layout == "linear" else (0.040, 0.24)
    mk_pct = 22 if layout == "linear" else 32
    for m, ev, sc in G["mlinks"]:
        if ev in ev_xy and m in mk_xy:
            n_edges_total += 1
            if not keep(m, ev, pct=mk_pct):
                continue
            groups[("mk", col(theme["market"]), mk_a, mk_w, 1)].append(f_mk(ev_xy[ev], mk_xy[m]))
    return groups, n_edges_total


# ---- chrome (title / legend / footer) --------------------------------------
def draw_legend(ax, G, theme, layout, order=DEFAULT_ORDER):
    """Band legend in the top-right corner: which band is which, with counts.

    Reads outer -> inner so it mirrors the eye travelling into the graph.
    `order` is (outer_type, mid_type, inner_type) — same tuple passed to
    layout_radial — so the legend always matches whichever ring layout was
    actually rendered instead of assuming the default markets/entities/news
    arrangement.

    Fixed-width monospace columns keep the three rows aligned like a table.
    The entities row is drawn in many type colours, so its swatch is a
    rainbow bar rather than a flat dot, wherever that row lands.
    """
    n_ev, n_mk = len(G["events"]), G["n_mk"]
    pos = {"radial": ("outer", "middle", "center"),
           "linear": ("top", "middle", "bottom")}[layout]
    type_meta = {
        "market": ("markets", n_mk, theme["market"], False),
        "entity": ("entities", G["n_ent"], theme["fg"], True),
        "event": ("news", n_ev, theme["event"], False),
    }
    # (position, label, count, color, rainbow?)
    rows = [(p, *type_meta[t]) for p, t in zip(pos, order)]
    # rainbow swatch follows a warm -> cool spectrum across the entity types
    rainbow_cols = [theme["types"][k] for k in
                    ("EVENT", "ECON_INDICATOR", "COMPANY", "ASSET",
                     "PERSON", "ORG", "COUNTRY")]
    # y0 matches the title's own baseline so the legend's top row sits flush
    # with the top edge instead of leaving a gap above it in the corner.
    x0, y0, dy = 0.75, 1.405, 0.082
    sx = x0 + 0.015                       # swatch centre
    for i, (where, label, count, c, rainbow) in enumerate(rows):
        if theme["mono"]:
            c = theme["mono"]  # monochrome themes: swatches match the ink
        y = y0 - i * dy
        if rainbow and not theme["mono"]:
            xs = np.linspace(sx - 0.025, sx + 0.025, len(rainbow_cols) + 1)
            segs = [[(xs[j], y), (xs[j + 1], y)] for j in range(len(rainbow_cols))]
            ax.add_collection(LineCollection(
                segs, colors=rainbow_cols, linewidths=11,
                capstyle="butt", zorder=9))
        else:
            ax.scatter([sx], [y], s=95, c=c, linewidths=0, zorder=9)
        ax.text(x0 + 0.065, y, f"{where + ':':<8}{label:<9}{count:>7,}",
                color=theme["fg"], fontsize=42 * FS, fontfamily=MONO,
                ha="left", va="center", zorder=9)


def draw_chrome(ax, G, theme, n_edges):
    accent = theme["market"] if theme["mono"] is None else theme["mono"]
    # title hugs the top edge, left-aligned; the brand line sits directly
    # below it. The band legend takes the top-right corner instead, so the
    # whole top edge reads left (title/brand) vs right (legend) rather than
    # center vs corners. Subtitle hugs the bottom edge — center is freed for
    # the graph.
    ax.text(-1.45, 1.405, "DYNAMIC KNOWLEDGE GRAPH", color=theme["fg"],
            fontsize=116 * FS, fontfamily=MONO, fontweight="bold",
            ha="left", va="center", zorder=9)
    ax.text(-1.45, 1.31, "NEXUS · IRENE · GEN-2", color=accent,
            fontsize=40 * FS, fontfamily=MONO, fontweight="bold",
            ha="left", va="center", zorder=9, alpha=0.95)

    # bottom edge: pipeline subtitle centered, two corner stats flanking
    ax.text(0, -1.405, SUBTITLE, color=theme["types"]["ASSET"]
            if theme["mono"] is None else accent,
            fontsize=58 * FS, fontfamily=MONO, fontweight="bold",
            ha="center", va="center", zorder=9)
    ax.text(-1.45, -1.30, f"{len(G['facts']):,} facts", color=theme["dim"],
            fontsize=40 * FS, fontfamily=MONO, ha="left", va="center", zorder=9)
    ax.text(1.45, -1.30, f"{n_edges:,} edges", color=theme["dim"],
            fontsize=40 * FS, fontfamily=MONO, ha="right", va="center", zorder=9)


# ---- render -----------------------------------------------------------------
def render(G, P, cfg):
    c = {**DEFAULTS, **cfg}
    name, theme, layout = c["name"], c["theme"], c["layout"]
    lw_scale, alpha_scale, node_scale = c["lw"], c["alpha"], c["node"]
    glow, glow_scale, mono = c["glow"], c["glow_scale"], theme["mono"]
    order = c.get("order", DEFAULT_ORDER)
    # Whichever type ends up in the innermost band is the one crammed into
    # the smallest area — that's what needs the low-alpha treatment to read
    # as a cloud of points instead of a solid disc, not "events" specifically.
    inner_type = order[2] if layout == "radial" else "event"

    edge_groups, n_edges = build_edges(G, P, theme, layout)

    fig = plt.figure(figsize=(FIG_IN, FIG_IN), dpi=DPI)
    fig.patch.set_facecolor(theme["bg"])
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(theme["bg"])
    ax.set_xlim(-1.48, 1.48)
    ax.set_ylim(-1.48, 1.48)
    ax.set_aspect("equal")
    ax.axis("off")

    # band guides
    if layout == "radial":
        th = np.linspace(0, 2 * math.pi, 720)
        for rr in P["guides"]:
            ax.plot(rr * np.cos(th), rr * np.sin(th), color=theme["ring"],
                    lw=0.6, zorder=0)
    else:
        for yy in P["guides"]:
            ax.plot([X_LO - 0.05, X_HI + 0.05], [yy, yy], color=theme["ring"],
                    lw=0.6, zorder=0)

    # edges (zorder carried per group so the event->entity bridge sits on top).
    # Mono (ink) themes render every group in the same ink color, so there's
    # no hue separation to keep overlapping groups from reading as one flat
    # mass the way the typed colors do in DARK — they need a lower ceiling.
    mono_damp = 0.42 if mono else 1.0
    for (kind, color, alpha, lw, z), lines in edge_groups.items():
        style = KIND_STYLE[kind]
        segs = polylines_to_segments(lines)
        if "fixed_lw" in style:
            final_lw = lw * style["fixed_lw"]
            final_alpha = min(1.0, alpha * style["fixed_alpha"] * mono_damp)
        else:
            final_lw = lw * lw_scale * style["mult_lw"]
            final_alpha = min(1.0, alpha * alpha_scale * style["mult_alpha"] * mono_damp)
        ax.add_collection(LineCollection(
            segs, colors=color, alpha=final_alpha, linewidths=final_lw, zorder=z))

    # nodes
    def draw_nodes(xys, sizes, color, zorder, alpha=None):
        if mono:
            color = mono
        pts = np.array(xys)
        s = np.array(sizes) * node_scale * NODE_K
        if glow:
            ax.scatter(pts[:, 0], pts[:, 1], s=s * 5.0 * glow_scale, c=color,
                       alpha=min(0.9, 0.09 * glow_scale), linewidths=0,
                       zorder=zorder)
        base_alpha = 0.90 if glow else 0.95
        ax.scatter(pts[:, 0], pts[:, 1], s=s, c=color,
                   alpha=alpha if alpha is not None else base_alpha,
                   linewidths=0, zorder=zorder + 1)

    # Whichever type is innermost gets a materially lower per-point alpha —
    # tens of thousands of points crammed into the smallest band overplot
    # into a solid disc at normal alpha regardless of point size. Now that
    # the innermost type's angular jitter is much wider (see layout_radial),
    # the same point count is spread over more area, so alpha can come back
    # up some without re-solidifying — the earlier 0.32-0.35 was compensating
    # for both density AND the tight streaking, and streaking is fixed now.
    def alpha_for(kind):
        return (0.55 if not mono else 0.48) if kind == inner_type \
            else (0.90 if glow else 0.95)

    epts = [P["ev_xy"][i] for i, _ in G["events"]]
    esz = [2.2 + 10.0 * math.log1p(G["ev_deg"].get(i, 0)) for i, _ in G["events"]]
    draw_nodes(epts, esz, theme["event"], 3, alpha=alpha_for("event"))

    ent_alpha = alpha_for("entity")
    for etype in G["type_order"]:
        ids = G["by_type"][etype]
        pts = [P["ent_xy"][i] for i in ids]
        sz = [3.0 + 22.0 * math.log1p(G["ent_deg"].get(i, 0)) for i in ids]
        draw_nodes(pts, sz, theme["types"].get(etype, theme["dim"]), 5,
                   alpha=ent_alpha)

    mids = list(P["mk_xy"])
    mpts = [P["mk_xy"][m] for m in mids]
    msz = [1.5 + 10.0 * G["mk_score"][m] ** 2 for m in mids]
    draw_nodes(mpts, msz, theme["market"], 4, alpha=alpha_for("market"))

    draw_legend(ax, G, theme, layout, order=order)
    draw_chrome(ax, G, theme, n_edges)

    out = os.path.join(HERE, f"09_dkg_{name}.png")
    fig.savefig(out, dpi=DPI, facecolor=theme["bg"])
    plt.close(fig)
    sz_mb = os.path.getsize(out) / 1e6
    print(f"  {name:<16} edges={n_edges:,}  ({c['note']})  "
          f"-> {os.path.basename(out)}  {sz_mb:.1f}MB")


def main():
    G = load_graph()
    layouts = {"radial": layout_radial(G), "linear": layout_linear(G)}
    G["n_mk"] = len(layouts["radial"]["mk_xy"])
    n_nodes = G["n_ent"] + len(G["events"]) + G["n_mk"]

    variants = list(VARIANTS)
    for ov in ORDER_VARIANTS:
        geom_key = f"radial_{ov['name']}"
        layouts[geom_key] = layout_radial(G, order=ov["order"])
        variants.append({**ov, "geom": geom_key})

    print(f"nodes={n_nodes:,}  rendering {len(variants)} variants @ {PX}x{PX} ...")
    for cfg in variants:
        geom_key = cfg.get("geom", cfg["layout"])
        render(G, layouts[geom_key], cfg)


if __name__ == "__main__":
    main()
