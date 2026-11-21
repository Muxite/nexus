#!/usr/bin/env python3
"""Sparse DKG background layer for a LinkedIn cover slide.

The full DKG render (122k+ nodes) is the most striking asset in this repo,
but it's too dense to sit behind readable text. This produces a much sparser
sample of the same graph — same radial geometry, same DARK palette family,
just breathable — with every scrap of text/legend/chrome stripped out, so a
cover slide's own headline text can be composited on top of it.

Design choices (see reports/linkedin-reel-2026-08/ORDER.md for the slide
that uses this):
  - ~1,750 nodes (500 entities + 800 events + 450 markets), chosen to read
    as "constellation" rather than "mesh" — about 1.5% of the live graph's
    122k nodes. Sampled with a fixed seed so it's reproducible.
  - Positions and edges reuse the SAME geometry code as the full render
    (layout_radial / bezier_radial / build_edges from make_dkg_graph_live),
    just fed the sampled subgraph directly — not a full layout recomputed
    at a smaller n — so sector angles and radial bands still correspond
    to the same data, keeping it recognizably the same graph.
  - DARK theme, chosen over INK: a dark cover reads more "hype tech" for a
    LinkedIn post and gives large light-colored headline text somewhere
    to sit with contrast. Colors are desaturated ~35% and alpha values are
    cut to roughly half of the full render's already-conservative alphas,
    so the graph reads as ambient texture, not a competing subject.
  - No title, subtitle, legend, footer stats, or band-guide rings — a pure
    background layer, 2048x2048, transparent-alpha-free (solid near-black
    bg) so it composites cleanly under text added later.

Reads irene.db read-only; never writes to it.
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

DB = os.environ.get(
    "DKG_DB_PATH", "/home/muk/projects/jennie/irene/data/irene.db")
HERE = os.path.dirname(os.path.abspath(__file__))

PX = 2048
DPI = 200
FIG_IN = PX / DPI

# ---- sample sizes -----------------------------------------------------
# Two densities, deliberately decoupled:
#   - NODE sample (this section): dense enough that the three radial bands
#     (events / entities / markets) read as visible ring silhouettes from
#     dot density ALONE, even for a node with no drawn edge. ~4.6% of the
#     live 122k-node graph -- still a sparse constellation, just enough
#     dots per ring to trace its circle.
#   - EDGE sample (see EDGE_*_TARGET below): a much smaller subset of the
#     node sample that actually gets bezier edges drawn. This is what
#     keeps the "starburst" look in check -- at the old 1:2 edge:node
#     ratio, radial edge-bundling toward center dominated the image and
#     it read as a single firework instead of a graph, because there
#     weren't enough undrawn dots to fill out the ring shapes around it.
#     Kept near the original ~1,750-node sample's edge count (~800-900)
#     so the "connections" texture looks the same as before, just now
#     sitting on top of a visible ring structure instead of being the
#     only thing visible.
N_ENT_TARGET = 1600
N_EV_TARGET = 2400
N_MK_TARGET = 1600

EDGE_ENT_TARGET = 500
EDGE_EV_TARGET = 800
EDGE_MK_TARGET = 450

SEED = 1337
rng = np.random.default_rng(SEED)

# ---- desaturated DARK palette (dimmer + more muted than the full-render
# DARK theme, so it reads as background texture, not a competing subject) ---
BG = "#05070a"


def _desat(hexcolor, factor=0.65):
    """Blend a hex color toward mid-gray by `factor` (0=no change origin
    kept, 1=full gray) — a cheap desaturate that keeps hue identity but
    dims vibrancy, so the constellation still reads as "the same graph"
    without competing with foreground text."""
    r = int(hexcolor[1:3], 16)
    g = int(hexcolor[3:5], 16)
    b = int(hexcolor[5:7], 16)
    gray = (r + g + b) / 3.0
    r2 = r + (gray - r) * factor
    g2 = g + (gray - g) * factor
    b2 = b + (gray - b) * factor
    return f"#{int(r2):02x}{int(g2):02x}{int(b2):02x}"


RAW_TYPES = {
    "PERSON": "#4fa8ff", "ORG": "#b08cff", "COMPANY": "#3ad07a",
    "EVENT": "#ff6b6b", "ASSET": "#2dd4bf", "ECON_INDICATOR": "#facc15",
    "COUNTRY": "#f472b6",
}
TYPES = {k: _desat(v, 0.40) for k, v in RAW_TYPES.items()}
EVENT_COLOR = _desat("#ffffff", 0.55)   # dimmed white -> soft gray-white
MARKET_COLOR = _desat("#f59e0b", 0.45)  # dimmed amber

# 3x the per-node marker area of the original sparse variant: with 3.2x
# as many dots (5,600 vs 1,749) the old NODE_K left individual dots too
# small/dim to read as a ring silhouette against the edge lines -- they
# were only ever meant to be visible as a texture riding on top of a
# dense edge-starburst. Now that edges are capped to a much smaller
# subset (see EDGE_*_TARGET), the dots have to carry the ring shape on
# their own, so they need to be bigger and a bit more opaque.
NODE_K = 0.30


def bezier_radial(p0, p1, pull, n=10):
    mid = (p0 + p1) / 2.0
    ctrl = mid * pull
    t = np.linspace(0.0, 1.0, n)[:, None]
    return (1 - t) ** 2 * p0 + 2 * t * (1 - t) * ctrl + t ** 2 * p1


def polylines_to_segments(lines):
    segs = [np.stack([ln[:-1], ln[1:]], axis=1) for ln in lines]
    return np.concatenate(segs) if segs else np.empty((0, 2, 2))


def circmean(angles):
    s, c = np.sin(angles).sum(), np.cos(angles).sum()
    return math.atan2(s, c) % (2 * math.pi)


# ---- load + sample ----------------------------------------------------------
def load_and_sample():
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
    n_ent_total = len(entities)

    # stratified entity sample: proportional per type, so sector balance in
    # the sparse render still echoes the full graph's type mix
    ent_sample = set()
    for etype in type_order:
        ids = by_type[etype]
        k = max(1, round(N_ENT_TARGET * len(ids) / n_ent_total))
        k = min(k, len(ids))
        ent_sample.update(rng.choice(ids, size=k, replace=False))

    # Events: biased toward ones already touching a sampled entity (so the
    # bright event->entity "bridge" lines actually have both endpoints in
    # the sample most of the time), topped up with a random slice for
    # coverage/diversity so the sample isn't just the highest-degree corner
    # of the graph.
    ev_touch = defaultdict(int)
    for s, o, ev in facts:
        if ev is None:
            continue
        if s in ent_sample or (o is not None and o in ent_sample):
            ev_touch[ev] += 1
    touching = [e for e, _ in events if e in ev_touch]
    n_touch = min(round(N_EV_TARGET * 0.55), len(touching))
    weights = np.array([ev_touch[e] for e in touching], dtype=float)
    weights = weights / weights.sum()
    ev_from_touch = set(int(x) for x in rng.choice(
        touching, size=n_touch, replace=False, p=weights)) if n_touch else set()
    remaining_pool = [e for e, _ in events if e not in ev_from_touch]
    n_random = min(N_EV_TARGET - len(ev_from_touch), len(remaining_pool))
    ev_from_random = set(int(x) for x in rng.choice(
        remaining_pool, size=n_random, replace=False)) if n_random else set()
    ev_sample = ev_from_touch | ev_from_random

    # Markets: same bias toward events already in the sample.
    mk_by_ev_hit = [(m, ev) for m, ev, sc in mlinks if ev in ev_sample]
    n_mk_biased = min(round(N_MK_TARGET * 0.5), len(mk_by_ev_hit))
    mk_biased = set()
    if n_mk_biased:
        idx = rng.choice(len(mk_by_ev_hit), size=n_mk_biased, replace=False)
        mk_biased = {mk_by_ev_hit[i][0] for i in idx}
    mk_pool = [m for m, _, _ in mlinks if m not in mk_biased]
    n_mk_random = min(N_MK_TARGET - len(mk_biased), len(mk_pool))
    mk_random = set(rng.choice(mk_pool, size=n_mk_random, replace=False)) \
        if n_mk_random else set()
    mk_sample = mk_biased | mk_random

    # Edge-eligible subsets: a smaller, degree-biased slice of each node
    # sample that is allowed to have edges drawn to it. Every node in
    # ent_sample/ev_sample/mk_sample still gets a dot (that's what makes
    # the rings read as rings); only these subsets feed build_edges, which
    # is what keeps the edge count near the original ~800-900 instead of
    # scaling up with the much bigger node sample.
    def _biased_subset(ids, deg, target):
        ids = list(ids)
        if len(ids) <= target:
            return set(ids)
        w = np.array([1.0 + deg.get(i, 0) for i in ids], dtype=float)
        w /= w.sum()
        idx = rng.choice(len(ids), size=target, replace=False, p=w)
        return {ids[i] for i in idx}

    ent_edge_sample = _biased_subset(ent_sample, ent_deg, EDGE_ENT_TARGET)
    ev_edge_sample = _biased_subset(ev_sample, ev_deg, EDGE_EV_TARGET)
    mk_edge_sample = _biased_subset(mk_sample, {}, EDGE_MK_TARGET)

    return dict(
        entities=entities, facts=facts, events=events, eedges=eedges,
        mlinks=mlinks, ent_type=dict(entities), by_type=by_type,
        type_order=type_order, ent_deg=ent_deg, ev_deg=ev_deg,
        n_ent_total=n_ent_total,
        ent_sample=ent_sample, ev_sample=ev_sample, mk_sample=mk_sample,
        ent_edge_sample=ent_edge_sample, ev_edge_sample=ev_edge_sample,
        mk_edge_sample=mk_edge_sample,
    )


# ---- radial layout (mirrors make_dkg_graph_live.layout_radial, restricted
# to the sampled node sets) ----------------------------------------------
BAND_INNER = (0.07, 0.26)   # events (center)
BAND_MID = (0.66, 0.82)     # entities (ring)
BAND_OUTER = (1.10, 1.26)   # markets (outer)


def _gauss_r(mid, std, lo, hi, rng_):
    return float(np.clip(rng_.normal(mid, std), lo, hi))


def layout(G):
    ent_sample, ev_sample, mk_sample = G["ent_sample"], G["ev_sample"], G["mk_sample"]
    ent_deg, ev_deg = G["ent_deg"], G["ev_deg"]
    by_type, type_order = G["by_type"], G["type_order"]
    n_ent_sample = len(ent_sample)

    ENT_LO, ENT_HI = BAND_MID
    EV_LO, EV_HI = BAND_INNER
    MK_LO, MK_HI = BAND_OUTER
    ent_mid, ent_w = (ENT_LO + ENT_HI) / 2, ENT_HI - ENT_LO
    gap = 0.05

    ent_angle, ent_r = {}, {}
    cursor = -math.pi / 2
    span_avail = 2 * math.pi - gap * len(type_order)
    for etype in type_order:
        ids = [i for i in by_type[etype] if i in ent_sample]
        if not ids:
            cursor += gap
            continue
        # span proportional to this type's share of the SAMPLE, not the
        # full population, so sectors stay populated at this node count
        span = span_avail * len(ids) / max(1, n_ent_sample)
        ids.sort(key=lambda i: ent_deg.get(i, 0))
        order = ids[::2] + ids[1::2][::-1]
        for k, eid in enumerate(order):
            ent_angle[eid] = cursor + span * (k + 0.5) / len(order) \
                + rng.normal(0, span * 0.02)
            d = ent_deg.get(eid, 0)
            shift = -0.28 * ent_w * min(1.0, math.log1p(d) / 6.0)
            ent_r[eid] = _gauss_r(ent_mid + shift, ent_w * 0.46,
                                  max(0.0, ENT_LO - 0.35 * ent_w),
                                  ENT_HI + 0.35 * ent_w, rng)
        cursor += span + gap

    ev_ent = defaultdict(list)
    for s, o, ev in G["facts"]:
        if ev not in ev_sample:
            continue
        if s in ent_angle:
            ev_ent[ev].append(ent_angle[s])
        if o is not None and o in ent_angle:
            ev_ent[ev].append(ent_angle[o])

    ev_mid, ev_w = (EV_LO + EV_HI) / 2, EV_HI - EV_LO
    ev_angle, ev_r = {}, {}
    support = dict(G["events"])
    for evid in ev_sample:
        angs = ev_ent.get(evid)
        a = (circmean(np.array(angs)) + rng.normal(0, 0.18)) if angs \
            else rng.uniform(0, 2 * math.pi)
        ev_angle[evid] = a
        d = ev_deg.get(evid, 0) + (support.get(evid) or 0)
        shift = -0.30 * ev_w * min(1.0, math.log1p(d) / 5.0)
        ev_r[evid] = _gauss_r(ev_mid + shift, ev_w * 0.42,
                              max(0.01, EV_LO - 0.35 * ev_w),
                              EV_HI + 0.35 * ev_w, rng)

    mk_mid, mk_w = (MK_LO + MK_HI) / 2, MK_HI - MK_LO
    mk_angle, mk_r = {}, {}
    for m, ev, sc in G["mlinks"]:
        if m not in mk_sample:
            continue
        mk_angle[m] = ev_angle.get(ev, rng.uniform(0, 2 * math.pi)) \
            + rng.normal(0, 0.04)
        mk_r[m] = _gauss_r(mk_mid, mk_w * 0.46,
                           max(0.01, MK_LO - 0.35 * mk_w),
                           MK_HI + 0.35 * mk_w, rng)

    def xy(a, r):
        return np.array([r * math.cos(a), r * math.sin(a)])

    return dict(
        ent_xy={i: xy(ent_angle[i], ent_r[i]) for i in ent_angle},
        ev_xy={i: xy(ev_angle[i], ev_r[i]) for i in ev_angle},
        mk_xy={m: xy(mk_angle[m], mk_r[m]) for m in mk_angle},
    )


def build_edges(G, P):
    # Edges are drawn only between the smaller edge-eligible subsets, NOT
    # the full node sample used for `ax.scatter` dots -- this is what
    # decouples "enough dots to trace a ring" from "enough edges to read
    # as bundled connections rather than a starburst."
    ent_e, ev_e, mk_e = G["ent_edge_sample"], G["ev_edge_sample"], G["mk_edge_sample"]
    ent_xy, ev_xy, mk_xy = P["ent_xy"], P["ev_xy"], P["mk_xy"]
    ent_type = G["ent_type"]
    groups = defaultdict(list)

    for s, o, ev in G["facts"]:
        if s in ent_e and s in ent_xy and o is not None and o in ent_e and o in ent_xy:
            groups[("ee", "#5a6270", 0.09, 0.5)].append(
                bezier_radial(ent_xy[s], ent_xy[o], 0.78))
    for s, o, ev in G["facts"]:
        if ev in ev_e and ev in ev_xy and s in ent_e and s in ent_xy:
            c = TYPES.get(ent_type.get(s), "#5a6270")
            groups[("ve", c, 0.11, 0.55)].append(
                bezier_radial(ev_xy[ev], ent_xy[s], 0.62))
    for a, b in G["eedges"]:
        if a in ev_e and a in ev_xy and b in ev_e and b in ev_xy:
            groups[("vv", TYPES["ORG"], 0.08, 0.5)].append(
                bezier_radial(ev_xy[a], ev_xy[b], 0.50))
    for m, ev, sc in G["mlinks"]:
        if ev in ev_e and ev in ev_xy and m in mk_e and m in mk_xy:
            groups[("mk", MARKET_COLOR, 0.09, 0.55)].append(
                bezier_radial(ev_xy[ev], mk_xy[m], 0.55))
    return groups


def render(G, P, out_path):
    edge_groups = build_edges(G, P)

    fig = plt.figure(figsize=(FIG_IN, FIG_IN), dpi=DPI)
    fig.patch.set_facecolor(BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(BG)
    ax.set_xlim(-1.48, 1.48)
    ax.set_ylim(-1.48, 1.48)
    ax.set_aspect("equal")
    ax.axis("off")

    for (kind, color, alpha, lw), lines in edge_groups.items():
        segs = polylines_to_segments(lines)
        ax.add_collection(LineCollection(
            segs, colors=color, alpha=alpha, linewidths=lw, zorder=1))

    def draw_nodes(xys, sizes, color, alpha, zorder):
        if not xys:
            return
        pts = np.array(xys)
        s = np.array(sizes) * NODE_K
        ax.scatter(pts[:, 0], pts[:, 1], s=s, c=color, alpha=alpha,
                   linewidths=0, zorder=zorder)

    epts = [P["ev_xy"][i] for i in P["ev_xy"]]
    esz = [3.0 + 10.0 * math.log1p(G["ev_deg"].get(i, 0)) for i in P["ev_xy"]]
    draw_nodes(epts, esz, EVENT_COLOR, 0.45, 3)

    for etype in G["type_order"]:
        ids = [i for i in G["by_type"][etype] if i in P["ent_xy"]]
        pts = [P["ent_xy"][i] for i in ids]
        sz = [4.0 + 20.0 * math.log1p(G["ent_deg"].get(i, 0)) for i in ids]
        draw_nodes(pts, sz, TYPES.get(etype, "#5a6270"), 0.50, 5)

    mids = list(P["mk_xy"])
    mpts = [P["mk_xy"][m] for m in mids]
    msz = [2.0 + 8.0 for _ in mids]
    draw_nodes(mpts, msz, MARKET_COLOR, 0.45, 4)

    fig.savefig(out_path, dpi=DPI, facecolor=BG)
    plt.close(fig)
    n_nodes = len(P["ent_xy"]) + len(P["ev_xy"]) + len(P["mk_xy"])
    n_edges = sum(len(v) for v in edge_groups.values())
    sz_mb = os.path.getsize(out_path) / 1e6
    print(f"sparse bg: nodes={n_nodes:,} edges={n_edges:,} -> "
          f"{os.path.basename(out_path)} {sz_mb:.1f}MB")


def main():
    G = load_and_sample()
    P = layout(G)
    out = os.path.join(HERE, "09_dkg_sparse_bg.png")
    render(G, P, out)


if __name__ == "__main__":
    main()
