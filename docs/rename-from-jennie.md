# Extraction record — 2026-07-25

nexus was extracted from a private multi-project repository (`jennie`) that
held both these agents and unrelated work by collaborators. This file records
what moved, what did not, and what was renamed, so that the older documents in
`irene/` and `seulgi/` stay interpretable.

## What moved

`irene/`, `seulgi/`, and `reports/` — 203 commits, every one authored and
committed by `muk <muk.chunpongtong@gmail.com>`. Verified after the rewrite:

```
$ git log --all --format='%an <%ae>' | sort -u
muk <muk.chunpongtong@gmail.com>
```

## What did not move, and why

| Left behind | Reason |
|---|---|
| `main/` | The original Tkinter GUI, Polymarket API wrapper, and paper-trading framework, written by collaborators. Their files were `git mv`'d into `main/app/` early on, so a path-scoped filter would have pruned their commits while carrying their code in under a rename commit. Excluding the directory entirely avoids that misattribution. |
| `frontend/` | Mine by authorship, but it drives `main/server`'s trading GUI (`/api/state`, `/api/trading/start`, `/api/trading/stop`) — it belongs with `main/`. |
| `services/scraper`, `services/scraper-v2` | Superseded by `services/ujin`. |
| `mirage/`, `main/app/evolution/` | Mine, but out of scope for this repository. |

The collaborators' 7 commits were pruned automatically: they never touched
`irene/`, `seulgi/`, or `reports/`, so `git filter-repo` dropped them as empty.
The repository's original root commit was theirs; nexus's root commit is now
`irene is the next big thing trust` (2026-04-01).

**The one timeline entry that still refers to their work** is in
`reports/2026-06-12-irene-big-retrospective.md` — the 2026-03-14 row. It is
deliberately *not* renamed to "nexus", because that project is not this one.

## What was stripped from history

| Path | Size | Reason |
|---|---|---|
| `seulgi/snapshots/` | 246 MB, 1139 blobs | Chroma vector stores and dashboard captures — derived artifacts, rebuildable from database rows. About 89% of the repository's total weight. |
| `irene/logs/` | 7.6 MB | Committed runtime logs. |

Packed history went from 132 MB to 3.1 MB. Three commits touched only these
paths and were pruned as empty (206 → 203).

See `data/README.md` for the data layout that replaces the snapshot convention.

## Renames

Every occurrence of `jennie` became `nexus`, across 78 files:

| Old | New | Count |
|---|---|---|
| `jennie-seulgi` | `nexus-seulgi` | 37 |
| `jennie-scraper-v2`, `jennie-scraper` | `nexus-ujin` | 29 |
| `jennie_default` | `nexus_default` | 11 |
| `jennie-scraper-v2-dev` | `nexus-ujin-dev` | 3 |
| `jennie` (bare, incl. `/home/muk/projects/jennie` paths) | `nexus` | 103 |

Case variants `JENNIE` (12) and `Jennie` (4) were mapped to `NEXUS` and
`Nexus`.

**The original names are still recoverable** — history was preserved, not
rewritten in content, so `git log -S jennie` and `git log -p` before the
rename commit show every original string.

## Scraper migration

The two in-tree scrapers were replaced by `services/ujin` (submodule). ujin is
derived from them and is a strict superset. irene needed configuration changes
only; seulgi needed two call sites moved:

| Agent | Call | Before | After |
|---|---|---|---|
| irene | headline extraction | `POST /scrape` (v2 :9001) | `POST /scrape` (ujin :8901) — `mode=links`/`combined` both native |
| irene | RSS | `POST /feed` | unchanged, identical `FeedRequest` |
| seulgi | link extraction | `POST /fetch` (v1 :9000) | `POST /scrape` with `{"mode": "links"}` |
| seulgi | X/Twitter | `POST /twitter/search` | `POST /social/twitter` — identical request and response models |
| seulgi | RSS | `POST /feed` | unchanged |

The one response-shape difference: v1 returned links nested under
`{"extraction": {"links": [...]}}`; ujin returns them at the top level as
`{"links": [...]}`. `seulgi/app/watcher.py` accepts both shapes so existing
test fixtures still parse.

**Not yet verified against a running ujin instance.** The model definitions
match on both sides, but no end-to-end smoke test has been run. Do that before
trusting seulgi's ingest in this repository.
