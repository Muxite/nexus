# Seulgi → irene one-time data migration

> Bootstrapping irene's DB from seulgi's historical corpus. Data layer
> only; trades / engine_results / observability stay seulgi-only.
> Seulgi continues to run as a live demo of its thinking system; irene
> becomes the canonical analytics + DKG store.

- **Date**: 2026-05-23
- **Importer**: `scripts/migrate/from_seulgi.py` (this commit).
- **Cycle**: `seulgi-import-2026-05-23` (irene `cycles.id=1`).
- **Batch id**: `seulgi-import-20260523T233201Z-3840b3`.
- **Duration**: 1.5s end-to-end.
- **irene.db size after**: 35.9 MB.

## TL;DR

A clean import. **27,914 articles** + **10,462 markets** landed in
`irene/data/irene.db` with embeddings + cycle stamping + dedup
working correctly. Three explicit out-of-scope decisions (price_impacts,
engine_results, trades) are documented below so the next person to
touch this doesn't expect them. Tests: 295 passed.

## Scope locked during planning

| dimension | choice |
|---|---|
| Shape | one-time snapshot now, no periodic refresh, no live mirror |
| Tables | data layer only — `articles` (from `signals` + `headline_articles`) and `markets` (with embeddings) |
| engine_results | **skipped entirely** — the original filter `executed=1 OR edge IS NOT NULL` returned 100% of rows (edge is non-null for every row), so the "<100k" plan target was invalid. Skipped per re-clarification. |
| trades / positions | seulgi-only |
| price_impacts | **skipped** — irene's schema requires `trade_id NOT NULL UNIQUE REFERENCES trades(id)`. With trades out, price_impacts can't be migrated without either back-filling 564 synthetic trades or relaxing the FK. Both are follow-ups. |
| DKG extraction | NOT touched — handled by the in-flight `dkg_bulk_sample.py` thread, separately |

## Result

```
batch: seulgi-import-20260523T233201Z-3840b3
cycle: seulgi-import-2026-05-23 (id=1)
elapsed: 1.5s

articles:
  rows_seen      94,789
  rows_inserted  27,914
  rows_skipped   66,875   (re-detection of same article = same URL)

markets:
  rows_seen      10,462
  rows_inserted  10,462
  rows_skipped        0
```

Post-import row counts (irene.db):

| table | rows |
|---|---|
| articles | 27,914 |
| markets | 10,462 |
| cycles | 1 |
| seulgi_migration_batches | 2 |

## Article-corpus shape

The 27,914 articles break down as:

| url scheme | count | comes from |
|---|---|---|
| `http(s)://...` | 11,618 | distinct `headline_articles.article_url` values referenced by signals |
| `seulgi-headline://<headline_id>` | 16,296 | synthesized for signals lacking a body row in `headline_articles` |

Body coverage (only `headline_articles.body` rows survive the LEFT JOIN
+ url dedup):

| summary | count |
|---|---|
| with body | 3,314 (11.9%) |
| without body | 24,600 (88.1%) |

The bulk of articles are headline-only — the same shape the DKG
bulk-extraction thread (`reports/2026-05-23-irene-dkg-bulk-1k-validation.md`)
was designed for.

## Markets

All 10,462 markets came across with their 384-d float32 embeddings
intact (confirmed by spot-check: seulgi stores raw `np.float32.tobytes()`,
not pickled numpy — easier than the merger doc's draft assumed).
Every imported market is stamped:

- `embedder_id = 'sentence-transformers/all-MiniLM-L6-v2@384'`
- `embedding_dim = 384`

Both irene and seulgi use the same embedder model, so cosine search
across markets in irene works out of the box without re-embedding.

Active vs closed:

| state | count |
|---|---|
| active (`closed_at IS NULL`) | 9,024 |
| closed | 1,438 |

`market_type` (seulgi has 'binary'/'scalar'/etc.) is not migrated —
irene's `markets` schema doesn't track it; query seulgi.markets
directly if needed.

## Key design choices

### Synthetic URL for headline-only signals

Initial attempt fell back to `signals.site_url` when no
`headline_articles.article_url` existed. That collapsed ~16k distinct
headlines down to **46 publisher rows** (only 46 distinct site_urls
across seulgi). Fix: synthesize `seulgi-headline://<headline_id>` so
each unique headline stays distinct. Preserved 16,296 extra headlines
that would otherwise have been lost.

Trade-off: these synthetic URLs don't link to anywhere — they're
opaque irene-internal identifiers. Anything that wants to read the
actual article body must check `summary IS NOT NULL`.

### Re-detection dedup is correct

The 66,875 "skipped" rows are not data loss — they're seulgi detecting
the same article multiple times under different freshness labels
(EVOLVING / REHASH / STALE_RETURN). irene's `articles` table is meant
to hold unique articles, not detection events. If a future use case
needs the detection-event history, that's a separate import (seulgi's
`signals.detected_at` per row).

### Bootstrap goes through `DB.connect()` once

The importer is sync sqlite3 for the actual work, but the irene schema
is registered via `db.register_schema()` calls scattered across
stage modules. To pick those up without duplicating the registry, the
importer does one `asyncio.run(DB(path).connect())` to bootstrap the
schema, then switches to sync. Side benefit: the seulgi-path
assertion in `_assert_not_seulgi` fires automatically — passing a
seulgi-shaped irene path errors out (verified by
`test_irene_db_must_not_be_seulgi_path`).

## What this enables

1. **DKG bulk-extraction can re-target.** The in-flight bulk thread
   (`dkg_bulk_sample.py`) currently reads `seulgi.signals.headline`.
   Once the SBERT-replay step lands, it can switch to reading
   `irene.articles WHERE source='seulgi_migration'` — same content,
   inside irene's DB, no more cross-DB join.

2. **Cosine match works.** With markets+embeddings landed, irene's
   `MarketIndex` can load against the imported markets table and
   match articles to markets without re-embedding the 10k market
   questions.

3. **Resumable extension.** If a 28k-article corpus turns out to be
   too small for a particular use case, the importer is idempotent on
   `UNIQUE(url)` — re-running after an inflated source corpus is
   safe.

## Verification

```bash
# Tests green before and after
PYTHONPATH=. python3 -m pytest -q             # 295 passed
PYTHONPATH=. python3 -m scripts.lint.no_facts_in_trade_path

# Run the importer (idempotent — safe to repeat)
PYTHONPATH=. python3 -m scripts.migrate.from_seulgi \
    --seulgi-db /home/muk/projects/jennie/seulgi/data/seulgi.db \
    --irene-db /home/muk/projects/jennie/irene/data/irene.db

# Post-import sanity
python3 -c "import sqlite3; c=sqlite3.connect('file:/home/muk/projects/jennie/irene/data/irene.db?mode=ro',uri=True); \
  print('articles', c.execute(\"SELECT COUNT(*) FROM articles WHERE source='seulgi_migration'\").fetchone()[0]); \
  print('markets', c.execute('SELECT COUNT(*) FROM markets').fetchone()[0])"
```

## Open follow-ups (NOT done here)

1. **price_impacts migration** — needs either (a) backfill 564
   trades + 44 positions first, or (b) loosen the FK on
   `price_impacts.trade_id`. Defer until someone needs historical
   edge-realization data.
2. **engine_results / predictions / decisions migration** — skipped
   entirely. If at some future date irene wants seulgi's decision
   history, the original filter must be revisited (it returns 100%
   of rows). Probably needs a different filter dimension, e.g.
   `skip_reason NOT LIKE 'RULE_%'` to drop the 6M+ rule-based
   price-bucket skips and keep only the LLM-evaluated rows
   (~1.3M).
3. **Merger-doc schema-mapping update.** The merger doc
   (`docs/irene_seulgi_merger.md`) was drafted before seulgi
   refactored `headlines` out of existence. Its §"Schema mapping"
   needs corrections to reflect: (a) seulgi has no `headlines`
   table, (b) seulgi has no `mapped_signals` table, (c) the locked
   data-layer-only scope. Doc edit pending in the same PR as this
   report.
4. **A fourth end-state in the merger doc.** The doc names three
   futures (A: irene wins, B: seulgi wins, C: coexist). The user's
   chosen path is a fourth: "irene as canonical analytics store;
   seulgi as live demo of its thinking system". Worth documenting
   explicitly so future readers don't have to infer.
