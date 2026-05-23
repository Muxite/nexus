# DKG Design Spec (user-supplied)

> **User-supplied 2026-05-22.** Treat as the **design north star**, not the v1 build target. The v1 phases (and which parts of this spec land in which phase) are in [`../master-plan.md`](../master-plan.md). This file is verbatim — never edit it. If our interpretation changes, write a new dated report under `../reports/` and reference both.

---

## Scope

This spec assumes you keep the current trading pipeline live, keep the existing database intact, and introduce new DKG tables, queues, and services that can read old rows and dual-write new structured outputs.

It also assumes a Python/FastAPI, PostgreSQL, Redis or RabbitMQ, Docker-style stack, which fits your existing backend patterns and lets you develop the graph system in parallel.

## Objectives

Do not reset or rewrite legacy tables in place.

Add new append-only event and graph tables.

Backfill from historical news, markets, and trades in batches.

Dual-write new structured outputs while the old papertrader keeps running.

Compare old-vs-new read models before routing live decisions.

## Storage design

Use a sidecar schema such as dkg_* or a separate database with foreign keys back to existing ids where possible. Incremental KG work emphasizes continuous updates, validation, and reuse of current graph state, while event-sourcing migration patterns recommend parallel projections instead of a big-bang replacement.

### New core tables

| Table | Purpose |
|---|---|
| dkg_raw_article | Immutable normalized article record; references existing news/article id when available. |
| dkg_article_cluster | Near-duplicate cluster for same event across many sources. |
| dkg_extraction_run | One extraction attempt per article version, model, prompt version, and ontology version. |
| dkg_entity | Canonical entity store, typed like FinDKG's heterogeneous entity categories rather than plain strings. |
| dkg_entity_alias | Alias, ticker, symbol, slug, and source-name mappings for entity resolution at scale. |
| dkg_fact | Time-aware triples: subject, relation, object/value, valid time, observed time, confidence, provenance. Dynamic KGs rely on timestamped facts rather than static triples. |
| dkg_event | Canonical event object built from one or more facts and article clusters. |
| dkg_event_edge | Event evolution links such as confirms, contradicts, escalates, resolves; event-evolution graphs are useful because financial news arrives as chains. |
| dkg_market_node | Normalized market proposition, deadline, resolution rule, polarity map, and links to existing market ids. |
| dkg_market_link | Event-to-market candidate links with directness, path score, novelty, and deadline fit. |
| dkg_trade_feature_snapshot | Frozen DKG feature vector used at trade time for audit and model training. |
| dkg_projection_health | Drift, parity, lag, and validation checks between legacy and DKG read models. |

### Recommended columns

```sql
dkg_raw_article(
  id bigserial pk,
  legacy_article_id bigint null,
  source_id bigint not null,
  url text not null,
  title text not null,
  body text,
  published_at timestamptz not null,
  ingested_at timestamptz not null default now(),
  title_hash bytea not null,
  body_hash bytea,
  language text,
  metadata jsonb,
  unique(source_id, url)
)

dkg_entity(
  id bigserial pk,
  entity_type text not null,           -- COMPANY, PERSON, REGULATOR, COUNTRY, EVENT, ECON_INDICATOR, etc.
  canonical_name text not null,
  external_refs jsonb,                 -- ticker, wikidata, sec cik, polymarket slugs
  status text not null default 'active',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
)

dkg_fact(
  id bigserial pk,
  event_id bigint null,
  subj_entity_id bigint not null,
  relation text not null,
  obj_entity_id bigint null,
  obj_value jsonb null,
  valid_from timestamptz null,
  valid_to timestamptz null,
  observed_at timestamptz not null,
  confidence numeric(5,4) not null,
  extraction_run_id bigint not null,
  source_article_id bigint not null,
  evidence_span jsonb,
  is_active boolean not null default true
)

dkg_event(
  id bigserial pk,
  event_type text not null,
  status text not null,                -- emerging, confirmed, contradicted, resolved, stale
  first_seen_at timestamptz not null,
  last_seen_at timestamptz not null,
  novelty_z numeric(8,4) null,
  support_count int not null default 1,
  canonical_summary text,
  attributes jsonb
)

dkg_market_link(
  id bigserial pk,
  event_id bigint not null,
  legacy_market_id bigint null,
  market_node_id bigint not null,
  link_class text not null,            -- direct, indirect, narrative
  causal_path jsonb not null,
  directness_score numeric(6,4) not null,
  novelty_score numeric(6,4) not null,
  deadline_fit_score numeric(6,4) not null,
  resolution_fit_score numeric(6,4) not null,
  final_score numeric(6,4) not null,
  created_at timestamptz not null default now()
)
```

### Indexes

- dkg_raw_article(source_id, published_at desc)
- dkg_raw_article(title_hash), dkg_raw_article(body_hash)
- dkg_entity(entity_type, canonical_name)
- dkg_entity using gin (external_refs jsonb_path_ops)
- dkg_fact(subj_entity_id, relation, observed_at desc)
- dkg_fact(event_id, observed_at desc)
- dkg_event(event_type, last_seen_at desc)
- dkg_market_link(legacy_market_id, final_score desc)
- dkg_market_link(event_id, final_score desc)

## Services and queue contracts

Build this as six bounded services: ingest, dedup, extract, resolve, graph-upsert, and market-link, with the trader consuming only the final scored event bundle. That follows both your existing microservice style and the literature's emphasis on multi-source integration, ontology constraints, validation, and update efficiency.

### Service boundaries

- **news-normalizer**: reads existing and live news rows, emits normalized article records.
- **article-clusterer**: groups cross-source duplicates and rewrites updates onto one cluster timeline.
- **kg-extractor**: produces schema-constrained entities, relations, event type, polarity, evidence spans, and confidence; recent KG work reports better extraction when LLMs are paired with ontology and rule constraints.
- **entity-resolver**: blocking, scoring, and clustering over aliases and known entities, which is the standard layered ER pattern for reliable KGs.
- **event-builder**: merges facts into canonical events and adds confirm/contradict/escalate edges.
- **market-linker**: converts events into candidate market links with evidence paths and feature scores.
- **trader-adapter**: exposes DKG outputs in a format the legacy papertrader can consume without full rewrites.

### Queue topics

```
news.article.normalized.v1
news.article.clustered.v1
kg.extraction.completed.v1
kg.entity_resolution.completed.v1
kg.event.updated.v1
kg.market_links.updated.v1
kg.trade_features.ready.v1
kg.projection_health.v1
```

### Event payload examples

```json
{
  "event_name": "kg.event.updated.v1",
  "event_id": 982113,
  "cluster_id": 55102,
  "event_type": "REGULATORY_DECISION",
  "status": "confirmed",
  "first_seen_at": "2026-05-22T22:14:03Z",
  "last_seen_at": "2026-05-22T22:16:11Z",
  "support_count": 7,
  "entities": [
    {"entity_id": 101, "type": "COMPANY", "name": "Pfizer"},
    {"entity_id": 302, "type": "REGULATOR", "name": "FDA"}
  ],
  "facts": [
    {
      "subj_entity_id": 302,
      "relation": "rejects",
      "obj_entity_id": 101,
      "confidence": 0.93
    }
  ],
  "novelty_z": 2.41,
  "source_article_ids": [8910021, 8910043, 8910117]
}
```

```json
{
  "event_name": "kg.market_links.updated.v1",
  "event_id": 982113,
  "candidate_links": [
    {
      "legacy_market_id": 44021,
      "market_node_id": 222,
      "link_class": "direct",
      "causal_path": [
        "FDA",
        "rejects",
        "Pfizer drug",
        "affects",
        "Pfizer"
      ],
      "directness_score": 0.96,
      "novelty_score": 0.81,
      "deadline_fit_score": 0.88,
      "resolution_fit_score": 0.91,
      "final_score": 0.90
    }
  ]
}
```

## API and read models

Expose the DKG through narrow read APIs so the legacy system can call it as an optional enrichment step instead of being rewritten. Migration guides for event-sourced systems recommend building parallel projections, comparing them against legacy reads, and shifting consumers gradually once mismatch rates are low.

### Internal APIs

```
POST /v1/articles/normalize
POST /v1/extractions/run
POST /v1/entities/resolve
POST /v1/events/build
POST /v1/markets/link
GET  /v1/events/{event_id}
GET  /v1/events/{event_id}/market-links
GET  /v1/markets/{legacy_market_id}/latest-event-context
GET  /v1/health/projections
```

### Response contract for trader integration

```json
{
  "legacy_market_id": 44021,
  "event_id": 982113,
  "decision_context": {
    "link_class": "direct",
    "event_type": "REGULATORY_DECISION",
    "status": "confirmed",
    "novelty_z": 2.41,
    "support_count": 7,
    "causal_path": ["FDA", "rejects", "Pfizer drug", "affects", "Pfizer"],
    "evidence_quality": 0.92,
    "resolution_fit_score": 0.91,
    "deadline_fit_score": 0.88,
    "neutrality_risk": 0.12
  }
}
```

### Projection tables for legacy consumption

- dkg_market_latest_context
- dkg_event_latest_state
- dkg_entity_latest_state
- dkg_trade_decision_features

These should be rebuilt from append-only facts and events, because projection rebuild and checksum comparison are the standard way to detect drift in gradual migration.

## Migration plan

Do this in four phases: shadow ingest, historical backfill, dual-write, then gradual read cutover. That is consistent with incremental KG construction and with legacy-to-event-sourced migration patterns that add event publishing, build projections, compare outputs, and only then shift reads.

### Phase 0 — schema add only

- Add all dkg_* tables and indexes.
- Add nullable dkg_event_id, dkg_entity_id, and dkg_snapshot_id reference columns to existing audit tables only if useful.
- No existing write paths change.

### Phase 1 — shadow ingest

- Stream all new articles into dkg_raw_article.
- Backfill a rolling window first, for example last 30–90 days, before touching older history.
- Build dkg_article_cluster and dkg_extraction_run without affecting trading.

### Phase 2 — historical backfill

- Replay historical news, markets, and trades in batch order by publish time.
- Create "genesis" events for old state where full event history is missing, because migration guidance notes that older snapshot state often needs a starting event or ersatz event stream for replayable projections.
- Persist backfill provenance so you can distinguish origin = live from origin = replay.

### Phase 3 — dual-write

- On every new article, keep the old embedding pipeline and also write DKG events and market links.
- On every trade decision, store both legacy features and DKG feature snapshots in dkg_trade_feature_snapshot.
- Compare old-vs-new candidate markets, skipped-neutral rates, and wrong-way outcomes.

### Phase 4 — gradual read cutover

- Start with one non-critical read: latest market context.
- Then route only candidate filtering through DKG while legacy AI still makes the final call.
- Finally, let DKG-derived features participate in the trade decision once parity and PnL attribution are stable for a sustained window; migration advice suggests waiting for consistent projection agreement before full cutover.

## Decision logic and validation

Your biggest win is to make the DKG a filter and evidence layer before it becomes a full decision engine, because your current problem is more false direction and neutral trades than raw latency. Use it first to veto weak links and raise confidence on direct or strongly indirect ones, not to replace the whole trader at once.

### Initial scoring formula

```
final_score =
  0.30 * directness_score +
  0.20 * resolution_fit_score +
  0.15 * deadline_fit_score +
  0.15 * novelty_score +
  0.10 * evidence_quality +
  0.10 * confirmation_score
```

### Hard gates

- Reject link_class = narrative.
- Reject status in (contradicted, stale, resolved).
- Reject support_count < 2 for indirect links.
- Reject if no explicit causal path reaches the market subject or resolution variable.
- Reject if novelty is low and the event is only a cross-source echo; FinDKG explicitly tracks novelty as a useful signal.

### Validation metrics

- Extraction precision, recall, and F1.
- Graph coverage, edge density, clustering, and update latency; these are explicitly reported as graph-quality metrics in recent KG construction work.
- Entity-resolution merge precision and split/merge error counts.
- Market-link precision@k and wrong-way rate.
- Neutral-trade rate before vs after DKG.
- Net edge after fees per executed trade, since your goal is fewer medium wins rather than many fee-eaten small ones.

### Operational safeguards

- Version every ontology, prompt, extractor model, and resolver model.
- Store evidence spans and source article ids on every fact and event.
- Make all projections rebuildable from append-only records.
- Run nightly drift checks between legacy and DKG projections, because gradual cutovers fail most often through silent projection drift.
