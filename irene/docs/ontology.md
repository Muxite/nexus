# Ontology — entity types + relations

> The locked ontology for irene's extraction layer (P2+). Bump
> `ONTOLOGY_VERSION` (in `app/stages/extract.py`) whenever this file
> changes — extraction runs carry the version so old facts stay
> interpretable.

- **Version:** `v1`
- **Status:** active as of 2026-05-23 (P2 kickoff)

## Rationale

Open-ended NER + relation extraction produces a mess: synonyms,
typos, half-formed entities, relations that change meaning over time.
A **small, fixed ontology** keeps the LLM's hand close to the wheel.
P2 is data accumulation; downstream phases (eventize, link) need a
stable substrate to cluster against.

The shape mirrors FinDKG's heterogeneous-entity approach (per the DKG
spec § "Storage design") but truncated to the **smallest useful
working set** for Polymarket-relevant news.

## Entity types

| Type | What it covers | Examples |
|---|---|---|
| `COMPANY` | For-profit corporations + their subsidiaries | "Pfizer", "Apple Inc.", "Goldman Sachs" |
| `PERSON` | Named individuals | "Joe Biden", "Elon Musk" |
| `ORG` | Non-corporate organisations (regulators, governments, NGOs, central banks, sports teams) | "FDA", "Federal Reserve", "United Nations", "Lakers" |
| `COUNTRY` | Sovereign states | "United States", "Germany", "China" |
| `ECON_INDICATOR` | Quantitative economic series | "CPI", "GDP", "unemployment rate", "fed funds rate" |
| `ASSET` | Tradable instruments (stock, crypto, commodity, currency) | "BTC", "AAPL", "gold", "EUR" |
| `EVENT` | Named scheduled events that markets resolve against | "2026 US Presidential Election", "FOMC meeting", "G20 summit" |

**No other types are valid.** Anything that doesn't fit one of these is
dropped at the parse layer. If real-world data demands a new type,
that's a new `ONTOLOGY_VERSION` and a paper trail (`reports/`).

## Relations

Relations are **verbs in present tense, lowercase**, taking a typed
subject and optionally a typed object or a free `obj_value` (for
numeric / date / percentage targets).

| Relation | Subject types | Object types | Notes |
|---|---|---|---|
| `announces` | COMPANY / ORG / PERSON / COUNTRY | (anything) or `obj_value` | catch-all for "X says Y happened/will happen" |
| `decides` | ORG / PERSON | COMPANY / PERSON / EVENT or `obj_value` | regulatory/judicial decisions ("FDA decides on drug X") |
| `elects` | COUNTRY / ORG | PERSON / COMPANY | election outcomes |
| `acquires` | COMPANY | COMPANY | M&A; obj is the acquiree |
| `launches` | COMPANY / PERSON / ORG | ASSET / EVENT or `obj_value` | product, service, satellite, drug, etc. |
| `accuses` | ORG / PERSON / COUNTRY | PERSON / COMPANY / COUNTRY | allegations; not yet `decides` |
| `signs` | COMPANY / ORG / COUNTRY / PERSON | (anything) | agreements, treaties, deals |
| `changes` | ORG | ECON_INDICATOR with `obj_value` | rate cuts/hikes, policy moves |
| `resigns` | PERSON | (none — subject-only) | leadership changes |
| `nominates` | PERSON / ORG / COUNTRY | PERSON | political/judicial nominations |

**Ten relations only.** Anything outside this set is silently dropped at
parse time; the article's `extraction_runs.status` stays `ok` but the
unrecognised fact is omitted. A counter for dropped facts lives in
`extraction_runs.dropped_fact_count` (numeric).

## obj_value

For relations whose object is a number / date / percentage (e.g. `changes`
on a `ECON_INDICATOR`), the LLM populates `obj_value` instead of
`obj_entity_id`. Shape (JSON-string in the column):

```json
{"kind": "percentage", "value": 0.25, "unit": "bp", "raw": "25 basis points"}
{"kind": "currency", "value": 50000000000, "unit": "USD", "raw": "$50B"}
{"kind": "date", "value": "2026-09-01", "raw": "September 1"}
{"kind": "string", "raw": "no comment"}
```

`kind` is one of: `percentage` | `currency` | `count` | `date` | `string`. Anything
unparseable falls back to `{"kind":"string","raw":"<verbatim>"}` — keeps the row alive
without forcing a typed value.

## evidence_span

Every fact carries `evidence_span` — a verbatim quote from the article
that supports the fact. Required, non-empty. Used by the P2 quality
report to compute precision against held-out human labels, and by the
P3 linker to reason about novelty.

## confidence

LLM-emitted floating-point in `[0.0, 1.0]`. Out-of-range values are
clamped at the parse layer with a log warning; severely malformed
values (NaN, infinity, non-numeric) cause the fact to be dropped.

## Versioning

This file is the truth. To change anything:

1. Bump `ONTOLOGY_VERSION` (currently `v1`) in `app/stages/extract.py`.
2. Update this file.
3. Mention the change in `changelogs/cycles.md` and link a `reports/`
   entry justifying the new types/relations.

Existing facts are NOT migrated — they stay tagged with the old version
and continue to be queryable. New facts go through the new ontology.
