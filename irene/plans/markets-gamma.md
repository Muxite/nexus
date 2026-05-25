# Plan — markets.py Gamma parser

> Source: `irene/questions/unstructured answers.md` lines 23-52, batch
> `2026-05-25-unstructured-1`. Cross-refs: `research_answers.md` B1-B5
> (batch 3, 2026-05-25), `research_questions.md` O-B1/O-B2/O-B3/O-B4 and
> i-B1/i-B2.

## Context

This plan covers the Gamma API surface owned by `app/stages/markets.py` —
specifically the row parser (`_parse_gamma_row`), the pagination loop
(`_fetch_all_gamma`), the upsert path (`_upsert_market`), and the
resolved-market handoff into `_close_orphaned_trades_for`.

A new external research batch (unstructured-1) overlaps with the
already-distilled B-section in `research_answers.md`, but it **contradicts
the earlier batch on three load-bearing claims**. We must resolve those
contradictions with a live `curl` capture before merging any parser
change — committing to one batch's wording is a coin-flip otherwise.

Three contradictions, in order of blast radius:

1. **Pagination scheme.** Prior batch (B4): cursor-based via
   `after_cursor` / `next_cursor`. New batch (lines 47-49): "strictly
   offset-based, requiring you to iterate using `limit` and `offset`".
   `markets.py:_fetch_all_gamma:562-577` is **already offset-based**, so
   if the new batch is right our code is correct and B4 was wrong.
2. **Volume field semantics.** Prior batch (B1): `volume24hr` is 24h,
   `volumeNum` is lifetime. New batch (lines 28-34): both fields
   represent 24h volume — but the sample payload shows `volume24hr:
   250450.50` and `volumeNum: 1500000.75` on the same market, so the
   batch's prose contradicts its own example. One of the two
   descriptions has to be wrong.
3. **Rate limits.** Prior batch (B4): `/markets` 300/10s, combined
   `/markets`+`/events` 900/10s, overall 4000/10s. New batch (lines
   46-49): "extremely generous 4000 requests per 10 seconds on the
   Gamma API" with no per-endpoint breakdown. Treat the per-endpoint
   numbers as the more conservative (and safer) interpretation.

## Findings

### Protocol facts to act on (no contradiction, both batches agree)

- **Resolved-market payload shape** (lines 36-44). Once a market
  resolves Gamma flips `closed=true`, `acceptingOrders=false`,
  `umaResolutionStatus="resolved"`, sets `resolvedBy=<address>`, and
  the `outcomePrices` array carries the final payout as
  `["1","0"]`-style strings. Matches B2's documented field list.
- **Tags are NOT the fee-rate driver** (lines 40-44). Tag IDs come from
  a fixed backend taxonomy enumerable via `/tags` and exist purely for
  UI categorisation. Fee rates live in the **CLOB market config**, not
  Gamma. Reinforces B3 + A2: fee_rate lookup belongs in a
  `getClobMarketInfo(conditionID)` call, not in the Gamma parser.
- **429 backoff** (line 51). Polymarket returns a `Retry-After` header
  on Cloudflare 429s; honour it precisely instead of using a fixed
  backoff constant.
- **Gamma is discovery-only** (B5, unchanged this batch). Never read
  hot-path prices from Gamma; CLOB / WebSocket only.

### Contradictions to resolve BEFORE coding (verify with a live curl)

| # | Question | Test |
|---|----------|------|
| 1 | Pagination: offset or cursor? | Hit `/markets?limit=100&offset=100`, then `/markets?limit=100&offset=200`. If both return 100 distinct rows with non-overlapping `conditionId`s, offset works. If results look stale or duplicated, fall back to cursor. |
| 2 | Volume fields: which is 24h? | Curl one known-high-volume market (e.g. a top-of-`/markets?order=volume24hr` row), record `volume`, `volume24hr`, `volumeNum`, `volume24hrClob`. Wait 24h, re-fetch. The field that resets/shifts is the 24h field; the field that monotonically grows is lifetime. |
| 3 | Rate limits: per-endpoint or only global? | Best signal here is the official docs page (Polymarket docs site) — if it's silent, default to the conservative per-endpoint reading (300/10s on `/markets`) since that's the worst-case constraint. |

Findings from these three tests are the prerequisite for the code
changes in the next section.

### Seeds only (no commit yet)

- The "4000/10s overall" number, until we either confirm or refute the
  per-endpoint 300/10s limit. Polling at `_DEFAULT_POLL_SECONDS=120`
  with ~500 paginated requests per cycle = ~4 req/s = well under
  either limit, so the cadence is safe under both readings — but the
  backoff math depends on which limit binds first.

### Stale (replace once tests above resolve)

- `research_answers.md:B1` "best evidence: `volume24hr`" — keep until
  the live capture either confirms or contradicts it.
- `research_answers.md:B4` "cursor-based via `after_cursor` /
  `next_cursor`" — currently contradicted by both our code AND the new
  batch; very likely wrong, but verify before rewriting the entry.

## Proposed code changes

All changes are in `app/stages/markets.py` and gated on the live-capture
results above. Numbers are line refs in the file as it stands today.

### `_parse_gamma_row` (lines 155-198) — volume field switch

Current:
```python
volume = float(raw.get("volume") or 0.0)   # line 184
```

The `volume` field on Gamma is generally **lifetime, in USDC**. The
`thin_market` sub-rule in `trade.py:684` interprets `volume_usdc`
against `cfg.min_market_volume_usdc = 10_000.0` (config.py:250). If
`volume` is lifetime, the gate is much looser than intended — a market
with $11k lifetime volume but $50 in the last 24h would pass.

After live-capture #2 confirms the 24h field name, change to:
```python
volume = float(raw.get("volume24hr") or raw.get("volume") or 0.0)
```
Keep `raw.get("volume")` as the fallback so we don't NULL-out the
column if Gamma briefly drops the 24h field. Add a parser comment
naming the verification curl (date + conditionId) so the next person
doesn't redo the work.

If the live capture instead shows `volumeNum` is the 24h field, the
swap is `raw.get("volumeNum")` — but document explicitly that this
contradicts every Gamma docs page that sorts by `order=volume24hr`.

### `_parse_gamma_row` — new fields needed for resolution-oracle plan

Add three reads, all `or None`:
- `accepting_orders` ← `raw.get("acceptingOrders")` (bool)
- `uma_resolution_status` ← `raw.get("umaResolutionStatus")` (str)
- `resolved_by` ← `raw.get("resolvedBy")` (str / address)

These are needed by the three-state close machine in the
`resolution-oracle.md` plan (D1's "closed → proposed → finalized"
state), NOT by this parser plan's primary scope. Schema additions
(new columns on `markets`) belong in that plan — flag the boundary
here, don't add columns in this plan's PR.

### `_fetch_all_gamma` (lines 550-580) — backoff on 429

Current code: `resp.raise_for_status()` on line 571 — a 429 raises and
the outer `poll_once` skips the cycle (line 476). That means a single
Cloudflare-triggered 429 kills a whole poll.

Add a 429-aware retry:
- On `httpx.HTTPStatusError` with `resp.status_code == 429`, read
  `Retry-After` (seconds or HTTP-date), sleep, retry **the same page**
  (don't advance the offset).
- Cap retries at 3 per page; after that, raise so the outer cycle
  skips and the next poll tries again.
- Add `_GAMMA_MAX_429_RETRIES = 3` next to the other constants
  (lines 92-101).
- Add jitter (±20%) to the sleep to avoid synchronised retries if
  multiple stages get throttled at once.

Pagination scheme change is conditional on test #1. If it confirms
offset, no change needed (current code is correct). If it shows
cursor, replace the `offset += len(page)` loop with cursor handoff
through `resp.headers` or response body `next_cursor` per Gamma's
actual contract.

### Poll cadence / config

Currently `_DEFAULT_POLL_SECONDS = 120`, overridable via
`IRENE__MARKETS__POLL_SECONDS` env or `cfg.markets.poll_seconds`.
`config.yaml` does not currently have a `markets:` section — the
`60` values for `poll_seconds` in `config.yaml` belong to other
stages (closer, ingest, etc., per the grep — lines 27, 123, 134,
185, 200, 204).

No change recommended. ~500 pages per 120s cycle = ~4 req/s, fine
under both 300/10s and 4000/10s readings. Document the math in the
module docstring next to `_DEFAULT_POLL_SECONDS` so the next person
sizing the universe doesn't have to redo it.

### Resolved-market detection — DEFER to `resolution-oracle.md`

`markets.py:_close_orphaned_trades_for` (lines 666-734) already closes
trades on markets that **disappear** from Gamma's open list, exiting
at `entry_mid` (`gross_pnl=0, net_pnl=-fees`). The new batch's
resolved-payload shape (lines 36-44) is exactly the data that would
let us do better — close at the **actual** $1/$0 by reading
`outcomePrices`.

But: the three-state machine (closed → proposed → finalized) lives
in the `resolution-oracle.md` plan, and reads from `umaResolutionStatus`
+ `resolvedBy` + the 2h UMA dispute window (research_answers.md D1).
The parser plan only adds the field reads; the close-at-$1/$0 logic
is the oracle plan's job. **Boundary:** this plan writes the columns,
the oracle plan reads them.

## Open questions

- **Q1: should we persist `raw_gamma` as a JSONB column?** Several
  downstream questions (i-B3 `feeType` audit; future `resolvedBy`
  audits; future `tags` queries) would be trivial with a stored raw
  payload, and would let us answer questions without re-curling
  Gamma. Cost: ~2-5 KB per row × 50k rows = 100-250 MB. Decision
  belongs to a separate plan (`markets-raw-payload.md`) but flag it
  here.
- **Q2: per-market `fee_rate` lookup.** A2 says retire the hardcoded
  category table for held markets; fetch per-market via CLOB
  `getClobMarketInfo(conditionID)`. That call could live either in
  `markets.py` (cache it at discovery time on the `markets` row) or
  in `closer-fees.md` (lazy at close-time). Strong preference for
  `markets.py` — fee_rate is a market property, not a closer
  property; cache at the natural ownership boundary. Decide before
  writing either plan's PR.
- **Q3: `/tags` enumeration job.** The new batch confirms `tags`
  comes from a fixed backend taxonomy. Worth a one-shot script
  (`scripts/inspect/gamma_tags.py`) that dumps the full taxonomy
  into `data/reference/gamma_tags.json` so future category logic
  doesn't have to re-query.

## Verification

Before any code change merges:

1. **One-shot Gamma capture script.** Create
   `scripts/inspect/gamma_sample.py` (referenced in i-B1) that
   takes a `--condition-id` flag and pretty-prints the raw payload.
   Run against:
   - One known-high-volume market (top of `/markets?order=volume24hr`).
   - One known-resolved market (pick a `WHERE active=0 LIMIT 1` row
     from our `markets` table, then curl Gamma's `/markets?condition_ids=...`).
2. **Diff the captured payloads against the assumptions in this plan.**
   In particular, confirm the volume-field semantics (test #2)
   and the resolved-payload field names match the batch's wording.
3. **Pagination smoke test.** `curl -s 'https://gamma-api.polymarket.com/markets?limit=100&offset=0'`
   and `...&offset=100`, diff `conditionId` sets, confirm
   no overlap and both return 100 rows. Settles test #1.
4. **429 backoff test.** Either intentionally trip the limit in a
   staging env, or unit-test the retry path with an `httpx.MockTransport`
   that returns 429 with a `Retry-After: 1` header then 200.
5. **Update `research_answers.md`.** Once the three contradictions
   are resolved, mark the stale B1/B4 entries with the corrected
   information and a link to the verification capture.

## Cross-cutting notes

- **Per-market fee_rate fetching** belongs in `markets.py` (cache on
  the row) per Q2 above — flag for `closer-fees.md` to read from
  the `markets` table rather than fetch itself.
- **Resolved-payload close logic** is split: this plan adds the
  field reads (`acceptingOrders`, `umaResolutionStatus`, `resolvedBy`,
  `outcomePrices`); `resolution-oracle.md` owns the state machine
  and the close-at-$1/$0 path. Don't duplicate.
- **`/tags` taxonomy snapshot** (Q3) is small enough to belong in
  this plan's PR if appetite allows, but logically separable — it's
  a one-shot reference dump, not a parser change.
- **`raw_gamma` column** (Q1) is a separate plan if we decide to
  do it; flag here, don't decide here.
