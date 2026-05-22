# markets.py + news_memory.py — bug list (Phase 0 / A0.9)

Date: 2026-05-22.
Scope: news → market matching (`markets.py` 407 lines,
`news_memory.py` 302 lines).

## TL;DR (top 3, most impactful first)

1. **No concept-link boost exists.** Master plan A.5 / "cycle-3 shipped
   48 concept-link seeds" is **wrong** — no `concept_links` table, no
   `concept_link` reference in code/config. Heat boost (`match_count`)
   is the only non-cosine signal in `find_matches`. Concept graph
   already deferred to Phase 4E.2 per master-plan.md:720.
2. **No automatic HNSW corruption recovery.** Per
   `handoffs/2026-05-22.md` Plot 1, an operator renamed
   `data/news_chroma/` manually after a SIGSEGV.
   `NewsMemory._init`'s except branch only logs + disables; nothing
   rebuilds. Next dual-container race silently blinds freshness with
   no alert.
3. **Polymarket universe is at its ~10100-row ceiling**, and the
   wrapper conflates the 422 with a network error. Live DB: 10,429
   markets total, 10,129 fetched in today's refresh — we hit the cap
   on every refresh and don't know it; `mark_markets_closed` then
   closes real markets that fell off the end of the fetch.

---

## 1. `find_matches` (markets.py:362-407)

`scores = matrix @ q` (cosine — both sides L2-normalized). Heat boost
adds `min(heat/100, 0.05)` per market. Filter rows below
`cfg.markets.similarity_threshold=0.55`, argsort desc, take top
`cfg.markets.top_k=25`.

- **SBERT cosine only.** Heat is the only non-cosine signal. No tag
  routing, no learned ranker, no cross-encoder re-rank.
- **K is fixed.** The 0.55 threshold is the effective gate; K just
  caps the LLM batch. Political headlines with 40 matches >0.55
  silently drop 15 (no log); niche headlines with 3 get 3.
- **Heat boost applies BEFORE threshold AND argsort.** A 0.547
  cosine with `match_count=10` jumps to 0.647 and now both clears
  the floor and outranks honest 0.60 cosines. Today's top heat
  scores are 4.5k-7.4k → every "hot" market saturates the +0.05 cap
  → heat is effectively binary at production levels.

### concept-link boost — confirm/deny

**DENIED.**
- `grep -rn "concept_link" seulgi/app/ seulgi/config.yaml` → 0 hits.
- `sqlite> SELECT name FROM sqlite_master WHERE name LIKE '%concept%'`
  → empty.

Same doc/code gap pattern that bit cycle-2 (handoff
`2026-05-22-v2.md` §1). Suggest adding to master-plan.md:175-204
cross-validation table as item #21.

### Kalshi / Predictit scaffolding

`grep -rn "kalshi\|predictit" seulgi/app/ seulgi/config.yaml` → 0
hits. **Confirmed absent** — consistent with Phase 4E.3 deferral.

---

## 2. Polymarket universe cap

| What | Value |
|---|---|
| Master-plan claim | "Gamma API 422 at offset=10100" |
| Live DB total | 10,429 |
| Live DB open (closed_at NULL) | 9,205 |
| Markets fetched today | 10,129 |
| `cfg.markets.max_tracked_markets` | 20,000 (unreachable) |
| `cfg.markets.refresh_hours` | 4 |

**Confirmed.** `_fetch_gamma_markets` paginates 100/page from offset=0.
At offset≈10100, Gamma returns 422 → falls through the bare
`except Exception` at markets.py:110 → `logger.warning("Gamma API
error at offset %d: %s", offset, exc)` and breaks the loop. The 422
is never distinguished from a transient 5xx.

Effects:
- Index caps at ~10100 markets, not the configured 20k.
- `mark_markets_closed` (called unconditionally after partial fetch)
  closes every market past offset 10100 even if still trading. 1,224
  closed rows include real expirations AND universe-overflow markets.
- On a real network 5xx mid-pagination, the same silent break ALSO
  closes everything past the failure point.

**Fix scope.** Master plan A.1 / Phase 4E.3. Cheap mitigation:
distinguish 422 from other exceptions, log one explicit
`GAMMA_UNIVERSE_CAP_HIT n=%d`, and skip the `mark_markets_closed`
side effect when the cap (or any non-200) fires.

---

## 3. HNSW corruption recovery — denied

`NewsMemory._init` (news_memory.py:115-135):

```python
except Exception as exc:
    logger.warning("[news_memory] init failed (%s) — dedup disabled", exc)
```

Failure chain on next dual-container race:
1. SIGSEGV → faulthandler dumps C-trace → container restart.
2. Index still corrupted → `_init` raises → caught → `_enabled=False`.
3. Every `assess()` returns default `FreshnessResult("BREAKING")`
   (fail-open). Engine.process never short-circuits, pays full
   Stage-3 LLM tokens forever.
4. One `[news_memory] init failed` line is the only signal to the
   operator.

Master plan C.3 ("auto chroma rebuild on corruption") is **confirmed
pending**.

**Recommended fix.** On `_init` exception: (a) close client, (b) rename
`path` → `path.bad_<ts>`, (c) recreate empty collection, (d) optional
backfill from `signals.headline WHERE detected_at > now-7d`.

---

## 4. MarketIndex storage + currency

- Embeddings live in `markets.embedding` BLOB (verified via
  `PRAGMA table_info`).
- Re-embedded only on `refresh()` (every 4h). Model id hard-coded to
  `sentence-transformers/all-MiniLM-L6-v2`. If we ever swap embedders,
  the index becomes a **mix of two embedding spaces** — there's no
  `model_version` column so detecting drift requires matrix-diffing.
  Flag for Phase 4E.2.
- `upsert_market`'s `ON CONFLICT DO UPDATE` refreshes the embedding
  every cycle (good). In-memory `self._matrix` swaps only at end of
  `refresh()` — no torn reads during the refresh.

---

## 5. Freshness thresholds

Configured in `config.yaml:681-694`:

| Knob | Value | Effect |
|---|---|---|
| `dedup_distance_threshold` | 0.12 (~sim 0.88) | d < this → REHASH candidate |
| `fresh_dist_threshold` | 0.30 | d > this → BREAKING regardless |
| `rehash_age_secs` | 21600 (6h) | REHASH window |
| `stale_age_secs` | 86400 (24h) | STALE_RETURN window |
| `recent_context_window_secs` | 86400 (24h) | context retrieval window |
| `recent_context_n` | 3 | top-N priors → Stage-3 |
| `short_circuit_labels` | `[]` (EMPTY) | nothing is short-circuited |

**Soft bug.** `short_circuit_labels: []` overrides the constructor
default `["REHASH", "STALE_RETURN"]`. The cycle-13 token-saving
short-circuit is **currently OFF**. Likely a relic of the Stage-2
strip (handoff `2026-05-22-v2.md` §2). Reset to
`["REHASH", "STALE_RETURN"]` before re-enabling any LLM-gated stage.

---

## 6. Race conditions in find_matches

**No locks.** `refresh()` mutates `self._markets`, `self._matrix`,
`self._last_refresh` via plain attribute writes. Concrete race:

- Reader: `scores = self._matrix @ q` reads old (N, 384) matrix.
- Writer: `self._markets = markets` (new len M ≠ N) lands BEFORE
  `self._matrix = embeddings`.
- Reader's heat-boost loop iterates `enumerate(self._markets)` against
  OLD `scores`: heat applied to wrong market — or `IndexError` if M < N.

Same pattern with `load_heat_from_db` (main.py:300 every refresh):
single attribute swap of `_heat_scores` is racy with the find_matches
reader.

**Documentation status: implicit.** The docstring doesn't acknowledge
concurrent refresh/lookup; engine.process holds no lock around
find_matches.

**Fix scope.** Either add `threading.Lock` around the two state
mutations + the read+enumerate, OR atomic-swap via a single tuple
`self._state = (markets, matrix)` and read once into a local. Window
is small (two assignments) but real.

---

## 7. Smaller items

- `find_matches` logs nothing — cannot diagnose "why did this match
  zero" without offline repro. Phase 1 logging item.
- `Embedder.embed` lazy-loads if `_model is None`. main.py calls
  `load()` explicitly at boot, but a forgotten call would block ~2s
  on the asyncio loop downloading the model.
- `_parse_market` silently drops markets with empty `question` — log
  the count per refresh.
- `mark_markets_closed` full-scans `markets WHERE closed_at IS NULL`
  every refresh. Fine at 10k; needs an index on `closed_at` at the
  50k target.

---

## Cross-references

- `master-plan.md:138` — A0.9 deliverable (this report).
- `master-plan.md:720` — A.3 concept graph → Phase 4E.2 (confirms no
  concept-link today).
- `handoffs/2026-05-22.md` Plot 1 — HNSW corruption story.
- `handoffs/2026-05-22-v2.md` §2 — Stage-2 strip (informs the
  `short_circuit_labels=[]` analysis).
- `reports/2026-05-22-engine-bugs.md` — engine.process consumer
  (orthogonal scope).
