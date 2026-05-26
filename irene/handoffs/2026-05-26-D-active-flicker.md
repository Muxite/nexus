# Handoff D — Active-flicker repair + cleanup migration

**Date:** 2026-05-26
**Reference:** `irene/handoffs/2026-05-26-A-iran-funnel-audit.md`
**Dependency:** none.
**Blocks:** any code path that trusts `active==1` alone (including handoff C's `_gate_market_state` if `closed_at` lookups race).

## The problem

`SELECT COUNT(*) FROM markets WHERE active=1 AND closed_at IS NOT NULL` returns **1,022** today — ~10% of the active universe in an inconsistent state. These rows say "market is open" AND "market closed at <past date>" simultaneously.

Cycle 18 partially fixed this by COALESCEing `closed_at` so a once-stamped value isn't blown away by a later Gamma poll. But cycle 18 didn't force `active=0` when `closed_at` is preserved. So when Gamma re-emits a previously-closed market with `closed=false` (the active-flicker bug from cycle 12), the upsert leaves us with `closed_at` intact (good) and `active=1` (wrong — Gamma is wrong; markets don't re-open on Polymarket).

This contradictory state is dangerous:
- Handoff C's `_gate_market_state` reads `closed_at` and `active` independently. If `closed_at` is preserved but `active=1` slips through (e.g. via a different upsert path that doesn't include closed_at), we trade on a dead market.
- Trade 40 in handoff A is the worst-case example — DKG bought NO on a market with `active=1, closed_at='2026-05-23', end_date='2026-03-31'`.

## The fix

### Step 1 — SQL-side guard in `_upsert_market`

File: `app/stages/markets.py` (around line 1037 for the `INSERT ... ON CONFLICT` block).

Current:
```sql
ON CONFLICT(condition_id) DO UPDATE SET
    ...,
    active = excluded.active,
    closed_at = COALESCE(markets.closed_at, excluded.closed_at),
    ...
```

Replace with:
```sql
ON CONFLICT(condition_id) DO UPDATE SET
    ...,
    -- Cycle 25+: markets do not re-open after closing on Polymarket.
    -- If we have a historical closed_at, force active=0 regardless of
    -- what Gamma reports. This is the active-flicker fix that cycle-18
    -- left incomplete.
    active = CASE WHEN markets.closed_at IS NOT NULL THEN 0 ELSE excluded.active END,
    closed_at = COALESCE(markets.closed_at, excluded.closed_at),
    ...
```

Single-line semantic change, schema-side; no Python edits needed for the gate logic.

### Step 2 — one-shot data cleanup

The 1,022 existing flickered rows need to be reconciled. Ship a migration:

File: `scripts/migrate/2026_05_26_active_flicker.py` (new, mirrors any existing migration pattern; if there isn't one, this can also live in `app/db.py`'s migrations list — check current pattern via `grep -rn "schema_migrations" app/`).

Contents:

```sql
UPDATE markets SET active = 0
 WHERE active = 1
   AND closed_at IS NOT NULL;
```

This is idempotent; safe to re-run. Expected change: 1,022 rows updated to active=0 on first run, 0 on re-run.

### Step 3 — regression test

File: `app/tests/stages/test_markets.py` — new test case:

```python
async def test_upsert_does_not_reactivate_closed_market(db_ready, bus, embedder):
    stage = _make_stage(db_ready, bus, embedder)
    # First upsert: market closed.
    row = _gamma_row("0xFLK1", "Test Q", closed=True, closedTime="2026-05-23T05:51:21Z")
    await stage._upsert_market(_parse_gamma_row(row))
    # Confirm initial state.
    r = await (await db_ready.conn.execute(
        "SELECT active, closed_at FROM markets WHERE condition_id='0xFLK1'"
    )).fetchone()
    assert r["active"] == 0
    assert r["closed_at"] is not None
    # Second upsert: Gamma re-emits the same market with closed=false (flicker).
    row2 = _gamma_row("0xFLK1", "Test Q", closed=False)
    await stage._upsert_market(_parse_gamma_row(row2))
    r = await (await db_ready.conn.execute(
        "SELECT active, closed_at FROM markets WHERE condition_id='0xFLK1'"
    )).fetchone()
    # Cycle-25 fix: active stays 0 because closed_at is preserved.
    assert r["active"] == 0, "active should NOT flip back to 1"
    assert r["closed_at"] is not None
```

## Files

| Change | File |
|---|---|
| SQL CASE in upsert | `app/stages/markets.py` (line 1037 area) |
| Cleanup migration | `scripts/migrate/2026_05_26_active_flicker.py` (new) — or appended to existing migration list in `app/db.py` |
| Regression test | `app/tests/stages/test_markets.py` (new case) |

## Verification

1. `pytest app/tests/stages/test_markets.py` → all pass including the new flicker case.
2. Before applying migration: `SELECT COUNT(*) FROM markets WHERE active=1 AND closed_at IS NOT NULL` → 1,022.
3. Apply migration. After: same query → 0.
4. Rebuild + restart. Let one full poll cycle run (≤2 min). Re-query → still 0 (the new SQL CASE prevents new flickers).
5. Optional: monitor for 24h. The count should stay 0; if it drifts above 0, dig into how a non-`_upsert_market` writer is bypassing the CASE.

## Coordinate with…

- **Handoff C** — C's `_gate_market_state` checks both `active` and `closed_at`. After D, those two fields are guaranteed to be consistent. Together, the bot will never trade on a closed market.
- **Handoff E** — E displays "unpriceable" for closed-market positions; once D normalizes the data, E's logic becomes simpler (just check `closed_at IS NOT NULL` instead of `closed_at OR active==0`).

## Subagent prompt suggestion

> Read `irene/handoffs/2026-05-26-D-active-flicker.md` end-to-end. Apply the SQL CASE change in `app/stages/markets.py:_upsert_market`. Write and run the cleanup migration. Add the regression test. Run `pytest app/tests/stages/test_markets.py`. Rebuild + restart. Verify the count before/after migration matches the doc. Do NOT touch any other handoff scope.
