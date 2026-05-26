# Handoff C — DKG path safety-gate parity (CRITICAL — DKG is unguarded)

**Date:** 2026-05-26
**Reference:** `irene/handoffs/2026-05-26-A-iran-funnel-audit.md` (the trade 40 case).
**Dependency:** none. Can ship in parallel with B and D.
**Blocks:** any decision to enable DKG more aggressively (right now DKG can fire blind).

## The problem

Cycle 23 surgically added three pre-trade safety gates to the MVP path (`app/stages/trade.py`):
- `_gate_mid_unavailable` — rejects when CLOB returns `None`
- `_gate_market_state` — rejects on closed / inactive / near-expiry markets
- An `_insert_trade` post-condition assert refusing degenerate mid

And a kill switch via `cfg.trade.execution_enabled`.

**The DKG path (`app/stages/dkg_trade.py`) got none of these.** Live proof: trade 40 (cycle_id=35, 2026-05-26T10:05:39, NO $200 @ 0.825) went through the DKG cascade on the market *"Will Hezbollah disarm by December 31?"* — a market with `closed_at='2026-05-23T05:51:21'` and `end_date='2026-03-31T00:00:00'`. Two months past end_date. Three days closed. The MVP cycle-23 gates would have rejected this on `market_near_expiry` AND `market_closed`. DKG let it through.

`app/stages/dkg_trade.py:519-526` still has the cycle-23 silent fallback bug:

```python
async def _resolve_mid(self, *, direction: str, market: dict) -> float:
    if self._clob is None:
        return self._trade_cfg.default_mid_price  # silent 0.5 fallback
    ...
    return self._trade_cfg.default_mid_price       # again
```

## The fix — extract gates to a shared module, then port

### Step 1 — `app/stages/_gates.py` (new, shared helpers)

Modularization rule 1 forbids `app/stages/*.py` from importing each other, but it does NOT forbid stages from importing from `app/stages/_<helper>.py` modules — the `app/stages/_entity_overlap.py` precedent is already in use (imported by both `trade.py` and `dkg_trade.py`).

Move from `trade.py` into `_gates.py`:
- `_gate_entry` (line 216)
- `_gate_similarity_floor` (line 340)
- `_gate_freshness` (line 355)
- `_gate_entity_overlap` (line 370)
- `_gate_direction` (line 389)
- `_gate_mid_unavailable` (cycle-23 addition)
- `_gate_market_state` (cycle-23 addition)
- `_gate_mid_degenerate` (cycle-23 wraps it)
- `_gate_wide_spread` (line 424)
- `_gate_market_news_imbalance` (line 483)
- `_gate_edge` (line 538)
- `_gate_size` (line 573)
- `_gate_cash` (line 602)

`trade.py` becomes `from app.stages._gates import _gate_*`. Same for `dkg_trade.py`. No behavior change in trade.py.

### Step 2 — wire the cycle-23 gates into `dkg_trade.py`

Current cascade structure (lines 166–401):

```python
async def handle(self, payload: dict) -> None:
    # … similarity_floor, freshness, market+article lookup, direction, etc. …
    # line 324:
    mid = await self._resolve_mid(direction=direction, market=market)
    # line 330: existing degenerate-mid check
    if mid <= self._trade_cfg.min_mid or mid >= self._trade_cfg.max_mid:
        gate_history.append(_gate_entry("edge", "reject", mid, "mid_degenerate"))
        # ... reject
```

Insert (mirroring trade.py post-cycle-23):

1. **At the top of `handle()`** (after payload validation, before similarity_floor): kill-switch check.
   ```python
   if not self._trade_cfg.execution_enabled:
       gate_history.append(_gate_entry("execution_enabled", "reject", False, True))
       await self._record_reject(..., reason="execution_disabled", ...)
       return
   ```

2. **After market lookup, before entity_overlap** (around line 280): market-state gate.
   ```python
   status, entry, reason = _gate_market_state(market, self._trade_cfg)
   if status == "reject":
       gate_history.append(entry)
       await self._record_reject(..., reason=reason, ...)
       return
   ```

3. **After `_resolve_mid`, before mid_degenerate** (around line 325): mid-unavailable gate.
   ```python
   status, entry, reason = _gate_mid_unavailable(mid)
   if status == "reject":
       gate_history.append(entry)
       await self._record_reject(..., reason=reason, ...)
       return
   ```

4. **Modify `_resolve_mid`** (lines 519–526) — return `Optional[float]`, drop all silent fallbacks (mirror the cycle-23 trade.py fix exactly).

5. **In `_insert_trade`** (DKG's writer): copy the cycle-23 assertion: `assert mid is not None and 0.0 < mid < 1.0, …`.

6. **WARN-once log** in DKG's `_resolve_mid` for the `self._clob is None` path. Mirror trade.py's `_warn_once_clob_missing`.

7. **Widen DKG's `_fetch_market`** SELECT to include `active, closed_at` (cycle-23 trade.py change). Verify whether DKG has its own `_fetch_market` or reuses `trade.py`'s — if shared, no change needed.

### Step 3 — tests

Create `app/tests/stages/test_dkg_trade_safety_gates.py` mirroring `app/tests/stages/test_trade_safety_gates.py` 1:1. Same 10 cases:
- mid-unavailable (clob None) → reject
- mid-unavailable (token missing) → reject
- market-closed → reject
- market-inactive → reject
- market-near-expiry (4h out) → reject
- market-far-from-expiry (12h out) → not rejected by this gate
- market-state permissive when end_date NULL
- execution-disabled → reject early
- execution-enabled default → reject ≠ execution_disabled
- post-condition assert refuses None / 0.0 / 1.0 in `_insert_trade`

Note: the existing `test_trade_safety_gates.py` imports test helpers from `test_trade.py` (`_FakeBus`, `_FakeAI`, `_FakeCLOB`, `_make_stage`, `_payload`, `_single_decision`, `db_seeded`). DKG test will need similar helpers — there's likely an existing `test_dkg_trade.py` to mirror; reuse its setup.

## Files

| Change | File |
|---|---|
| Extract gate helpers | `app/stages/_gates.py` (new) |
| Refactor imports | `app/stages/trade.py` (replace inline `_gate_*` defs with imports) |
| Port gates + assert + kill-switch | `app/stages/dkg_trade.py` (handle cascade + `_resolve_mid` + `_insert_trade`) |
| Mirror tests | `app/tests/stages/test_dkg_trade_safety_gates.py` (new, 10 cases) |

## Verification

1. `pytest app/tests/stages/test_trade_safety_gates.py app/tests/stages/test_dkg_trade_safety_gates.py` → 20/20 pass.
2. Full sweep: `pytest app/tests/ scripts/replay/tests/` — 655 baseline (or whatever current) + new DKG cases all pass.
3. Restart container. After 24h, query: `SELECT COUNT(*) FROM trades t JOIN markets m ON m.condition_id=t.market_id WHERE t.decided_at >= datetime('now','-24 hours') AND t.decision_path='dkg' AND (m.closed_at IS NOT NULL OR m.active!=1)` → expect 0.
4. Trade 40 itself remains in the DB as a historical artifact; it cannot be retroactively rejected. Consider whether to SQL-close it at the current frozen mid (PnL ≈ 0).

## Coordinate with…

- **Handoff D** (active-flicker) — D fixes the data layer so `closed_at IS NOT NULL` is consistent with `active=0`. Together with C, the market-state gate becomes airtight.

## Subagent prompt suggestion

> Read `irene/handoffs/2026-05-26-C-dkg-safety-gates.md` end-to-end. Step 1: extract gate helpers from `app/stages/trade.py` to `app/stages/_gates.py`; trade.py imports from `_gates`. Step 2: wire the 4 cycle-23 gates into `app/stages/dkg_trade.py`. Step 3: write `app/tests/stages/test_dkg_trade_safety_gates.py` mirroring `test_trade_safety_gates.py`. Run pytest. Rebuild + restart. Verify per the doc. Do NOT touch any other handoff scope.
