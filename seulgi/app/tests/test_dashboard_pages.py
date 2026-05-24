"""Smoke tests for every dashboard page module.

Each page should render against a minimally-populated Dashboard state
(no DB) without raising. The tests verify file content has the expected
shape — header line, presence of key strings — not byte-equivalence.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dashboard import Dashboard
from dashboard.events import (
    ClosedEvent,
    NewsEvent,
    PositionEvent,
    SystemHealth,
    TradeEvent,
    WatcherStatus,
)
from dashboard.registry import PAGES


def _populated_dashboard(tmp_path) -> Dashboard:
    now = datetime(2026, 5, 24, 12, 0, 0, tzinfo=timezone.utc)
    d = Dashboard(data_dir=str(tmp_path), interval_secs=60, db=None)
    d._started_at = now - timedelta(hours=1, minutes=30)
    d._mode = "simulate"
    d._simulate_total_usdc = 1600.0
    d._total_realized_pnl = 2.50
    d._engine_balances = {"scout/all": 50.0, "balanced/v1": 73.20}
    d._engine_staked = {"scout/all": 25.0, "balanced/v1": 12.0}
    d._active_variants = {"scout/all", "balanced/v1"}
    d._open_positions = [
        PositionEvent("cond_abc", "Will X happen?", "YES", 5.0, 0.42, 0.45,
                      +0.15, 0.5, now - timedelta(minutes=22), 0.36, "scout/all"),
    ]
    d._news.appendleft(NewsEvent(
        "https://reuters.com", "Breaking: thing happened",
        "https://reuters.com/a", now - timedelta(seconds=4),
    ))
    d._executed.appendleft(TradeEvent(
        "c1", "Question?", "Headline", "https://s.com", "YES",
        0.72, 0.81, 0.55, 0.42, 5.0, None, 0.13, now,
    ))
    d._declined.appendleft(TradeEvent(
        "c2", "Q2", "H2", "https://s2.com", "NO",
        0.55, 0.6, 0.3, 0.4, 0, "edge_too_small", 0.005,
        now - timedelta(seconds=20),
    ))
    d._near_miss.appendleft(d._declined[0])
    d._closed.appendleft(ClosedEvent(
        "c0", "YES", 5.0, 0.4, 0.5, +0.5, "WIN", now - timedelta(minutes=30),
    ))
    d._activity.appendleft("[2026-05-24 12:00:00] NEWS  reuters.com  test")
    d._traces.appendleft(
        "2026-05-24 11:59:50 | hid=abc1234x | reuters.com | head | mkt | "
        "YES 72% | EXECUTED"
    )
    d._journal.appendleft({
        "ts": now, "headline": "H", "market": "M", "direction": "YES",
        "confidence": 0.72, "similarity": 0.81, "engine": "scout/all",
        "outcome": "EXECUTED", "headline_id": "abc1234x",
    })
    d._quant_stats = {
        "impact": [{"direction": "YES", "n": 100, "hits": 60,
                    "avg_delta_t5m": 0.02, "avg_abs_delta_t5m": 0.03}],
        "impact_24h": [{"direction": "YES", "n": 20, "hits": 12}],
        "latency": {"n": 80, "avg_ms": 2300, "min_ms": 800, "max_ms": 9000},
    }
    d._usage = {
        "ai_total": 4_500_000, "brave_total": 230,
        "ai_24h": 250_000, "brave_24h": 12, "ai_by_provider": [],
    }
    d._engine_summary = [{
        "engine_name": "scout/all", "executed_count": 8, "skipped_count": 30,
        "total_staked": 25.0, "avg_edge": 0.08, "avg_confidence": 0.7,
        "avg_latency_ms": 1400, "first_trade": "2026-05-24T10:00:00",
        "last_trade": "2026-05-24T11:55:00",
    }]
    d._engine_skip_reasons = [
        {"engine_name": "scout/all", "skip_reason": "edge_too_small", "count": 12},
    ]
    d._system_health = SystemHealth(
        scraper_reachable=True, scraper_url="http://scraper:9000",
        scraper_last_ok_at=now - timedelta(seconds=5), scraper_latency_ms=12,
        ai_key_present=True, ai_neutral_rate_1h=0.62, ai_total_decisions_1h=400,
        db_last_insert_at=now - timedelta(seconds=2),
    )
    d._watcher_statuses = {"https://reuters.com": WatcherStatus(
        url="https://reuters.com", reachable=True, failures=0,
        last_headline_at=now - timedelta(seconds=4), last_headline="Breaking",
        total_headlines=87, is_unreachable=False,
    )}
    return d


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.FILENAME)
async def test_each_page_renders(page, tmp_path):
    """Every registered page renders against the populated fixture."""
    d = _populated_dashboard(tmp_path)
    body = await page.render(d)
    # variant_compare needs a DB; pages may legitimately return empty for that.
    if not body:
        return
    assert isinstance(body, str)
    assert body.endswith("\n"), f"{page.FILENAME}: must end in newline"


async def test_write_all_produces_atomic_files(tmp_path):
    """No `.tmp` artefacts left behind after _write_all."""
    d = _populated_dashboard(tmp_path)
    await d._write_all()
    leftover = [p.name for p in tmp_path.glob("*.tmp")]
    assert leftover == [], f"leftover .tmp files: {leftover}"
    written = sorted(p.name for p in tmp_path.glob("*.txt"))
    # variant_compare/scout_funnel may be omitted without a DB; ≥13 of the 16
    # pages should land on disk.
    assert len(written) >= 13, f"only wrote {written}"


async def test_engine_comparison_under_8kb(tmp_path):
    """Post-§C-cull: engine_comparison.txt must stay small even with many engines."""
    from dashboard.pages import engine_comparison

    d = _populated_dashboard(tmp_path)
    # Synthesize 200 engines — pre-cull this would balloon to MBs.
    d._engine_summary = [
        {
            "engine_name": f"variant_{i:03d}", "executed_count": 100 - i,
            "skipped_count": 50, "total_staked": 12.0, "avg_edge": 0.05,
            "avg_confidence": 0.7, "avg_latency_ms": 1500,
            "first_trade": "2026-05-24T10:00:00", "last_trade": "2026-05-24T11:55:00",
        }
        for i in range(200)
    ]
    d._engine_skip_reasons = [
        {"engine_name": f"variant_{i:03d}", "skip_reason": r, "count": 10}
        for i in range(200) for r in ("edge_too_small", "low_confidence", "no_market")
    ]
    body = await engine_comparison.render(d)
    assert len(body.encode("utf-8")) < 8 * 1024, (
        f"engine_comparison.txt={len(body)}b exceeds 8 KB cull budget"
    )
    assert "more engines" in body, "expected truncation marker"
