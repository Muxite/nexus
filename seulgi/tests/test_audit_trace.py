"""Integration test for scripts/audit/trace_trades.py (plan §8).

Builds an in-memory SQLite DB with one synthesized simulate-mode trade
(closed, RESOLVE_WIN) and asserts the audit script's output renders
every section and labels each check correctly.
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


SCHEMA = """
CREATE TABLE signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_url TEXT, headline TEXT, detected_at TEXT,
    headline_id TEXT, freshness_label TEXT, nearest_age_secs REAL
);
CREATE TABLE trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER, condition_id TEXT, question TEXT,
    token_id TEXT, direction TEXT, similarity REAL,
    p_hat REAL, mid_price REAL, size_usdc REAL,
    skip_reason TEXT, decided_at TEXT, realized_pnl REAL,
    closed_at TEXT, close_reason TEXT, headline_id TEXT,
    confidence_raw REAL, p_hat_if_raw REAL, signed_edge REAL,
    signed_edge_if_raw REAL, ai_reason TEXT, ai_raw_response TEXT,
    ai_prompt_meta TEXT, trader_trace TEXT, mid_yes REAL,
    mid_no REAL, inverted INTEGER, variant_name TEXT,
    engine_name TEXT, agreement REAL, p_alpha REAL, risk_score REAL,
    freshness_label TEXT, relevance_score TEXT, relevance_reason TEXT,
    mkt_vol_30m REAL, mkt_mean_30m REAL, mkt_min_30m REAL,
    mkt_max_30m REAL, mkt_drift_30m REAL, mkt_n_samples INTEGER,
    entry_fee_usdc REAL, exit_fee_usdc REAL
);
CREATE TABLE positions (
    trade_id INTEGER PRIMARY KEY,
    condition_id TEXT, question TEXT, token_id TEXT, direction TEXT,
    size_usdc REAL, entry_price REAL, opened_at TEXT, last_price REAL,
    unrealized_pnl REAL, p_hat REAL, engine_name TEXT,
    high_water_price REAL, shares REAL
);
CREATE TABLE usage_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT, kind TEXT, count REAL, provider TEXT, meta TEXT
);
"""


def _seed(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO signals (id, site_url, headline, detected_at, headline_id) "
        "VALUES (1, 'https://reuters.com', 'Test headline', '2026-05-17T14:02:11+00:00', 'hid-1')"
    )
    # Closed RESOLVE_WIN trade: 5 shares × $1.00 settlement, size_usdc = $2.00.
    # realized_pnl = 5 - 2 = +$3.00.
    cur.execute("""
        INSERT INTO trades (id, signal_id, condition_id, question, token_id, direction,
                            similarity, p_hat, mid_price, size_usdc, skip_reason,
                            decided_at, realized_pnl, closed_at, close_reason,
                            headline_id, ai_reason, trader_trace, mid_yes, mid_no,
                            engine_name, variant_name, entry_fee_usdc, exit_fee_usdc)
        VALUES (10, 1, '0xabc', 'Test market?', 'tok1', 'YES',
                0.80, 0.78, 0.40, 2.00, NULL,
                '2026-05-17T14:02:14+00:00', 3.00, '2026-05-17T14:30:00+00:00',
                'RESOLVE_WIN', 'hid-1', 'Powell said so.', '[{"step":"executed"}]',
                0.40, 0.60, 'balanced', 'balanced/test_variant', 0.05, 0.0)
    """)
    cur.execute("""
        INSERT INTO positions (trade_id, condition_id, question, token_id, direction,
                               size_usdc, entry_price, opened_at, last_price,
                               unrealized_pnl, p_hat, engine_name, shares)
        VALUES (10, '0xabc', 'Test market?', 'tok1', 'YES',
                2.00, 0.39, '2026-05-17T14:02:14+00:00', 1.00, 3.00,
                0.78, 'balanced', 5.0)
    """)
    conn.commit()
    conn.close()


@pytest.fixture
def seeded_db(tmp_path):
    db = tmp_path / "seulgi.db"
    _seed(db)
    return db


def test_audit_script_runs_and_passes(seeded_db, tmp_path):
    out_dir = tmp_path / "audit"
    res = subprocess.run(
        [sys.executable, "-m", "scripts.audit.trace_trades",
         "--engines", "balanced", "-n", "5",
         "--include-open",
         "--out", str(out_dir),
         "--db", str(seeded_db)],
        cwd=str(REPO),
        capture_output=True, text=True,
    )
    assert res.returncode == 0, f"stderr: {res.stderr}\nstdout: {res.stdout}"
    # Trade file written.
    trade_files = list(out_dir.glob("trade_*.txt"))
    assert len(trade_files) == 1, trade_files
    body = trade_files[0].read_text()
    # Every section header is present.
    for header in ["[1] HEADLINE", "[2] MARKET MATCH", "[3] AI VERDICT",
                   "[4] ENGINE RULE EVAL", "[5] FILL", "[6] CLOSE",
                   "[7] AUDIT CHECKS"]:
        assert header in body, f"missing {header}"
    # RESOLVE_WIN math check passes.
    assert "[PASS]" in body
    assert "FAIL" not in body
    # INDEX cross-check renders.
    index = (out_dir / "INDEX.md").read_text()
    assert "Dashboard cross-check" in index
    assert "balanced/test_variant" in index
