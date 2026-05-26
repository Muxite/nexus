"""Fixture helpers for replay tests — builds a synthetic snapshot directory
with canned JSONL rows. Mirrors the shape `snapshot_cycle.py` writes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pytest


def _write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")


def _make_trade(
    *,
    trade_id: int,
    signal_id: int,
    decision_id: int,
    direction: str = "YES",
    confidence: float = 0.70,
    similarity: float = 0.80,
    freshness: str = "BREAKING",
    mid: float = 0.50,
    edge: float = 0.18,
    size: float = 100.0,
    p_hat: float = 0.70,
    decision_path: str = "mvp",
    market_id: str = "m1",
    cycle_id: int = 29,
    market_at_trade: dict | None = None,
) -> dict:
    features = {
        "gate_history": [],
        "prompt": "test prompt",
        "llm_response_excerpt": '{"a":"Y","c":7}',
        "similarity": similarity,
        "freshness": freshness,
        "direction": direction,
        "confidence": confidence,
        "p_hat": p_hat,
        "mid": mid,
        "edge": edge,
        "edge_components": {"mid": mid, "fee_buffer": 0.02, "fallback": True},
        "kelly_full": 0.4,
        "kelly_capped": 0.1,
        "size_usdc": size,
        "balance_usdc": 1000.0,
        "market_at_trade": market_at_trade or {
            "fee_rate": None, "volume_usdc": 50000.0,
            "category": "politics", "min_order_size": None,
            "end_date": "2026-12-31",
        },
    }
    return {
        "id": trade_id,
        "signal_id": signal_id,
        "decision_id": decision_id,
        "prediction_id": 100 + trade_id,
        "market_id": market_id,
        "direction": direction,
        "confidence": confidence,
        "similarity": similarity,
        "freshness": freshness,
        "p_hat": p_hat,
        "mid_price": mid,
        "edge": edge,
        "kelly_fraction": 0.1,
        "size_usdc": size,
        "decision_path": decision_path,
        "features_snapshot": json.dumps(features),
        "cycle_id": cycle_id,
        "is_tracer": 0,
        "tracer_id": None,
        "decided_at": "2026-05-25T04:55:00+00:00",
        "closed_at": None,
        "realized_pnl": None,
        "target_mid": 0.68,
        "decision_latency_ms": 1234.5,
        "decision_latency_breakdown": json.dumps({"fetch_article": 10, "ai_direction": 800}),
    }


def _make_closure(*, trade_id: int, exit_mid: float, pnl: float, reason: str = "timeout") -> dict:
    return {
        "id": 1000 + trade_id,
        "trade_id": trade_id,
        "closed_at": "2026-05-25T06:00:00+00:00",
        "close_reason": reason,
        "exit_mid_price": exit_mid,
        "realized_pnl_usdc": pnl,
        "cycle_id": 29,
        "is_tracer": 0,
        "tracer_id": None,
        "entry_fee_usdc": 0.0,
        "exit_fee_usdc": 0.0,
    }


def _make_impact(
    *,
    trade_id: int,
    direction: str,
    entry_mid: float,
    deltas: dict[str, float],
    edge_realised: dict[str, float],
    pre_drift_60m: float | None = None,
) -> dict:
    row = {
        "id": trade_id,
        "trade_id": trade_id,
        "market_id": "m1",
        "token_id": "ty",
        "direction": direction,
        "t0_iso": "2026-05-25T04:55:00+00:00",
        "entry_mid": entry_mid,
        "is_tracer": 0,
        "tracer_id": None,
        "created_at": "2026-05-25T04:55:00+00:00",
        "last_polled_at": "2026-05-26T04:55:00+00:00",
        "pre_news_drift_15m": None,
        "pre_news_drift_60m": pre_drift_60m,
    }
    for sec in [60, 300, 900, 1800, 3600, 7200, 14400, 28800, 86400]:
        row[f"price_t{sec}s"] = entry_mid + deltas.get(f"t{sec}s", 0.0)
        row[f"delta_t{sec}s"] = deltas.get(f"t{sec}s")
        row[f"edge_realised_t{sec}s"] = edge_realised.get(f"t{sec}s")
    return row


def _make_decision(
    *, signal_id: int, decision_id: int, outcome: str = "execute",
    reason: str = "ok", cycle_id: int = 29, prediction_id: int | None = None,
) -> dict:
    return {
        "id": decision_id,
        "signal_id": signal_id,
        "outcome": outcome,
        "reason": reason,
        "gate_history": json.dumps([]),
        "prediction_id": prediction_id,
        "cycle_id": cycle_id,
        "is_tracer": 0,
        "tracer_id": None,
        "created_at": "2026-05-25T04:55:00+00:00",
        "decision_latency_ms": None,
        "decision_latency_breakdown": None,
    }


def _make_signal(*, signal_id: int, article_id: int = 1, market_id: str = "m1",
                  similarity: float = 0.80, freshness: str = "BREAKING",
                  cycle_id: int = 29) -> dict:
    return {
        "id": signal_id,
        "article_id": article_id,
        "market_id": market_id,
        "similarity": similarity,
        "cluster_id": None,
        "freshness": freshness,
        "embedder_id": "fake@4",
        "cycle_id": cycle_id,
        "is_tracer": 0,
        "tracer_id": None,
        "created_at": "2026-05-25T04:55:00+00:00",
    }


def _make_prediction(*, prediction_id: int, direction: str = "YES",
                      confidence: float = 0.70, prompt_sha: str = "abc123",
                      cycle_id: int = 29) -> dict:
    return {
        "id": prediction_id,
        "prompt_sha": prompt_sha,
        "direction": direction,
        "confidence": confidence,
        "cached": 0,
        "raw_response": '{"a":"Y","c":7}',
        "label": "Y7",
        "cycle_id": cycle_id,
        "is_tracer": 0,
        "tracer_id": None,
        "created_at": "2026-05-25T04:55:00+00:00",
    }


def _make_decision_input(*, signal_id: int, payload: dict, snapshot: dict, cycle_id: int = 29) -> dict:
    return {
        "id": signal_id,
        "kind": "signal",
        "ref_id": signal_id,
        "payload_json": json.dumps(payload),
        "snapshot_json": json.dumps(snapshot),
        "cycle_id": cycle_id,
        "is_tracer": 0,
        "tracer_id": None,
        "captured_at": "2026-05-25T04:54:00+00:00",
    }


@pytest.fixture
def snapshot_dir(tmp_path: Path) -> Path:
    """Builds a synthetic snapshot directory with 3 trades:
        - trade 1: clean execute, fired+closed, impact shows positive edge realised
        - trade 2: NO trade, fired+closed, impact shows negative drift
        - trade 3: fired, NOT closed, no impact (edge case)
    Plus matching decisions, signals, predictions, decision_inputs."""
    d = tmp_path / "cycle_0099_test"
    d.mkdir()

    trades = [
        _make_trade(trade_id=1, signal_id=10, decision_id=100, direction="YES",
                    confidence=0.80, mid=0.40, edge=0.38, size=100.0, p_hat=0.80,
                    freshness="BREAKING"),
        _make_trade(trade_id=2, signal_id=20, decision_id=200, direction="NO",
                    confidence=0.75, mid=0.60, edge=0.13, size=80.0, p_hat=0.25,
                    freshness="EVOLVING"),
        _make_trade(trade_id=3, signal_id=30, decision_id=300, direction="YES",
                    confidence=0.65, mid=0.50, edge=0.13, size=50.0, p_hat=0.65,
                    freshness="BREAKING", market_at_trade={
                        "fee_rate": 0.072, "volume_usdc": 100000.0,
                        "category": "crypto", "min_order_size": None,
                        "end_date": "2026-12-31",
                    }),
    ]

    closures = [
        _make_closure(trade_id=1, exit_mid=0.50, pnl=25.0),  # YES, mid 0.40→0.50 = profitable
        _make_closure(trade_id=2, exit_mid=0.55, pnl=10.0, reason="take_profit"),  # NO, mid 0.60→0.55 = profitable
    ]

    impacts = [
        _make_impact(
            trade_id=1, direction="YES", entry_mid=0.40,
            deltas={"t60s": 0.0, "t300s": 0.0, "t900s": 0.02, "t1800s": 0.05,
                    "t3600s": 0.05, "t7200s": 0.10, "t14400s": 0.10},
            edge_realised={"t60s": 0.0, "t300s": 0.0, "t900s": 0.02, "t1800s": 0.05,
                           "t3600s": 0.05, "t7200s": 0.10, "t14400s": 0.10,
                           "t28800s": 0.10, "t86400s": 0.10},
            pre_drift_60m=0.01,
        ),
        _make_impact(
            trade_id=2, direction="NO", entry_mid=0.60,
            deltas={"t60s": 0.0, "t300s": -0.01, "t900s": -0.02, "t1800s": -0.03,
                    "t3600s": -0.05},
            edge_realised={"t60s": 0.0, "t300s": 0.01, "t900s": 0.02, "t1800s": 0.03,
                           "t3600s": 0.05, "t7200s": 0.05, "t14400s": 0.05,
                           "t28800s": 0.05, "t86400s": 0.05},
            pre_drift_60m=-0.02,
        ),
        # trade 3 has NO impact row (edge case — we want some trades without)
    ]

    signals = [
        _make_signal(signal_id=10, article_id=1, freshness="BREAKING"),
        _make_signal(signal_id=20, article_id=2, freshness="EVOLVING"),
        _make_signal(signal_id=30, article_id=3, freshness="BREAKING"),
    ]

    predictions = [
        _make_prediction(prediction_id=101, direction="YES", confidence=0.80, prompt_sha="s1"),
        _make_prediction(prediction_id=102, direction="NO", confidence=0.75, prompt_sha="s2"),
        _make_prediction(prediction_id=103, direction="YES", confidence=0.65, prompt_sha="s3"),
    ]

    decisions = [
        _make_decision(signal_id=10, decision_id=100, prediction_id=101),
        _make_decision(signal_id=20, decision_id=200, prediction_id=102),
        _make_decision(signal_id=30, decision_id=300, prediction_id=103),
    ]

    decision_inputs = [
        _make_decision_input(signal_id=10, payload={"article_id": 1, "prompt_sha": "s1"},
                             snapshot={"similarity": 0.80, "freshness": "BREAKING",
                                       "market_id": "m1", "title": "t1"}),
        _make_decision_input(signal_id=20, payload={"article_id": 2, "prompt_sha": "s2"},
                             snapshot={"similarity": 0.70, "freshness": "EVOLVING",
                                       "market_id": "m1", "title": "t2"}),
        _make_decision_input(signal_id=30, payload={"article_id": 3, "prompt_sha": "s3"},
                             snapshot={"similarity": 0.85, "freshness": "BREAKING",
                                       "market_id": "m1", "title": "t3"}),
    ]

    articles = [
        {"id": 1, "source": "reuters", "source_detail": "rss", "url": "https://r/1",
         "title": "Article 1", "summary": "s1", "published_at": "2026-05-25T04:50:00+00:00",
         "ingested_at": "2026-05-25T04:54:00+00:00", "cycle_id": 29, "is_tracer": 0, "tracer_id": None},
        {"id": 2, "source": "axios", "source_detail": "rss", "url": "https://a/2",
         "title": "Article 2", "summary": "s2", "published_at": "2026-05-25T04:50:00+00:00",
         "ingested_at": "2026-05-25T04:54:00+00:00", "cycle_id": 29, "is_tracer": 0, "tracer_id": None},
        {"id": 3, "source": "reuters", "source_detail": "rss", "url": "https://r/3",
         "title": "Article 3", "summary": "s3", "published_at": "2026-05-25T04:50:00+00:00",
         "ingested_at": "2026-05-25T04:54:00+00:00", "cycle_id": 29, "is_tracer": 0, "tracer_id": None},
    ]

    _write_jsonl(d / "trades.jsonl", trades)
    _write_jsonl(d / "trade_closures.jsonl", closures)
    _write_jsonl(d / "price_impacts.jsonl", impacts)
    _write_jsonl(d / "signals.jsonl", signals)
    _write_jsonl(d / "predictions.jsonl", predictions)
    _write_jsonl(d / "decisions.jsonl", decisions)
    _write_jsonl(d / "decision_inputs.jsonl", decision_inputs)
    _write_jsonl(d / "articles.jsonl", articles)
    _write_jsonl(d / "llm_probe_calls.jsonl", [])
    _write_jsonl(d / "trade_features.jsonl", [])
    _write_jsonl(d / "llm_calls.jsonl", [])
    _write_jsonl(d / "llm_parse_failures.jsonl", [])

    manifest = {
        "cycle_id": 99,
        "cycle_name": "test",
        "opened_at": "2026-05-25T00:00:00+00:00",
        "snapshotted_at": "2026-05-26T00:00:00+00:00",
        "tables": {
            "trades": 3, "decisions": 3, "predictions": 3, "trade_closures": 2,
            "llm_calls": 0, "llm_parse_failures": 0, "decision_inputs": 3,
            "trade_features": 0, "signals": 3, "price_impacts": 2,
            "llm_probe_calls": 0,
        },
        "wallet": {
            "initial_balance": 1000.0,
            "cash": 765.0,
            "open_invested": 50.0,
            "open_current_value": 50.0,
            "realized_pnl": 35.0,
            "equity": 1035.0,
            "accounting_identity_ok": True,
            "accounting_drift": 0.0,
            "n_trades": 3, "n_open": 1, "n_closed": 2,
            "by_decision_path": {"mvp": {"trades": 3, "closed": 2, "realized": 35.0, "open": 1}},
            "by_close_reason": {"timeout": 1, "take_profit": 1},
        },
    }
    (d / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return d
