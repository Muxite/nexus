"""Deadline parsing — ET-by-default, explicit-offset respected, title fallback.

Background: Polymarket's frontend and API both surface deadlines in Eastern
Time. A naïve ISO timestamp from Gamma (``"2026-06-30T23:59:59"``, no `Z`,
no offset) must be interpreted as ET — not UTC — because a DST-related
platform incident confirmed ET as the canonical clock.

Source: ``irene/questions/unstructured answers.md`` lines 183-185:
- "Polymarket's help center says the market rules define the end date,
  resolution source, and edge cases" — the market page is the source of
  truth, not the English phrase alone.
- "At least some Polymarket markets use Eastern Time as the operative
  clock, because a DST-related incident was specifically described as
  arising from ET-based timing on the platform."

These tests pin the four cases from the resolution-oracle.md D2 spec:
naïve→ET, explicit-UTC→UTC, explicit-offset→respected, title→ET-EOD.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

# DDL side-effect imports — same pattern as test_trade.py so the stage
# module loads cleanly.
import app.stages.trade  # noqa: F401
from app.stages.trade import TradeStage
from app.tests._dates import iso_days_from_now


# ---------------------------------------------------------------------------
# _parse_end_date_string — direct unit tests
# ---------------------------------------------------------------------------


def test_naive_iso_treated_as_eastern_time():
    """Naïve `2026-06-30T23:59:59` → 2026-07-01T03:59:59 UTC (June = EDT, UTC-4)."""
    parsed = TradeStage._parse_end_date_string("2026-06-30T23:59:59")
    assert parsed is not None
    assert parsed.tzinfo is timezone.utc
    assert parsed == datetime(2026, 7, 1, 3, 59, 59, tzinfo=timezone.utc)


def test_explicit_utc_z_suffix_preserved():
    """`...Z` is explicit UTC and must stay UTC regardless of ET default."""
    parsed = TradeStage._parse_end_date_string("2026-06-30T23:59:59Z")
    assert parsed is not None
    assert parsed.tzinfo is timezone.utc
    assert parsed == datetime(2026, 6, 30, 23, 59, 59, tzinfo=timezone.utc)


def test_explicit_offset_respected():
    """`+02:00` is explicit; we convert to UTC but do not re-interpret it."""
    parsed = TradeStage._parse_end_date_string("2026-06-30T23:59:59+02:00")
    assert parsed is not None
    # 23:59:59 +02:00 → 21:59:59 UTC.
    assert parsed == datetime(2026, 6, 30, 21, 59, 59, tzinfo=timezone.utc)


def test_explicit_zero_offset_variants_preserved():
    """`+00:00` and `+0000` are both UTC and must round-trip unchanged."""
    parsed_colon = TradeStage._parse_end_date_string("2026-06-30T23:59:59+00:00")
    parsed_compact = TradeStage._parse_end_date_string("2026-06-30T23:59:59+0000")
    expected = datetime(2026, 6, 30, 23, 59, 59, tzinfo=timezone.utc)
    assert parsed_colon == expected
    assert parsed_compact == expected


def test_winter_naive_iso_treated_as_eastern_standard_time():
    """January = EST (UTC-5). Naïve 23:59:59 → +05:00 = next-day 04:59:59 UTC.
    Regression guard for the DST-aware ZoneInfo behaviour."""
    parsed = TradeStage._parse_end_date_string("2026-01-15T23:59:59")
    assert parsed == datetime(2026, 1, 16, 4, 59, 59, tzinfo=timezone.utc)


def test_unparseable_string_returns_none():
    assert TradeStage._parse_end_date_string("not-a-date") is None
    assert TradeStage._parse_end_date_string("") is None


# ---------------------------------------------------------------------------
# Title-string fallback
# ---------------------------------------------------------------------------


def test_title_fallback_fires_when_end_date_missing():
    """`by June 30, 2026` in title → 2026-06-30 23:59:59 ET = 2026-07-01 03:59:59 UTC."""
    market = {
        "end_date": None,
        "question": "Will rates be cut by June 30, 2026?",
        "description": None,
    }
    parsed = TradeStage._parse_market_deadline(market)
    assert parsed == datetime(2026, 7, 1, 3, 59, 59, tzinfo=timezone.utc)


def test_title_fallback_does_not_fire_when_end_date_present():
    """End_date wins; title-fallback only fires as a last resort.
    `end_date=2027-01-01T00:00:00Z` (UTC) must NOT be overridden by
    a `by June 30` phrase in the question."""
    market = {
        "end_date": "2027-01-01T00:00:00Z",
        "question": "Will rates be cut by June 30, 2026?",
        "description": None,
    }
    parsed = TradeStage._parse_market_deadline(market)
    assert parsed == datetime(2027, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_title_fallback_in_description_when_question_silent():
    market = {
        "end_date": None,
        "question": "Will X happen?",
        "description": "Market resolves YES if X occurs by December 1, 2026.",
    }
    parsed = TradeStage._parse_market_deadline(market)
    # Dec 1 = EST (UTC-5). 23:59:59 ET → next-day 04:59:59 UTC.
    assert parsed == datetime(2026, 12, 2, 4, 59, 59, tzinfo=timezone.utc)


def test_title_fallback_returns_none_when_no_match():
    market = {"end_date": None, "question": "Will X happen?", "description": None}
    assert TradeStage._parse_market_deadline(market) is None


# ---------------------------------------------------------------------------
# _days_to — accepts both dict and string for backwards compatibility
# ---------------------------------------------------------------------------


def test_days_to_accepts_dict_payload():
    """The new call site passes the full market dict so title-fallback can fire.

    `_days_to` measures against `datetime.now()`, so the date is relative — a
    pinned literal would eventually go past and flip `days > 0`.
    """
    market = {"end_date": iso_days_from_now(90), "question": "Q?"}
    days = TradeStage._days_to(market)
    assert days is not None and days > 0


def test_days_to_accepts_legacy_string():
    """Legacy callers that still pass a raw end_date string keep working."""
    days = TradeStage._days_to(iso_days_from_now(90))
    assert days is not None and days > 0


def test_days_to_none_for_missing_inputs():
    assert TradeStage._days_to(None) is None
    assert TradeStage._days_to("") is None
    assert TradeStage._days_to({}) is None
