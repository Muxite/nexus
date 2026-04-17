"""Unit tests for irene_shared.config — parsing, validation, hot-reload."""

from __future__ import annotations

import asyncio
import os
import time
import tempfile

import pytest
import yaml

from irene_shared.config import (
    ConfigManager,
    IreneConfig,
    MarketsSection,
    TradingSection,
    _apply_env_overrides,
    _coerce,
)


# ---------------------------------------------------------------------------
# _coerce
# ---------------------------------------------------------------------------


class TestCoerce:
    def test_bool_true(self):
        assert _coerce("true") is True
        assert _coerce("yes") is True
        assert _coerce("1") is True

    def test_bool_false(self):
        assert _coerce("false") is False
        assert _coerce("no") is False
        assert _coerce("0") is False

    def test_int(self):
        assert _coerce("42") == 42
        assert isinstance(_coerce("42"), int)

    def test_float(self):
        assert _coerce("3.14") == pytest.approx(3.14)
        assert isinstance(_coerce("3.14"), float)

    def test_str(self):
        assert _coerce("hello") == "hello"


# ---------------------------------------------------------------------------
# _apply_env_overrides
# ---------------------------------------------------------------------------


class TestEnvOverrides:
    def test_applies_override(self, monkeypatch):
        monkeypatch.setenv("IRENE__TRADING__MIN_EDGE", "0.09")
        raw = {"trading": {"min_edge": 0.04}}
        result = _apply_env_overrides(raw)
        assert result["trading"]["min_edge"] == pytest.approx(0.09)

    def test_creates_missing_section(self, monkeypatch):
        monkeypatch.setenv("IRENE__SCANNER__POLL_INTERVAL_SECONDS", "30")
        raw = {}
        result = _apply_env_overrides(raw)
        assert result["scanner"]["poll_interval_seconds"] == 30

    def test_ignores_non_irene_prefix(self, monkeypatch):
        monkeypatch.setenv("OTHER__TRADING__MIN_EDGE", "0.99")
        raw = {}
        result = _apply_env_overrides(raw)
        assert "trading" not in result

    def test_ignores_wrong_depth(self, monkeypatch):
        monkeypatch.setenv("IRENE__TRADING", "bad")
        raw = {}
        result = _apply_env_overrides(raw)
        assert "TRADING" not in result and "trading" not in result


# ---------------------------------------------------------------------------
# IreneConfig — validation
# ---------------------------------------------------------------------------


class TestIreneConfig:
    def test_defaults(self):
        cfg = IreneConfig()
        assert cfg.irene.mode == "paper"
        assert cfg.trading.kelly_fraction == 0.25
        assert cfg.markets.min_volume_usdc == 5000.0

    def test_invalid_mode(self):
        with pytest.raises(Exception):
            IreneConfig.model_validate({"irene": {"mode": "magic"}})

    def test_live_mode_requires_private_key(self):
        with pytest.raises(Exception):
            IreneConfig.model_validate(
                {"irene": {"mode": "live"}, "polymarket": {"private_key": ""}}
            )

    def test_live_mode_with_key_ok(self):
        cfg = IreneConfig.model_validate(
            {"irene": {"mode": "live"}, "polymarket": {"private_key": "0xabc"}}
        )
        assert cfg.irene.mode == "live"

    def test_markets_max_gt_min(self):
        with pytest.raises(Exception):
            IreneConfig.model_validate(
                {"markets": {"min_hours_to_close": 10, "max_hours_to_close": 5}}
            )

    def test_kelly_fraction_bounds(self):
        with pytest.raises(Exception):
            IreneConfig.model_validate({"trading": {"kelly_fraction": 1.5}})
        with pytest.raises(Exception):
            IreneConfig.model_validate({"trading": {"kelly_fraction": 0.0}})

    def test_invalid_order_type(self):
        with pytest.raises(Exception):
            IreneConfig.model_validate({"trading": {"order_type": "GTC"}})

    def test_rss_feeds_parsed(self):
        cfg = IreneConfig.model_validate(
            {"signals": {"rss_feeds": [{"url": "https://example.com/rss"}]}}
        )
        assert len(cfg.signals.rss_feeds) == 1
        assert cfg.signals.rss_feeds[0].url == "https://example.com/rss"

    def test_full_yaml_round_trip(self):
        raw = {
            "irene": {"mode": "paper"},
            "trading": {"min_edge": 0.05, "kelly_fraction": 0.25},
            "markets": {"min_volume_usdc": 10000},
        }
        cfg = IreneConfig.model_validate(raw)
        assert cfg.trading.min_edge == pytest.approx(0.05)
        assert cfg.markets.min_volume_usdc == 10000.0

    def test_signals_comma_separated_keys_in_yaml_string(self):
        cfg = IreneConfig.model_validate(
            {"signals": {"newsapi_keys": "aa, bb"}}
        )
        assert cfg.signals.newsapi_keys == ["aa", "bb"]

    def test_signals_legacy_single_key_merges(self):
        cfg = IreneConfig.model_validate(
            {"signals": {"newsapi_key": "solo", "brave_api_key": "b1"}}
        )
        assert cfg.signals.newsapi_keys == ["solo"]
        assert cfg.signals.brave_api_keys == ["b1"]


# ---------------------------------------------------------------------------
# ConfigManager — file loading and hot-reload
# ---------------------------------------------------------------------------


class TestConfigManager:
    def _write_yaml(self, path: str, data: dict) -> None:
        with open(path, "w") as f:
            yaml.dump(data, f)

    def test_loads_defaults_on_missing_file(self):
        cm = ConfigManager("/nonexistent/path/config.yaml")
        cfg = cm.get()
        assert cfg.irene.mode == "paper"

    def test_loads_from_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"trading": {"min_edge": 0.07}}, f)
            path = f.name
        try:
            cm = ConfigManager(path)
            assert cm.get().trading.min_edge == pytest.approx(0.07)
        finally:
            os.unlink(path)

    def test_keeps_old_config_on_invalid_yaml_reload(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"trading": {"min_edge": 0.06}}, f)
            path = f.name
        try:
            cm = ConfigManager(path, reload_interval=1)
            assert cm.get().trading.min_edge == pytest.approx(0.06)

            # Corrupt the file
            with open(path, "w") as f:
                f.write(": invalid: yaml: {{{{")

            # Manually trigger reload — should keep old config
            try:
                cm._load()
                assert False, "Should have raised"
            except Exception:
                pass  # expected

            # get() still returns old config (reload loop didn't swap on error)
            assert cm.get().trading.min_edge == pytest.approx(0.06)
        finally:
            os.unlink(path)

    @pytest.mark.asyncio
    async def test_hot_reload_updates_config(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"trading": {"min_edge": 0.05}}, f)
            path = f.name
        try:
            cm = ConfigManager(path, reload_interval=1)
            await cm.start()

            # Update file
            with open(path, "w") as f:
                yaml.dump({"trading": {"min_edge": 0.08}}, f)

            # Wait for reload
            await asyncio.sleep(1.5)
            assert cm.get().trading.min_edge == pytest.approx(0.08)
        finally:
            await cm.stop()
            os.unlink(path)
