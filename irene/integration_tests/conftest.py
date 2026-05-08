"""
Pytest configuration for integration tests.

Marks all tests in this directory as `integration` and sets up asyncio mode.
"""

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: requires real PostgreSQL, Redis, and optionally network access",
    )
