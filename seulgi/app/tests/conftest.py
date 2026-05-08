"""Shared fixtures for Seulgi tests."""
import asyncio
import sys
import os

import pytest

# Make app modules importable from tests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
