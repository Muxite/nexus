"""Shared test fixtures for irene_shared tests."""

from __future__ import annotations

import sys
import os

# Make irene_shared importable without installation
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
