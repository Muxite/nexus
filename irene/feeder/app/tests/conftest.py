"""Feeder test fixtures."""

from __future__ import annotations

import sys
import os

_feeder_app = os.path.join(os.path.dirname(__file__), "..")
_scanner_app = os.path.join(os.path.dirname(__file__), "..", "..", "..", "scanner", "app")
_shared = os.path.join(os.path.dirname(__file__), "..", "..", "..", "shared")

for p in [_feeder_app, _scanner_app, _shared]:
    if p not in sys.path:
        sys.path.insert(0, p)
