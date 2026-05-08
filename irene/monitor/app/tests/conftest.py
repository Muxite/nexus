"""Monitor test fixtures."""

from __future__ import annotations

import sys
import os

_monitor_app = os.path.join(os.path.dirname(__file__), "..")
_shared = os.path.join(os.path.dirname(__file__), "..", "..", "..", "shared")

for p in [_monitor_app, _shared]:
    if p not in sys.path:
        sys.path.insert(0, p)
