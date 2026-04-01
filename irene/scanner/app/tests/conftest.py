"""Scanner test fixtures."""

from __future__ import annotations

import sys
import os

# Make scanner app and irene_shared importable
_app_dir = os.path.join(os.path.dirname(__file__), "..")
_shared_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "shared")
for p in [_app_dir, _shared_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)
