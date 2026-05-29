"""Filesystem layout for the sandbox. One place to change roots."""

from __future__ import annotations

import os

REPO_ROOT = "/home/muk/projects/jennie"
DB_PATH = os.path.join(REPO_ROOT, "irene/data/irene.db")

PKG_ROOT = os.path.join(REPO_ROOT, "irene/sandbox")
STRATEGIES_DIR = os.path.join(PKG_ROOT, "strategies")

DATA_ROOT = os.path.join(REPO_ROOT, "irene/data/sandbox")
SETS_DIR = os.path.join(DATA_ROOT, "sets")
CACHE_DIR = os.path.join(DATA_ROOT, "cache", "llm")
RUNS_DIR = os.path.join(DATA_ROOT, "runs")
VERDICTS_DIR = os.path.join(DATA_ROOT, "verdicts")


def ensure_data_dirs() -> None:
    for d in (SETS_DIR, CACHE_DIR, RUNS_DIR, VERDICTS_DIR):
        os.makedirs(d, exist_ok=True)
