"""Repeatable sandbox for iterating on the explorer's LLM prompt + strategy.

Read-only against irene.db. Strategies live in `irene/sandbox/strategies/`
(versioned with the code). Runtime artifacts (sets, runs, llm cache,
verdicts) live under `irene/data/sandbox/` (gitignored).
"""
