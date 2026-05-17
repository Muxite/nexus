"""Pluggable trading strategies carved out of the legacy app/trader.py.

Each strategy package owns its own Trader ABC + Paper/Live implementations,
guard cascade, exit policy, and CoreConfig. The legacy app/trader.py is left
untouched and stays paper-only forever; new strategies live here.
"""
