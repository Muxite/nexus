"""
seulgi/scripts/replay — historical headline replay harness.

Used to stress-test the database write path against a fresh, isolated
test DB by re-injecting headlines that already ran through production.
Eliminates the "wait for live traffic" feedback loop.

See /home/muk/.claude/plans/code-shipped-all-137-tranquil-turtle.md
Part 1.
"""
