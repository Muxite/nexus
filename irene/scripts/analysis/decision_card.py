"""decision — one decision row's full provenance.

Given a `--decision-id` or a `--trade-id`, pull the entire chain:
article → cluster → signal → prediction → decision → trade →
trade_closures → price_impacts. The 'decision card' for a single trade.
"""

from __future__ import annotations

from scripts.analysis._common import add_common_args
from scripts.analysis._format import Section


def add_parser(p) -> None:
    add_common_args(p)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--decision-id", type=int)
    g.add_argument("--trade-id", type=int)
    g.add_argument("--signal-id", type=int)


def run(args, conn) -> Section:
    # Resolve to a signal_id from whichever input we got.
    if args.signal_id is not None:
        signal_id = args.signal_id
    elif args.trade_id is not None:
        row = conn.execute(
            "SELECT signal_id FROM trades WHERE id = ?", (args.trade_id,)
        ).fetchone()
        if row is None:
            raise SystemExit(f"trade_id={args.trade_id} not found")
        signal_id = row["signal_id"]
    else:
        row = conn.execute(
            "SELECT signal_id FROM decisions WHERE id = ?", (args.decision_id,)
        ).fetchone()
        if row is None:
            raise SystemExit(f"decision_id={args.decision_id} not found")
        signal_id = row["signal_id"]

    chain = conn.execute(
        """
        SELECT
            a.id              AS article_id,
            a.title           AS article_title,
            a.published_at    AS article_published_at,
            a.ingested_at     AS article_ingested_at,
            s.id              AS signal_id,
            s.market_id       AS market_id,
            s.similarity      AS similarity,
            s.freshness       AS freshness,
            s.created_at      AS signal_created_at,
            d.id              AS decision_id,
            d.outcome         AS outcome,
            d.reason          AS reason,
            d.created_at      AS decision_created_at,
            t.id              AS trade_id,
            t.direction       AS direction,
            t.mid_price       AS mid_price,
            t.edge            AS edge,
            t.size_usdc       AS size_usdc,
            t.decided_at      AS trade_decided_at,
            c.closed_at       AS closed_at,
            c.exit_mid_price  AS exit_mid_price,
            c.realized_pnl_usdc AS realized_pnl_usdc,
            c.close_reason    AS close_reason
        FROM signals s
        JOIN articles a   ON a.id = s.article_id
        LEFT JOIN decisions d ON d.signal_id = s.id
        LEFT JOIN trades t    ON t.signal_id = s.id
        LEFT JOIN trade_closures c ON c.trade_id = t.id
        WHERE s.id = ?
        """,
        (signal_id,),
    ).fetchone()
    if chain is None:
        raise SystemExit(f"signal_id={signal_id} has no DB rows")

    impacts = conn.execute(
        """
        SELECT t0_iso, entry_mid,
               price_t60s, price_t300s, price_t900s, price_t1800s, price_t3600s,
               edge_realised_t60s, edge_realised_t300s, edge_realised_t900s,
               edge_realised_t1800s, edge_realised_t3600s
        FROM price_impacts
        WHERE trade_id = ?
        """,
        (chain["trade_id"],) if chain["trade_id"] is not None else (-1,),
    ).fetchone()

    rows = [
        ["article.id", chain["article_id"]],
        ["article.title", chain["article_title"]],
        ["article.published_at", chain["article_published_at"]],
        ["article.ingested_at", chain["article_ingested_at"]],
        ["signal.id", chain["signal_id"]],
        ["signal.market_id", chain["market_id"]],
        ["signal.similarity", chain["similarity"]],
        ["signal.freshness", chain["freshness"]],
        ["signal.created_at", chain["signal_created_at"]],
        ["decision.id", chain["decision_id"]],
        ["decision.outcome", chain["outcome"]],
        ["decision.reason", chain["reason"]],
        ["decision.created_at", chain["decision_created_at"]],
    ]
    if chain["trade_id"] is not None:
        rows += [
            ["trade.id", chain["trade_id"]],
            ["trade.direction", chain["direction"]],
            ["trade.mid_price", chain["mid_price"]],
            ["trade.edge", chain["edge"]],
            ["trade.size_usdc", chain["size_usdc"]],
            ["trade.decided_at", chain["trade_decided_at"]],
        ]
    if chain["closed_at"]:
        rows += [
            ["close.exit_mid", chain["exit_mid_price"]],
            ["close.realized_pnl", chain["realized_pnl_usdc"]],
            ["close.reason", chain["close_reason"]],
            ["close.closed_at", chain["closed_at"]],
        ]
    if impacts is not None:
        rows += [
            ["impact.entry_mid", impacts["entry_mid"]],
            ["impact.price_t60s", impacts["price_t60s"]],
            ["impact.price_t300s", impacts["price_t300s"]],
            ["impact.price_t900s", impacts["price_t900s"]],
            ["impact.price_t1800s", impacts["price_t1800s"]],
            ["impact.price_t3600s", impacts["price_t3600s"]],
            ["impact.edge_realised_t60s", impacts["edge_realised_t60s"]],
            ["impact.edge_realised_t300s", impacts["edge_realised_t300s"]],
            ["impact.edge_realised_t900s", impacts["edge_realised_t900s"]],
            ["impact.edge_realised_t1800s", impacts["edge_realised_t1800s"]],
            ["impact.edge_realised_t3600s", impacts["edge_realised_t3600s"]],
        ]
    return Section(
        title=f"decision card — signal {signal_id}",
        headers=["field", "value"],
        rows=rows,
        aligns=["l", "l"],
    )
