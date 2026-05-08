-- Irene initial schema
-- Idempotent: safe to run multiple times on the same database.

CREATE TABLE IF NOT EXISTS markets (
    condition_id            TEXT PRIMARY KEY,
    slug                    TEXT,
    question                TEXT NOT NULL,
    description             TEXT,
    category                TEXT,
    end_date                TIMESTAMPTZ NOT NULL,
    volume_usdc             REAL,
    token_yes               TEXT,
    token_no                TEXT,
    embedding               BYTEA,
    embedding_updated_at    TIMESTAMPTZ,
    active                  BOOLEAN DEFAULT true,
    first_seen_at           TIMESTAMPTZ NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
    id              SERIAL PRIMARY KEY,
    source          TEXT NOT NULL,
    source_detail   TEXT NOT NULL,
    url             TEXT UNIQUE NOT NULL,
    title           TEXT NOT NULL,
    description     TEXT,
    published_at    TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ NOT NULL,
    embedding       BYTEA,
    mapped          BOOLEAN DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_signals_ingested ON signals(ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_mapped ON signals(mapped) WHERE mapped = false;

CREATE TABLE IF NOT EXISTS mapped_signals (
    id                      SERIAL PRIMARY KEY,
    signal_id               INTEGER NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
    condition_id            TEXT NOT NULL REFERENCES markets(condition_id),
    similarity_score        REAL NOT NULL,
    direction               TEXT NOT NULL,
    direction_confidence    REAL NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL,
    dispatched              BOOLEAN DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_mapped_signals_condition ON mapped_signals(condition_id);
CREATE INDEX IF NOT EXISTS idx_mapped_signals_dispatched ON mapped_signals(dispatched) WHERE dispatched = false;

CREATE TABLE IF NOT EXISTS trades (
    id                  SERIAL PRIMARY KEY,
    mapped_signal_id    INTEGER REFERENCES mapped_signals(id),
    condition_id        TEXT NOT NULL,
    token_id            TEXT NOT NULL,
    direction           TEXT NOT NULL,
    p_hat               REAL NOT NULL,
    mid_price           REAL NOT NULL,
    edge                REAL NOT NULL,
    kelly_size_usdc     REAL NOT NULL,
    executed            BOOLEAN NOT NULL DEFAULT false,
    order_type          TEXT,
    fill_price          REAL,
    fill_size_usdc      REAL,
    fill_at             TIMESTAMPTZ,
    order_id            TEXT,
    decided_at          TIMESTAMPTZ NOT NULL,
    closed_at           TIMESTAMPTZ,
    exit_price          REAL,
    exit_reason         TEXT,
    realized_pnl        REAL,
    outcome             TEXT,
    skip_reason         TEXT,
    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_trades_condition ON trades(condition_id);
CREATE INDEX IF NOT EXISTS idx_trades_decided ON trades(decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_open ON trades(closed_at) WHERE closed_at IS NULL AND executed = true;

CREATE TABLE IF NOT EXISTS positions (
    trade_id        INTEGER PRIMARY KEY REFERENCES trades(id) ON DELETE CASCADE,
    condition_id    TEXT NOT NULL,
    token_id        TEXT NOT NULL,
    direction       TEXT NOT NULL,
    size_usdc       REAL NOT NULL,
    entry_price     REAL NOT NULL,
    opened_at       TIMESTAMPTZ NOT NULL,
    last_price      REAL,
    unrealized_pnl  REAL
);

CREATE INDEX IF NOT EXISTS idx_positions_condition ON positions(condition_id);

CREATE TABLE IF NOT EXISTS market_events (
    id              SERIAL PRIMARY KEY,
    condition_id    TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_market_events_condition ON market_events(condition_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_events_type ON market_events(event_type, created_at DESC);

CREATE TABLE IF NOT EXISTS brier_scores (
    id              SERIAL PRIMARY KEY,
    trade_id        INTEGER NOT NULL REFERENCES trades(id) ON DELETE CASCADE,
    signal_source   TEXT NOT NULL,
    p_hat           REAL NOT NULL,
    actual_outcome  REAL NOT NULL,
    brier_component REAL NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_brier_source ON brier_scores(signal_source, recorded_at DESC);
