-- Add scalp_signals_1m table for fast scalping signals

CREATE TABLE IF NOT EXISTS scalp_signals_1m (
    ts TIMESTAMP NOT NULL,
    instrument VARCHAR(20) NOT NULL,
    signal VARCHAR(10) NOT NULL,  -- 'CE' or 'PE'
    entry_price NUMERIC(12,2),
    target_price NUMERIC(12,2),
    stop_loss NUMERIC(12,2),
    score INTEGER,
    reason TEXT,
    status VARCHAR(20) DEFAULT 'ACTIVE',  -- 'ACTIVE', 'EXITED', 'STOPPED'
    exit_ts TIMESTAMP,
    exit_price NUMERIC(12,2),
    pnl NUMERIC(12,2),
    PRIMARY KEY (ts, instrument),
    CONSTRAINT valid_signal CHECK (signal IN ('CE', 'PE')),
    CONSTRAINT valid_status CHECK (status IN ('ACTIVE', 'EXITED', 'STOPPED', 'EXPIRED'))
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_scalp_instrument_ts ON scalp_signals_1m(instrument, ts DESC);
CREATE INDEX IF NOT EXISTS idx_scalp_status ON scalp_signals_1m(status) WHERE status = 'ACTIVE';

-- Add comment
COMMENT ON TABLE scalp_signals_1m IS 'Fast scalping signals (1-minute timeframe, 3-5 min hold)';
