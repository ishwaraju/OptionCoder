CREATE TABLE IF NOT EXISTS option_signal_horizon_outcomes (
  id BIGSERIAL PRIMARY KEY,
  signal_ts TIMESTAMPTZ NOT NULL,
  horizon_minutes INTEGER NOT NULL,
  observed_ts TIMESTAMPTZ,
  instrument TEXT NOT NULL,
  signal TEXT NOT NULL,
  strike INTEGER NOT NULL,
  underlying_entry_price NUMERIC(12,2),
  underlying_price NUMERIC(12,2),
  option_entry_ltp NUMERIC(12,2),
  option_ltp NUMERIC(12,2),
  pnl_points NUMERIC(12,2),
  pnl_percent NUMERIC(12,4),
  max_favorable_points NUMERIC(12,2),
  max_adverse_points NUMERIC(12,2),
  outcome_label TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (signal_ts, horizon_minutes, instrument, signal, strike)
);

CREATE INDEX IF NOT EXISTS idx_option_sig_horizon_inst_ts
  ON option_signal_horizon_outcomes (instrument, signal_ts DESC);

ALTER TABLE entry_decisions_1m
  ADD COLUMN IF NOT EXISTS option_buyer_entry_score INTEGER,
  ADD COLUMN IF NOT EXISTS option_buyer_action TEXT;
