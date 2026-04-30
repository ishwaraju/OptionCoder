BEGIN;

ALTER TABLE signals_issued
  ADD COLUMN IF NOT EXISTS underlying_price NUMERIC(12,2),
  ADD COLUMN IF NOT EXISTS atm_strike INTEGER,
  ADD COLUMN IF NOT EXISTS distance_from_atm INTEGER,
  ADD COLUMN IF NOT EXISTS option_entry_ltp NUMERIC(12,2),
  ADD COLUMN IF NOT EXISTS entry_bid NUMERIC(12,2),
  ADD COLUMN IF NOT EXISTS entry_ask NUMERIC(12,2),
  ADD COLUMN IF NOT EXISTS entry_spread NUMERIC(12,2),
  ADD COLUMN IF NOT EXISTS entry_iv NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS entry_delta NUMERIC(12,6),
  ADD COLUMN IF NOT EXISTS strike_reason TEXT,
  ADD COLUMN IF NOT EXISTS option_data_source TEXT;

CREATE TABLE IF NOT EXISTS option_signal_candidates_5m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  underlying_price NUMERIC(12,2),
  underlying_bias TEXT,
  setup_type TEXT,
  candidate_direction TEXT NOT NULL,
  strike INTEGER NOT NULL,
  atm_strike INTEGER,
  distance_from_atm INTEGER,
  option_ltp NUMERIC(12,2),
  bid_price NUMERIC(12,2),
  ask_price NUMERIC(12,2),
  spread NUMERIC(12,2),
  spread_percent NUMERIC(12,4),
  iv NUMERIC(12,4),
  delta NUMERIC(12,6),
  theta NUMERIC(12,6),
  oi BIGINT,
  volume BIGINT,
  candidate_score NUMERIC(12,2),
  candidate_rank INTEGER,
  expected_edge NUMERIC(12,2),
  selected_for_signal BOOLEAN NOT NULL DEFAULT FALSE,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ts, instrument, candidate_direction, strike)
);

CREATE TABLE IF NOT EXISTS option_signal_outcomes_1m (
  id BIGSERIAL PRIMARY KEY,
  signal_ts TIMESTAMPTZ NOT NULL,
  observed_ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  signal TEXT NOT NULL,
  strike INTEGER NOT NULL,
  underlying_entry_price NUMERIC(12,2),
  underlying_price NUMERIC(12,2),
  option_entry_ltp NUMERIC(12,2),
  option_ltp NUMERIC(12,2),
  option_bid NUMERIC(12,2),
  option_ask NUMERIC(12,2),
  option_spread NUMERIC(12,2),
  pnl_points NUMERIC(12,2),
  max_favorable_ltp NUMERIC(12,2),
  max_adverse_ltp NUMERIC(12,2),
  minutes_since_signal INTEGER,
  guidance TEXT,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (signal_ts, observed_ts, instrument, signal, strike)
);

CREATE INDEX IF NOT EXISTS idx_sig_issued_inst_ts
  ON signals_issued (instrument, ts DESC);

CREATE INDEX IF NOT EXISTS idx_option_sig_candidates_inst_ts
  ON option_signal_candidates_5m (instrument, ts DESC);

CREATE INDEX IF NOT EXISTS idx_option_sig_outcomes_inst_signal_ts
  ON option_signal_outcomes_1m (instrument, signal_ts DESC);

COMMIT;
