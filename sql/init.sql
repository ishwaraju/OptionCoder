BEGIN;

DROP TABLE IF EXISTS trade_monitor_events_1m CASCADE;
DROP TABLE IF EXISTS option_signal_outcomes_1m CASCADE;
DROP TABLE IF EXISTS option_signal_candidates_5m CASCADE;
DROP TABLE IF EXISTS signals_issued CASCADE;
DROP TABLE IF EXISTS strategy_decisions_5m CASCADE;
DROP TABLE IF EXISTS option_band_snapshots_1m CASCADE;
DROP TABLE IF EXISTS oi_snapshots_1m CASCADE;
DROP TABLE IF EXISTS candles_5m CASCADE;
DROP TABLE IF EXISTS candles_1m CASCADE;
DROP TABLE IF EXISTS instrument_profiles CASCADE;

CREATE TABLE instrument_profiles (
  instrument TEXT PRIMARY KEY,
  exchange_segment TEXT NOT NULL,
  security_id BIGINT NOT NULL,
  strike_step INTEGER NOT NULL,
  lot_size INTEGER NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  min_score_threshold NUMERIC(8,2),
  atr_multiplier NUMERIC(8,4),
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE candles_1m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  open NUMERIC(12,2) NOT NULL,
  high NUMERIC(12,2) NOT NULL,
  low NUMERIC(12,2) NOT NULL,
  close NUMERIC(12,2) NOT NULL,
  volume BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ts, instrument)
);

CREATE TABLE candles_5m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  open NUMERIC(12,2) NOT NULL,
  high NUMERIC(12,2) NOT NULL,
  low NUMERIC(12,2) NOT NULL,
  close NUMERIC(12,2) NOT NULL,
  volume BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ts, instrument)
);

CREATE TABLE oi_snapshots_1m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  underlying_price NUMERIC(12,2),
  ce_oi BIGINT DEFAULT 0,
  pe_oi BIGINT DEFAULT 0,
  ce_volume BIGINT DEFAULT 0,
  pe_volume BIGINT DEFAULT 0,
  ce_volume_band BIGINT DEFAULT 0,
  pe_volume_band BIGINT DEFAULT 0,
  pcr NUMERIC(8,4) DEFAULT 0.0,
  ce_oi_change BIGINT DEFAULT 0,
  pe_oi_change BIGINT DEFAULT 0,
  total_oi_change BIGINT DEFAULT 0,
  oi_sentiment TEXT DEFAULT 'NEUTRAL',
  oi_bias_strength NUMERIC(8,4) DEFAULT 0.0,
  total_volume BIGINT DEFAULT 0,
  volume_change BIGINT DEFAULT 0,
  volume_pcr NUMERIC(8,4) DEFAULT 0.0,
  max_ce_oi_strike INTEGER DEFAULT 0,
  max_pe_oi_strike INTEGER DEFAULT 0,
  oi_concentration NUMERIC(8,4) DEFAULT 0.0,
  oi_trend TEXT DEFAULT 'SIDEWAYS',
  trend_strength NUMERIC(8,4) DEFAULT 0.0,
  support_level NUMERIC(12,2) DEFAULT 0.0,
  resistance_level NUMERIC(12,2) DEFAULT 0.0,
  oi_range_width NUMERIC(8,2) DEFAULT 0.0,
  previous_ts TIMESTAMPTZ,
  data_age_seconds INTEGER DEFAULT 0,
  data_quality TEXT DEFAULT 'GOOD',
  max_ce_oi_amount BIGINT DEFAULT 0,
  max_pe_oi_amount BIGINT DEFAULT 0,
  oi_spread NUMERIC(12,4) DEFAULT 0.0,
  liquidity_score NUMERIC(8,4) DEFAULT 0.0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ts, instrument)
);

CREATE TABLE option_band_snapshots_1m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  atm_strike INTEGER NOT NULL,
  strike INTEGER NOT NULL,
  distance_from_atm INTEGER NOT NULL,
  option_type TEXT NOT NULL,
  security_id BIGINT,
  oi BIGINT DEFAULT 0,
  volume BIGINT DEFAULT 0,
  ltp NUMERIC(12,2),
  iv NUMERIC(8,4),
  top_bid_price NUMERIC(12,2),
  top_bid_quantity BIGINT,
  top_ask_price NUMERIC(12,2),
  top_ask_quantity BIGINT,
  spread NUMERIC(12,2),
  average_price NUMERIC(12,2),
  previous_oi BIGINT,
  previous_volume BIGINT,
  delta NUMERIC(12,6),
  theta NUMERIC(12,6),
  gamma NUMERIC(12,6),
  vega NUMERIC(12,6),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ts, instrument, strike, option_type)
);

CREATE TABLE strategy_decisions_5m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  price NUMERIC(12,2),
  signal TEXT,
  reason TEXT,
  strategy_score INTEGER,
  score_factors TEXT,
  volume_signal TEXT,
  oi_bias TEXT,
  oi_trend TEXT,
  build_up TEXT,
  pressure_bias TEXT,
  pcr NUMERIC(8,4),
  orb_high NUMERIC(12,2),
  orb_low NUMERIC(12,2),
  vwap NUMERIC(12,2),
  atr NUMERIC(12,2),
  strike INTEGER,
  ce_delta_total BIGINT,
  pe_delta_total BIGINT,
  base_bias TEXT,
  setup_type TEXT,
  signal_quality TEXT,
  tradability TEXT,
  time_regime TEXT,
  oi_mode TEXT,
  blockers_json JSONB,
  cautions_json JSONB,
  candidate_signal_type TEXT,
  candidate_signal_grade TEXT,
  candidate_confidence TEXT,
  actionable_block_reason TEXT,
  watch_bucket TEXT,
  pressure_conflict_level TEXT,
  confidence_summary TEXT,
  entry_above NUMERIC(12,2),
  entry_below NUMERIC(12,2),
  invalidate_price NUMERIC(12,2),
  first_target_price NUMERIC(12,2),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ts, instrument)
);

CREATE TABLE signals_issued (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  signal TEXT NOT NULL,
  price NUMERIC(12,2),
  underlying_price NUMERIC(12,2),
  strike INTEGER,
  atm_strike INTEGER,
  distance_from_atm INTEGER,
  option_entry_ltp NUMERIC(12,2),
  entry_bid NUMERIC(12,2),
  entry_ask NUMERIC(12,2),
  entry_spread NUMERIC(12,2),
  entry_iv NUMERIC(12,4),
  entry_delta NUMERIC(12,6),
  strategy_score INTEGER,
  signal_quality TEXT,
  setup_type TEXT,
  tradability TEXT,
  time_regime TEXT,
  oi_mode TEXT,
  reason TEXT,
  strike_reason TEXT,
  option_data_source TEXT,
  confidence_summary TEXT,
  entry_above NUMERIC(12,2),
  entry_below NUMERIC(12,2),
  invalidate_price NUMERIC(12,2),
  first_target_price NUMERIC(12,2),
  telegram_sent BOOLEAN NOT NULL DEFAULT FALSE,
  monitor_started BOOLEAN NOT NULL DEFAULT FALSE,
  entry_window_end TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ts, instrument, signal, strike)
);

CREATE TABLE option_signal_candidates_5m (
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

CREATE TABLE option_signal_outcomes_1m (
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

CREATE TABLE trade_monitor_events_1m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  signal TEXT NOT NULL,
  entry_ts TIMESTAMPTZ NOT NULL,
  entry_price NUMERIC(12,2),
  current_price NUMERIC(12,2),
  pnl_points NUMERIC(12,2),
  guidance TEXT,
  reason TEXT,
  structure_state TEXT,
  quality TEXT,
  time_regime TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ts, instrument, entry_ts, signal)
);

CREATE TABLE alert_reviews_5m (
  id BIGSERIAL PRIMARY KEY,
  alert_ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  alert_kind TEXT NOT NULL,
  direction TEXT,
  setup_type TEXT,
  watch_bucket TEXT,
  usefulness TEXT,
  outcome_tag TEXT,
  lookahead_minutes INTEGER,
  max_favorable_points NUMERIC(12,2),
  max_adverse_points NUMERIC(12,2),
  close_move_points NUMERIC(12,2),
  blockers_json JSONB,
  cautions_json JSONB,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (alert_ts, instrument, alert_kind, direction, setup_type)
);

CREATE INDEX idx_instrument_profiles_enabled ON instrument_profiles (enabled, instrument);

CREATE INDEX idx_c1m_inst_ts ON candles_1m (instrument, ts DESC);
CREATE INDEX idx_c1m_ts ON candles_1m (ts DESC);

CREATE INDEX idx_c5m_inst_ts ON candles_5m (instrument, ts DESC);
CREATE INDEX idx_c5m_ts ON candles_5m (ts DESC);
CREATE INDEX idx_sig_issued_inst_ts ON signals_issued (instrument, ts DESC);
CREATE INDEX idx_option_sig_candidates_inst_ts ON option_signal_candidates_5m (instrument, ts DESC);
CREATE INDEX idx_option_sig_outcomes_inst_signal_ts ON option_signal_outcomes_1m (instrument, signal_ts DESC);

CREATE INDEX idx_oi_inst_ts ON oi_snapshots_1m (instrument, ts DESC);
CREATE INDEX idx_oi_sentiment ON oi_snapshots_1m (instrument, oi_sentiment, ts DESC);
CREATE INDEX idx_oi_trend ON oi_snapshots_1m (instrument, oi_trend, ts DESC);
CREATE INDEX idx_oi_strength ON oi_snapshots_1m (instrument, oi_bias_strength DESC, ts DESC);
CREATE INDEX idx_oi_support ON oi_snapshots_1m (instrument, support_level, ts DESC);
CREATE INDEX idx_oi_resistance ON oi_snapshots_1m (instrument, resistance_level, ts DESC);
CREATE INDEX idx_oi_quality ON oi_snapshots_1m (instrument, data_quality, ts DESC);

CREATE INDEX idx_option_band_inst_ts ON option_band_snapshots_1m (instrument, ts DESC);
CREATE INDEX idx_option_band_ts_strike ON option_band_snapshots_1m (ts DESC, strike, option_type);
CREATE INDEX idx_option_band_inst_atm_ts ON option_band_snapshots_1m (instrument, atm_strike, ts DESC);

CREATE INDEX idx_strategy_decisions_inst_ts ON strategy_decisions_5m (instrument, ts DESC);
CREATE INDEX idx_strategy_signal_quality ON strategy_decisions_5m (instrument, signal_quality, ts DESC);
CREATE INDEX idx_strategy_time_regime ON strategy_decisions_5m (instrument, time_regime, ts DESC);
CREATE INDEX idx_strategy_signal ON strategy_decisions_5m (instrument, signal, ts DESC);

CREATE INDEX idx_signals_issued_inst_ts ON signals_issued (instrument, ts DESC);
CREATE INDEX idx_signals_issued_signal ON signals_issued (instrument, signal, ts DESC);
CREATE INDEX idx_signals_issued_quality ON signals_issued (instrument, signal_quality, ts DESC);

CREATE INDEX idx_monitor_inst_ts ON trade_monitor_events_1m (instrument, ts DESC);
CREATE INDEX idx_monitor_entry ON trade_monitor_events_1m (instrument, entry_ts DESC);
CREATE INDEX idx_alert_reviews_inst_ts ON alert_reviews_5m (instrument, alert_ts DESC);

INSERT INTO instrument_profiles (
  instrument, exchange_segment, security_id, strike_step, lot_size, enabled, min_score_threshold, atr_multiplier, notes
) VALUES
  ('NIFTY', 'IDX_I', 13, 50, 65, TRUE, 55.00, 0.3000, 'Default index profile'),
  ('BANKNIFTY', 'IDX_I', 25, 100, 30, TRUE, 55.00, 0.3000, 'BankNifty profile'),
  ('SENSEX', 'IDX_I', 51, 100, 10, TRUE, 55.00, 0.3000, 'Sensex profile placeholder')
ON CONFLICT (instrument) DO UPDATE
SET exchange_segment = EXCLUDED.exchange_segment,
    security_id = EXCLUDED.security_id,
    strike_step = EXCLUDED.strike_step,
    lot_size = EXCLUDED.lot_size,
    enabled = EXCLUDED.enabled,
    min_score_threshold = EXCLUDED.min_score_threshold,
    atr_multiplier = EXCLUDED.atr_multiplier,
    notes = EXCLUDED.notes,
    updated_at = NOW();


COMMIT;
