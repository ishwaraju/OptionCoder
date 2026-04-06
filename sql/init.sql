CREATE TABLE IF NOT EXISTS candles_1m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  open NUMERIC(12,2) NOT NULL,
  high NUMERIC(12,2) NOT NULL,
  low NUMERIC(12,2) NOT NULL,
  close NUMERIC(12,2) NOT NULL,
  volume BIGINT NOT NULL DEFAULT 0,
  UNIQUE (ts, instrument)
);

CREATE TABLE IF NOT EXISTS candles_5m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  open NUMERIC(12,2) NOT NULL,
  high NUMERIC(12,2) NOT NULL,
  low NUMERIC(12,2) NOT NULL,
  close NUMERIC(12,2) NOT NULL,
  volume BIGINT NOT NULL DEFAULT 0,
  UNIQUE (ts, instrument)
);

CREATE TABLE IF NOT EXISTS oi_snapshots_1m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  underlying_price NUMERIC(12,2),
  ce_oi BIGINT,
  pe_oi BIGINT,
  ce_volume BIGINT,
  pe_volume BIGINT,
  ce_volume_band BIGINT,
  pe_volume_band BIGINT,
  pcr NUMERIC(8,4),
  UNIQUE (ts, instrument)
);

CREATE TABLE IF NOT EXISTS option_band_snapshots_1m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  atm_strike INTEGER NOT NULL,
  strike INTEGER NOT NULL,
  distance_from_atm INTEGER NOT NULL,
  option_type TEXT NOT NULL,
  security_id BIGINT,
  oi BIGINT,
  volume BIGINT,
  ltp NUMERIC(12,2),
  iv NUMERIC(8,4),
  UNIQUE (ts, instrument, strike, option_type)
);

CREATE TABLE IF NOT EXISTS strategy_decisions_5m (
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
  ce_delta_total BIGINT,
  pe_delta_total BIGINT,
  pcr NUMERIC(8,4),
  orb_high NUMERIC(12,2),
  orb_low NUMERIC(12,2),
  vwap NUMERIC(12,2),
  atr NUMERIC(12,2),
  strike INTEGER,
  UNIQUE (ts, instrument)
);

CREATE INDEX IF NOT EXISTS idx_c1m_inst_ts ON candles_1m (instrument, ts DESC);
CREATE INDEX IF NOT EXISTS idx_c5m_inst_ts ON candles_5m (instrument, ts DESC);
CREATE INDEX IF NOT EXISTS idx_oi_inst_ts ON oi_snapshots_1m (instrument, ts DESC);
CREATE INDEX IF NOT EXISTS idx_option_band_inst_ts ON option_band_snapshots_1m (instrument, ts DESC);
CREATE INDEX IF NOT EXISTS idx_option_band_ts_strike ON option_band_snapshots_1m (ts DESC, strike, option_type);
CREATE INDEX IF NOT EXISTS idx_strategy_decisions_inst_ts ON strategy_decisions_5m (instrument, ts DESC);
