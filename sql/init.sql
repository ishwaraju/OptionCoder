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

CREATE INDEX IF NOT EXISTS idx_c1m_inst_ts ON candles_1m (instrument, ts DESC);
CREATE INDEX IF NOT EXISTS idx_c5m_inst_ts ON candles_5m (instrument, ts DESC);
CREATE INDEX IF NOT EXISTS idx_oi_inst_ts ON oi_snapshots_1m (instrument, ts DESC);
