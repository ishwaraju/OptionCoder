-- Extend strategy/alert tables with watch buckets, trade-plan fields, and review storage
ALTER TABLE strategy_decisions_5m
ADD COLUMN IF NOT EXISTS watch_bucket TEXT,
ADD COLUMN IF NOT EXISTS pressure_conflict_level TEXT,
ADD COLUMN IF NOT EXISTS confidence_summary TEXT,
ADD COLUMN IF NOT EXISTS entry_above NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS entry_below NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS invalidate_price NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS first_target_price NUMERIC(12,2);

ALTER TABLE signals_issued
ADD COLUMN IF NOT EXISTS confidence_summary TEXT,
ADD COLUMN IF NOT EXISTS entry_above NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS entry_below NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS invalidate_price NUMERIC(12,2),
ADD COLUMN IF NOT EXISTS first_target_price NUMERIC(12,2);

CREATE TABLE IF NOT EXISTS alert_reviews_5m (
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

CREATE INDEX IF NOT EXISTS idx_alert_reviews_inst_ts
ON alert_reviews_5m (instrument, alert_ts DESC);
