BEGIN;

CREATE TABLE IF NOT EXISTS option_market_state_1m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  instrument TEXT NOT NULL,
  direction TEXT NOT NULL,
  underlying_price NUMERIC(12,2),
  atm_strike INTEGER,
  strike INTEGER,
  option_ltp NUMERIC(12,2),
  premium_change_1m NUMERIC(12,2),
  premium_change_3m NUMERIC(12,2),
  volume_delta BIGINT,
  oi_delta BIGINT,
  iv NUMERIC(12,4),
  spread NUMERIC(12,2),
  spread_percent NUMERIC(12,4),
  bid_price NUMERIC(12,2),
  ask_price NUMERIC(12,2),
  bid_quantity BIGINT,
  ask_quantity BIGINT,
  option_breadth_score NUMERIC(12,2),
  premium_state TEXT,
  liquidity_quality TEXT,
  recommended_action TEXT,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ts, instrument, direction)
);

COMMIT;
