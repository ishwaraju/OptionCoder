CREATE TABLE IF NOT EXISTS manual_trade_journal (
  id BIGSERIAL PRIMARY KEY,
  trade_date DATE NOT NULL DEFAULT CURRENT_DATE,
  instrument TEXT NOT NULL,
  direction TEXT NOT NULL,
  strike INTEGER,
  entry_ts TIMESTAMPTZ,
  exit_ts TIMESTAMPTZ,
  entry_premium NUMERIC(12,2),
  exit_premium NUMERIC(12,2),
  quantity INTEGER,
  pnl_points NUMERIC(12,2),
  pnl_rupees NUMERIC(12,2),
  bot_readiness TEXT,
  smart_money_state TEXT,
  gamma_state TEXT,
  liquidity_sweep_state TEXT,
  setup_reason TEXT,
  mistake_tag TEXT,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_manual_trade_journal_date
  ON manual_trade_journal (trade_date DESC, instrument);

CREATE INDEX IF NOT EXISTS idx_manual_trade_journal_context
  ON manual_trade_journal (smart_money_state, gamma_state, liquidity_sweep_state);
