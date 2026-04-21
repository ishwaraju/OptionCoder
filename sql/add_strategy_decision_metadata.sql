-- Add structured strategy-decision metadata for easier analysis
ALTER TABLE strategy_decisions_5m
ADD COLUMN IF NOT EXISTS blockers_json JSONB,
ADD COLUMN IF NOT EXISTS cautions_json JSONB,
ADD COLUMN IF NOT EXISTS candidate_signal_type TEXT,
ADD COLUMN IF NOT EXISTS candidate_signal_grade TEXT,
ADD COLUMN IF NOT EXISTS candidate_confidence TEXT,
ADD COLUMN IF NOT EXISTS actionable_block_reason TEXT;
