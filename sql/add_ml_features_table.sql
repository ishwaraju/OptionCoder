-- ML Features Logging Table
-- Stores signal features for ML training and prediction

CREATE TABLE IF NOT EXISTS ml_features_log (
    id BIGSERIAL PRIMARY KEY,
    alert_ts TIMESTAMPTZ NOT NULL,
    instrument TEXT NOT NULL,
    signal_direction TEXT NOT NULL, -- 'CE' or 'PE'
    
    -- Signal Features
    score INTEGER,
    confidence TEXT,
    adx NUMERIC(5,2),
    volume_ratio NUMERIC(5,2),
    oi_change_pct NUMERIC(6,2),
    vwap_distance NUMERIC(6,3), -- percentage from VWAP
    time_hour INTEGER,
    time_regime TEXT,
    
    -- Market Context
    iv_rank NUMERIC(5,2),
    spread_pct NUMERIC(6,3),
    atr NUMERIC(8,2),
    price_momentum NUMERIC(6,3), -- 5m price change %
    
    -- OI/Pressure Features
    pressure_conflict_level TEXT,
    oi_bias TEXT,
    oi_trend TEXT,
    wall_break_alert TEXT,
    support_wall_state TEXT,
    resistance_wall_state TEXT,
    oi_divergence TEXT,
    
    -- Strategy Features
    signal_type TEXT,
    signal_grade TEXT,
    has_hybrid_mode BOOLEAN,
    entry_score INTEGER,
    context_score INTEGER,
    
    -- Multi-timeframe
    trend_15m TEXT,
    trend_5m TEXT,
    trend_aligned BOOLEAN,
    
    -- Target & Risk
    target_points NUMERIC(6,2),
    stop_points NUMERIC(6,2),
    risk_reward_ratio NUMERIC(4,2),
    
    -- Labels (filled later from alert_reviews)
    actual_outcome TEXT, -- 'PROFIT', 'LOSS', 'BREAKEVEN', 'OPEN'
    max_favorable_points NUMERIC(6,2),
    max_adverse_points NUMERIC(6,2),
    close_pnl_points NUMERIC(6,2),
    outcome_tag TEXT, -- e.g., 'TARGET_HIT', 'STOPPED_OUT', 'TIME_EXIT'
    
    -- ML Prediction (filled when model is trained)
    ml_predicted_prob NUMERIC(5,4), -- 0.0 to 1.0
    ml_prediction TEXT, -- 'TAKE', 'AVOID'
    ml_was_correct BOOLEAN,
    
    -- Metadata
    model_version TEXT, -- which model version made prediction
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_ml_features_alert_ts ON ml_features_log (alert_ts DESC);
CREATE INDEX IF NOT EXISTS idx_ml_features_instrument ON ml_features_log (instrument);
CREATE INDEX IF NOT EXISTS idx_ml_features_outcome ON ml_features_log (actual_outcome) WHERE actual_outcome IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ml_features_prediction ON ml_features_log (ml_prediction);
CREATE INDEX IF NOT EXISTS idx_ml_features_train ON ml_features_log (instrument, actual_outcome, score) WHERE actual_outcome IS NOT NULL;

-- View for training data (only completed trades with known outcomes)
CREATE OR REPLACE VIEW ml_training_data AS
SELECT 
    id,
    instrument,
    signal_direction,
    score,
    adx,
    volume_ratio,
    oi_change_pct,
    vwap_distance,
    time_hour,
    time_regime,
    iv_rank,
    spread_pct,
    atr,
    price_momentum,
    pressure_conflict_level,
    oi_bias,
    trend_15m,
    trend_aligned,
    risk_reward_ratio,
    CASE 
        WHEN actual_outcome = 'PROFIT' THEN 1 
        WHEN actual_outcome = 'LOSS' THEN 0 
        ELSE NULL 
    END as target_label,
    max_favorable_points,
    max_adverse_points,
    close_pnl_points
FROM ml_features_log
WHERE actual_outcome IS NOT NULL
AND actual_outcome IN ('PROFIT', 'LOSS');

COMMENT ON TABLE ml_features_log IS 'Stores signal features for ML training and prediction. Features logged at signal time, outcomes filled later from alert_reviews.';
