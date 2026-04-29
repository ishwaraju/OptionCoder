-- Required for ON CONFLICT (alert_ts, instrument, signal_direction)
-- so ML feature logs can upsert cleanly.

CREATE UNIQUE INDEX IF NOT EXISTS uq_ml_features_alert_signal
ON ml_features_log (alert_ts, instrument, signal_direction);
