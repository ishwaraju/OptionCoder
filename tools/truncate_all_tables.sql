-- Truncate all tables except instrument_profiles for fresh start
-- WARNING: This will delete all data except instrument configuration

BEGIN;

-- Truncate data tables (keep instrument_profiles)
TRUNCATE TABLE candles_1m RESTART IDENTITY CASCADE;
TRUNCATE TABLE candles_5m RESTART IDENTITY CASCADE;
TRUNCATE TABLE oi_snapshots_1m RESTART IDENTITY CASCADE;
TRUNCATE TABLE option_band_snapshots_1m RESTART IDENTITY CASCADE;
TRUNCATE TABLE strategy_decisions_5m RESTART IDENTITY CASCADE;
TRUNCATE TABLE signals_issued RESTART IDENTITY CASCADE;
TRUNCATE TABLE trade_monitor_events_1m RESTART IDENTITY CASCADE;

COMMIT;

-- Verify truncation
SELECT 'candles_1m' as table_name, COUNT(*) as row_count FROM candles_1m
UNION ALL
SELECT 'candles_5m', COUNT(*) FROM candles_5m
UNION ALL
SELECT 'oi_snapshots_1m', COUNT(*) FROM oi_snapshots_1m
UNION ALL
SELECT 'option_band_snapshots_1m', COUNT(*) FROM option_band_snapshots_1m
UNION ALL
SELECT 'strategy_decisions_5m', COUNT(*) FROM strategy_decisions_5m
UNION ALL
SELECT 'signals_issued', COUNT(*) FROM signals_issued
UNION ALL
SELECT 'trade_monitor_events_1m', COUNT(*) FROM trade_monitor_events_1m
UNION ALL
SELECT 'instrument_profiles (KEPT)', COUNT(*) FROM instrument_profiles;
