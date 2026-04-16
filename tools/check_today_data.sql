-- ============================================================
-- SQL QUERIES TO CHECK TODAY'S DATA (April 16, 2026)
-- Run these in your PostgreSQL client (pgAdmin, DBeaver, etc.)
-- ============================================================

-- 1. Check 1-Minute Candles (NIFTY Top 10)
SELECT 
    ts AT TIME ZONE 'Asia/Kolkata' as ist_time,
    instrument,
    open,
    high,
    low,
    close,
    volume
FROM candles_1m
WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = '2026-04-16'
  AND instrument = 'NIFTY'
ORDER BY ts DESC
LIMIT 10;

-- 2. Check 5-Minute Candles (All Instruments)
SELECT 
    ts AT TIME ZONE 'Asia/Kolkata' as ist_time,
    instrument,
    open,
    high,
    low,
    close,
    volume
FROM candles_5m
WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = '2026-04-16'
ORDER BY ts DESC;

-- 3. Check OI Snapshots (NIFTY Latest)
SELECT 
    ts AT TIME ZONE 'Asia/Kolkata' as ist_time,
    instrument,
    underlying_price,
    ce_oi,
    pe_oi,
    pcr,
    ce_oi_change,
    pe_oi_change,
    oi_sentiment
FROM oi_snapshots_1m
WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = '2026-04-16'
  AND instrument = 'NIFTY'
ORDER BY ts DESC
LIMIT 5;

-- 4. Check Option Band Data (NIFTY CE Options)
SELECT 
    ts AT TIME ZONE 'Asia/Kolkata' as ist_time,
    instrument,
    strike,
    option_type,
    oi,
    volume,
    ltp,
    iv
FROM option_band_snapshots_1m
WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = '2026-04-16'
  AND instrument = 'NIFTY'
  AND option_type = 'CE'
ORDER BY ts DESC, strike
LIMIT 10;

-- 5. Check Swing Signals (if any generated today)
SELECT 
    ts AT TIME ZONE 'Asia/Kolkata' as signal_time,
    instrument,
    signal,
    price,
    strategy_score,
    signal_quality,
    tradability,
    reason
FROM signals_issued
WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = '2026-04-16'
ORDER BY ts DESC;

-- 6. Check Scalp Signals (if any generated today)
SELECT 
    timestamp AT TIME ZONE 'Asia/Kolkata' as signal_time,
    instrument,
    signal_type,
    entry_price,
    target_points,
    stop_points,
    score,
    status
FROM scalp_signals_1m
WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata') = '2026-04-16'
ORDER BY timestamp DESC;

-- 7. Count Total Rows Today (Summary)
SELECT '1m Candles' as data_type, instrument, COUNT(*) as total_rows
FROM candles_1m
WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = '2026-04-16'
GROUP BY instrument
UNION ALL
SELECT '5m Candles' as data_type, instrument, COUNT(*) as total_rows
FROM candles_5m
WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = '2026-04-16'
GROUP BY instrument
UNION ALL
SELECT 'OI Snapshots' as data_type, instrument, COUNT(*) as total_rows
FROM oi_snapshots_1m
WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = '2026-04-16'
GROUP BY instrument
UNION ALL
SELECT 'Option Band' as data_type, instrument, COUNT(*) as total_rows
FROM option_band_snapshots_1m
WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = '2026-04-16'
GROUP BY instrument
ORDER BY data_type, instrument;

-- ============================================================
-- HOW TO RUN:
-- 1. Open pgAdmin, DBeaver, or any PostgreSQL client
-- 2. Connect to your database
-- 3. Copy-paste any query above
-- 4. Press F5 or click Execute
-- ============================================================
