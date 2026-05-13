#!/usr/bin/env python3
"""Print a daily calibration/debug table for strategy decisions and option outcomes."""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config


def parse_args():
    parser = argparse.ArgumentParser(description="Show daily signal calibration/debug table")
    parser.add_argument("--date", required=True, help="Trading day in YYYY-MM-DD (Asia/Kolkata)")
    parser.add_argument("--instrument", choices=["NIFTY", "BANKNIFTY", "SENSEX"], required=True)
    parser.add_argument("--horizon", type=int, default=5, choices=[1, 2, 3, 5])
    parser.add_argument("--min-score", type=float, default=60.0)
    return parser.parse_args()


def fmt(value, digits=2):
    if value is None:
        return "-"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value)


def main():
    args = parse_args()
    query = """
    WITH decisions AS (
        SELECT
            d.ts,
            d.instrument,
            d.price,
            d.signal,
            d.reason,
            d.strategy_score,
            d.setup_type,
            d.signal_quality,
            d.time_regime,
            d.actionable_block_reason,
            d.watch_bucket,
            d.pressure_conflict_level,
            d.blockers_json,
            d.cautions_json,
            d.entry_above,
            d.entry_below,
            d.invalidate_price,
            d.first_target_price
        FROM strategy_decisions_5m d
        WHERE d.instrument = %s
          AND DATE(d.ts AT TIME ZONE 'Asia/Kolkata') = %s
          AND (
                d.signal IS NOT NULL
                OR COALESCE(d.strategy_score, 0) >= %s
              )
    ),
    live_signals AS (
        SELECT
            s.ts,
            s.instrument,
            s.signal AS live_signal,
            s.strike,
            s.option_entry_ltp,
            s.entry_spread,
            s.entry_delta,
            s.reason,
            s.underlying_price,
            s.telegram_sent
        FROM signals_issued s
        WHERE s.instrument = %s
          AND DATE(s.ts AT TIME ZONE 'Asia/Kolkata') = %s
    ),
    linked_signals AS (
        SELECT
            d.ts AS decision_ts,
            d.instrument,
            d.signal AS decision_signal,
            s.ts AS signal_ts,
            s.live_signal,
            s.strike,
            s.option_entry_ltp,
            s.entry_spread,
            s.entry_delta,
            s.reason AS signal_reason,
            s.underlying_price,
            s.telegram_sent,
            ROW_NUMBER() OVER (
                PARTITION BY d.ts, d.instrument, d.signal
                ORDER BY
                    CASE
                        WHEN s.ts = d.ts THEN 0
                        WHEN s.reason ILIKE '1m entry trigger after 5m watch%%' THEN 1
                        ELSE 2
                    END,
                    ABS(EXTRACT(EPOCH FROM (s.ts - d.ts))),
                    s.ts
            ) AS rn
        FROM decisions d
        LEFT JOIN live_signals s
          ON s.instrument = d.instrument
         AND s.live_signal = d.signal
         AND (
                s.ts = d.ts
                OR (
                    s.reason ILIKE '1m entry trigger after 5m watch%%'
                    AND s.ts >= d.ts
                    AND s.ts <= d.ts + INTERVAL '15 minutes'
                    AND (
                        (d.entry_above IS NOT NULL AND s.underlying_price >= d.entry_above - 5)
                        OR (d.entry_below IS NOT NULL AND s.underlying_price <= d.entry_below + 5)
                    )
                )
             )
    )
    SELECT
        d.ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        d.signal,
        d.strategy_score,
        d.setup_type,
        d.signal_quality,
        d.time_regime,
        d.pressure_conflict_level,
        d.actionable_block_reason,
        d.watch_bucket,
        d.entry_above,
        d.entry_below,
        d.invalidate_price,
        d.first_target_price,
        s.signal_ts AT TIME ZONE 'Asia/Kolkata' AS live_ts_ist,
        s.live_signal,
        s.strike,
        s.option_entry_ltp,
        s.entry_spread,
        s.entry_delta,
        s.signal_reason,
        s.telegram_sent,
        h.pnl_points,
        h.pnl_percent,
        h.max_favorable_points,
        h.max_adverse_points,
        d.blockers_json::text,
        d.cautions_json::text,
        d.reason
    FROM decisions d
    LEFT JOIN linked_signals s
      ON s.decision_ts = d.ts
     AND s.instrument = d.instrument
     AND s.decision_signal = d.signal
     AND s.rn = 1
    LEFT JOIN option_signal_horizon_outcomes h
      ON h.signal_ts = s.signal_ts
     AND h.instrument = s.instrument
     AND h.signal = s.live_signal
     AND h.strike = s.strike
     AND h.horizon_minutes = %s
    ORDER BY d.ts;
    """

    orphan_query = """
    WITH decision_links AS (
        SELECT DISTINCT s.ts, s.instrument, s.signal, s.strike
        FROM signals_issued s
        JOIN strategy_decisions_5m d
          ON d.instrument = s.instrument
         AND d.signal = s.signal
         AND DATE(d.ts AT TIME ZONE 'Asia/Kolkata') = %s
         AND DATE(s.ts AT TIME ZONE 'Asia/Kolkata') = %s
         AND (
                s.ts = d.ts
                OR (
                    s.reason ILIKE '1m entry trigger after 5m watch%%'
                    AND s.ts >= d.ts
                    AND s.ts <= d.ts + INTERVAL '15 minutes'
                )
             )
        WHERE s.instrument = %s
    )
    SELECT
        s.ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        s.signal,
        s.strike,
        s.option_entry_ltp,
        s.underlying_price,
        s.entry_spread,
        s.entry_delta,
        s.reason,
        s.telegram_sent,
        h.pnl_points,
        h.pnl_percent,
        h.max_favorable_points,
        h.max_adverse_points
    FROM signals_issued s
    LEFT JOIN decision_links dl
      ON dl.ts = s.ts
     AND dl.instrument = s.instrument
     AND dl.signal = s.signal
     AND dl.strike = s.strike
    LEFT JOIN option_signal_horizon_outcomes h
      ON h.signal_ts = s.ts
     AND h.instrument = s.instrument
     AND h.signal = s.signal
     AND h.strike = s.strike
     AND h.horizon_minutes = %s
    WHERE s.instrument = %s
      AND DATE(s.ts AT TIME ZONE 'Asia/Kolkata') = %s
      AND dl.ts IS NULL
    ORDER BY s.ts;
    """

    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    args.instrument,
                    args.date,
                    args.min_score,
                    args.instrument,
                    args.date,
                    args.horizon,
                ),
            )
            rows = cur.fetchall()
            cur.execute(
                orphan_query,
                (
                    args.date,
                    args.date,
                    args.instrument,
                    args.horizon,
                    args.instrument,
                    args.date,
                ),
            )
            orphan_rows = cur.fetchall()

    print("=" * 150)
    print(f"DAILY SIGNAL CALIBRATION | {args.date} | {args.instrument} | horizon={args.horizon}m | min_score={args.min_score}")
    print("=" * 150)
    print("time | cand_sig | score | setup | grade | regime | conflict | live | strike | opt_ltp | pnl | mfe | mae | block")
    for row in rows:
        (
            ts_ist,
            cand_signal,
            score,
            setup_type,
            signal_quality,
            time_regime,
            conflict_level,
            actionable_block_reason,
            watch_bucket,
            entry_above,
            entry_below,
            invalidate_price,
            first_target_price,
            live_ts_ist,
            live_signal,
            strike,
            option_entry_ltp,
            entry_spread,
            entry_delta,
            signal_reason,
            telegram_sent,
            pnl_points,
            pnl_percent,
            max_favorable_points,
            max_adverse_points,
            blockers_json,
            cautions_json,
            reason,
        ) = row
        time_label = ts_ist.strftime("%H:%M") if ts_ist else "-"
        live_time_label = live_ts_ist.strftime("%H:%M") if live_ts_ist else "-"
        block_label = actionable_block_reason or watch_bucket or "-"
        print(
            " | ".join(
                [
                    time_label,
                    cand_signal or "-",
                    fmt(score, 0),
                    setup_type or "-",
                    signal_quality or "-",
                    time_regime or "-",
                    conflict_level or "-",
                    f"YES@{live_time_label}" if live_signal and live_ts_ist else ("YES" if live_signal else "NO"),
                    str(strike or "-"),
                    fmt(option_entry_ltp),
                    fmt(pnl_points),
                    fmt(max_favorable_points),
                    fmt(max_adverse_points),
                    block_label,
                ]
            )
        )
        detail_bits = []
        if entry_above is not None:
            detail_bits.append(f"above {fmt(entry_above)}")
        if entry_below is not None:
            detail_bits.append(f"below {fmt(entry_below)}")
        if invalidate_price is not None:
            detail_bits.append(f"inv {fmt(invalidate_price)}")
        if first_target_price is not None:
            detail_bits.append(f"t1 {fmt(first_target_price)}")
        if entry_spread is not None:
            detail_bits.append(f"spread {fmt(entry_spread)}")
        if entry_delta is not None:
            detail_bits.append(f"delta {fmt(entry_delta)}")
        if pnl_percent is not None:
            detail_bits.append(f"pnl% {fmt(pnl_percent, 1)}")
        if live_signal and telegram_sent is not None:
            detail_bits.append(f"telegram {'YES' if telegram_sent else 'NO'}")
        if detail_bits:
            print("  plan:", " | ".join(detail_bits))
        if blockers_json and blockers_json != "null":
            print("  blockers:", blockers_json[:220])
        if cautions_json and cautions_json != "null":
            print("  cautions:", cautions_json[:220])
        if reason:
            print("  reason:", str(reason)[:220])
        if signal_reason and live_signal:
            print("  live_reason:", str(signal_reason)[:220])

    if orphan_rows:
        print("-" * 150)
        print("LIVE SIGNALS WITHOUT 5M ROW MATCH")
        print("-" * 150)
        print("time | live_sig | strike | opt_ltp | spot | pnl | mfe | mae")
        for row in orphan_rows:
            (
                ts_ist,
                live_signal,
                strike,
                option_entry_ltp,
                underlying_price,
                entry_spread,
                entry_delta,
                signal_reason,
                telegram_sent,
                pnl_points,
                pnl_percent,
                max_favorable_points,
                max_adverse_points,
            ) = row
            print(
                " | ".join(
                    [
                        ts_ist.strftime("%H:%M") if ts_ist else "-",
                        live_signal or "-",
                        str(strike or "-"),
                        fmt(option_entry_ltp),
                        fmt(underlying_price),
                        fmt(pnl_points),
                        fmt(max_favorable_points),
                        fmt(max_adverse_points),
                    ]
                )
            )
            extras = []
            if entry_spread is not None:
                extras.append(f"spread {fmt(entry_spread)}")
            if entry_delta is not None:
                extras.append(f"delta {fmt(entry_delta)}")
            if pnl_percent is not None:
                extras.append(f"pnl% {fmt(pnl_percent, 1)}")
            extras.append(f"telegram {'YES' if telegram_sent else 'NO'}")
            if extras:
                print("  details:", " | ".join(extras))
            if signal_reason:
                print("  reason:", str(signal_reason)[:220])


if __name__ == "__main__":
    main()
