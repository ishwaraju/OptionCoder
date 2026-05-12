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
            s.entry_delta
        FROM signals_issued s
        WHERE s.instrument = %s
          AND DATE(s.ts AT TIME ZONE 'Asia/Kolkata') = %s
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
        s.live_signal,
        s.strike,
        s.option_entry_ltp,
        s.entry_spread,
        s.entry_delta,
        h.pnl_points,
        h.pnl_percent,
        h.max_favorable_points,
        h.max_adverse_points,
        d.blockers_json::text,
        d.cautions_json::text,
        d.reason
    FROM decisions d
    LEFT JOIN live_signals s
      ON s.ts = d.ts
     AND s.instrument = d.instrument
     AND s.live_signal = d.signal
    LEFT JOIN option_signal_horizon_outcomes h
      ON h.signal_ts = s.ts
     AND h.instrument = s.instrument
     AND h.signal = s.live_signal
     AND h.strike = s.strike
     AND h.horizon_minutes = %s
    ORDER BY d.ts;
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
            live_signal,
            strike,
            option_entry_ltp,
            entry_spread,
            entry_delta,
            pnl_points,
            pnl_percent,
            max_favorable_points,
            max_adverse_points,
            blockers_json,
            cautions_json,
            reason,
        ) = row
        time_label = ts_ist.strftime("%H:%M") if ts_ist else "-"
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
                    "YES" if live_signal else "NO",
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
        if detail_bits:
            print("  plan:", " | ".join(detail_bits))
        if blockers_json and blockers_json != "null":
            print("  blockers:", blockers_json[:220])
        if cautions_json and cautions_json != "null":
            print("  cautions:", cautions_json[:220])
        if reason:
            print("  reason:", str(reason)[:220])


if __name__ == "__main__":
    main()
