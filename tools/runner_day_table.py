#!/usr/bin/env python3
"""Show runner-aware daily option signal outcomes and monitor state."""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config


def parse_args():
    parser = argparse.ArgumentParser(description="Runner-aware daily signal table")
    parser.add_argument("--date", required=True, help="Trading day in YYYY-MM-DD (Asia/Kolkata)")
    parser.add_argument("--instrument", choices=["NIFTY", "BANKNIFTY", "SENSEX"], required=True)
    return parser.parse_args()


def fmt(value, digits=2):
    if value is None:
        return "-"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value)


def table_has_columns(cur, table_name, columns):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (table_name,),
    )
    existing = {row[0] for row in cur.fetchall()}
    return all(column in existing for column in columns)


def main():
    args = parse_args()
    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            has_runner_cols = table_has_columns(cur, "trade_monitor_events_1m", ["run_profile", "runner_mode"])
            monitor_select = (
                "m.run_profile, m.runner_mode,"
                if has_runner_cols
                else "NULL::text AS run_profile, NULL::boolean AS runner_mode,"
            )
            query = f"""
            WITH latest_monitor AS (
                SELECT *
                FROM (
                    SELECT
                        m.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY m.entry_ts, m.instrument, m.signal
                            ORDER BY m.ts DESC
                        ) AS rn
                    FROM trade_monitor_events_1m m
                    WHERE m.instrument = %s
                      AND DATE(m.entry_ts AT TIME ZONE 'Asia/Kolkata') = %s
                ) ranked
                WHERE rn = 1
            ),
            outcome_summary AS (
                SELECT
                    o.signal_ts,
                    o.instrument,
                    o.signal,
                    o.strike,
                    MAX(o.max_favorable_ltp) AS peak_ltp,
                    MIN(o.max_adverse_ltp) AS trough_ltp,
                    MAX(o.pnl_points) AS best_pnl_points,
                    MIN(o.pnl_points) AS worst_pnl_points,
                    MAX(o.minutes_since_signal) AS minutes_tracked
                FROM option_signal_outcomes_1m o
                WHERE o.instrument = %s
                  AND DATE(o.signal_ts AT TIME ZONE 'Asia/Kolkata') = %s
                GROUP BY o.signal_ts, o.instrument, o.signal, o.strike
            )
            SELECT
                s.ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
                s.signal,
                s.strike,
                s.option_entry_ltp,
                s.setup_type,
                s.time_regime,
                os.peak_ltp,
                os.trough_ltp,
                os.best_pnl_points,
                os.worst_pnl_points,
                os.minutes_tracked,
                lm.guidance,
                {monitor_select}
                lm.reason
            FROM signals_issued s
            LEFT JOIN outcome_summary os
              ON os.signal_ts = s.ts
             AND os.instrument = s.instrument
             AND os.signal = s.signal
             AND os.strike = s.strike
            LEFT JOIN latest_monitor lm
              ON lm.entry_ts = s.ts
             AND lm.instrument = s.instrument
             AND lm.signal = s.signal
            WHERE s.instrument = %s
              AND DATE(s.ts AT TIME ZONE 'Asia/Kolkata') = %s
            ORDER BY s.ts;
            """
            cur.execute(
                query,
                (
                    args.instrument,
                    args.date,
                    args.instrument,
                    args.date,
                    args.instrument,
                    args.date,
                ),
            )
            rows = cur.fetchall()

    print("=" * 148)
    print(f"RUNNER DAY TABLE | {args.date} | {args.instrument}")
    print("=" * 148)
    print("time | side | strike | entry | setup | regime | peak | best_pts | worst_pts | mins | guidance | run_profile | runner")
    for row in rows:
        (
            ts_ist,
            signal,
            strike,
            option_entry_ltp,
            setup_type,
            time_regime,
            peak_ltp,
            trough_ltp,
            best_pnl_points,
            worst_pnl_points,
            minutes_tracked,
            guidance,
            run_profile,
            runner_mode,
            reason,
        ) = row
        time_label = ts_ist.strftime("%H:%M") if ts_ist else "-"
        print(
            " | ".join(
                [
                    time_label,
                    signal or "-",
                    str(strike or "-"),
                    fmt(option_entry_ltp),
                    setup_type or "-",
                    time_regime or "-",
                    fmt(peak_ltp),
                    fmt(best_pnl_points),
                    fmt(worst_pnl_points),
                    str(minutes_tracked or "-"),
                    guidance or "-",
                    run_profile or "-",
                    "YES" if runner_mode else "NO",
                ]
            )
        )
        if reason:
            print("  reason:", str(reason)[:220])


if __name__ == "__main__":
    main()
