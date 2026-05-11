#!/usr/bin/env python3
"""Print an option-buyer day table for fired signals and premium outcomes."""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config


DEFAULT_HORIZON = 5


def parse_args():
    parser = argparse.ArgumentParser(description="Show option-buyer signal outcome table for a trading day")
    parser.add_argument("--date", required=True, help="Trading day in YYYY-MM-DD (Asia/Kolkata)")
    parser.add_argument("--instrument", choices=["NIFTY", "BANKNIFTY", "SENSEX"])
    parser.add_argument(
        "--horizon",
        type=int,
        default=DEFAULT_HORIZON,
        choices=[1, 2, 3, 5],
        help="Premium outcome horizon in minutes",
    )
    return parser.parse_args()


def fmt(value, digits=2):
    if value is None:
        return "-"
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def fmt_pct(value):
    if value is None:
        return "-"
    return f"{float(value):.1f}%"


def verdict(pnl_points, max_favorable_points, max_adverse_points, guidance):
    pnl = _safe_float(pnl_points)
    mfe = _safe_float(max_favorable_points)
    mae = _safe_float(max_adverse_points)
    guidance = (guidance or "").upper()

    if "EXIT" in guidance and pnl is not None and pnl <= 0:
        return "CUT FAST"
    if pnl is not None and pnl > 0 and mfe is not None and mae is not None and mfe >= abs(mae) * 1.5:
        return "CLEAN"
    if mfe is not None and mfe >= 0 and pnl is not None and pnl <= 0:
        return "GAVE BACK"
    if mae is not None and mae < 0 and pnl is not None and pnl < 0:
        return "WEAK"
    if pnl is not None and pnl > 0:
        return "OK"
    return "REVIEW"


def _safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_where(args):
    clauses = ["DATE(s.ts AT TIME ZONE 'Asia/Kolkata') = %s"]
    params = [args.date]
    if args.instrument:
        clauses.append("s.instrument = %s")
        params.append(args.instrument)
    return " AND ".join(clauses), params


def fetch_rows(cur, args):
    where_sql, params = build_where(args)
    params.append(args.horizon)
    cur.execute(
        f"""
        WITH latest_monitor AS (
            SELECT *
            FROM (
                SELECT
                    m.entry_ts,
                    m.instrument,
                    m.signal,
                    m.guidance,
                    m.reason,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.entry_ts, m.instrument, m.signal
                        ORDER BY m.ts DESC
                    ) AS rn
                FROM trade_monitor_events_1m m
                WHERE DATE(m.entry_ts AT TIME ZONE 'Asia/Kolkata') = %s
            ) ranked
            WHERE rn = 1
        )
        SELECT
            s.ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
            s.instrument,
            s.signal,
            s.strike,
            s.price,
            s.strategy_score,
            s.setup_type,
            s.time_regime,
            s.entry_spread,
            h.pnl_points,
            h.pnl_percent,
            h.max_favorable_points,
            h.max_adverse_points,
            h.outcome_label,
            lm.guidance,
            lm.reason
        FROM signals_issued s
        LEFT JOIN option_signal_horizon_outcomes h
          ON h.signal_ts = s.ts
         AND h.instrument = s.instrument
         AND h.signal = s.signal
         AND h.strike = s.strike
         AND h.horizon_minutes = %s
        LEFT JOIN latest_monitor lm
          ON lm.entry_ts = s.ts
         AND lm.instrument = s.instrument
         AND lm.signal = s.signal
        WHERE {where_sql}
        ORDER BY s.instrument, s.ts;
        """,
        [args.date, args.horizon, *params[:-1]],
    )
    return cur.fetchall()


def fetch_summary(cur, args):
    where_sql, params = build_where(args)
    cur.execute(
        f"""
        SELECT
            h.horizon_minutes,
            COUNT(*)::int,
            ROUND(AVG(h.pnl_points)::numeric, 2),
            ROUND(AVG(h.pnl_percent)::numeric, 2),
            ROUND((AVG(CASE WHEN h.pnl_points > 0 THEN 1 ELSE 0 END) * 100)::numeric, 1),
            ROUND(AVG(h.max_favorable_points)::numeric, 2),
            ROUND(AVG(h.max_adverse_points)::numeric, 2)
        FROM signals_issued s
        JOIN option_signal_horizon_outcomes h
          ON h.signal_ts = s.ts
         AND h.instrument = s.instrument
         AND h.signal = s.signal
         AND h.strike = s.strike
        WHERE {where_sql}
        GROUP BY h.horizon_minutes
        ORDER BY h.horizon_minutes;
        """,
        params,
    )
    return cur.fetchall()


def print_table(day, horizon, rows, summary_rows):
    print("=" * 118)
    print(f"OPTION BUYER DAY TABLE | {day} | horizon={horizon}m")
    print("=" * 118)

    if summary_rows:
        print("\nHorizon summary:")
        print("min | rows | avg_pts | avg_% | win_% | avg_mfe | avg_mae")
        for row in summary_rows:
            minutes, count, avg_pts, avg_pct, win_pct, avg_mfe, avg_mae = row
            print(
                f"{minutes} | {count} | {fmt(avg_pts)} | {fmt_pct(avg_pct)} | "
                f"{fmt_pct(win_pct)} | {fmt(avg_mfe)} | {fmt(avg_mae)}"
            )

    print("\nSignals:")
    if not rows:
        print("No fired option signals found.")
        return

    headers = [
        "time",
        "inst",
        "side",
        "strike",
        "entry",
        "score",
        "setup",
        "regime",
        "spread",
        "pnl",
        "pnl%",
        "mfe",
        "mae",
        "guidance",
        "verdict",
    ]
    print(" | ".join(headers))
    for row in rows:
        (
            ts_ist,
            instrument,
            signal,
            strike,
            entry_price,
            strategy_score,
            setup_type,
            time_regime,
            entry_spread,
            pnl_points,
            pnl_percent,
            max_favorable_points,
            max_adverse_points,
            outcome_label,
            guidance,
            reason,
        ) = row
        time_label = ts_ist.strftime("%H:%M") if ts_ist else "-"
        print(
            " | ".join(
                [
                    time_label,
                    instrument or "-",
                    signal or "-",
                    str(strike or "-"),
                    fmt(entry_price),
                    str(strategy_score or "-"),
                    setup_type or "-",
                    time_regime or "-",
                    fmt(entry_spread),
                    fmt(pnl_points),
                    fmt_pct(pnl_percent),
                    fmt(max_favorable_points),
                    fmt(max_adverse_points),
                    guidance or outcome_label or "-",
                    verdict(pnl_points, max_favorable_points, max_adverse_points, guidance),
                ]
            )
        )
        if reason:
            print(f"  reason: {reason[:180]}")


def main():
    args = parse_args()
    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            rows = fetch_rows(cur, args)
            summary_rows = fetch_summary(cur, args)
    print_table(args.date, args.horizon, rows, summary_rows)
    if rows and not any(row[9] is not None for row in rows):
        print(
            "\nNo premium horizon data yet. Run: "
            "python3 tools/backfill_option_signal_outcomes.py --date-from YYYY-MM-DD --date-to YYYY-MM-DD"
        )


if __name__ == "__main__":
    main()
