#!/usr/bin/env python3
"""Replay option-buyer fired signals using fixed 1/2/3/5 minute premium outcomes."""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config


def parse_args():
    parser = argparse.ArgumentParser(description="Replay option-buyer premium edge by fixed horizons")
    parser.add_argument("--date-from", help="Start date YYYY-MM-DD")
    parser.add_argument("--date-to", help="End date YYYY-MM-DD")
    parser.add_argument("--instrument", choices=["NIFTY", "BANKNIFTY", "SENSEX"])
    return parser.parse_args()


def build_where(args, alias="s"):
    clauses = []
    params = []
    if args.instrument:
        clauses.append(f"{alias}.instrument = %s")
        params.append(args.instrument)
    if args.date_from:
        clauses.append(f"DATE({alias}.ts AT TIME ZONE 'Asia/Kolkata') >= %s")
        params.append(args.date_from)
    if args.date_to:
        clauses.append(f"DATE({alias}.ts AT TIME ZONE 'Asia/Kolkata') <= %s")
        params.append(args.date_to)
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def print_rows(title, headers, rows):
    print(f"\n{title}")
    if not rows:
        print("  no data")
        return
    print("  " + " | ".join(headers))
    for row in rows:
        print("  " + " | ".join("-" if value is None else str(value) for value in row))


def main():
    args = parse_args()
    signal_where, signal_params = build_where(args, alias="s")
    horizon_where, horizon_params = build_where(args, alias="s")

    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*)::int
                FROM signals_issued s
                {signal_where};
                """,
                signal_params,
            )
            signal_count = cur.fetchone()[0]

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
                FROM option_signal_horizon_outcomes h
                JOIN signals_issued s
                  ON s.ts = h.signal_ts
                 AND s.instrument = h.instrument
                 AND s.signal = h.signal
                 AND s.strike = h.strike
                {horizon_where}
                GROUP BY h.horizon_minutes
                ORDER BY h.horizon_minutes;
                """,
                horizon_params,
            )
            horizon_rows = cur.fetchall()

            cur.execute(
                f"""
                SELECT
                    COALESCE(s.setup_type, 'UNKNOWN') AS setup,
                    h.horizon_minutes,
                    COUNT(*)::int,
                    ROUND(AVG(h.pnl_points)::numeric, 2),
                    ROUND((AVG(CASE WHEN h.pnl_points > 0 THEN 1 ELSE 0 END) * 100)::numeric, 1)
                FROM option_signal_horizon_outcomes h
                JOIN signals_issued s
                  ON s.ts = h.signal_ts
                 AND s.instrument = h.instrument
                 AND s.signal = h.signal
                 AND s.strike = h.strike
                {horizon_where}
                GROUP BY COALESCE(s.setup_type, 'UNKNOWN'), h.horizon_minutes
                ORDER BY setup, h.horizon_minutes;
                """,
                horizon_params,
            )
            setup_rows = cur.fetchall()

            cur.execute(
                f"""
                SELECT
                    s.instrument,
                    s.signal,
                    s.ts AT TIME ZONE 'Asia/Kolkata',
                    s.setup_type,
                    h.horizon_minutes,
                    h.pnl_points,
                    h.pnl_percent,
                    h.outcome_label
                FROM option_signal_horizon_outcomes h
                JOIN signals_issued s
                  ON s.ts = h.signal_ts
                 AND s.instrument = h.instrument
                 AND s.signal = h.signal
                 AND s.strike = h.strike
                {horizon_where}
                ORDER BY h.pnl_points ASC NULLS LAST, s.ts DESC
                LIMIT 10;
                """,
                horizon_params,
            )
            worst_rows = cur.fetchall()

    print(f"Signals found: {signal_count}")
    print_rows(
        "Premium Horizon Summary",
        ["min", "rows", "avg_pts", "avg_pct", "win_%", "avg_mfe", "avg_mae"],
        horizon_rows,
    )
    print_rows(
        "By Setup",
        ["setup", "min", "rows", "avg_pts", "win_%"],
        setup_rows,
    )
    print_rows(
        "Worst Outcomes",
        ["instrument", "signal", "ts_ist", "setup", "min", "pnl_pts", "pnl_%", "label"],
        worst_rows,
    )
    if signal_count and not horizon_rows:
        print("\nNo horizon outcomes yet. Run: python3 tools/backfill_option_signal_outcomes.py --date-from YYYY-MM-DD --date-to YYYY-MM-DD")


if __name__ == "__main__":
    main()
