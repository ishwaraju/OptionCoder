#!/usr/bin/env python3
"""Analyze signal expectancy using live fired signals and premium outcomes."""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config


HORIZONS = ("5m", "10m", "15m", "20m", "EOD")


def parse_args():
    parser = argparse.ArgumentParser(description="Show expectancy report for fired option signals")
    parser.add_argument("--date-from", required=True, help="Start date in YYYY-MM-DD (Asia/Kolkata)")
    parser.add_argument("--date-to", required=True, help="End date in YYYY-MM-DD (Asia/Kolkata)")
    parser.add_argument("--instrument", choices=["NIFTY", "BANKNIFTY", "SENSEX"])
    parser.add_argument("--setup")
    parser.add_argument("--min-count", type=int, default=2)
    return parser.parse_args()


def fmt(value, digits=2):
    if value is None:
        return "-"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value)


def print_rows(title, headers, rows):
    print(f"\n{title}")
    if not rows:
        print("  no data")
        return
    print("  " + " | ".join(headers))
    for row in rows:
        print("  " + " | ".join("-" if value is None else str(value) for value in row))


def build_filters(args, alias="s"):
    clauses = [
        f"DATE({alias}.ts AT TIME ZONE 'Asia/Kolkata') >= %s",
        f"DATE({alias}.ts AT TIME ZONE 'Asia/Kolkata') <= %s",
    ]
    params = [args.date_from, args.date_to]
    if args.instrument:
        clauses.append(f"{alias}.instrument = %s")
        params.append(args.instrument)
    if args.setup:
        clauses.append(f"COALESCE({alias}.setup_type, '') = %s")
        params.append(args.setup)
    return " AND ".join(clauses), params


def main():
    args = parse_args()
    where_sql, params = build_filters(args, alias="s")
    shared_cte = f"""
    WITH base_signals AS (
        SELECT
            s.ts,
            s.instrument,
            s.signal,
            s.strike,
            s.setup_type,
            s.time_regime,
            s.active_day_state,
            s.day_state_direction,
            s.entry_delta,
            s.entry_spread,
            CASE
                WHEN s.entry_delta IS NULL THEN 'UNK'
                WHEN ABS(s.entry_delta) < 0.35 THEN 'LOW'
                WHEN ABS(s.entry_delta) < 0.55 THEN 'MID'
                ELSE 'HIGH'
            END AS delta_bucket,
            CASE
                WHEN s.entry_spread IS NULL THEN 'UNK'
                WHEN s.entry_spread <= 1 THEN 'TIGHT'
                WHEN s.entry_spread <= 3 THEN 'OK'
                ELSE 'WIDE'
            END AS spread_bucket
        FROM signals_issued s
        WHERE {where_sql}
    ),
    decision_context AS (
        SELECT
            b.ts,
            b.instrument,
            b.signal,
            b.strike,
            COALESCE(d.pressure_conflict_level, 'NONE') AS pressure_conflict_level
        FROM base_signals b
        LEFT JOIN LATERAL (
            SELECT d.pressure_conflict_level
            FROM strategy_decisions_5m d
            WHERE d.instrument = b.instrument
              AND d.signal = b.signal
              AND d.ts <= b.ts
              AND d.ts >= b.ts - INTERVAL '15 minutes'
            ORDER BY ABS(EXTRACT(EPOCH FROM (b.ts - d.ts))), d.ts DESC
            LIMIT 1
        ) d ON TRUE
    ),
    ml_context AS (
        SELECT
            b.ts,
            b.instrument,
            b.signal,
            b.strike,
            CASE
                WHEN m.wall_break_alert IN ('SUPPORT_BREAK_RISK', 'RESISTANCE_BREAK_RISK') THEN 'BREAK_RISK'
                WHEN m.support_wall_state = 'WEAKENING' OR m.resistance_wall_state = 'WEAKENING' THEN 'WEAKENING'
                WHEN m.support_wall_state = 'STRENGTHENING' OR m.resistance_wall_state = 'STRENGTHENING' THEN 'STRENGTHENING'
                ELSE 'NEUTRAL'
            END AS wall_confirmation_state
        FROM base_signals b
        LEFT JOIN LATERAL (
            SELECT
                m.wall_break_alert,
                m.support_wall_state,
                m.resistance_wall_state
            FROM ml_features_log m
            WHERE m.instrument = b.instrument
              AND m.signal_direction = b.signal
              AND m.alert_ts <= b.ts
              AND m.alert_ts >= b.ts - INTERVAL '15 minutes'
            ORDER BY ABS(EXTRACT(EPOCH FROM (b.ts - m.alert_ts))), m.alert_ts DESC
            LIMIT 1
        ) m ON TRUE
    ),
    horizons AS (
        SELECT
            h.signal_ts AS ts,
            h.instrument,
            h.signal,
            h.strike,
            CASE h.horizon_minutes
                WHEN 5 THEN '5m'
                WHEN 10 THEN '10m'
                WHEN 15 THEN '15m'
                WHEN 20 THEN '20m'
                ELSE NULL
            END AS horizon_label,
            h.pnl_points,
            h.pnl_percent,
            h.max_favorable_points,
            h.max_adverse_points
        FROM option_signal_horizon_outcomes h
        WHERE h.horizon_minutes IN (5, 10, 15, 20)
    ),
    eod_outcomes AS (
        SELECT
            signal_ts AS ts,
            instrument,
            signal,
            strike,
            'EOD' AS horizon_label,
            pnl_points,
            CASE
                WHEN option_entry_ltp IS NOT NULL AND option_entry_ltp <> 0 AND pnl_points IS NOT NULL
                THEN ROUND((pnl_points / option_entry_ltp) * 100.0, 2)
                ELSE NULL
            END AS pnl_percent,
            CASE
                WHEN max_favorable_ltp IS NOT NULL AND option_entry_ltp IS NOT NULL
                THEN ROUND(max_favorable_ltp - option_entry_ltp, 2)
                ELSE NULL
            END AS max_favorable_points,
            CASE
                WHEN max_adverse_ltp IS NOT NULL AND option_entry_ltp IS NOT NULL
                THEN ROUND(max_adverse_ltp - option_entry_ltp, 2)
                ELSE NULL
            END AS max_adverse_points
        FROM (
            SELECT
                o.*,
                ROW_NUMBER() OVER (
                    PARTITION BY o.signal_ts, o.instrument, o.signal, o.strike
                    ORDER BY o.observed_ts DESC
                ) AS rn
            FROM option_signal_outcomes_1m o
        ) ranked
        WHERE rn = 1
    ),
    all_outcomes AS (
        SELECT * FROM horizons
        UNION ALL
        SELECT * FROM eod_outcomes
    )
    """

    summary_query = shared_cte + """
    SELECT
        o.horizon_label,
        COUNT(*)::int AS rows,
        ROUND(AVG(o.pnl_points)::numeric, 2) AS avg_pts,
        ROUND(AVG(o.pnl_percent)::numeric, 2) AS avg_pct,
        ROUND((AVG(CASE WHEN o.pnl_points > 0 THEN 1 ELSE 0 END) * 100)::numeric, 1) AS win_pct,
        ROUND(AVG(o.max_favorable_points)::numeric, 2) AS avg_mfe,
        ROUND(AVG(o.max_adverse_points)::numeric, 2) AS avg_mae
    FROM base_signals b
    JOIN all_outcomes o
      ON o.ts = b.ts
     AND o.instrument = b.instrument
     AND o.signal = b.signal
     AND o.strike = b.strike
    GROUP BY o.horizon_label
    ORDER BY ARRAY_POSITION(ARRAY['5m','10m','15m','20m','EOD'], o.horizon_label);
    """

    grouped_query = shared_cte + """
    SELECT
        b.instrument,
        COALESCE(b.setup_type, 'UNKNOWN') AS setup,
        COALESCE(b.active_day_state, 'UNKNOWN') AS day_state,
        COALESCE(c.pressure_conflict_level, 'NONE') AS pressure_conflict,
        COALESCE(m.wall_confirmation_state, 'NEUTRAL') AS wall_state,
        b.delta_bucket,
        b.spread_bucket,
        o.horizon_label,
        COUNT(*)::int AS rows,
        ROUND(AVG(o.pnl_points)::numeric, 2) AS avg_pts,
        ROUND((AVG(CASE WHEN o.pnl_points > 0 THEN 1 ELSE 0 END) * 100)::numeric, 1) AS win_pct,
        ROUND(AVG(o.max_favorable_points)::numeric, 2) AS avg_mfe,
        ROUND(AVG(o.max_adverse_points)::numeric, 2) AS avg_mae
    FROM base_signals b
    JOIN all_outcomes o
      ON o.ts = b.ts
     AND o.instrument = b.instrument
     AND o.signal = b.signal
     AND o.strike = b.strike
    LEFT JOIN decision_context c
      ON c.ts = b.ts
     AND c.instrument = b.instrument
     AND c.signal = b.signal
     AND c.strike = b.strike
    LEFT JOIN ml_context m
      ON m.ts = b.ts
     AND m.instrument = b.instrument
     AND m.signal = b.signal
     AND m.strike = b.strike
    GROUP BY
        b.instrument, setup, day_state, pressure_conflict, wall_state,
        b.delta_bucket, b.spread_bucket, o.horizon_label
    HAVING COUNT(*) >= %s
    ORDER BY avg_pts DESC, rows DESC
    LIMIT 40;
    """

    worst_query = shared_cte + """
    SELECT
        b.instrument,
        b.signal,
        COALESCE(b.setup_type, 'UNKNOWN') AS setup,
        b.ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        o.horizon_label,
        o.pnl_points,
        o.max_favorable_points,
        o.max_adverse_points,
        COALESCE(c.pressure_conflict_level, 'NONE') AS pressure_conflict,
        COALESCE(m.wall_confirmation_state, 'NEUTRAL') AS wall_state
    FROM base_signals b
    JOIN all_outcomes o
      ON o.ts = b.ts
     AND o.instrument = b.instrument
     AND o.signal = b.signal
     AND o.strike = b.strike
    LEFT JOIN decision_context c
      ON c.ts = b.ts
     AND c.instrument = b.instrument
     AND c.signal = b.signal
     AND c.strike = b.strike
    LEFT JOIN ml_context m
      ON m.ts = b.ts
     AND m.instrument = b.instrument
     AND m.signal = b.signal
     AND m.strike = b.strike
    WHERE o.horizon_label IN ('5m', '20m', 'EOD')
    ORDER BY o.pnl_points ASC NULLS LAST, b.ts DESC
    LIMIT 15;
    """

    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(summary_query, params)
            summary_rows = cur.fetchall()
            cur.execute(grouped_query, [*params, args.min_count])
            grouped_rows = cur.fetchall()
            cur.execute(worst_query, params)
            worst_rows = cur.fetchall()

    print(f"EXPECTANCY REPORT | {args.date_from} -> {args.date_to}")
    if args.instrument:
        print(f"Instrument: {args.instrument}")
    if args.setup:
        print(f"Setup filter: {args.setup}")

    print_rows(
        "Overall Horizons",
        ["horizon", "rows", "avg_pts", "avg_%", "win_%", "avg_mfe", "avg_mae"],
        summary_rows,
    )
    print_rows(
        "Best Groups",
        ["inst", "setup", "day_state", "pressure", "wall", "delta", "spread", "horizon", "rows", "avg_pts", "win_%", "avg_mfe", "avg_mae"],
        grouped_rows,
    )
    print_rows(
        "Worst Cases",
        ["inst", "side", "setup", "ts_ist", "horizon", "pnl_pts", "mfe", "mae", "pressure", "wall"],
        worst_rows,
    )


if __name__ == "__main__":
    main()
