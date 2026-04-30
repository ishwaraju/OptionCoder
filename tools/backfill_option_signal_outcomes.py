#!/usr/bin/env python3
"""Backfill option_signal_outcomes_1m using signals_issued and option snapshots."""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config


MAX_MINUTES = 20


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill option signal outcomes")
    parser.add_argument("--date-from", help="Start date YYYY-MM-DD")
    parser.add_argument("--date-to", help="End date YYYY-MM-DD")
    return parser.parse_args()


def build_where(args):
    clauses = []
    params = []
    if args.date_from:
        clauses.append("DATE(s.ts AT TIME ZONE 'Asia/Kolkata') >= %s")
        params.append(args.date_from)
    if args.date_to:
        clauses.append("DATE(s.ts AT TIME ZONE 'Asia/Kolkata') <= %s")
        params.append(args.date_to)
    where = (" AND " + " AND ".join(clauses)) if clauses else ""
    return where, params


def fetch_all(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()


def main():
    args = parse_args()
    where, params = build_where(args)

    signals_query = f"""
    SELECT
        s.ts,
        s.instrument,
        s.signal,
        s.strike,
        s.underlying_price,
        s.option_entry_ltp
    FROM signals_issued s
    WHERE s.strike IS NOT NULL
      AND s.option_entry_ltp IS NOT NULL
      {where}
    ORDER BY s.ts;
    """

    inserted = 0
    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            signals = fetch_all(cur, signals_query, params)
            for signal_ts, instrument, signal, strike, underlying_entry_price, option_entry_ltp in signals:
                observed_rows = fetch_all(
                    cur,
                    """
                    WITH monitor_times AS (
                        SELECT DISTINCT ts AS observed_ts
                        FROM trade_monitor_events_1m
                        WHERE instrument = %s
                          AND signal = %s
                          AND entry_ts = %s
                    ),
                    fallback_times AS (
                        SELECT DISTINCT date_trunc('minute', ob.ts) AS observed_ts
                        FROM option_band_snapshots_1m ob
                        WHERE ob.instrument = %s
                          AND ob.strike = %s
                          AND ob.option_type = %s
                          AND ob.ts >= %s
                          AND ob.ts <= %s + (%s || ' minutes')::interval
                    ),
                    all_times AS (
                        SELECT observed_ts FROM monitor_times
                        UNION
                        SELECT observed_ts FROM fallback_times
                    )
                    SELECT
                        at.observed_ts,
                        contract.ltp,
                        contract.top_bid_price,
                        contract.top_ask_price,
                        contract.spread,
                        underlying.close
                    FROM all_times at
                    LEFT JOIN LATERAL (
                        SELECT ltp, top_bid_price, top_ask_price, spread
                        FROM option_band_snapshots_1m ob
                        WHERE ob.instrument = %s
                          AND ob.strike = %s
                          AND ob.option_type = %s
                          AND ob.ts <= at.observed_ts
                        ORDER BY ob.ts DESC
                        LIMIT 1
                    ) contract ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT close
                        FROM candles_1m c
                        WHERE c.instrument = %s
                          AND c.ts <= at.observed_ts
                        ORDER BY c.ts DESC
                        LIMIT 1
                    ) underlying ON TRUE
                    ORDER BY at.observed_ts;
                    """,
                    (
                        instrument,
                        signal,
                        signal_ts,
                        instrument,
                        strike,
                        signal,
                        signal_ts,
                        signal_ts,
                        MAX_MINUTES,
                        instrument,
                        strike,
                        signal,
                        instrument,
                    ),
                )

                max_fav = option_entry_ltp
                max_adv = option_entry_ltp
                for observed_ts, option_ltp, option_bid, option_ask, option_spread, underlying_price in observed_rows:
                    if option_ltp is None:
                        continue
                    max_fav = max(max_fav, float(option_ltp))
                    max_adv = min(max_adv, float(option_ltp))
                    pnl_points = float(option_ltp) - float(option_entry_ltp)
                    minutes_since_signal = max(
                        0,
                        int((observed_ts - signal_ts).total_seconds() // 60),
                    )
                    cur.execute(
                        """
                        INSERT INTO option_signal_outcomes_1m
                        (
                            signal_ts, observed_ts, instrument, signal, strike,
                            underlying_entry_price, underlying_price, option_entry_ltp, option_ltp,
                            option_bid, option_ask, option_spread, pnl_points, max_favorable_ltp,
                            max_adverse_ltp, minutes_since_signal, guidance, reason
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (signal_ts, observed_ts, instrument, signal, strike) DO UPDATE
                        SET underlying_entry_price = EXCLUDED.underlying_entry_price,
                            underlying_price = EXCLUDED.underlying_price,
                            option_entry_ltp = EXCLUDED.option_entry_ltp,
                            option_ltp = EXCLUDED.option_ltp,
                            option_bid = EXCLUDED.option_bid,
                            option_ask = EXCLUDED.option_ask,
                            option_spread = EXCLUDED.option_spread,
                            pnl_points = EXCLUDED.pnl_points,
                            max_favorable_ltp = EXCLUDED.max_favorable_ltp,
                            max_adverse_ltp = EXCLUDED.max_adverse_ltp,
                            minutes_since_signal = EXCLUDED.minutes_since_signal;
                        """,
                        (
                            signal_ts,
                            observed_ts,
                            instrument,
                            signal,
                            strike,
                            underlying_entry_price,
                            underlying_price,
                            option_entry_ltp,
                            option_ltp,
                            option_bid,
                            option_ask,
                            option_spread,
                            pnl_points,
                            max_fav,
                            max_adv,
                            minutes_since_signal,
                            None,
                            "BACKFILLED_FROM_OPTION_SNAPSHOTS",
                        ),
                    )
                    inserted += 1
        conn.commit()

    print(f"outcome_rows_upserted={inserted}")


if __name__ == "__main__":
    main()
