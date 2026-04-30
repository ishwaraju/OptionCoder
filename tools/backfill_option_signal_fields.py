#!/usr/bin/env python3
"""Backfill option-aware fields in signals_issued from option_band_snapshots_1m."""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill option fields in signals_issued")
    parser.add_argument("--date-from", help="Start date YYYY-MM-DD")
    parser.add_argument("--date-to", help="End date YYYY-MM-DD")
    return parser.parse_args()


def fetch_all(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()


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


def main():
    args = parse_args()
    where, params = build_where(args)

    select_query = f"""
    SELECT
        s.ts,
        s.instrument,
        s.signal,
        s.strike,
        s.price,
        s.underlying_price,
        s.option_entry_ltp,
        s.reason,
        obs.atm_strike,
        obs.distance_from_atm,
        obs.ltp,
        obs.top_bid_price,
        obs.top_ask_price,
        obs.spread,
        obs.iv,
        obs.delta
    FROM signals_issued s
    LEFT JOIN LATERAL (
        SELECT atm_strike, distance_from_atm, ltp, top_bid_price, top_ask_price, spread, iv, delta
        FROM option_band_snapshots_1m ob
        WHERE ob.instrument = s.instrument
          AND ob.strike = s.strike
          AND ob.option_type = s.signal
          AND ob.ts <= s.ts
        ORDER BY ob.ts DESC
        LIMIT 1
    ) obs ON TRUE
    WHERE s.strike IS NOT NULL
    {where}
    ORDER BY s.ts;
    """

    updated = 0
    no_snapshot = 0
    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            rows = fetch_all(cur, select_query, params)
            for row in rows:
                (
                    ts,
                    instrument,
                    signal,
                    strike,
                    old_price,
                    existing_underlying,
                    existing_option_ltp,
                    reason,
                    atm_strike,
                    distance_from_atm,
                    option_ltp,
                    bid,
                    ask,
                    spread,
                    iv,
                    delta,
                ) = row

                if option_ltp is None:
                    no_snapshot += 1
                    continue

                # Old rows stored spot in price. New rows already store option price and underlying separately.
                if existing_underlying is None:
                    underlying_price = old_price
                    stored_price = option_ltp
                else:
                    underlying_price = existing_underlying
                    stored_price = existing_option_ltp if existing_option_ltp is not None else old_price

                update_query = """
                UPDATE signals_issued
                SET price = %s,
                    underlying_price = %s,
                    atm_strike = %s,
                    distance_from_atm = %s,
                    option_entry_ltp = %s,
                    entry_bid = %s,
                    entry_ask = %s,
                    entry_spread = %s,
                    entry_iv = %s,
                    entry_delta = %s,
                    option_data_source = COALESCE(option_data_source, 'BACKFILLED_OPTION_SNAPSHOT')
                WHERE ts = %s
                  AND instrument = %s
                  AND signal = %s
                  AND strike = %s;
                """
                cur.execute(
                    update_query,
                    (
                        stored_price,
                        underlying_price,
                        atm_strike,
                        distance_from_atm,
                        option_ltp,
                        bid,
                        ask,
                        spread,
                        iv,
                        delta,
                        ts,
                        instrument,
                        signal,
                        strike,
                    ),
                )
                updated += 1
        conn.commit()

    print(f"signals_scanned={updated + no_snapshot}")
    print(f"signals_updated={updated}")
    print(f"signals_without_option_snapshot={no_snapshot}")


if __name__ == "__main__":
    main()
