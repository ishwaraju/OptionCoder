#!/usr/bin/env python3
"""Score WATCH/ACTION alerts for a trading day and persist review rows."""

import argparse
import json
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config
from shared.db.writer import DBWriter


LOOKAHEAD_MINUTES = 30


def parse_args():
    parser = argparse.ArgumentParser(description="Score alert outcomes for a trading day")
    parser.add_argument("--date", required=True, help="Trading day in YYYY-MM-DD (Asia/Kolkata)")
    return parser.parse_args()


def fetch_all(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()


def direction_threshold(instrument):
    if instrument == "BANKNIFTY":
        return 55.0, 35.0
    if instrument == "SENSEX":
        return 60.0, 38.0
    return 28.0, 18.0


def classify_watch(instrument, direction, max_fav, max_adv, close_move, converted_to_action):
    good_move, bad_move = direction_threshold(instrument)
    if converted_to_action and max_fav >= good_move:
        return "WATCH useful", "converted_to_action"
    if max_fav >= good_move and max_adv <= bad_move:
        return "WATCH useful", "setup_worked"
    if max_adv >= good_move and max_fav < bad_move:
        return "WATCH useful", "saved_from_bad_trade"
    if max_fav < bad_move and max_adv < bad_move:
        return "WATCH noisy", "low_follow_through"
    return "WATCH noisy", "mixed_follow_through"


def classify_action(instrument, direction, max_fav, max_adv, close_move):
    good_move, bad_move = direction_threshold(instrument)
    if close_move >= good_move * 0.5 and max_adv <= bad_move:
        return "ACTION good", "follow_through"
    if max_fav >= good_move and close_move < good_move * 0.25:
        return "ACTION late", "gave_back_move"
    if max_adv >= bad_move and max_fav < good_move * 0.5:
        return "ACTION weak", "failed_early"
    return "ACTION mixed", "unclear"


def compute_future_move(cur, instrument, ts_ist, direction):
    rows = fetch_all(
        cur,
        """
        SELECT high, low, close
        FROM candles_5m
        WHERE instrument = %s
          AND ts AT TIME ZONE 'Asia/Kolkata' > %s
          AND ts AT TIME ZONE 'Asia/Kolkata' <= %s + (%s || ' minutes')::interval
        ORDER BY ts ASC;
        """,
        (instrument, ts_ist, ts_ist, LOOKAHEAD_MINUTES),
    )
    if not rows:
        return 0.0, 0.0, 0.0

    highs = [float(row[0]) for row in rows if row[0] is not None]
    lows = [float(row[1]) for row in rows if row[1] is not None]
    last_close = float(rows[-1][2]) if rows[-1][2] is not None else None
    return highs, lows, last_close


def score_day(day):
    writer = DBWriter()
    review_rows = []
    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            watch_rows = fetch_all(
                cur,
                """
                SELECT
                    instrument,
                    ts AT TIME ZONE 'Asia/Kolkata',
                    price,
                    COALESCE(signal, CASE WHEN base_bias = 'BULLISH' THEN 'CE' WHEN base_bias = 'BEARISH' THEN 'PE' END),
                    setup_type,
                    watch_bucket,
                    blockers_json::text,
                    cautions_json::text
                FROM strategy_decisions_5m
                WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = %s
                  AND tradability = 'WATCH'
                  AND COALESCE(watch_bucket, 'NONE') <> 'WATCH_CONTEXT'
                  AND COALESCE(setup_type, 'NONE') <> 'NONE'
                ORDER BY instrument, ts;
                """,
                (day,),
            )

            action_rows = fetch_all(
                cur,
                """
                SELECT
                    instrument,
                    ts AT TIME ZONE 'Asia/Kolkata',
                    price,
                    signal,
                    setup_type,
                    reason
                FROM signals_issued
                WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = %s
                ORDER BY instrument, ts;
                """,
                (day,),
            )

            for instrument, ts_ist, price, direction, setup_type, watch_bucket, blockers_json, cautions_json in watch_rows:
                if direction not in {"CE", "PE"} or price is None:
                    continue
                highs, lows, last_close = compute_future_move(cur, instrument, ts_ist, direction)
                if not highs or not lows or last_close is None:
                    continue
                entry_price = float(price)
                max_fav = (max(highs) - entry_price) if direction == "CE" else (entry_price - min(lows))
                max_adv = (entry_price - min(lows)) if direction == "CE" else (max(highs) - entry_price)
                close_move = (last_close - entry_price) if direction == "CE" else (entry_price - last_close)
                converted = fetch_all(
                    cur,
                    """
                    SELECT 1
                    FROM signals_issued
                    WHERE instrument = %s
                      AND signal = %s
                      AND ts AT TIME ZONE 'Asia/Kolkata' > %s
                      AND ts AT TIME ZONE 'Asia/Kolkata' <= %s + (%s || ' minutes')::interval
                    LIMIT 1;
                    """,
                    (instrument, direction, ts_ist, ts_ist, LOOKAHEAD_MINUTES),
                )
                usefulness, outcome_tag = classify_watch(instrument, direction, max_fav, max_adv, close_move, bool(converted))
                review_rows.append(
                    (
                        ts_ist,
                        instrument,
                        "WATCH",
                        direction,
                        setup_type,
                        watch_bucket,
                        usefulness,
                        outcome_tag,
                        LOOKAHEAD_MINUTES,
                        round(max_fav, 2),
                        round(max_adv, 2),
                        round(close_move, 2),
                        json.loads(blockers_json) if blockers_json else [],
                        json.loads(cautions_json) if cautions_json else [],
                        None,
                    )
                )

            for instrument, ts_ist, price, direction, setup_type, reason in action_rows:
                if direction not in {"CE", "PE"} or price is None:
                    continue
                highs, lows, last_close = compute_future_move(cur, instrument, ts_ist, direction)
                if not highs or not lows or last_close is None:
                    continue
                entry_price = float(price)
                max_fav = (max(highs) - entry_price) if direction == "CE" else (entry_price - min(lows))
                max_adv = (entry_price - min(lows)) if direction == "CE" else (max(highs) - entry_price)
                close_move = (last_close - entry_price) if direction == "CE" else (entry_price - last_close)
                usefulness, outcome_tag = classify_action(instrument, direction, max_fav, max_adv, close_move)
                review_rows.append(
                    (
                        ts_ist,
                        instrument,
                        "ACTION",
                        direction,
                        setup_type,
                        None,
                        usefulness,
                        outcome_tag,
                        LOOKAHEAD_MINUTES,
                        round(max_fav, 2),
                        round(max_adv, 2),
                        round(close_move, 2),
                        [],
                        [],
                        reason.split("|")[0].strip() if reason else None,
                    )
                )

    for row in review_rows:
        writer.insert_alert_review_5m(row)
    print(f"Scored {len(review_rows)} alerts for {day}.")


def main():
    args = parse_args()
    score_day(args.date)


if __name__ == "__main__":
    main()
