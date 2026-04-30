#!/usr/bin/env python3
"""Reconstruct option_signal_candidates_5m from historical option snapshots."""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill reconstructed option candidates")
    parser.add_argument("--date-from", help="Start date YYYY-MM-DD")
    parser.add_argument("--date-to", help="End date YYYY-MM-DD")
    return parser.parse_args()


def build_where(args):
    clauses = []
    params = []
    if args.date_from:
        clauses.append("DATE(sd.ts AT TIME ZONE 'Asia/Kolkata') >= %s")
        params.append(args.date_from)
    if args.date_to:
        clauses.append("DATE(sd.ts AT TIME ZONE 'Asia/Kolkata') <= %s")
        params.append(args.date_to)
    where = (" AND " + " AND ".join(clauses)) if clauses else ""
    return where, params


def fetch_all(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()


def spread_percent(ltp, spread):
    if ltp in (None, 0) or spread is None:
        return None
    return round((float(spread) / float(ltp)) * 100, 4)


def preferred_strike(atm, strike_step, direction, strategy_score, time_ts):
    if atm is None:
        return None
    late_or_weak = time_ts.hour >= 13 or (strategy_score or 0) < 60
    if late_or_weak:
        return atm - (2 * strike_step) if direction == "CE" else atm + (2 * strike_step)
    if (strategy_score or 0) < 75:
        return atm - strike_step if direction == "CE" else atm + strike_step
    return atm


def score_candidate(row, preferred, target_move, score):
    strike_gap = abs((preferred or row["strike"]) - row["strike"]) or 50
    ltp = float(row["ltp"] or 0)
    spread = float(row["spread"] or 0)
    spread_pct = spread_percent(ltp, spread) or 999.0
    bid_qty = int(row["top_bid_quantity"] or 0)
    ask_qty = int(row["top_ask_quantity"] or 0)
    volume = int(row["volume"] or 0)
    oi = int(row["oi"] or 0)
    delta_abs = abs(float(row["delta"] or 0))
    theta_abs = abs(float(row["theta"] or 0))
    distance = abs(int(row["strike"] or 0) - int(preferred or row["strike"] or 0))

    target_delta = 0.5 if (score or 0) >= 75 else 0.62
    spread_score = max(0.0, 30.0 - min(spread_pct, 10.0) * 4.0)
    depth_score = min(15.0, min(bid_qty, ask_qty) / 20.0)
    volume_score = min(18.0, volume / 300.0)
    oi_score = min(10.0, oi / 20000.0)
    delta_score = max(0.0, 15.0 * (1.0 - min(abs(delta_abs - target_delta) / 0.45, 1.0)))
    proximity_score = max(0.0, 12.0 - (distance / max(strike_gap, 1)) * 4.0)
    expected_move = delta_abs * float(target_move)
    theta_penalty = theta_abs * 0.25
    expected_edge = round(expected_move - spread - theta_penalty, 2)
    edge_score = max(0.0, min(20.0, expected_edge))
    candidate_score = round(
        spread_score + depth_score + volume_score + oi_score + delta_score + proximity_score + edge_score,
        2,
    )
    reason = (
        f"backfilled | spread={spread:.2f} ({spread_pct:.2f}%) | delta={delta_abs:.2f} | "
        f"vol={volume} | oi={oi} | edge={expected_edge:.2f}"
    )
    return candidate_score, expected_edge, spread_pct, reason


def main():
    args = parse_args()
    where, params = build_where(args)

    query = f"""
    SELECT
        sd.ts,
        sd.instrument,
        sd.price,
        sd.base_bias,
        sd.setup_type,
        sd.signal,
        sig.strike,
        sd.strategy_score,
        sd.first_target_price,
        obs.atm_strike,
        obs.strike,
        obs.distance_from_atm,
        obs.option_type,
        obs.oi,
        obs.volume,
        obs.ltp,
        obs.iv,
        obs.top_bid_price,
        obs.top_bid_quantity,
        obs.top_ask_price,
        obs.top_ask_quantity,
        obs.spread,
        obs.delta,
        obs.theta
    FROM strategy_decisions_5m sd
    LEFT JOIN signals_issued sig
      ON sig.ts = sd.ts
     AND sig.instrument = sd.instrument
     AND sig.signal = sd.signal
    JOIN LATERAL (
        SELECT *
        FROM option_band_snapshots_1m ob
        WHERE ob.instrument = sd.instrument
          AND ob.ts = (
              SELECT MAX(ts)
              FROM option_band_snapshots_1m x
              WHERE x.instrument = sd.instrument
                AND x.ts <= sd.ts + interval '5 minute'
          )
          AND abs(ob.distance_from_atm) <= 3
        ORDER BY ob.strike, ob.option_type
    ) obs ON TRUE
    WHERE (sd.signal IN ('CE','PE') OR sd.candidate_signal_type IS NOT NULL)
      {where}
    ORDER BY sd.ts, sd.instrument, obs.option_type, obs.strike;
    """

    upserted = 0
    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            rows = fetch_all(cur, query, params)
            grouped = {}
            for row in rows:
                key = row[0], row[1]
                grouped.setdefault(key, []).append(row)

            for (ts, instrument), group_rows in grouped.items():
                base_price = float(group_rows[0][2]) if group_rows[0][2] is not None else None
                base_bias = group_rows[0][3]
                setup_type = group_rows[0][4]
                chosen_signal = group_rows[0][5]
                chosen_strike = group_rows[0][6]
                strategy_score = int(group_rows[0][7] or 0)
                first_target_price = group_rows[0][8]
                target_move = abs(float(first_target_price) - float(base_price)) if first_target_price is not None and base_price is not None else 50.0
                strike_step = Config.STRIKE_STEP.get(instrument, 50)

                per_direction = {"CE": [], "PE": []}
                for row in group_rows:
                    direction = row[12]
                    preferred = preferred_strike(row[9], strike_step, direction, strategy_score, ts)
                    candidate_score, expected_edge, spread_pct_value, reason = score_candidate(
                        {
                            "strike": row[10],
                            "distance_from_atm": row[11],
                            "option_type": row[12],
                            "oi": row[13],
                            "volume": row[14],
                            "ltp": row[15],
                            "iv": row[16],
                            "top_bid_price": row[17],
                            "top_bid_quantity": row[18],
                            "top_ask_price": row[19],
                            "top_ask_quantity": row[20],
                            "spread": row[21],
                            "delta": row[22],
                            "theta": row[23],
                        },
                        preferred,
                        target_move,
                        strategy_score,
                    )
                    per_direction[direction].append(
                        (
                            ts,
                            instrument,
                            base_price,
                            base_bias,
                            setup_type,
                            direction,
                            row[10],
                            row[9],
                            row[11],
                            row[15],
                            row[17],
                            row[19],
                            row[21],
                            spread_pct_value,
                            row[16],
                            row[22],
                            row[23],
                            row[13],
                            row[14],
                            candidate_score,
                            expected_edge,
                            reason,
                            chosen_signal,
                            chosen_strike,
                        )
                    )

                for direction, items in per_direction.items():
                    items.sort(key=lambda item: (item[19], item[20]), reverse=True)
                    chosen_item = None
                    if chosen_signal == direction and chosen_strike is not None:
                        chosen_item = next((item for item in items if item[6] == chosen_strike), None)

                    selected_items = items[:3]
                    if chosen_item and all(item[6] != chosen_strike for item in selected_items):
                        selected_items = [chosen_item] + selected_items[:2]

                    # Re-rank selected set by score so comparisons remain readable.
                    selected_items = sorted(selected_items, key=lambda item: (item[19], item[20]), reverse=True)

                    for rank, item in enumerate(selected_items, start=1):
                        selected_for_signal = bool(chosen_signal == direction and item[6] == item[23])
                        cur.execute(
                            """
                            INSERT INTO option_signal_candidates_5m
                            (
                                ts, instrument, underlying_price, underlying_bias, setup_type, candidate_direction,
                                strike, atm_strike, distance_from_atm, option_ltp, bid_price, ask_price, spread,
                                spread_percent, iv, delta, theta, oi, volume, candidate_score, candidate_rank,
                                expected_edge, selected_for_signal, reason
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ts, instrument, candidate_direction, strike) DO UPDATE
                            SET underlying_price = EXCLUDED.underlying_price,
                                underlying_bias = EXCLUDED.underlying_bias,
                                setup_type = EXCLUDED.setup_type,
                                atm_strike = EXCLUDED.atm_strike,
                                distance_from_atm = EXCLUDED.distance_from_atm,
                                option_ltp = EXCLUDED.option_ltp,
                                bid_price = EXCLUDED.bid_price,
                                ask_price = EXCLUDED.ask_price,
                                spread = EXCLUDED.spread,
                                spread_percent = EXCLUDED.spread_percent,
                                iv = EXCLUDED.iv,
                                delta = EXCLUDED.delta,
                                theta = EXCLUDED.theta,
                                oi = EXCLUDED.oi,
                                volume = EXCLUDED.volume,
                                candidate_score = EXCLUDED.candidate_score,
                                candidate_rank = EXCLUDED.candidate_rank,
                                expected_edge = EXCLUDED.expected_edge,
                                selected_for_signal = EXCLUDED.selected_for_signal,
                                reason = EXCLUDED.reason;
                            """,
                            (
                                item[0], item[1], item[2], item[3], item[4], item[5],
                                item[6], item[7], item[8], item[9], item[10], item[11], item[12],
                                item[13], item[14], item[15], item[16], item[17], item[18], item[19],
                                rank, item[20], selected_for_signal, item[21],
                            ),
                        )
                        upserted += 1
        conn.commit()

    print(f"candidate_rows_upserted={upserted}")


if __name__ == "__main__":
    main()
