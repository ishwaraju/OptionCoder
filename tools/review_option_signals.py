#!/usr/bin/env python3
"""Review option-signal quality, selected strike quality, and better alternatives."""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config


def parse_args():
    parser = argparse.ArgumentParser(description="Review option signal quality for a trading day")
    parser.add_argument(
        "--date",
        required=True,
        help="Trading day in YYYY-MM-DD (Asia/Kolkata)",
    )
    return parser.parse_args()


def fetch_all(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()


def print_signal_review(day, rows):
    print("=" * 88)
    print(f"OPTION SIGNAL REVIEW | {day}")
    print("=" * 88)

    if not rows:
        print("No fired option signals found for this day.")
        return

    for row in rows:
        (
            signal_ts,
            instrument,
            signal,
            strike,
            buy_price,
            underlying_price,
            score,
            setup_type,
            strike_reason,
            chosen_candidate_score,
            chosen_expected_edge,
            chosen_rank,
            chosen_outcome_pnl,
            chosen_max_fav,
            top_strike,
            top_buy_price,
            top_candidate_score,
            top_expected_edge,
            top_outcome_pnl,
            top_max_fav,
        ) = row

        time_label = signal_ts.strftime("%H:%M")
        print(f"\n[{instrument}] {time_label} | {signal} | setup={setup_type} | score={score}")
        print(
            "chosen: strike={strike} | buy={buy_price} | cand_score={cand_score} | "
            "edge={edge} | rank={rank} | final_pnl={final_pnl} | best_seen={best_seen}".format(
                strike=strike,
                buy_price=_fmt(buy_price),
                cand_score=_fmt(chosen_candidate_score),
                edge=_fmt(chosen_expected_edge),
                rank=chosen_rank if chosen_rank is not None else "-",
                final_pnl=_fmt(chosen_outcome_pnl),
                best_seen=_fmt(_spread(chosen_max_fav, buy_price)),
            )
        )
        print(
            "top_alt: strike={strike} | buy={buy_price} | cand_score={cand_score} | "
            "edge={edge} | final_pnl={final_pnl} | best_seen={best_seen}".format(
                strike=top_strike if top_strike is not None else "-",
                buy_price=_fmt(top_buy_price),
                cand_score=_fmt(top_candidate_score),
                edge=_fmt(top_expected_edge),
                final_pnl=_fmt(top_outcome_pnl),
                best_seen=_fmt(_spread(top_max_fav, top_buy_price)),
            )
        )

        if top_strike is None:
            print("verdict: no alternative candidate snapshot found")
        elif top_strike == strike:
            print("verdict: bot chose the top-ranked candidate")
        else:
            edge_gap = _spread(top_expected_edge, chosen_expected_edge)
            score_gap = _spread(top_candidate_score, chosen_candidate_score)
            print(
                f"verdict: better alternative existed | top_strike={top_strike} | "
                f"score_gap={_fmt(score_gap)} | edge_gap={_fmt(edge_gap)}"
            )

        if strike_reason:
            print(f"why_chosen: {strike_reason}")
        print(f"spot_context: spot={_fmt(underlying_price)}")


def _fmt(value):
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _spread(a, b):
    if a is None or b is None:
        return None
    return float(a) - float(b)


def main():
    args = parse_args()

    query = """
    WITH signals AS (
        SELECT
            ts,
            instrument,
            signal,
            strike,
            price,
            underlying_price,
            strategy_score,
            setup_type,
            strike_reason
        FROM signals_issued
        WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = %s
    ),
    candidate_snapshot AS (
        SELECT
            s.ts AS signal_ts,
            s.instrument,
            s.signal,
            (
                SELECT MAX(c.ts)
                FROM option_signal_candidates_5m c
                WHERE c.instrument = s.instrument
                  AND c.candidate_direction = s.signal
                  AND c.ts <= s.ts
                  AND c.ts >= s.ts - interval '10 minute'
            ) AS candidate_ts
        FROM signals s
    ),
    chosen_candidate AS (
        SELECT
            s.ts,
            s.instrument,
            s.signal,
            s.strike,
            c.candidate_score,
            c.expected_edge,
            c.candidate_rank
        FROM signals s
        LEFT JOIN candidate_snapshot cs
          ON cs.signal_ts = s.ts
         AND cs.instrument = s.instrument
         AND cs.signal = s.signal
        LEFT JOIN option_signal_candidates_5m c
          ON c.ts = cs.candidate_ts
         AND c.instrument = s.instrument
         AND c.candidate_direction = s.signal
         AND c.strike = s.strike
    ),
    top_candidate AS (
        SELECT *
        FROM (
            SELECT
                cs.signal_ts AS ts,
                c.instrument,
                c.candidate_direction,
                c.strike,
                c.option_ltp,
                c.candidate_score,
                c.expected_edge,
                ROW_NUMBER() OVER (
                    PARTITION BY cs.signal_ts, c.instrument, c.candidate_direction
                    ORDER BY c.candidate_score DESC, c.expected_edge DESC, c.candidate_rank ASC
                ) AS rn
            FROM option_signal_candidates_5m c
            JOIN candidate_snapshot cs
              ON cs.candidate_ts = c.ts
             AND cs.instrument = c.instrument
             AND cs.signal = c.candidate_direction
        ) ranked
        WHERE rn = 1
    ),
    chosen_outcome AS (
        SELECT *
        FROM (
            SELECT
                o.signal_ts,
                o.instrument,
                o.signal,
                o.strike,
                o.pnl_points,
                o.max_favorable_ltp,
                ROW_NUMBER() OVER (
                    PARTITION BY o.signal_ts, o.instrument, o.signal, o.strike
                    ORDER BY o.observed_ts DESC
                ) AS rn
            FROM option_signal_outcomes_1m o
            WHERE DATE(o.signal_ts AT TIME ZONE 'Asia/Kolkata') = %s
        ) ranked
        WHERE rn = 1
    ),
    top_outcome AS (
        SELECT *
        FROM (
            SELECT
                tc.ts,
                tc.instrument,
                tc.candidate_direction,
                tc.strike,
                o.pnl_points,
                o.max_favorable_ltp,
                ROW_NUMBER() OVER (
                    PARTITION BY tc.ts, tc.instrument, tc.candidate_direction, tc.strike
                    ORDER BY o.observed_ts DESC
                ) AS rn
            FROM top_candidate tc
            LEFT JOIN option_signal_outcomes_1m o
              ON o.signal_ts = tc.ts
             AND o.instrument = tc.instrument
             AND o.signal = tc.candidate_direction
             AND o.strike = tc.strike
        ) ranked
        WHERE rn = 1
    )
    SELECT
        s.ts AT TIME ZONE 'Asia/Kolkata',
        s.instrument,
        s.signal,
        s.strike,
        s.price,
        s.underlying_price,
        s.strategy_score,
        s.setup_type,
        s.strike_reason,
        cc.candidate_score,
        cc.expected_edge,
        cc.candidate_rank,
        co.pnl_points,
        co.max_favorable_ltp,
        tc.strike,
        tc.option_ltp,
        tc.candidate_score,
        tc.expected_edge,
        to2.pnl_points,
        to2.max_favorable_ltp
    FROM signals s
    LEFT JOIN chosen_candidate cc
      ON cc.ts = s.ts
     AND cc.instrument = s.instrument
     AND cc.signal = s.signal
     AND cc.strike = s.strike
    LEFT JOIN chosen_outcome co
      ON co.signal_ts = s.ts
     AND co.instrument = s.instrument
     AND co.signal = s.signal
     AND co.strike = s.strike
    LEFT JOIN top_candidate tc
      ON tc.ts = s.ts
     AND tc.instrument = s.instrument
     AND tc.candidate_direction = s.signal
    LEFT JOIN top_outcome to2
      ON to2.ts = tc.ts
     AND to2.instrument = tc.instrument
     AND to2.candidate_direction = tc.candidate_direction
     AND to2.strike = tc.strike
    ORDER BY s.ts;
    """

    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            rows = fetch_all(cur, query, (args.date, args.date))
    print_signal_review(args.date, rows)


if __name__ == "__main__":
    main()
