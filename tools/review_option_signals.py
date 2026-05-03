#!/usr/bin/env python3
"""Review option-signal quality, selected strike quality, and better alternatives."""

import argparse
import sys
from pathlib import Path
from collections import defaultdict

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
            chosen_max_adv,
            chosen_minutes,
            top_strike,
            top_buy_price,
            top_candidate_score,
            top_expected_edge,
            top_outcome_pnl,
            top_max_fav,
            top_max_adv,
            top_minutes,
        ) = row

        time_label = signal_ts.strftime("%H:%M")
        print(f"\n[{instrument}] {time_label} | {signal} | setup={setup_type} | score={score}")
        print(
            "chosen: strike={strike} | buy={buy_price} | cand_score={cand_score} | "
            "edge={edge} | rank={rank} | final_pnl={final_pnl} | best_seen={best_seen} | worst_seen={worst_seen} | mins={mins}".format(
                strike=strike,
                buy_price=_fmt(buy_price),
                cand_score=_fmt(chosen_candidate_score),
                edge=_fmt(chosen_expected_edge),
                rank=chosen_rank if chosen_rank is not None else "-",
                final_pnl=_fmt(chosen_outcome_pnl),
                best_seen=_fmt(_spread(chosen_max_fav, buy_price)),
                worst_seen=_fmt(_spread(chosen_max_adv, buy_price)),
                mins=chosen_minutes if chosen_minutes is not None else "-",
            )
        )
        print(
            "top_alt: strike={strike} | buy={buy_price} | cand_score={cand_score} | "
            "edge={edge} | final_pnl={final_pnl} | best_seen={best_seen} | worst_seen={worst_seen} | mins={mins}".format(
                strike=top_strike if top_strike is not None else "-",
                buy_price=_fmt(top_buy_price),
                cand_score=_fmt(top_candidate_score),
                edge=_fmt(top_expected_edge),
                final_pnl=_fmt(top_outcome_pnl),
                best_seen=_fmt(_spread(top_max_fav, top_buy_price)),
                worst_seen=_fmt(_spread(top_max_adv, top_buy_price)),
                mins=top_minutes if top_minutes is not None else "-",
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


def _time_bucket(signal_ts):
    if signal_ts is None:
        return "UNKNOWN"
    hhmm = signal_ts.strftime("%H:%M")
    if hhmm < "09:40":
        return "OPENING"
    if hhmm < "11:30":
        return "MID_MORNING"
    if hhmm < "13:30":
        return "MIDDAY"
    if hhmm < "14:45":
        return "LATE_DAY"
    return "ENDGAME"


def _safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_daily_summary(rows):
    summary = {
        "total_signals": len(rows),
        "chosen_top_rank_count": 0,
        "better_alternative_count": 0,
        "positive_pnl_count": 0,
        "sum_pnl": 0.0,
        "pnl_count": 0,
        "weak_premium_response_count": 0,
        "time_buckets": defaultdict(lambda: {"signals": 0, "wins": 0, "sum_pnl": 0.0, "pnl_count": 0}),
        "setups": defaultdict(lambda: {"signals": 0, "wins": 0, "sum_pnl": 0.0, "pnl_count": 0}),
    }

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
            chosen_max_adv,
            chosen_minutes,
            top_strike,
            top_buy_price,
            top_candidate_score,
            top_expected_edge,
            top_outcome_pnl,
            top_max_fav,
            top_max_adv,
            top_minutes,
        ) = row

        pnl = _safe_float(chosen_outcome_pnl)
        top_pnl = _safe_float(top_outcome_pnl)
        buy_price_value = _safe_float(buy_price)
        max_fav_value = _safe_float(chosen_max_fav)
        bucket = _time_bucket(signal_ts)
        setup_key = setup_type or "UNKNOWN"

        bucket_stats = summary["time_buckets"][bucket]
        setup_stats = summary["setups"][setup_key]
        for stats in (bucket_stats, setup_stats):
            stats["signals"] += 1
            if pnl is not None:
                stats["sum_pnl"] += pnl
                stats["pnl_count"] += 1
                if pnl > 0:
                    stats["wins"] += 1

        if pnl is not None:
            summary["sum_pnl"] += pnl
            summary["pnl_count"] += 1
            if pnl > 0:
                summary["positive_pnl_count"] += 1
        if buy_price_value and max_fav_value and (max_fav_value - buy_price_value) <= max(buy_price_value * 0.05, 2.0):
            summary["weak_premium_response_count"] += 1

        if top_strike is not None and top_strike == strike:
            summary["chosen_top_rank_count"] += 1
        elif top_strike is not None:
            summary["better_alternative_count"] += 1
            if top_pnl is not None and pnl is not None and top_pnl > pnl:
                summary["better_alternative_count"] += 0

    return summary


def print_daily_summary(day, rows):
    summary = build_daily_summary(rows)
    print()
    print("DAILY SUMMARY")
    print("-" * 88)
    print(
        "signals={signals} | wins={wins} | avg_pnl={avg_pnl} | chosen_top_rank={top_rank}/{signals} | better_alt={better_alt} | weak_premium={weak_premium}".format(
            signals=summary["total_signals"],
            wins=summary["positive_pnl_count"],
            avg_pnl=_fmt(summary["sum_pnl"] / summary["pnl_count"]) if summary["pnl_count"] else "-",
            top_rank=summary["chosen_top_rank_count"],
            better_alt=summary["better_alternative_count"],
            weak_premium=summary["weak_premium_response_count"],
        )
    )

    if summary["time_buckets"]:
        print("time_buckets:")
        for bucket, stats in sorted(summary["time_buckets"].items()):
            avg_pnl = _fmt(stats["sum_pnl"] / stats["pnl_count"]) if stats["pnl_count"] else "-"
            print(f"  {bucket}: signals={stats['signals']} wins={stats['wins']} avg_pnl={avg_pnl}")

    if summary["setups"]:
        print("setups:")
        ranked = sorted(
            summary["setups"].items(),
            key=lambda item: (-(item[1]["sum_pnl"] / item[1]["pnl_count"]) if item[1]["pnl_count"] else float("inf"), -item[1]["signals"], item[0]),
        )
        for setup, stats in ranked[:6]:
            avg_pnl = _fmt(stats["sum_pnl"] / stats["pnl_count"]) if stats["pnl_count"] else "-"
            print(f"  {setup}: signals={stats['signals']} wins={stats['wins']} avg_pnl={avg_pnl}")
    recommendations = build_recommendations(summary)
    if recommendations:
        print("recommendations:")
        for item in recommendations:
            print(f"  - {item}")


def build_recommendations(summary):
    recommendations = []
    total = int(summary.get("total_signals") or 0)
    better_alt = int(summary.get("better_alternative_count") or 0)
    pnl_count = int(summary.get("pnl_count") or 0)
    win_count = int(summary.get("positive_pnl_count") or 0)

    if total >= 3 and better_alt / max(total, 1) >= 0.35:
        recommendations.append(
            "strike selector ko tighten karo; bot ka chosen strike kaafi baar top-ranked candidate nahi tha."
        )

    if pnl_count >= 3 and win_count / max(pnl_count, 1) < 0.4:
        recommendations.append(
            "overall signal quality weak hai; breakout/continuation score floors 2-3 points badhao."
        )
    if total >= 3 and int(summary.get("weak_premium_response_count") or 0) / max(total, 1) >= 0.4:
        recommendations.append(
            "signals aa rahe hain par premium response weak hai; spread/IV/premium expansion checks ko aur tighten karo."
        )

    for bucket, stats in sorted(summary.get("time_buckets", {}).items()):
        signals = int(stats.get("signals") or 0)
        pnl_samples = int(stats.get("pnl_count") or 0)
        wins = int(stats.get("wins") or 0)
        avg_pnl = (stats["sum_pnl"] / pnl_samples) if pnl_samples else None
        if signals >= 2 and pnl_samples >= 2 and wins / max(pnl_samples, 1) <= 0.34:
            recommendations.append(
                f"{bucket} bucket weak lag raha hai; is window me threshold stricter rakho ya watch-only mode use karo."
            )
        elif signals >= 2 and avg_pnl is not None and avg_pnl >= 8:
            recommendations.append(
                f"{bucket} bucket strong perform kar raha hai; yahan confirmation setups ko thoda priority de sakte ho."
            )

    ranked_setups = sorted(
        summary.get("setups", {}).items(),
        key=lambda item: (
            -((item[1]["sum_pnl"] / item[1]["pnl_count"]) if item[1]["pnl_count"] else -9999),
            -item[1]["signals"],
            item[0],
        ),
    )
    for setup, stats in ranked_setups[:2]:
        if int(stats.get("signals") or 0) >= 2 and int(stats.get("wins") or 0) == 0:
            recommendations.append(
                f"{setup} setup recent sample me weak raha; iske liye stricter confirmation ya temporary downgrade socho."
            )

    return recommendations[:6]


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
                o.max_adverse_ltp,
                o.minutes_since_signal,
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
                o.max_adverse_ltp,
                o.minutes_since_signal,
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
        co.max_adverse_ltp,
        co.minutes_since_signal,
        tc.strike,
        tc.option_ltp,
        tc.candidate_score,
        tc.expected_edge,
        to2.pnl_points,
        to2.max_favorable_ltp,
        to2.max_adverse_ltp,
        to2.minutes_since_signal
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
    print_daily_summary(args.date, rows)
    print_signal_review(args.date, rows)


if __name__ == "__main__":
    main()
