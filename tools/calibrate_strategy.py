import argparse
import csv
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from config import Config


POINT_TARGET = 20.0


def parse_args():
    parser = argparse.ArgumentParser(description="Calibrate strategy blockers using stored DB data.")
    parser.add_argument("--from-date", dest="from_date", help="Start date in YYYY-MM-DD")
    parser.add_argument("--to-date", dest="to_date", help="End date in YYYY-MM-DD")
    parser.add_argument("--instrument", default=Config.SYMBOL, help="Instrument name, default from config")
    parser.add_argument("--threshold", type=float, default=POINT_TARGET, help="Point move threshold for good/bad move tagging")
    return parser.parse_args()


def parse_date(value):
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_csv_list(value):
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_reason_tags(reason):
    result = {}
    if not reason:
        return result
    parts = [part.strip() for part in reason.split("|")]
    for part in parts:
        if "=" in part:
            key, value = part.split("=", 1)
            result[key.strip()] = value.strip()
    return result


def infer_direction(score_factors):
    bullish_votes = 0
    bearish_votes = 0

    for factor in parse_csv_list(score_factors):
        token = factor.lower()
        if "bullish" in token or token == "price_above_vwap" or token == "orb_breakout_up" or token == "atm_pe_concentration":
            bullish_votes += 1
        if "bearish" in token or token == "price_below_vwap" or token == "orb_breakout_down" or token == "atm_ce_concentration":
            bearish_votes += 1

    if bullish_votes > bearish_votes:
        return "CE"
    if bearish_votes > bullish_votes:
        return "PE"
    return None


def _psql_fetch(query, params):
    command = [
        "psql",
        Config.get_db_dsn(),
        "-At",
        "-F",
        "\t",
        "-c",
        query,
    ]
    env = None
    completed = subprocess.run(command, check=True, capture_output=True, text=True, env=env)
    raw = completed.stdout.strip()
    if not raw:
        return []

    reader = csv.reader(raw.splitlines(), delimiter="\t")
    rows = []
    for line in reader:
        processed = []
        for value in line:
            if value == "":
                processed.append(None)
            else:
                processed.append(value)
        rows.append(processed)
    return rows


def fetch_rows(instrument, from_date, to_date):
    query = """
    SELECT
        d.ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        DATE(d.ts AT TIME ZONE 'Asia/Kolkata') AS session_day,
        d.price,
        d.signal,
        d.reason,
        d.strategy_score,
        d.score_factors,
        d.volume_signal,
        d.oi_bias,
        d.oi_trend,
        d.build_up,
        d.pressure_bias,
        d.orb_high,
        d.orb_low,
        d.vwap,
        d.atr,
        c.close AS close_5m,
        LEAD(c.close, 1) OVER (
            PARTITION BY DATE(d.ts AT TIME ZONE 'Asia/Kolkata')
            ORDER BY d.ts
        ) AS next_close_1,
        LEAD(c.close, 2) OVER (
            PARTITION BY DATE(d.ts AT TIME ZONE 'Asia/Kolkata')
            ORDER BY d.ts
        ) AS next_close_2
    FROM strategy_decisions_5m d
    JOIN candles_5m c
      ON c.instrument = d.instrument
     AND c.ts = d.ts
    WHERE d.instrument = '{instrument}'
      AND DATE(d.ts AT TIME ZONE 'Asia/Kolkata') BETWEEN DATE '{from_date}' AND DATE '{to_date}'
    ORDER BY d.ts ASC
    """
    columns = [
        "ts_ist",
        "session_day",
        "price",
        "signal",
        "reason",
        "strategy_score",
        "score_factors",
        "volume_signal",
        "oi_bias",
        "oi_trend",
        "build_up",
        "pressure_bias",
        "orb_high",
        "orb_low",
        "vwap",
        "atr",
        "close_5m",
        "next_close_1",
        "next_close_2",
    ]
    raw_rows = _psql_fetch(
        query.format(
            instrument=instrument,
            from_date=from_date.isoformat(),
            to_date=to_date.isoformat(),
        ),
        None,
    )
    rows = []
    for raw in raw_rows:
        if len(raw) < len(columns):
            raw = raw + [None] * (len(columns) - len(raw))
        row = dict(zip(columns, raw))
        row["session_day"] = parse_date(row["session_day"])
        rows.append(row)
    return rows


def compute_outcome(row, direction, threshold):
    start = float(row["close_5m"]) if row["close_5m"] is not None else None
    next_1 = float(row["next_close_1"]) if row["next_close_1"] is not None else None
    next_2 = float(row["next_close_2"]) if row["next_close_2"] is not None else None
    if start is None or next_1 is None:
        return None

    if direction == "CE":
        move_1 = next_1 - start
        move_2 = (next_2 - start) if next_2 is not None else move_1
        best_move = max(move_1, move_2)
        worst_move = min(move_1, move_2)
    else:
        move_1 = start - next_1
        move_2 = (start - next_2) if next_2 is not None else move_1
        best_move = max(move_1, move_2)
        worst_move = min(move_1, move_2)

    if best_move >= threshold:
        label = "good_move"
    elif worst_move <= -threshold:
        label = "bad_move"
    else:
        label = "mixed_move"

    return {
        "move_1": round(move_1, 2),
        "move_2": round(move_2, 2),
        "best_move": round(best_move, 2),
        "worst_move": round(worst_move, 2),
        "label": label,
    }


def default_date_range(rows):
    session_days = sorted({row["session_day"] for row in rows})
    if not session_days:
        today = date.today()
        return today, today
    return session_days[0], session_days[-1]


def render_summary(rows, threshold):
    blocker_stats = defaultdict(Counter)
    blocker_examples = defaultdict(list)
    suspicious_rows = []

    for row in rows:
        tags = parse_reason_tags(row["reason"])
        blockers = parse_csv_list(tags.get("blockers", ""))
        signal = row["signal"] if row["signal"] in {"CE", "PE"} else None
        direction = signal or infer_direction(row["score_factors"])
        if not direction:
            continue

        outcome = compute_outcome(row, direction, threshold)
        if outcome is None:
            continue

        if signal:
            blocker_stats["__actual_signal__"][outcome["label"]] += 1
            continue

        if not blockers:
            blocker_stats["__no_blocker__"][outcome["label"]] += 1
            continue

        for blocker in blockers:
            blocker_stats[blocker][outcome["label"]] += 1
            if outcome["label"] == "good_move" and len(blocker_examples[blocker]) < 3:
                blocker_examples[blocker].append(
                    {
                        "ts": row["ts_ist"],
                        "direction": direction,
                        "score": row["strategy_score"],
                        "move_1": outcome["move_1"],
                        "move_2": outcome["move_2"],
                        "reason": row["reason"],
                    }
                )

        if outcome["label"] == "good_move":
            suspicious_rows.append(
                {
                    "ts": row["ts_ist"],
                    "direction": direction,
                    "score": row["strategy_score"],
                    "blockers": blockers,
                    "move_1": outcome["move_1"],
                    "move_2": outcome["move_2"],
                    "reason": row["reason"],
                }
            )

    print("Calibration Summary")
    print("Threshold:", threshold, "points")
    print()
    print("Blocker Impact")
    for blocker, counts in sorted(
        blocker_stats.items(),
        key=lambda item: (item[0].startswith("__"), -(item[1]["good_move"] + item[1]["bad_move"] + item[1]["mixed_move"]), item[0]),
    ):
        total = counts["good_move"] + counts["bad_move"] + counts["mixed_move"]
        print(
            f"{blocker}: total={total}, good_moves_blocked={counts['good_move']}, "
            f"bad_moves_avoided={counts['bad_move']}, mixed={counts['mixed_move']}"
        )
        for example in blocker_examples.get(blocker, []):
            print(
                "  example:",
                example["ts"],
                example["direction"],
                f"score={example['score']}",
                f"next1={example['move_1']}",
                f"next2={example['move_2']}",
            )

    print()
    print("Top Suspicious Missed Moves")
    suspicious_rows.sort(key=lambda row: (row["move_2"], row["move_1"], row["score"]), reverse=True)
    for row in suspicious_rows[:10]:
        print(
            row["ts"],
            row["direction"],
            f"score={row['score']}",
            f"blockers={','.join(row['blockers'])}",
            f"next1={row['move_1']}",
            f"next2={row['move_2']}",
        )


def main():
    args = parse_args()
    if args.from_date and args.to_date:
        from_date = parse_date(args.from_date)
        to_date = parse_date(args.to_date)
    else:
        preview_rows = fetch_rows(args.instrument, date(2000, 1, 1), date(2100, 1, 1))
        if not preview_rows:
            print("No rows found in strategy_decisions_5m")
            return
        all_days = sorted({row["session_day"] for row in preview_rows})
        chosen_days = all_days[-2:] if len(all_days) >= 2 else all_days
        from_date = chosen_days[0]
        to_date = chosen_days[-1]

    rows = fetch_rows(args.instrument, from_date, to_date)
    if not rows:
        print("No rows found for selected date range")
        return

    print(f"Instrument: {args.instrument}")
    print(f"Date Range: {from_date} to {to_date}")
    print(f"Rows: {len(rows)}")
    print()
    render_summary(rows, args.threshold)


if __name__ == "__main__":
    main()
