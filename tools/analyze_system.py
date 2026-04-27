#!/usr/bin/env python3
"""Summarize system behavior for a trading day."""

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config


BLOCKERS_RE = re.compile(r"blockers=([^|]+)")
CAUTIONS_RE = re.compile(r"cautions=([^|]+)")


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze strategy decisions and live alerts")
    parser.add_argument(
        "--date",
        help="Trading day in YYYY-MM-DD (Asia/Kolkata). Defaults to latest strategy-decision day.",
    )
    return parser.parse_args()


def fetch_all(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()


def load_target_day(cur, explicit_day=None):
    if explicit_day:
        return explicit_day

    rows = fetch_all(
        cur,
        """
        SELECT MAX(DATE(ts AT TIME ZONE 'Asia/Kolkata'))
        FROM strategy_decisions_5m;
        """,
    )
    return rows[0][0].isoformat() if rows and rows[0][0] else None


def extract_list(raw_json, reason, regex):
    if raw_json:
        if isinstance(raw_json, list):
            return [str(item) for item in raw_json]
        try:
            parsed = json.loads(raw_json)
            return [str(item) for item in parsed]
        except Exception:
            pass
    if not reason:
        return []
    match = regex.search(reason)
    if not match:
        return []
    return [item.strip() for item in match.group(1).split(",") if item.strip()]


def summarize_day(cur, day):
    rows = fetch_all(
        cur,
        """
        SELECT
            instrument,
            ts AT TIME ZONE 'Asia/Kolkata',
            signal,
            strategy_score,
            setup_type,
            tradability,
            reason,
            blockers_json::text,
            cautions_json::text,
            candidate_signal_type,
            candidate_signal_grade,
            candidate_confidence,
            actionable_block_reason,
            watch_bucket
        FROM strategy_decisions_5m
        WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = %s
        ORDER BY instrument, ts;
        """,
        (day,),
    )

    issued = fetch_all(
        cur,
        """
        SELECT instrument, signal, COUNT(*), ROUND(AVG(strategy_score)::numeric, 2)
        FROM signals_issued
        WHERE DATE(ts AT TIME ZONE 'Asia/Kolkata') = %s
        GROUP BY instrument, signal
        ORDER BY instrument, signal;
        """,
        (day,),
    )

    outcomes = fetch_all(
        cur,
        """
        WITH ranked AS (
            SELECT
                instrument,
                signal,
                entry_ts,
                pnl_points,
                ROW_NUMBER() OVER (PARTITION BY instrument, signal, entry_ts ORDER BY ts DESC) AS rn
            FROM trade_monitor_events_1m
            WHERE DATE(entry_ts AT TIME ZONE 'Asia/Kolkata') = %s
        )
        SELECT
            instrument,
            signal,
            COUNT(*) FILTER (WHERE rn = 1) AS trades,
            ROUND(AVG(pnl_points) FILTER (WHERE rn = 1)::numeric, 2) AS avg_final_pnl,
            ROUND(SUM(pnl_points) FILTER (WHERE rn = 1)::numeric, 2) AS total_final_pnl
        FROM ranked
        GROUP BY instrument, signal
        ORDER BY instrument, signal;
        """,
        (day,),
    )

    try:
        reviews = fetch_all(
            cur,
            """
            SELECT instrument, alert_kind, usefulness, outcome_tag, COUNT(*)
            FROM alert_reviews_5m
            WHERE DATE(alert_ts AT TIME ZONE 'Asia/Kolkata') = %s
            GROUP BY instrument, alert_kind, usefulness, outcome_tag
            ORDER BY instrument, alert_kind, usefulness, outcome_tag;
            """,
            (day,),
        )
    except Exception:
        reviews = []

    by_instrument = defaultdict(lambda: {
        "decisions": 0,
        "signal_rows": 0,
        "candidate_rows": 0,
        "blocked_actionable": 0,
        "scores": [],
        "setup_counter": Counter(),
        "blockers": Counter(),
        "cautions": Counter(),
        "watch_buckets": Counter(),
        "best_rows": [],
    })

    for row in rows:
        instrument, ts_ist, signal, score, setup_type, tradability, reason, blockers_json, cautions_json, candidate_type, candidate_grade, candidate_confidence, actionable_block_reason, watch_bucket = row
        item = by_instrument[instrument]
        item["decisions"] += 1
        item["scores"].append(int(score or 0))
        item["setup_counter"][setup_type or "NONE"] += 1
        if signal in {"CE", "PE"}:
            item["signal_rows"] += 1
        if candidate_type and candidate_type != "NONE":
            item["candidate_rows"] += 1
        if actionable_block_reason:
            item["blocked_actionable"] += 1

        blockers = extract_list(blockers_json, reason, BLOCKERS_RE)
        cautions = extract_list(cautions_json, reason, CAUTIONS_RE)
        item["blockers"].update(blockers)
        item["cautions"].update(cautions)
        item["watch_buckets"][watch_bucket or "NONE"] += 1
        item["best_rows"].append(
            (
                int(score or 0),
                ts_ist.strftime("%H:%M"),
                signal or "-",
                setup_type or "NONE",
                tradability or "-",
                candidate_type or "NONE",
                candidate_grade or "-",
                candidate_confidence or "-",
                blockers,
                cautions,
            )
        )

    print(f"System analysis for {day} (Asia/Kolkata)")
    print("=" * 72)
    for instrument in sorted(by_instrument):
        item = by_instrument[instrument]
        avg_score = sum(item["scores"]) / len(item["scores"]) if item["scores"] else 0
        print(f"\n[{instrument}]")
        print(
            "Decisions: {decisions} | Signal rows: {signal_rows} | Candidate setups: {candidate_rows} | "
            "Blocked by actionable rules: {blocked_actionable} | Avg score: {avg_score:.2f} | Max score: {max_score}".format(
                decisions=item["decisions"],
                signal_rows=item["signal_rows"],
                candidate_rows=item["candidate_rows"],
                blocked_actionable=item["blocked_actionable"],
                avg_score=avg_score,
                max_score=max(item["scores"]) if item["scores"] else 0,
            )
        )
        print("Top setups:", ", ".join(f"{name}={count}" for name, count in item["setup_counter"].most_common(4)))
        print("Watch buckets:", ", ".join(f"{name}={count}" for name, count in item["watch_buckets"].most_common(4)) or "-")
        print("Top blockers:", ", ".join(f"{name}={count}" for name, count in item["blockers"].most_common(6)) or "-")
        print("Top cautions:", ", ".join(f"{name}={count}" for name, count in item["cautions"].most_common(6)) or "-")

        print("Nearest misses:")
        for best in sorted(item["best_rows"], reverse=True)[:3]:
            score, ts_ist, signal, setup_type, tradability, candidate_type, candidate_grade, candidate_confidence, blockers, cautions = best
            print(
                f"  {ts_ist} | score={score} | signal={signal} | setup={setup_type} | "
                f"candidate={candidate_type}/{candidate_grade}/{candidate_confidence} | "
                f"tradability={tradability} | blockers={','.join(blockers) or '-'} | cautions={','.join(cautions) or '-'}"
            )

    if issued:
        print("\nIssued alerts")
        print("-" * 72)
        for instrument, signal, count, avg_score in issued:
            print(f"{instrument} {signal}: issued={count}, avg_score={avg_score}")

    if outcomes:
        print("\nMonitor outcomes")
        print("-" * 72)
        for instrument, signal, trades, avg_final_pnl, total_final_pnl in outcomes:
            print(
                f"{instrument} {signal}: trades={trades}, avg_final_pnl={avg_final_pnl}, total_final_pnl={total_final_pnl}"
            )

    if reviews:
        print("\nAlert reviews")
        print("-" * 72)
        for instrument, alert_kind, usefulness, outcome_tag, count in reviews:
            print(f"{instrument} {alert_kind} {usefulness} [{outcome_tag}]: {count}")


def main():
    args = parse_args()
    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            day = load_target_day(cur, explicit_day=args.date)
            if not day:
                print("No strategy decision data found.")
                return
            summarize_day(cur, day)


if __name__ == "__main__":
    main()
