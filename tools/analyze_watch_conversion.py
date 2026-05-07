#!/usr/bin/env python3
"""Analyze watch-to-entry conversion from signal-service logs."""

import argparse
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config
from shared.utils.log_utils import LOGS_DIR


WATCH_HEADER_RE = re.compile(
    r"^(?P<instrument>[A-Z]+)\s+(?P<direction>CE|PE)\s+watch\s+(?:above|below)\s+(?P<trigger>[0-9.]+)",
)
ENTRY_HEADER_RE = re.compile(
    r"^(?P<instrument>[A-Z]+)\s+Confirmed\s+(?P<direction>CE|PE)\s+Entry$",
)
TIME_RE = re.compile(r"^\[(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})\]")
SCORE_RE = re.compile(r"S:(?P<score>\d+)\s+\|\s+E:(?P<entry_score>\d+)\s+\|\s+G:(?P<grade>[A-Z+]+)")
BREADTH_RE = re.compile(r"breadth\s+(?P<breadth>\d+)/(?P<breadth_total>\d+)")
VOL_RE = re.compile(r"vol\s+(?P<vol>\d+)/(?P<vol_total>\d+)")


@dataclass
class WatchEvent:
    instrument: str
    direction: str
    ts: datetime
    header: str
    lines: list[str]
    score: int | None
    entry_score: int | None
    grade: str | None
    breadth: int | None
    breadth_total: int | None
    volume_breadth: int | None
    volume_total: int | None


@dataclass
class EntryEvent:
    instrument: str
    direction: str
    ts: datetime
    header: str
    lines: list[str]


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze watch-to-entry conversion from logs")
    parser.add_argument("--date", help="Trading day in YYYY-MM-DD. Defaults to latest log folder.")
    parser.add_argument("--window-minutes", type=int, default=30, help="Max watch-to-entry mapping window.")
    return parser.parse_args()


def fetch_all(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()


def load_target_day(explicit_day: str | None) -> date | None:
    if explicit_day:
        return date.fromisoformat(explicit_day)
    folders = sorted([path.name for path in LOGS_DIR.iterdir() if path.is_dir() and path.name.isdigit()])
    if not folders:
        return None
    latest = folders[-1]
    return datetime.strptime(latest, "%Y%m%d").date()


def log_paths_for_day(day: date) -> list[Path]:
    folder = LOGS_DIR / day.strftime("%Y%m%d")
    return sorted(folder.glob("signal_service_*.log"))


def parse_timestamp(prefix: str, day: date) -> datetime | None:
    match = TIME_RE.match(prefix)
    if not match:
        return None
    return datetime(
        year=day.year,
        month=day.month,
        day=day.day,
        hour=int(match.group("hour")),
        minute=int(match.group("minute")),
        second=int(match.group("second")),
    )


def parse_alert_blocks(path: Path, day: date) -> tuple[list[WatchEvent], list[EntryEvent]]:
    watches: list[WatchEvent] = []
    entries: list[EntryEvent] = []

    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    i = 0
    current_ts: datetime | None = None
    while i < len(lines):
        raw_line = lines[i].replace("\a", "")
        parsed_ts = parse_timestamp(raw_line, day)
        if parsed_ts is not None:
            current_ts = parsed_ts

        ts = parsed_ts or current_ts
        if ts is None or "[ALERT]" not in raw_line:
            i += 1
            continue

        alert_text = raw_line.split("[ALERT]", 1)[1].strip()
        block = [alert_text]
        j = i + 1
        while j < len(lines):
            next_line = lines[j].replace("\a", "")
            if not next_line.strip():
                break
            if TIME_RE.match(next_line):
                break
            block.append(next_line.strip())
            j += 1

        watch_match = WATCH_HEADER_RE.match(alert_text)
        entry_match = ENTRY_HEADER_RE.match(alert_text)
        if watch_match:
            score = entry_score = None
            grade = None
            breadth = breadth_total = volume_breadth = volume_total = None
            for block_line in block[1:]:
                score_match = SCORE_RE.search(block_line)
                if score_match:
                    score = int(score_match.group("score"))
                    entry_score = int(score_match.group("entry_score"))
                    grade = score_match.group("grade")
                breadth_match = BREADTH_RE.search(block_line)
                if breadth_match and breadth is None:
                    breadth = int(breadth_match.group("breadth"))
                    breadth_total = int(breadth_match.group("breadth_total"))
                vol_match = VOL_RE.search(block_line)
                if vol_match and volume_breadth is None:
                    volume_breadth = int(vol_match.group("vol"))
                    volume_total = int(vol_match.group("vol_total"))
            watches.append(
                WatchEvent(
                    instrument=watch_match.group("instrument"),
                    direction=watch_match.group("direction"),
                    ts=ts,
                    header=alert_text,
                    lines=block,
                    score=score,
                    entry_score=entry_score,
                    grade=grade,
                    breadth=breadth,
                    breadth_total=breadth_total,
                    volume_breadth=volume_breadth,
                    volume_total=volume_total,
                )
            )
        elif entry_match:
            entries.append(
                EntryEvent(
                    instrument=entry_match.group("instrument"),
                    direction=entry_match.group("direction"),
                    ts=ts,
                    header=alert_text,
                    lines=block,
                )
            )
        i = j if j > i else i + 1

    return watches, entries


def infer_watch_failure_reason(
    watch: WatchEvent,
    next_same_direction_watch: WatchEvent | None,
    next_opposite_watch: WatchEvent | None,
    end_of_day: datetime,
) -> str:
    joined = " | ".join(watch.lines).lower()
    if next_same_direction_watch and (next_same_direction_watch.ts - watch.ts) <= timedelta(minutes=12):
        return "replaced_by_fresher_watch"
    if next_opposite_watch and (next_opposite_watch.ts - watch.ts) <= timedelta(minutes=10):
        return "opposite_setup_took_over"
    if "structure limited" in joined:
        return "structure_limited"
    if watch.volume_breadth is not None and watch.volume_total and watch.volume_breadth < watch.volume_total:
        return "weak_option_participation"
    if watch.breadth is not None and watch.breadth_total and watch.breadth <= max(watch.breadth_total - 3, 0):
        return "limited_breadth"
    if (end_of_day - watch.ts) <= timedelta(minutes=20):
        return "late_session_no_confirmation"
    return "no_confirmation"


def analyze_conversion(
    watches: list[WatchEvent],
    entries: list[EntryEvent],
    window_minutes: int,
) -> list[dict]:
    entries_by_key: dict[tuple[str, str], list[EntryEvent]] = defaultdict(list)
    for entry in entries:
        entries_by_key[(entry.instrument, entry.direction)].append(entry)

    watches_by_instrument: dict[str, list[WatchEvent]] = defaultdict(list)
    for watch in watches:
        watches_by_instrument[watch.instrument].append(watch)

    results = []
    for watch in watches:
        key = (watch.instrument, watch.direction)
        candidate_entry = None
        for entry in entries_by_key.get(key, []):
            if entry.ts < watch.ts:
                continue
            if entry.ts - watch.ts > timedelta(minutes=window_minutes):
                break
            candidate_entry = entry
            break

        same_direction_future = next(
            (
                future
                for future in watches_by_instrument[watch.instrument]
                if future.direction == watch.direction and future.ts > watch.ts
            ),
            None,
        )
        opposite_future = next(
            (
                future
                for future in watches_by_instrument[watch.instrument]
                if future.direction != watch.direction and future.ts > watch.ts
            ),
            None,
        )
        if candidate_entry and same_direction_future and same_direction_future.ts < candidate_entry.ts:
            candidate_entry = None

        result = {
            "watch": watch,
            "entry": candidate_entry,
            "converted": candidate_entry is not None,
            "latency_minutes": (
                round((candidate_entry.ts - watch.ts).total_seconds() / 60.0, 1)
                if candidate_entry else None
            ),
        }
        if not candidate_entry:
            end_of_day = datetime.combine(watch.ts.date(), datetime.min.time()).replace(hour=15, minute=30)
            result["failure_reason"] = infer_watch_failure_reason(
                watch,
                same_direction_future,
                opposite_future,
                end_of_day,
            )
        results.append(result)
    return results


def summarize_trade_points(trade_rows: list[dict]) -> tuple[list[dict], float]:
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in trade_rows:
        effective_points = row["points"] if row["points"] is not None else row["peak_points"]
        if effective_points is None:
            continue
        grouped[(row["instrument"], row["signal"])].append(float(effective_points))

    rows_out = []
    total_points = 0.0
    for instrument, signal in sorted(grouped):
        values = grouped[(instrument, signal)]
        subtotal = round(sum(values), 2)
        total_points += subtotal
        rows_out.append(
            {
                "instrument": instrument,
                "signal": signal,
                "trades": len(values),
                "points": subtotal,
                "avg_points": round(subtotal / len(values), 2) if values else 0.0,
            }
        )

    return rows_out, round(total_points, 2)


def fetch_trade_detail_rows(day: date) -> list[dict]:
    rows_out = []
    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            rows = fetch_all(
                cur,
                """
                WITH monitor_ranked AS (
                    SELECT
                        instrument,
                        signal,
                        entry_ts,
                        entry_price,
                        ts,
                        current_price,
                        pnl_points,
                        guidance,
                        ROW_NUMBER() OVER (
                            PARTITION BY instrument, signal, entry_ts
                            ORDER BY ts ASC
                        ) AS first_rn
                    FROM trade_monitor_events_1m
                    WHERE DATE(entry_ts AT TIME ZONE 'Asia/Kolkata') = %s
                ),
                monitor_groups AS (
                    SELECT
                        instrument,
                        signal,
                        entry_ts,
                        MIN(entry_price) AS monitor_base_premium,
                        MIN(ts) AS first_event_ts,
                        MAX(current_price) AS peak_premium,
                        MAX(pnl_points) AS peak_points,
                        MAX(CASE WHEN first_rn = 1 THEN current_price END) AS first_seen_premium
                    FROM monitor_ranked
                    GROUP BY instrument, signal, entry_ts
                ),
                exit_pick AS (
                    SELECT
                        instrument,
                        signal,
                        entry_ts,
                        ts AS exit_ts,
                        current_price AS exit_premium,
                        pnl_points,
                        guidance,
                        ROW_NUMBER() OVER (
                            PARTITION BY instrument, signal, entry_ts
                            ORDER BY
                                CASE
                                    WHEN guidance IN ('EXIT_BIAS', 'EXIT_STOPLOSS', 'EXIT_TRAIL', 'EXIT_TIMESTOP', 'EXIT_PROFIT_PROTECT') THEN ts
                                END ASC
                        ) AS rn
                    FROM trade_monitor_events_1m
                    WHERE DATE(entry_ts AT TIME ZONE 'Asia/Kolkata') = %s
                      AND guidance IN ('EXIT_BIAS', 'EXIT_STOPLOSS', 'EXIT_TRAIL', 'EXIT_TIMESTOP', 'EXIT_PROFIT_PROTECT')
                ),
                signal_rows AS (
                    SELECT
                        s.instrument,
                        s.signal,
                        s.ts,
                        s.strike,
                        COALESCE(s.option_entry_ltp, s.price) AS alert_premium
                    FROM signals_issued s
                    WHERE DATE(s.ts AT TIME ZONE 'Asia/Kolkata') = %s
                ),
                matched AS (
                    SELECT
                        s.instrument,
                        s.signal,
                        s.ts,
                        s.strike,
                        s.alert_premium,
                        m.entry_ts,
                        m.monitor_base_premium,
                        m.first_event_ts,
                        m.first_seen_premium,
                        m.peak_premium,
                        m.peak_points,
                        e.exit_ts,
                        e.exit_premium,
                        e.pnl_points,
                        e.guidance,
                        ROW_NUMBER() OVER (
                            PARTITION BY s.instrument, s.signal, s.ts
                            ORDER BY
                                ABS(COALESCE(s.alert_premium, 0) - COALESCE(m.first_seen_premium, s.alert_premium, 0)) ASC,
                                ABS(EXTRACT(EPOCH FROM (s.ts - m.first_event_ts))) ASC
                        ) AS rn
                    FROM signal_rows s
                    LEFT JOIN monitor_groups m
                      ON m.instrument = s.instrument
                     AND m.signal = s.signal
                     AND m.first_event_ts <= s.ts + INTERVAL '2 minutes'
                     AND m.first_event_ts >= s.ts - INTERVAL '30 minutes'
                    LEFT JOIN exit_pick e
                      ON e.instrument = m.instrument
                     AND e.signal = m.signal
                     AND e.entry_ts = m.entry_ts
                     AND e.rn = 1
                )
                SELECT
                    instrument,
                    signal,
                    TO_CHAR(ts AT TIME ZONE 'Asia/Kolkata', 'HH24:MI') AS entry_time,
                    strike,
                    alert_premium,
                    TO_CHAR(first_event_ts AT TIME ZONE 'Asia/Kolkata', 'HH24:MI') AS monitor_start_time,
                    monitor_base_premium,
                    peak_premium,
                    peak_points,
                    TO_CHAR(exit_ts AT TIME ZONE 'Asia/Kolkata', 'HH24:MI') AS exit_time,
                    exit_premium,
                    pnl_points,
                    guidance
                FROM matched
                WHERE rn = 1
                ORDER BY instrument, ts, signal;
                """,
                (day.isoformat(), day.isoformat(), day.isoformat()),
            )
    for (
        instrument,
        signal,
        entry_time,
        strike,
        alert_premium,
        monitor_start_time,
        monitor_base_premium,
        peak_premium,
        peak_points,
        exit_time,
        exit_premium,
        pnl_points,
        guidance,
    ) in rows:
        alert_premium_float = float(alert_premium) if alert_premium is not None else None
        peak_premium_float = float(peak_premium) if peak_premium is not None else None
        exit_premium_float = float(exit_premium) if exit_premium is not None else None
        rows_out.append(
            {
                "instrument": instrument,
                "signal": signal,
                "entry_time": entry_time,
                "strike": int(strike) if strike is not None else None,
                "alert_premium": alert_premium_float,
                "monitor_start_time": monitor_start_time or "-",
                "monitor_base_premium": float(monitor_base_premium) if monitor_base_premium is not None else None,
                "peak_premium": peak_premium_float,
                "peak_points": (
                    round(peak_premium_float - alert_premium_float, 2)
                    if peak_premium_float is not None and alert_premium_float is not None
                    else None
                ),
                "monitor_peak_points": float(peak_points) if peak_points is not None else None,
                "exit_time": exit_time or "-",
                "exit_premium": exit_premium_float,
                "points": (
                    round(exit_premium_float - alert_premium_float, 2)
                    if exit_premium_float is not None and alert_premium_float is not None
                    else None
                ),
                "monitor_exit_points": float(pnl_points) if pnl_points is not None else None,
                "guidance": guidance or "-",
            }
        )
    return rows_out


def render_table(headers: list[str], rows: list[list[str]]) -> str:
    all_rows = [headers] + rows
    widths = [max(len(str(row[idx])) for row in all_rows) for idx in range(len(headers))]

    def fmt(row):
        return " | ".join(str(cell).ljust(widths[idx]) for idx, cell in enumerate(row))

    divider = "-+-".join("-" * width for width in widths)
    lines = [fmt(headers), divider]
    lines.extend(fmt(row) for row in rows)
    return "\n".join(lines)


def print_summary(
    day: date,
    results: Iterable[dict],
    entries: list[EntryEvent],
    point_rows: list[dict],
    total_points: float,
    trade_rows: list[dict],
):
    results = list(results)
    by_instrument: dict[str, dict] = defaultdict(lambda: {"watches": 0, "converted": 0, "reasons": Counter(), "latencies": []})
    for item in results:
        watch = item["watch"]
        bucket = by_instrument[watch.instrument]
        bucket["watches"] += 1
        if item["converted"]:
            bucket["converted"] += 1
            if item["latency_minutes"] is not None:
                bucket["latencies"].append(item["latency_minutes"])
        else:
            bucket["reasons"][item["failure_reason"]] += 1

    total_watches = len(results)
    total_entries = len(entries)
    total_converted = sum(1 for item in results if item["converted"])
    print(f"Watch conversion analysis for {day.isoformat()} (Asia/Kolkata)")
    print("=" * 72)
    print(
        f"Total watches: {total_watches} | Confirmed entries: {total_entries} | "
        f"Converted watches: {total_converted} | Conversion rate: "
        f"{(total_converted / total_watches * 100):.2f}%"
        if total_watches
        else "No watch alerts found."
    )
    print(f"Total signal-based points: {total_points:.2f}")

    if point_rows:
        print("\nPoint Summary")
        print("-" * 72)
        table_rows = [
            [
                row["instrument"],
                row["signal"],
                str(row["trades"]),
                f"{row['points']:.2f}",
                f"{row['avg_points']:.2f}",
            ]
            for row in point_rows
        ]
        print(render_table(["Instrument", "Signal", "Trades", "Points", "Avg/Trade"], table_rows))

    if trade_rows:
        print("\nTrade Detail")
        print("-" * 72)
        detail_rows = [
            [
                row["instrument"],
                row["signal"],
                row["entry_time"],
                str(row["strike"] or "-"),
                f"{row['alert_premium']:.2f}" if row["alert_premium"] is not None else "-",
                row["monitor_start_time"],
                f"{row['peak_premium']:.2f}" if row["peak_premium"] is not None else "-",
                f"{row['peak_points']:.2f}" if row["peak_points"] is not None else "-",
                row["exit_time"],
                f"{row['exit_premium']:.2f}" if row["exit_premium"] is not None else "-",
                f"{row['points']:.2f}" if row["points"] is not None else "-",
                row["guidance"],
            ]
            for row in trade_rows
        ]
        print(
            render_table(
                ["Instrument", "Signal", "Entry", "Strike", "AlertPrem", "MonitorStart", "PeakPrem", "PeakPts", "Exit", "ExitPrem", "ExitPts", "ExitWhy"],
                detail_rows,
            )
        )

    for instrument in sorted(by_instrument):
        bucket = by_instrument[instrument]
        conversion_rate = (bucket["converted"] / bucket["watches"] * 100) if bucket["watches"] else 0.0
        avg_latency = (
            round(sum(bucket["latencies"]) / len(bucket["latencies"]), 2)
            if bucket["latencies"] else None
        )
        print(f"\n[{instrument}]")
        print(
            f"Watches={bucket['watches']} | Converted={bucket['converted']} | "
            f"Conversion={conversion_rate:.2f}% | Avg latency={avg_latency if avg_latency is not None else '-'} min"
        )
        print(
            "Top failure reasons:",
            ", ".join(f"{name}={count}" for name, count in bucket["reasons"].most_common(5)) or "-",
        )

    failed = [item for item in results if not item["converted"]]
    if failed:
        print("\nRepresentative missed watches")
        print("-" * 72)
        for item in failed[:10]:
            watch = item["watch"]
            summary = " | ".join(
                part for part in [
                    f"{watch.ts.strftime('%H:%M')} {watch.instrument} {watch.direction}",
                    f"grade={watch.grade or '-'}",
                    f"score={watch.score or '-'}",
                    f"entry={watch.entry_score or '-'}",
                    f"breadth={watch.breadth}/{watch.breadth_total}" if watch.breadth is not None else None,
                    f"vol={watch.volume_breadth}/{watch.volume_total}" if watch.volume_breadth is not None else None,
                    f"reason={item['failure_reason']}",
                ]
                if part
            )
            print(summary)


def main():
    args = parse_args()
    day = load_target_day(args.date)
    if not day:
        print("No dated log folders found.")
        return

    watch_events: list[WatchEvent] = []
    entry_events: list[EntryEvent] = []
    for path in log_paths_for_day(day):
        watches, entries = parse_alert_blocks(path, day)
        watch_events.extend(watches)
        entry_events.extend(entries)

    results = analyze_conversion(watch_events, entry_events, window_minutes=args.window_minutes)
    trade_rows = fetch_trade_detail_rows(day)
    point_rows, total_points = summarize_trade_points(trade_rows)
    print_summary(day, results, entry_events, point_rows, total_points, trade_rows)


if __name__ == "__main__":
    main()
