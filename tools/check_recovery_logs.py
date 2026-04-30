#!/usr/bin/env python3
"""Check whether sleep/reconnect recovery worked for the trading day logs."""

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOGS_DIR = ROOT / "logs"
INSTRUMENTS = ("nifty", "banknifty", "sensex")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Inspect collector/signal logs for reconnect recovery health"
    )
    parser.add_argument(
        "--date",
        help="Trading day in YYYYMMDD format. Defaults to latest logs folder.",
    )
    return parser.parse_args()


def latest_log_day():
    days = sorted(
        path.name for path in LOGS_DIR.iterdir()
        if path.is_dir() and re.fullmatch(r"\d{8}", path.name)
    )
    return days[-1] if days else None


def read_lines(path):
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()


def collector_summary(lines):
    text = "\n".join(lines)
    sleep_events = sum("SYSTEM SLEEP/WAKE DETECTED" in line for line in lines)
    anchor_events = sum("Preserved backfill anchor" in line for line in lines)
    warmup_events = sum("Restored 5m warmup history after reconnect" in line for line in lines)
    backfill_events = sum("Recovery Summary | backfilled:" in line for line in lines)
    recovery_complete = sum("Recovery complete. Resuming data collection" in line for line in lines)
    false_feed_heartbeats = sum(
        "Heartbeat" in line and "feed_connected: False" in line
        for line in lines
    )

    if sleep_events == 0:
        status = "PASS"
        note = "No sleep/reconnect event seen in collector logs."
    elif anchor_events == 0 or warmup_events == 0:
        status = "FAIL"
        note = "Recovery markers missing after sleep/reconnect."
    elif recovery_complete < sleep_events:
        status = "WARN"
        note = "Some recovery attempts did not finish cleanly."
    elif backfill_events == 0:
        status = "WARN"
        note = "Recovery completed but no backfill summary was logged."
    elif false_feed_heartbeats > 0:
        status = "WARN"
        note = "Recovery happened, but collector still saw disconnected heartbeat states."
    else:
        status = "PASS"
        note = "Collector recovery markers look healthy."

    return {
        "status": status,
        "sleep_events": sleep_events,
        "anchor_events": anchor_events,
        "warmup_events": warmup_events,
        "backfill_events": backfill_events,
        "recovery_complete": recovery_complete,
        "false_feed_heartbeats": false_feed_heartbeats,
        "note": note,
        "has_text": bool(text.strip()),
    }


def signal_summary(lines):
    processing_count = sum("Processing new 5m candle" in line for line in lines)
    stale_pauses = sum("Pausing signal generation: Latest 5m candle is stale" in line for line in lines)
    healthy_resumes = sum("Data stream healthy again. Resuming signal generation." in line for line in lines)
    sleep_events = sum("SYSTEM SLEEP/WAKE DETECTED" in line for line in lines)

    last_processed_line = None
    for line in reversed(lines):
        if "Processing new 5m candle" in line:
            last_processed_line = line.strip()
            break

    if not lines:
        status = "FAIL"
        note = "Signal log missing."
    elif processing_count == 0:
        status = "FAIL"
        note = "No 5m candle processed."
    elif sleep_events > 0 and healthy_resumes == 0:
        status = "FAIL"
        note = "Recovered sleep/wake events never resumed healthy stream."
    elif stale_pauses > max(healthy_resumes * 3, 6):
        status = "WARN"
        note = "Signal service spent a lot of time paused on stale 5m candles."
    else:
        status = "PASS"
        note = "Signal recovery looks acceptable."

    return {
        "status": status,
        "processing_count": processing_count,
        "stale_pauses": stale_pauses,
        "healthy_resumes": healthy_resumes,
        "sleep_events": sleep_events,
        "last_processed_line": last_processed_line,
        "note": note,
    }


def print_header(day):
    print("=" * 72)
    print(f"RECOVERY CHECK | {day}")
    print("=" * 72)


def print_collector(result):
    print("\n[data_collector_shared.log]")
    print(f"Status: {result['status']} | {result['note']}")
    print(
        "sleep_events={sleep_events} | anchors={anchor_events} | warmups={warmup_events} | "
        "backfills={backfill_events} | recoveries={recovery_complete} | feed_false_heartbeats={false_feed_heartbeats}".format(
            **result
        )
    )


def print_signal(name, result):
    print(f"\n[{name}]")
    print(f"Status: {result['status']} | {result['note']}")
    print(
        "processed_5m={processing_count} | stale_pauses={stale_pauses} | "
        "healthy_resumes={healthy_resumes} | sleep_events={sleep_events}".format(**result)
    )
    if result["last_processed_line"]:
        print(f"last_processed: {result['last_processed_line']}")


def main():
    args = parse_args()
    day = args.date or latest_log_day()
    if not day:
        print("No logs folder found.")
        raise SystemExit(1)

    day_dir = LOGS_DIR / day
    if not day_dir.exists():
        print(f"Log folder not found: {day_dir}")
        raise SystemExit(1)

    print_header(day)

    collector_lines = read_lines(day_dir / "data_collector_shared.log")
    collector_result = collector_summary(collector_lines)
    print_collector(collector_result)

    overall_fail = collector_result["status"] == "FAIL"
    overall_warn = collector_result["status"] == "WARN"

    for instrument in INSTRUMENTS:
        filename = f"signal_service_{instrument}.log"
        result = signal_summary(read_lines(day_dir / filename))
        print_signal(filename, result)
        overall_fail = overall_fail or result["status"] == "FAIL"
        overall_warn = overall_warn or result["status"] == "WARN"

    print("\n" + "=" * 72)
    if overall_fail:
        print("Overall: FAIL | Recovery pipeline still needs attention.")
    elif overall_warn:
        print("Overall: WARN | Recovery partly worked, but there are stability gaps.")
    else:
        print("Overall: PASS | Recovery markers look healthy in logs.")
    print("=" * 72)


if __name__ == "__main__":
    main()
