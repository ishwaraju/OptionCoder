"""
Runtime health summary for the trading bot.

Reads heartbeat and watchdog state files and prints a quick status:
- healthy
- stale
- restart-loop-risk
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


HEARTBEAT_FILE = "data/runtime_heartbeat.json"
WATCHDOG_STATE_FILE = "data/watchdog_state.json"


def load_json(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as exc:
        return {"error": str(exc)}


def main():
    parser = argparse.ArgumentParser(description="Show bot runtime heartbeat/watchdog status")
    parser.add_argument("--one-line", action="store_true", help="Print a compact single-line summary")
    args = parser.parse_args()

    heartbeat = load_json(HEARTBEAT_FILE)
    watchdog_state = load_json(WATCHDOG_STATE_FILE) or {"restart_epochs": []}

    if not heartbeat:
        if args.one_line:
            print("NO_HEARTBEAT | heartbeat file missing")
            return
        print("RUNTIME STATUS")
        print("=" * 40)
        print("Severity: ALERT")
        print("Status: NO_HEARTBEAT")
        print(f"Heartbeat file missing: {HEARTBEAT_FILE}")
        return

    if heartbeat.get("error"):
        if args.one_line:
            print(f"HEARTBEAT_READ_ERROR | {heartbeat['error']}")
            return
        print("RUNTIME STATUS")
        print("=" * 40)
        print("Severity: ALERT")
        print("Status: HEARTBEAT_READ_ERROR")
        print("Error:", heartbeat["error"])
        return

    now = time.time()
    heartbeat_epoch = heartbeat.get("epoch")
    heartbeat_age = None if heartbeat_epoch is None else max(0.0, now - heartbeat_epoch)
    status = heartbeat.get("status", {})

    restart_epochs = watchdog_state.get("restart_epochs", [])
    recent_restarts = [
        epoch for epoch in restart_epochs
        if now - epoch <= Config.WATCHDOG_RESTART_WINDOW_SECONDS
    ]

    derived_status = "HEALTHY"
    if heartbeat_age is None or heartbeat_age > Config.WATCHDOG_STALE_SECONDS:
        derived_status = "STALE"
    if len(recent_restarts) >= max(1, Config.WATCHDOG_MAX_RESTARTS - 1):
        derived_status = "RESTART_LOOP_RISK" if derived_status == "HEALTHY" else f"{derived_status} + RESTART_LOOP_RISK"

    severity = "OK"
    if "STALE" in derived_status or "NO_HEARTBEAT" in derived_status:
        severity = "ALERT"
    elif "RESTART_LOOP_RISK" in derived_status:
        severity = "WARN"

    if args.one_line:
        print(
            f"{severity} | {derived_status} | phase={status.get('phase')} | "
            f"feed_connected={status.get('feed_connected')} | "
            f"data_age={status.get('data_age_seconds')} | "
            f"hb_age={round(heartbeat_age, 1) if heartbeat_age is not None else 'unknown'} | "
            f"restarts={len(recent_restarts)}/{Config.WATCHDOG_MAX_RESTARTS}"
        )
        return

    print("RUNTIME STATUS")
    print("=" * 40)
    print("Severity:", severity)
    print("Status:", derived_status)
    print("Heartbeat Age Seconds:", round(heartbeat_age, 1) if heartbeat_age is not None else "unknown")
    print("Last Timestamp:", heartbeat.get("timestamp"))
    print("Phase:", status.get("phase"))
    print("Feed Connected:", status.get("feed_connected"))
    print("Data Age Seconds:", status.get("data_age_seconds"))
    print("Price:", status.get("price"))
    print("Reconnect Attempts:", status.get("reconnect_attempts"))
    print("Recent Restarts:", f"{len(recent_restarts)}/{Config.WATCHDOG_MAX_RESTARTS}")

    last_restart_epoch = watchdog_state.get("last_restart_epoch")
    if last_restart_epoch:
        print(
            "Last Restart:",
            datetime.fromtimestamp(last_restart_epoch).isoformat()
        )
        print("Last Restart Meta:", watchdog_state.get("last_restart_meta"))


if __name__ == "__main__":
    main()
