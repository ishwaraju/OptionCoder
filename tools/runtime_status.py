"""
Runtime health summary for the trading bot services.

Reads service heartbeat and watchdog state files and prints:
- healthy
- stale
- restart-loop-risk
- no-heartbeat
"""

import argparse
import glob
import json
import os
import sys
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


LEGACY_HEARTBEAT_FILE = "data/heartbeat/runtime.json"
LEGACY_WATCHDOG_STATE_FILE = "data/watchdog/runtime_state.json"


def load_json(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as exc:
        return {"error": str(exc)}


def discover_service_pairs():
    heartbeat_files = sorted(glob.glob("data/heartbeat/*.json"))
    pairs = []
    seen = set()

    for heartbeat_file in heartbeat_files:
        base = heartbeat_file[: -len(".json")]
        state_file = f"data/watchdog/{base}_state.json"
        service_key = os.path.basename(base)
        pairs.append((service_key, heartbeat_file, state_file))
        seen.add(service_key)

    
    return pairs


def derive_status(heartbeat, watchdog_state, now, watchdog_state_present=False):
    if not heartbeat:
        return {
            "severity": "ALERT",
            "status": "NO_HEARTBEAT",
            "heartbeat_age": None,
            "service_state": {},
            "recent_restarts": [],
            "last_restart_epoch": None,
            "last_restart_meta": None,
        }

    if heartbeat.get("error"):
        return {
            "severity": "ALERT",
            "status": "HEARTBEAT_READ_ERROR",
            "heartbeat_age": None,
            "service_state": heartbeat,
            "recent_restarts": [],
            "last_restart_epoch": None,
            "last_restart_meta": None,
        }

    heartbeat_epoch = heartbeat.get("epoch")
    heartbeat_age = None if heartbeat_epoch is None else max(0.0, now - heartbeat_epoch)
    service_state = heartbeat.get("status", {})

    restart_epochs = (watchdog_state or {}).get("restart_epochs", [])
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

    return {
        "severity": severity,
        "status": derived_status,
        "heartbeat_age": heartbeat_age,
        "service_state": service_state,
        "recent_restarts": recent_restarts,
        "last_restart_epoch": (watchdog_state or {}).get("last_restart_epoch"),
        "last_restart_meta": (watchdog_state or {}).get("last_restart_meta"),
        "timestamp": heartbeat.get("timestamp"),
        "watchdog_state_present": watchdog_state_present,
    }


def render_one_line(service_name, result):
    state = result["service_state"]
    watchdog_note = (
        f"restarts={len(result['recent_restarts'])}/{Config.WATCHDOG_MAX_RESTARTS}"
        if result["watchdog_state_present"]
        else "restarts=0/5 (no state yet)"
    )
    return (
        f"{service_name} | {result['severity']} | {result['status']} | "
        f"phase={state.get('phase')} | "
        f"hb_age={round(result['heartbeat_age'], 1) if result['heartbeat_age'] is not None else 'unknown'} | "
        f"{watchdog_note}"
    )


def render_full(service_name, result):
    state = result["service_state"]
    lines = [
        service_name.upper(),
        "=" * 40,
        f"Severity: {result['severity']}",
        f"Status: {result['status']}",
    ]

    if result["status"] == "NO_HEARTBEAT":
        lines.append("Heartbeat file missing")
        return "\n".join(lines)

    if result["status"] == "HEARTBEAT_READ_ERROR":
        lines.append(f"Error: {state.get('error')}")
        return "\n".join(lines)

    lines.extend(
        [
            f"Heartbeat Age Seconds: {round(result['heartbeat_age'], 1) if result['heartbeat_age'] is not None else 'unknown'}",
            f"Last Timestamp: {result.get('timestamp')}",
            f"Phase: {state.get('phase')}",
            f"Instrument: {state.get('instrument')}",
            f"Feed Connected: {state.get('feed_connected')}",
            f"Data Age Seconds: {state.get('data_age_seconds')}",
            f"Price: {state.get('price')}",
            f"Reconnect Attempts: {state.get('reconnect_attempts')}",
            (
                f"Recent Restarts: {len(result['recent_restarts'])}/{Config.WATCHDOG_MAX_RESTARTS}"
                if result["watchdog_state_present"]
                else "Recent Restarts: 0/5 (watchdog state not created yet)"
            ),
        ]
    )

    if result["last_restart_epoch"]:
        lines.append(f"Last Restart: {datetime.fromtimestamp(result['last_restart_epoch']).isoformat()}")
        lines.append(f"Last Restart Meta: {result['last_restart_meta']}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Show service heartbeat/watchdog status")
    parser.add_argument("--one-line", action="store_true", help="Print compact service summaries")
    args = parser.parse_args()

    now = time.time()
    pairs = discover_service_pairs()
    outputs = []

    for service_name, heartbeat_file, state_file in pairs:
        heartbeat = load_json(heartbeat_file)
        raw_watchdog_state = load_json(state_file)
        watchdog_state_present = raw_watchdog_state is not None
        watchdog_state = raw_watchdog_state or {"restart_epochs": []}
        result = derive_status(heartbeat, watchdog_state, now, watchdog_state_present=watchdog_state_present)
        outputs.append(render_one_line(service_name, result) if args.one_line else render_full(service_name, result))

    print("\n\n".join(outputs))


if __name__ == "__main__":
    main()
