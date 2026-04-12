"""
Telegram command listener for bot commands.

Supported commands:
- /status
- /health
- /signals
- /start_signal
- /stop_signal
- /start_data
- /stop_data
- /stop
- /shutdown
"""

import argparse
import glob
import json
import os
import re
import subprocess
import sys
import time

import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from shared.db.reader import DBReader
from shared.utils.time_utils import TimeUtils


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_INSTRUMENTS = ["NIFTY", "BANKNIFTY", "SENSEX"]
HA_BIAS_RE = re.compile(r"ha_bias=([A-Z_]+)")


class TelegramCommandService:
    def __init__(self, instruments=None):
        self.instruments = [instrument.upper() for instrument in (instruments or DEFAULT_INSTRUMENTS)]
        self.db_reader = DBReader()
        self.time_utils = TimeUtils()
        self.bot_token = Config.TELEGRAM_BOT_TOKEN
        self.allowed_chat_id = str(Config.TELEGRAM_CHAT_ID) if Config.TELEGRAM_CHAT_ID else None
        self.api_base = f"https://api.telegram.org/bot{self.bot_token}"
        self.offset = 0
        self.running = False
        self.python_executable = sys.executable

    def _validate(self):
        if not Config.TELEGRAM_ENABLED:
            raise RuntimeError("TELEGRAM_ENABLED is False")
        if not self.bot_token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
        if not self.allowed_chat_id:
            raise RuntimeError("TELEGRAM_CHAT_ID is not configured")

    def _telegram_get(self, method, params=None):
        response = requests.get(
            f"{self.api_base}/{method}",
            params=params or {},
            timeout=35,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(f"Telegram API error: {payload}")
        return payload["result"]

    def _telegram_post(self, method, payload):
        response = requests.post(
            f"{self.api_base}/{method}",
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        body = response.json()
        if not body.get("ok"):
            raise RuntimeError(f"Telegram API error: {body}")
        return body["result"]

    def _send_message(self, chat_id, text):
        self._telegram_post(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": text,
            },
        )

    def _sync_offset_to_latest(self):
        """Skip stale queued Telegram commands on startup."""
        updates = self._telegram_get(
            "getUpdates",
            {
                "timeout": 0,
            },
        )
        if updates:
            self.offset = updates[-1]["update_id"] + 1

    def _load_json(self, path):
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _find_running_pids(self, pattern):
        try:
            result = subprocess.run(
                ["pgrep", "-f", pattern],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                return []
            return [int(line.strip()) for line in result.stdout.splitlines() if line.strip().isdigit()]
        except Exception:
            return []

    def _heartbeat_process_info(self, service_key, instrument=None):
        instrument_upper = (instrument or "").upper()
        if service_key == "runtime":
            pids = self._find_running_pids("main.py")
            return {"running": bool(pids), "pids": pids}
        if service_key == "signal_service":
            pids = self._find_running_pids(f"services/signal_service.py --instrument {instrument_upper}")
            return {"running": bool(pids), "pids": pids}
        if service_key == "data_collector":
            pids = self._find_running_pids(f"services/data_collector.py --instrument {instrument_upper}")
            return {"running": bool(pids), "pids": pids}
        if service_key == "oi_collector":
            pids = self._find_running_pids(f"services/oi_collector.py --instrument {instrument_upper}")
            return {"running": bool(pids), "pids": pids}
        if service_key == "telegram_bot_service":
            return {"running": True, "pids": [os.getpid()]}
        return {"running": False, "pids": []}

    def _service_heartbeat_rows(self):
        rows = []
        now = time.time()
        seen = set()
        for heartbeat_file in sorted(glob.glob("data/*_heartbeat.json")):
            heartbeat = self._load_json(heartbeat_file)
            if not heartbeat:
                continue
            status = heartbeat.get("status", {})
            service_key = status.get("service") or os.path.basename(heartbeat_file).replace("_heartbeat.json", "")
            instrument = status.get("instrument")
            process_info = self._heartbeat_process_info(service_key, instrument)
            seen.add((service_key, instrument))
            rows.append(
                {
                    "service": service_key,
                    "phase": status.get("phase"),
                    "instrument": instrument,
                    "heartbeat_age": None if heartbeat.get("epoch") is None else max(0.0, now - heartbeat["epoch"]),
                    "running": process_info["running"],
                    "pids": process_info["pids"],
                }
            )

        expected = []
        for instrument in self.instruments:
            expected.extend(
                [
                    ("signal_service", instrument),
                    ("data_collector", instrument),
                    ("oi_collector", instrument),
                ]
            )
        expected.extend(
            [
                ("telegram_bot_service", None),
                ("runtime", None),
            ]
        )

        for service_key, instrument in expected:
            if (service_key, instrument) in seen:
                continue
            process_info = self._heartbeat_process_info(service_key, instrument)
            rows.append(
                {
                    "service": service_key,
                    "phase": "no_heartbeat",
                    "instrument": instrument,
                    "heartbeat_age": None,
                    "running": process_info["running"],
                    "pids": process_info["pids"],
                }
            )
        return rows

    def _format_status(self):
        rows = self._service_heartbeat_rows()
        if not rows:
            return "No service heartbeat files found."

        grouped = {
            "Signals": [],
            "Data": [],
            "OI": [],
            "Telegram": [],
            "Other": [],
        }

        for row in rows:
            age = "unknown" if row["heartbeat_age"] is None else f"{row['heartbeat_age']:.0f}s"
            lifecycle = "RUNNING" if row["running"] else "STOPPED"
            label = row["instrument"] or row["service"]
            line = f"{label} | {lifecycle} | phase={row['phase']} | hb_age={age}"

            if row["service"] == "signal_service":
                grouped["Signals"].append(line)
            elif row["service"] == "data_collector" or row["service"] == "runtime":
                grouped["Data"].append(line)
            elif row["service"] == "oi_collector":
                grouped["OI"].append(line)
            elif row["service"] == "telegram_bot_service":
                grouped["Telegram"].append(line)
            else:
                grouped["Other"].append(f"{row['service']} | {line}")

        lines = ["Status"]
        for heading in ("Signals", "Data", "OI", "Telegram", "Other"):
            if not grouped[heading]:
                continue
            lines.append(f"{heading}:")
            lines.extend(grouped[heading])
        return "\n".join(lines)

    def _format_health(self):
        rows = self._service_heartbeat_rows()
        if not rows:
            return "Health\nNo heartbeat data available."

        lines = ["Health"]
        for row in rows:
            age = row["heartbeat_age"]
            if not row["running"]:
                state = "STOPPED"
            elif age is None:
                state = "RUNNING"
            elif age > Config.WATCHDOG_STALE_SECONDS:
                state = "STALE"
            else:
                state = "HEALTHY"
            instrument_suffix = f" | {row['instrument']}" if row["instrument"] else ""
            age_text = "unknown" if age is None else f"{age:.0f}s"
            lines.append(
                f"{row['service']}{instrument_suffix} | {state} | hb_age={age_text}"
            )
        return "\n".join(lines)

    def _format_signals(self):
        lines = ["Signals"]
        found = False

        for instrument in self.instruments:
            latest_signal = self.db_reader.fetch_latest_signal_issued(instrument)
            latest_monitor = self.db_reader.fetch_latest_trade_monitor_event(instrument)

            if not latest_signal:
                lines.append(f"{instrument}: NO SIGNAL")
                continue

            found = True
            signal_time = latest_signal["time"].strftime("%H:%M") if latest_signal["time"] else "unknown"
            score = (
                f"{latest_signal['score']:.0f}"
                if latest_signal["score"] is not None
                else "na"
            )
            base = (
                f"{instrument}: {latest_signal['signal']} | {signal_time} | "
                f"score {score} | Q{latest_signal['quality']} | {latest_signal['setup_type']}"
            )
            reason = latest_signal.get("reason") or ""
            ha_match = HA_BIAS_RE.search(reason)
            if ha_match:
                base += f" | HA_{ha_match.group(1)}"
            if latest_monitor and latest_monitor.get("guidance"):
                base += f" | {latest_monitor['guidance']}"
            lines.append(base)

        if not found and all(line.endswith("NO SIGNAL") for line in lines[1:]):
            return "\n".join(lines)
        return "\n".join(lines)

    def _run_local_tool(self, relative_path, *args):
        command = [self.python_executable, os.path.join(REPO_ROOT, relative_path), *args]
        return subprocess.run(
            command,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def _start_local_tool(self, relative_path, *args):
        command = [self.python_executable, os.path.join(REPO_ROOT, relative_path), *args]
        process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return process

    def _pkill_patterns(self, patterns):
        outputs = []
        for pattern in patterns:
            try:
                result = subprocess.run(
                    ["pkill", "-f", pattern],
                    cwd=REPO_ROOT,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if result.returncode == 0:
                    outputs.append(f"killed: {pattern}")
            except Exception as exc:
                outputs.append(f"pkill failed ({pattern}): {exc}")
        return outputs

    def _build_stop_preview(self):
        previews = []
        for label, relative_path in (
            ("signals", "tools/run_signals.py"),
            ("collectors", "tools/run_collectors.py"),
        ):
            previews.append(f"{label}: stop requested")
        return "Stop requested\n" + "\n".join(previews)

    def _parse_command_args(self, text):
        tokens = (text or "").strip().split()
        if len(tokens) <= 1:
            return []
        return [token.upper() for token in tokens[1:]]

    def _valid_instruments(self, requested):
        if not requested:
            return list(DEFAULT_INSTRUMENTS)
        valid = [instrument for instrument in requested if instrument in DEFAULT_INSTRUMENTS]
        return valid or list(DEFAULT_INSTRUMENTS)

    def _build_start_preview(self, label, instruments):
        return f"{label} start requested\nInstruments: {', '.join(instruments)}"

    def _execute_start_signals(self, instruments):
        instruments = self._valid_instruments(instruments)
        if not self.time_utils.is_market_open():
            now_ist = self.time_utils.now_ist().strftime("%Y-%m-%d %H:%M:%S")
            return [f"signals: not started (market closed at {now_ist} IST)"]
        process = self._start_local_tool("tools/run_signals.py", "start", "--instruments", *instruments)
        return [f"signals: started launcher pid={process.pid} for {', '.join(instruments)}"]

    def _execute_stop_signals(self):
        outputs = []
        try:
            result = self._run_local_tool("tools/run_signals.py", "stop")
            detail = (result.stdout or result.stderr or "").strip()
            outputs.append(f"signals: {detail or 'stop requested'}")
        except Exception as exc:
            outputs.append(f"signals: stop failed ({exc})")
        outputs.extend(self._pkill_patterns(["services/signal_service.py", "tools/run_signals.py monitor"]))
        return outputs

    def _execute_start_data(self, instruments):
        instruments = self._valid_instruments(instruments)
        if not self.time_utils.is_market_open():
            now_ist = self.time_utils.now_ist().strftime("%Y-%m-%d %H:%M:%S")
            return [f"collectors: not started (market closed at {now_ist} IST)"]
        process = self._start_local_tool("tools/run_collectors.py", "start", "--instruments", *instruments)
        return [f"collectors: started launcher pid={process.pid} for {', '.join(instruments)}"]

    def _execute_stop_data(self):
        outputs = []
        try:
            result = self._run_local_tool("tools/run_collectors.py", "stop")
            detail = (result.stdout or result.stderr or "").strip()
            outputs.append(f"collectors: {detail or 'stop requested'}")
        except Exception as exc:
            outputs.append(f"collectors: stop failed ({exc})")
        outputs.extend(self._pkill_patterns(["services/data_collector.py", "services/oi_collector.py", "tools/run_collectors.py monitor"]))
        return outputs

    def _execute_stop(self):
        outputs = []
        for label, relative_path in (
            ("signals", "tools/run_signals.py"),
            ("collectors", "tools/run_collectors.py"),
        ):
            try:
                result = self._run_local_tool(relative_path, "stop")
                detail = (result.stdout or result.stderr or "").strip()
                outputs.append(f"{label}: {detail or 'stop requested'}")
            except Exception as exc:
                outputs.append(f"{label}: stop failed ({exc})")

        fallback_outputs = self._pkill_patterns(
            [
                "services/signal_service.py",
                "services/data_collector.py",
                "services/oi_collector.py",
                "tools/run_signals.py start",
                "tools/run_signals.py monitor",
                "tools/run_collectors.py",
                "tools/run_collectors.py monitor",
                "tools/run_telegram.py",
                "services/telegram_bot_service.py",
            ]
        )
        outputs.extend(fallback_outputs)

        self.running = False
        return outputs

    def _execute_shutdown(self):
        outputs = self._execute_stop()
        try:
            subprocess.Popen(
                [
                    "osascript",
                    "-e",
                    'tell application "System Events" to shut down',
                ],
                cwd=REPO_ROOT,
            )
            outputs.append("shutdown: requested")
        except Exception as exc:
            outputs.append(f"shutdown: failed ({exc})")
        return outputs

    def _handle_command(self, text):
        command = (text or "").strip().split()[0].lower()
        command_args = self._parse_command_args(text)
        if command == "/status":
            return {"reply": self._format_status(), "post_action": None}
        if command == "/health":
            return {"reply": self._format_health(), "post_action": None}
        if command == "/signals":
            return {"reply": self._format_signals(), "post_action": None}
        if command == "/start_signal":
            instruments = self._valid_instruments(command_args)
            return {
                "reply": self._build_start_preview("Signal", instruments),
                "post_action": {"type": "start_signal", "instruments": instruments},
            }
        if command == "/stop_signal":
            return {"reply": "Signal stop requested", "post_action": {"type": "stop_signal"}}
        if command == "/start_data":
            instruments = self._valid_instruments(command_args)
            return {
                "reply": self._build_start_preview("Data", instruments),
                "post_action": {"type": "start_data", "instruments": instruments},
            }
        if command == "/stop_data":
            return {"reply": "Data stop requested", "post_action": {"type": "stop_data"}}
        if command == "/stop":
            return {"reply": self._build_stop_preview(), "post_action": "stop"}
        if command == "/shutdown":
            return {"reply": self._build_stop_preview() + "\nshutdown: requested", "post_action": "shutdown"}
        return {
            "reply": (
                "Supported commands:\n"
                "/status\n"
                "/health\n"
                "/signals\n"
                "/start_signal [NIFTY BANKNIFTY SENSEX]\n"
                "/stop_signal\n"
                "/start_data [NIFTY BANKNIFTY SENSEX]\n"
                "/stop_data\n"
                "/stop\n"
                "/shutdown"
            ),
            "post_action": None,
        }

    def _process_update(self, update):
        self.offset = max(self.offset, update["update_id"] + 1)
        message = update.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id")) if chat.get("id") is not None else None
        text = message.get("text")

        if not chat_id or not text:
            return
        if chat_id != self.allowed_chat_id:
            return

        result = self._handle_command(text)
        post_action = result.get("post_action")
        if isinstance(post_action, dict):
            action_type = post_action.get("type")
            outputs = []
            if action_type == "start_signal":
                outputs = self._execute_start_signals(post_action.get("instruments"))
            elif action_type == "stop_signal":
                outputs = self._execute_stop_signals()
            elif action_type == "start_data":
                outputs = self._execute_start_data(post_action.get("instruments"))
            elif action_type == "stop_data":
                outputs = self._execute_stop_data()
            reply = "\n".join(outputs) if outputs else result["reply"]
            self._send_message(chat_id, reply)
            return

        self._send_message(chat_id, result["reply"])
        if post_action == "stop":
            self._execute_stop()
        elif post_action == "shutdown":
            self._execute_shutdown()

    def run_forever(self):
        self._validate()
        self.running = True
        self._sync_offset_to_latest()
        print("[Telegram Command Service] Started")
        print("[Telegram Command Service] Allowed chat:", self.allowed_chat_id)
        print("[Telegram Command Service] Startup offset:", self.offset)
        print("[Telegram Command Service] Commands: /status /health /signals /stop /shutdown")

        while self.running:
            try:
                updates = self._telegram_get(
                    "getUpdates",
                    {
                        "timeout": 30,
                        "offset": self.offset,
                    },
                )
                for update in updates:
                    self._process_update(update)
            except KeyboardInterrupt:
                print("\n[Telegram Command Service] Shutdown requested")
                self.running = False
            except Exception as exc:
                print(f"[Telegram Command Service] Error: {exc}")
                time.sleep(3)


def main():
    parser = argparse.ArgumentParser(description="Telegram read-only command listener")
    parser.add_argument("--instruments", nargs="+", default=DEFAULT_INSTRUMENTS)
    args = parser.parse_args()

    service = TelegramCommandService(instruments=args.instruments)
    try:
        service.run_forever()
    except KeyboardInterrupt:
        print("\n[Telegram Command Service] Stopped")


if __name__ == "__main__":
    main()
