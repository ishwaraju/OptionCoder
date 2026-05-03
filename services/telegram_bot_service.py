"""
Telegram command listener for bot commands.

Supported commands:
- /status
- /health
- /signals
- /start_signal
- /start_signal_force
- /stop_signal
- /start_data
- /start_data_force
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
from shared.utils.log_utils import build_log_path, build_instrument_log_path, cleanup_old_logs
from shared.utils.time_utils import TimeUtils
from datetime import datetime
import pytz
import logging


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_INSTRUMENTS = ["NIFTY", "BANKNIFTY", "SENSEX"]
HA_BIAS_RE = re.compile(r"ha_bias=([A-Z_]+)")
REASON_TAG_RE = re.compile(r"([a-zA-Z_]+)=([^|]+)")


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
        cleanup_old_logs(retention_days=7)
        
        # Setup logging
        self.ist = pytz.timezone('Asia/Kolkata')
        self._setup_logger()

    def _setup_logger(self):
        """Setup file logger for Telegram bot"""
        log_file = build_log_path("telegram_bot_service")
        
        # Create logger
        self.logger = logging.getLogger("telegram_bot")
        self.logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        self.logger.handlers = []
        
        # File handler
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        
        # Formatter with IST timestamp
        formatter = logging.Formatter(
            '[%(asctime)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        formatter.converter = self._ist_time_converter
        fh.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        self._log("Telegram Bot Logger initialized")
        self._log(f"Log file: {log_file}")

    def _ist_time_converter(self, *args):
        """Convert time to IST for logging"""
        return datetime.now(self.ist).timetuple()

    def _log(self, message, level="INFO"):
        """Log message with IST timestamp"""
        if self.logger:
            if level == "INFO":
                self.logger.info(message)
            elif level == "ERROR":
                self.logger.error(message)
            elif level == "WARNING":
                self.logger.warning(message)
        # Print only for interactive foreground runs; launcher already redirects stdout to the same log file.
        if sys.stdout.isatty():
            ist_time = datetime.now(self.ist).strftime('%H:%M:%S')
            print(f"[{ist_time}] [Telegram Bot] {message}")

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
        """Send message to Telegram with logging"""
        try:
            self._telegram_post(
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": text,
                },
            )
            # Log successful send
            msg_preview = text[:50].replace('\n', ' ')
            self._log(f"✅ Message sent to {chat_id}: {msg_preview}...")
        except Exception as e:
            self._log(f"❌ Failed to send message to {chat_id}: {e}", level="ERROR")
            raise

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

    def _pid_is_running(self, pid):
        """Check if a specific PID is still running"""
        try:
            result = subprocess.run(
                ["ps", "-p", str(pid), "-o", "pid="],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.returncode == 0 and str(pid) in result.stdout
        except Exception:
            return False

    def _cleanup_stopped_heartbeats(self):
        """Remove heartbeat files for processes that are no longer running"""
        import glob
        cleaned = 0
        for heartbeat_file in glob.glob(os.path.join(REPO_ROOT, "data", "heartbeat", "*.json")):
            try:
                heartbeat = self._load_json(heartbeat_file)
                if not heartbeat:
                    continue
                status = heartbeat.get("status", {})
                pid = status.get("pid")
                if pid and not self._pid_is_running(pid):
                    # Process is not running, remove stale heartbeat
                    os.remove(heartbeat_file)
                    cleaned += 1
            except Exception:
                pass
        return cleaned

    def _heartbeat_process_info(self, service_key, instrument=None):
        instrument_upper = (instrument or "").upper()
        if service_key == "runtime":
            pids = self._find_running_pids("main.py")
            return {"running": bool(pids), "pids": pids}
        if service_key == "signal_service":
            pids = self._find_running_pids(f"services/signal_service.py --instrument {instrument_upper}")
            return {"running": bool(pids), "pids": pids}
        if service_key in {"data_collector", "oi_collector"}:
            # Try to get PID from heartbeat data first
            heartbeat_file = f"data/heartbeat/{service_key}_{instrument_upper}.json"
            heartbeat = self._load_json(heartbeat_file)
            if heartbeat and "status" in heartbeat and "pid" in heartbeat["status"]:
                pid = heartbeat["status"]["pid"]
                # Verify if the PID is actually running
                if self._pid_is_running(pid):
                    return {"running": True, "pids": [pid]}
                else:
                    # PID exists in heartbeat but process not running yet - still return the PID
                    return {"running": False, "pids": [pid]}

            # Fallback to process list search
            pids = self._find_running_pids(f"services/{service_key}.py --instrument {instrument_upper}")
            return {"running": bool(pids), "pids": pids}
        if service_key == "telegram_bot_service":
            return {"running": True, "pids": [os.getpid()]}
        return {"running": False, "pids": []}

    def _collector_launcher_waiting(self):
        pids = self._find_running_pids("tools/run_collectors.py start")
        return {"waiting": bool(pids), "pids": pids}

    def _service_heartbeat_rows(self):
        rows = []
        now = time.time()
        seen = set()
        for heartbeat_file in sorted(glob.glob("data/heartbeat/*.json")):
            heartbeat = self._load_json(heartbeat_file)
            if not heartbeat:
                continue
            status = heartbeat.get("status", {})
            service_key = status.get("service") or os.path.basename(heartbeat_file).replace(".json", "")
            instrument = status.get("instrument")
            process_info = self._heartbeat_process_info(service_key, instrument)
            heartbeat_pid = status.get("pid")
            if heartbeat_pid and not process_info["running"]:
                try:
                    heartbeat_pid = int(heartbeat_pid)
                    if self._pid_is_running(heartbeat_pid):
                        process_info = {"running": True, "pids": [heartbeat_pid]}
                except Exception:
                    pass
            launcher_wait = (
                self._collector_launcher_waiting()
                if service_key in {"data_collector", "oi_collector"}
                else {"waiting": False, "pids": []}
            )
            phase = status.get("phase")
            if launcher_wait["waiting"] and not process_info["running"]:
                phase = "waiting"
            seen.add((service_key, instrument))
            rows.append(
                {
                    "service": service_key,
                    "phase": phase,
                    "status": status,
                    "instrument": instrument,
                    "heartbeat_age": None if heartbeat.get("epoch") is None else max(0.0, now - heartbeat["epoch"]),
                    "running": process_info["running"],
                    "pids": process_info["pids"],
                    "launcher_waiting": launcher_wait["waiting"],
                    "launcher_pids": launcher_wait["pids"],
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
            launcher_wait = (
                self._collector_launcher_waiting()
                if service_key in {"data_collector", "oi_collector"}
                else {"waiting": False, "pids": []}
            )
            rows.append(
                {
                    "service": service_key,
                    "phase": "waiting" if launcher_wait["waiting"] and not process_info["running"] else "no_heartbeat",
                    "status": {},
                    "instrument": instrument,
                    "heartbeat_age": None,
                    "running": process_info["running"],
                    "pids": process_info["pids"],
                    "launcher_waiting": launcher_wait["waiting"],
                    "launcher_pids": launcher_wait["pids"],
                }
            )
        return rows

    def _format_status(self):
        # Clean up stale heartbeats first to ensure accurate status
        self._cleanup_stopped_heartbeats()
        rows = self._service_heartbeat_rows()
        if not rows:
            return "📊 Service Status\n=================\n\nNo service heartbeat files found."

        # Group services by category
        signal_services = []
        data_services = []
        oi_services = []
        communication_services = []
        
        for row in rows:
            age = "unknown" if row["heartbeat_age"] is None else f"{row['heartbeat_age']:.0f}s"
            lifecycle = self._classify_service_state(row, for_health=False)
            label = row["instrument"] or row["service"]
            
            # Get status emoji
            if lifecycle == "RUNNING":
                status_emoji = "💚"
            elif lifecycle in ["IDLE", "PAUSED", "STALE", "WAITING"]:
                status_emoji = "💛"
            else:
                status_emoji = "💔"
            
            # Add PID if available
            pids = row.get("pids") or []

            # Always try to get PID from heartbeat status first
            status = row.get("status") or {}
            heartbeat_pid = status.get("pid")

            # Check which PIDs are actually running
            actual_pids = [pid for pid in pids if self._pid_is_running(pid)]

            # Only show PID if process is actually running or in waiting/starting state
            # Don't show PID for stopped services (stale heartbeat data)
            if actual_pids:
                pid_text = f" pid={actual_pids[0]}"
            elif heartbeat_pid and lifecycle not in {"STOPPED", "💔 STOPPED"}:
                # Only show heartbeat PID for non-stopped states
                pid_text = f" pid={heartbeat_pid}"
            else:
                pid_text = ""

            service_line = f"  {label} {status_emoji} {lifecycle}{pid_text} ({age})"
            
            if row["service"] == "signal_service":
                signal_services.append(service_line)
            elif row["service"] == "scalp_signal_service":
                # Add scalp service with ⚡ emoji
                scalp_line = f"  {label} ⚡ {status_emoji} {lifecycle}{pid_text} ({age})"
                signal_services.append(scalp_line)
            elif row["service"] in ["data_collector", "runtime"]:
                data_services.append(service_line)
            elif row["service"] == "oi_collector":
                oi_services.append(service_line)
            elif row["service"] == "telegram_bot_service":
                communication_services.append(service_line)
        
        # Build output with section headers
        lines = ["📊 Service Status", "================="]
        
        if signal_services:
            lines.append("🎯 Signals")
            lines.extend(sorted(signal_services))
            lines.append("")
        
        if data_services:
            lines.append("📈 Data Collection")
            lines.extend(sorted(data_services))
            lines.append("")
        
        if oi_services:
            lines.append("🧠 OI Collection")
            lines.extend(sorted(oi_services))
            lines.append("")
        
        if communication_services:
            lines.append("🤖 Communication")
            lines.extend(sorted(communication_services))
            lines.append("")
        
        # Add summary
        total_services = len(rows)
        running_services = sum(1 for row in rows if self._classify_service_state(row, for_health=False) == "RUNNING")
        lines.append(f"📈 {running_services}/{total_services} services running")
        
        return "\n".join(lines)

    def _format_health(self):
        # Clean up stale heartbeats first to ensure accurate status
        self._cleanup_stopped_heartbeats()
        rows = self._service_heartbeat_rows()
        if not rows:
            return "📊 Health Status\n==============\n\nNo heartbeat data available."

        service_icons = {
            "HEALTHY": "💚",
            "RUNNING": "💚",
            "IDLE": "💛",
            "PAUSED": "💛",
            "STALE": "💛",
            "WAITING": "💛",
            "STOPPED": "💔",
        }
        
        # Group services by instrument
        instrument_groups = {}
        global_services = []
        
        for row in rows:
            # Apply same PID verification as _format_verified_status
            pids = row.get("pids") or []
            actual_pids = [pid for pid in pids if self._pid_is_running(pid)]
            row["pids"] = actual_pids
            row["running"] = len(actual_pids) > 0
            
            instrument = row.get("instrument")
            if instrument:
                if instrument not in instrument_groups:
                    instrument_groups[instrument] = []
                instrument_groups[instrument].append(row)
            else:
                global_services.append(row)
        
        lines = ["📊 Health Status", "=============="]
        
        # Sort instruments
        for instrument in sorted(instrument_groups.keys()):
            services = instrument_groups[instrument]
            
            # Calculate instrument overall status
            running_count = 0
            total_count = len(services)
            
            for service_row in services:
                state = self._classify_service_state(service_row, for_health=True)
                if state in ["HEALTHY", "RUNNING"]:
                    running_count += 1
            
            # Instrument status emoji
            if running_count == total_count:
                instrument_emoji = "🟢"
            elif running_count > 0:
                instrument_emoji = "🟡"
            else:
                instrument_emoji = "🔴"
            
            lines.append(f"{instrument_emoji} {instrument}")
            
            # Sort services within instrument
            for service_row in sorted(services, key=lambda x: x["service"]):
                age = service_row["heartbeat_age"]
                state = self._classify_service_state(service_row, for_health=True)
                status_emoji = service_icons.get(state, "•")
                age_text = "unknown" if age is None else f"{age:.0f}s"
                status_text = "RUNNING" if state in ["HEALTHY", "RUNNING"] else state
                lines.append(f"  {service_row['service']} {status_emoji} {status_text} ({age_text})")
            
            lines.append("")  # Empty line between instruments
        
        # Add global services
        if global_services:
            # Calculate global status
            running_count = 0
            total_count = len(global_services)
            
            for service_row in global_services:
                state = self._classify_service_state(service_row, for_health=True)
                if state in ["HEALTHY", "RUNNING"]:
                    running_count += 1
            
            global_emoji = "🟢" if running_count == total_count else "🔴"
            lines.append(f"{global_emoji} Global")
            
            for service_row in sorted(global_services, key=lambda x: x["service"]):
                age = service_row["heartbeat_age"]
                state = self._classify_service_state(service_row, for_health=True)
                status_emoji = service_icons.get(state, "•")
                age_text = "unknown" if age is None else f"{age:.0f}s"
                status_text = "RUNNING" if state in ["HEALTHY", "RUNNING"] else state
                lines.append(f"  {service_row['service']} {status_emoji} {status_text} ({age_text})")
        
        # Add summary
        total_services = len(rows)
        running_services = sum(1 for row in rows if self._classify_service_state(row, for_health=True) in ["HEALTHY", "RUNNING"])
        lines.append(f"\n📈 {running_services}/{total_services} services running")
        
        return "\n".join(lines)

    def _classify_service_state(self, row, for_health=False):
        age = row["heartbeat_age"]
        phase = row.get("phase") or ""

        if phase == "waiting":
            return "WAITING"

        # Check phases that indicate service is starting/initializing even if not fully running
        if phase in {"starting", "boot"}:
            return "WAITING" if for_health else "WAITING"

        if not row["running"]:
            return "STOPPED"

        if row["service"] == "telegram_bot_service":
            return "RUNNING" if for_health else "RUNNING"

        if age is None:
            return "RUNNING" if for_health else "RUNNING"

        if age > Config.WATCHDOG_STALE_SECONDS:
            return "STALE"

        if row["service"] == "oi_collector":
            status = row.get("status") or {}
            if (
                phase == "heartbeat"
                and (status.get("oi_snapshots_collected") or 0) == 0
                and (status.get("option_bands_collected") or 0) == 0
            ):
                return "IDLE"

        if phase in {"data_pause", "no_heartbeat"}:
            return "IDLE"
        if phase in {"heartbeat", "loop_alive"}:
            return "HEALTHY" if for_health else "RUNNING"
        if phase in {"starting"}:
            return "IDLE"
        return "HEALTHY" if for_health else "RUNNING"

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
            reason = latest_signal.get("reason") or ""
            tags = self._parse_reason_tags(reason)
            regime = tags.get("regime") or "na"
            grade = tags.get("signal_grade") or latest_signal.get("quality") or "na"
            base = (
                f"{instrument}: {latest_signal['signal']} | {signal_time} | "
                f"score {score} | G{grade} | {latest_signal['setup_type']} | "
                f"{latest_signal.get('time_regime') or 'na'}/{regime}"
            )
            ha_match = HA_BIAS_RE.search(reason)
            if ha_match:
                base += f" | HA_{ha_match.group(1)}"
            if latest_monitor and latest_monitor.get("guidance"):
                base += f" | {latest_monitor['guidance']}"
            lines.append(base)
            levels = []
            if latest_signal.get("price") is not None:
                levels.append(f"buy {latest_signal['price']:.2f}")
            if latest_signal.get("invalidate_price") is not None:
                levels.append(f"sl {latest_signal['invalidate_price']:.2f}")
            if latest_signal.get("first_target_price") is not None:
                levels.append(f"t1 {latest_signal['first_target_price']:.2f}")
            if levels:
                lines.append(f"  {' | '.join(levels)}")
            why = self._signal_reason_summary(reason, latest_signal.get("confidence_summary"))
            if why:
                lines.append(f"  why: {why}")

        if not found and all(line.endswith("NO SIGNAL") for line in lines[1:]):
            return "\n".join(lines)
        return "\n".join(lines)

    @staticmethod
    def _parse_reason_tags(reason):
        tags = {}
        if not reason:
            return tags
        for key, value in REASON_TAG_RE.findall(reason):
            tags[key.strip()] = value.strip()
        return tags

    def _signal_reason_summary(self, reason, confidence_summary=None):
        if not reason:
            return confidence_summary
        primary = reason.split("|")[0].strip()
        tags = self._parse_reason_tags(reason)
        bits = []
        if primary:
            bits.append(primary)
        if confidence_summary:
            bits.append(confidence_summary)
        elif tags.get("confidence_summary"):
            bits.append(tags["confidence_summary"])
        if tags.get("pressure_conflict_level"):
            bits.append(f"pressure {tags['pressure_conflict_level']}")
        return " | ".join(bits[:3]) if bits else None

    def _format_scalp_signals(self):
        """Format latest scalp signals (1m timeframe)"""
        lines = ["⚡ Scalp Signals (1m)"]
        found = False

        try:
            for instrument in self.instruments:
                # Fetch latest scalp signal from database
                latest = self.db_reader.fetch_latest_scalp_signal(instrument)
                
                if not latest:
                    lines.append(f"{instrument}: NO SCALP SIGNAL")
                    continue
                
                found = True
                signal_time = latest["time"].strftime('%H:%M') if latest["time"] else "unknown"
                score = f"{latest['score']:.0f}" if latest['score'] is not None else "na"
                
                # Format with ⚡ emoji for scalp
                base = (
                    f"⚡ {instrument}: {latest['signal']} | {signal_time} | "
                    f"Score: {score} | "
                    f"Target: +{latest.get('target_price', 0):.0f} | "
                    f"SL: -{latest.get('stop_loss', 0):.0f}"
                )
                
                # Add status
                status = latest.get('status', 'ACTIVE')
                if status == 'ACTIVE':
                    base += " | 🟢 ACTIVE"
                elif status == 'EXITED':
                    pnl = latest.get('pnl', 0)
                    pnl_emoji = "🟢" if pnl > 0 else "🔴"
                    base += f" | {pnl_emoji} P&L: {pnl:+.0f}"
                
                lines.append(base)
                
        except Exception as e:
            lines.append(f"Error fetching scalp signals: {e}")
        
        if not found and all("NO SCALP" in line for line in lines[1:]):
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
        command = [self.python_executable, "-u", os.path.join(REPO_ROOT, relative_path), *args]
        
        # Use instrument-specific logging for run_collectors and run_signals
        tool_name = os.path.splitext(os.path.basename(relative_path))[0]
        if tool_name in ["run_collectors", "run_signals"]:
            # For these tools, use the main log file (they create instrument-specific logs internally)
            log_path = build_log_path(tool_name)
        else:
            log_path = build_log_path(tool_name)
            
        log_handle = open(log_path, "a", encoding="utf-8")
        process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        log_handle.close()
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
        rows = self._service_heartbeat_rows()

        signal_rows = []
        data_rows = []
        oi_rows = []

        for row in rows:
            if not row.get("running"):
                continue
            instrument = row.get("instrument") or row["service"]
            pids = row.get("pids") or []
            pid_text = f" (pid={pids[0]})" if pids else ""
            line = f"  {instrument}{pid_text}"
            if row["service"] == "signal_service":
                signal_rows.append(line)
            elif row["service"] == "data_collector":
                data_rows.append(line)
            elif row["service"] == "oi_collector":
                oi_rows.append(line)

        lines = ["🛑 Stop Requested", "================"]

        lines.append("🎯 Signals")
        if signal_rows:
            lines.extend(sorted(signal_rows))
        else:
            lines.append("  😴 No signal services running")
        lines.append("")

        lines.append("📈 Data Collection")
        if data_rows:
            lines.extend(sorted(data_rows))
        else:
            lines.append("  😴 No data collectors running")
        lines.append("")

        lines.append("🧠 OI Collection")
        if oi_rows:
            lines.extend(sorted(oi_rows))
        else:
            lines.append("  😴 No OI collectors running")

        total_running = len(signal_rows) + len(data_rows) + len(oi_rows)
        lines.append(f"\n📉 {total_running} running services will be stopped")
        return "\n".join(lines)

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

    @staticmethod
    def _extract_force_flag(tokens):
        return any(token.upper() in {"FORCE", "NOW", "TEST"} for token in (tokens or []))

    def _build_start_preview(self, label, instruments):
        return f"{label} start requested\nInstruments: {', '.join(instruments)}"

    def _get_latest_service_row(self, service_name, instrument):
        instrument_upper = (instrument or "").upper() or None
        matches = []
        for row in self._service_heartbeat_rows():
            if row["service"] != service_name:
                continue
            if (row.get("instrument") or None) != instrument_upper:
                continue
            matches.append(row)

        if not matches:
            return None

        matches.sort(
            key=lambda row: (
                0 if row.get("running") else 1,
                row["heartbeat_age"] if row["heartbeat_age"] is not None else 999999,
            )
        )
        return matches[0]

    def _format_verified_status(self, row, for_health=False):
        if not row:
            return "💔 STOPPED"

        state = self._classify_service_state(row, for_health=for_health)
        age = row.get("heartbeat_age")
        pids = row.get("pids") or []

        # Check if actual PIDs are running
        actual_pids = [pid for pid in pids if self._pid_is_running(pid)]

        age_text = "" if age is None else f" ({age:.0f}s)"

        # Get heartbeat PID from status if available
        status = row.get("status") or {}
        heartbeat_pid = status.get("pid")

        # Only show PID for non-stopped states
        if state == "STOPPED":
            pid_text = ""
        elif actual_pids:
            pid_text = f" pid={actual_pids[0]}"
        elif heartbeat_pid:
            pid_text = f" pid={heartbeat_pid}"
        else:
            pid_text = ""

        # Respect lifecycle state first
        if state == "WAITING":
            return f"💛 WAITING{pid_text}{age_text}"

        if state in {"IDLE", "PAUSED"}:
            return f"💛 IDLE{pid_text}{age_text}"

        if state == "STALE":
            return f"💛 STALE{pid_text}{age_text}"

        if state in {"HEALTHY", "RUNNING"}:
            if actual_pids:
                return f"💚 RUNNING{pid_text}{age_text}"
            return f"💛 WAITING{pid_text}{age_text}"

        return f"💔 STOPPED{age_text}"

    def _verify_started_services(self, service_names, instruments, retries=3, delay_seconds=1.5, require_all_pids=False):
        verified = {}
        pending = {(service_name, instrument.upper()) for service_name in service_names for instrument in instruments}
        total_services = len(pending)

        for attempt in range(retries):
            if attempt > 0:
                time.sleep(delay_seconds)

            # Log progress if still waiting
            if pending and require_all_pids:
                print(f"[TelegramBot] Waiting for {len(pending)}/{total_services} services to start... ({attempt}/{retries})")

            for service_name, instrument in list(pending):
                row = self._get_latest_service_row(service_name, instrument)
                if not row:
                    continue

                # Verify actual process is running
                pids = row.get("pids") or []
                actual_pids = []
                if pids:
                    for pid in pids:
                        if self._pid_is_running(pid):
                            actual_pids.append(pid)

                # Also get heartbeat PID from status if available
                status = row.get("status") or {}
                heartbeat_pid = status.get("pid")
                if heartbeat_pid and heartbeat_pid not in actual_pids:
                    # Keep heartbeat PID for display even if process state is waiting
                    pass  # We'll handle this below

                # Update row with actual PIDs, but also include heartbeat PID if no running PIDs
                # This ensures we show the PID even when service is WAITING
                if actual_pids:
                    row["pids"] = actual_pids
                elif heartbeat_pid:
                    row["pids"] = [heartbeat_pid]
                else:
                    row["pids"] = []
                row["running"] = len(actual_pids) > 0

                verified[(service_name, instrument)] = row

                # If require_all_pids is True, wait for heartbeat PID to be present
                if require_all_pids:
                    if heartbeat_pid:
                        pending.discard((service_name, instrument))
                else:
                    state = self._classify_service_state(row, for_health=True)
                    if len(actual_pids) > 0 or state in {"HEALTHY", "RUNNING", "IDLE", "PAUSED", "WAITING"}:
                        pending.discard((service_name, instrument))

            if not pending:
                break

        for service_name, instrument in pending:
            verified[(service_name, instrument)] = self._get_latest_service_row(service_name, instrument)

        return verified

    def _build_verified_signal_start_response(self, instruments):
        verified = self._verify_started_services(
            ["signal_service"],
            instruments,
            retries=4,
            delay_seconds=1.0,
        )
        lines = ["🎯 Signal Start Result", "===================="]

        for instrument in instruments:
            row = verified.get(("signal_service", instrument))
            lines.append(f"  {instrument} {self._format_verified_status(row, for_health=True)}")

        return "\n".join(lines)

    def _build_verified_data_start_response(self, instruments):
        # Collectors are launched sequentially with a stagger (2s between each).
        # With 6 services (3 instruments x 2 services), it takes ~10s to spawn all.
        # We wait longer to ensure all heartbeat files are created with PIDs.
        verified = self._verify_started_services(
            ["data_collector", "oi_collector"],
            instruments,
            retries=20,  # Wait up to 30 seconds (20 x 1.5s)
            delay_seconds=1.5,
            require_all_pids=True,  # Don't return until ALL services have PIDs
        )
        lines = ["📊 Service Status", "=================", "📈 Data Collection"]

        for instrument in instruments:
            row = verified.get(("data_collector", instrument))
            lines.append(f"  {instrument} {self._format_verified_status(row, for_health=True)}")

        lines.append("")
        lines.append("🧠 OI Collection")
        for instrument in instruments:
            row = verified.get(("oi_collector", instrument))
            lines.append(f"  {instrument} {self._format_verified_status(row, for_health=True)}")

        return "\n".join(lines)

    def _build_already_running_signal_response(self, instruments):
        lines = ["🎯 Signal Status", "===================="]
        for instrument in instruments:
            row = self._get_latest_service_row("signal_service", instrument)
            lines.append(f"  {instrument} {self._format_verified_status(row, for_health=True)}")
        return "\n".join(lines)

    def _build_already_running_data_response(self, instruments):
        lines = ["📊 Service Status", "=================", "📈 Data Collection"]
        for instrument in instruments:
            row = self._get_latest_service_row("data_collector", instrument)
            lines.append(f"  {instrument} {self._format_verified_status(row, for_health=True)}")

        lines.append("")
        lines.append("🧠 OI Collection")
        for instrument in instruments:
            row = self._get_latest_service_row("oi_collector", instrument)
            lines.append(f"  {instrument} {self._format_verified_status(row, for_health=True)}")

        return "\n".join(lines)

    def _check_services_running(self, service_type, instruments):
        """Check if specified services are already running"""
        rows = self._service_heartbeat_rows()
        already_running = []
        
        for instrument in instruments:
            # Check if this service for this instrument is running
            for row in rows:
                if (row["service"] == service_type and 
                    row["instrument"] == instrument and 
                    row["running"]):
                    already_running.append(instrument)
                    break
        
        return already_running

    def _execute_start_signals(self, instruments, force=False):
        instruments = self._valid_instruments(instruments)
        
        # Check if signal services are already running
        already_running = self._check_services_running("signal_service", instruments)
        if already_running:
            return [self._build_already_running_signal_response(instruments)]
        
        # Always allow start - user is in control
        # Market closed services will automatically enter appropriate mode
        self._start_local_tool("tools/run_signals.py", "start", "--instruments", *instruments)
        return [self._build_verified_signal_start_response(instruments)]

    def _execute_stop_signals(self):
        outputs = []
        try:
            result = self._run_local_tool("tools/run_signals.py", "stop")
            detail = (result.stdout or result.stderr or "").strip()
        except Exception as exc:
            outputs.append(f"❌ signals: stop failed ({exc})")
            return outputs
        outputs.extend(self._pkill_patterns(["services/signal_service.py", "tools/run_signals.py monitor"]))
        
        # Build beautiful response like data stop
        response_lines = ["🛑 signals: stopped successfully ✅"]
        
        # Parse signal services from detail or get from running processes
        signal_services = []
        if detail and "•" in detail:
            # Extract instrument names from the detail output
            import re
            matches = re.findall(r'• (\w+) \(pid=(\d+)\)', detail)
            signal_services = [f"{instrument} ({pid})" for instrument, pid in matches]
        
        if signal_services:
            response_lines.append(f"  🚦 {len(signal_services)} signal services terminated:")
            response_lines.extend([f"    ⚡ {service}" for service in signal_services])
        else:
            response_lines.append("  😴 No signal services were running")
        
        return response_lines

    def _execute_start_data(self, instruments, force=False):
        instruments = self._valid_instruments(instruments)
        
        # Check if data collector and OI collector services are already running
        data_running = self._check_services_running("data_collector", instruments)
        oi_running = self._check_services_running("oi_collector", instruments)
        
        if data_running or oi_running:
            return [self._build_already_running_data_response(instruments)]
        
        extra_args = ["--skip-market-wait"] if force else []
        self._start_local_tool(
            "tools/run_collectors.py",
            "start",
            "--instruments",
            *instruments,
            *extra_args,
        )
        return [self._build_verified_data_start_response(instruments)]

    def _execute_stop_data(self):
        # Get current running services before stopping
        rows = self._service_heartbeat_rows()
        data_services = []
        oi_services = []
        
        for row in rows:
            if row["service"] == "data_collector" and row["running"]:
                data_services.append(f"  {row['instrument']} ({row.get('pids', ['unknown'])[0]})")
            elif row["service"] == "oi_collector" and row["running"]:
                oi_services.append(f"  {row['instrument']} ({row.get('pids', ['unknown'])[0]})")
        
        # Perform the actual stop
        outputs = []
        try:
            result = self._run_local_tool("tools/run_collectors.py", "stop")
            detail = (result.stdout or result.stderr or "").strip()
        except Exception as exc:
            outputs.append(f"collectors: stop failed ({exc})")
        outputs.extend(self._pkill_patterns(["services/data_collector.py", "services/oi_collector.py", "tools/run_collectors.py monitor", "tools/run_collectors.py start"]))
        
        # Build beautiful response with emojis
        response_lines = ["🛑 collectors: stopped successfully ✅"]
        
        if data_services:
            response_lines.append(f"  📊 {len(data_services)} data services terminated:")
            response_lines.extend([f"    ⚡ {service}" for service in data_services])
        
        if oi_services:
            response_lines.append(f"  📈 {len(oi_services)} OI services terminated:")
            response_lines.extend([f"    🎯 {service}" for service in oi_services])
        
        if not data_services and not oi_services:
            response_lines.append("  😴 No data services were running")
        
        # Clean up stale heartbeat files for stopped processes
        import time
        time.sleep(1)  # Give processes time to fully terminate
        cleaned = self._cleanup_stopped_heartbeats()
        if cleaned > 0:
            response_lines.append(f"  🧹 Cleaned up {cleaned} stale heartbeat files")
        
        return response_lines

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
        
        # Log command received
        self._log(f"📩 Command received: {command} (args: {command_args})")
        
        if command == "/status":
            self._log("Processing /status command")
            return {"reply": self._format_status(), "post_action": None}
        if command == "/health":
            self._log("Processing /health command")
            return {"reply": self._format_health(), "post_action": None}
        if command == "/signals":
            self._log("Processing /signals command")
            return {"reply": self._format_signals(), "post_action": None}
        if command == "/scalp":
            self._log("Processing /scalp command")
            return {"reply": self._format_scalp_signals(), "post_action": None}
        if command == "/start_signal":
            instruments = self._valid_instruments(command_args)
            force = self._extract_force_flag(command_args)
            return {
                "reply": self._build_start_preview("Signal", instruments),
                "post_action": {"type": "start_signal", "instruments": instruments, "force": force},
            }
        if command == "/stop_signal":
            return {"reply": "Signal stop requested", "post_action": {"type": "stop_signal"}}
        if command == "/start_data":
            instruments = self._valid_instruments(command_args)
            force = self._extract_force_flag(command_args)
            return {
                "reply": self._build_start_preview("Data", instruments),
                "post_action": {"type": "start_data", "instruments": instruments, "force": force},
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
                "/scalp\n"
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
            self._log(f"⚠️  Unauthorized chat: {chat_id} (allowed: {self.allowed_chat_id})")
            return

        # Log incoming message
        self._log(f"📨 Message from {chat_id}: {text[:50]}...")
        
        result = self._handle_command(text)
        post_action = result.get("post_action")
        if isinstance(post_action, dict):
            action_type = post_action.get("type")
            outputs = []
            if action_type == "start_signal":
                outputs = self._execute_start_signals(post_action.get("instruments"), force=bool(post_action.get("force")))
            elif action_type == "stop_signal":
                outputs = self._execute_stop_signals()
            elif action_type == "start_data":
                outputs = self._execute_start_data(post_action.get("instruments"), force=bool(post_action.get("force")))
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
        self._log("🚀 Telegram Command Service Started")
        self._log(f"📱 Allowed chat: {self.allowed_chat_id}")
        self._log(f"📊 Startup offset: {self.offset}")
        self._log("📝 Commands: /status /health /signals /scalp /stop /shutdown")

        while self.running:
            try:
                updates = self._telegram_get(
                    "getUpdates",
                    {
                        "timeout": 30,
                        "offset": self.offset,
                    },
                )
                if updates:
                    self._log(f"📩 Received {len(updates)} update(s)")
                for update in updates:
                    self._process_update(update)
            except KeyboardInterrupt:
                self._log("🛑 Shutdown requested via KeyboardInterrupt")
                self.running = False
            except Exception as exc:
                error_str = str(exc)
                # Handle 409 Conflict - another bot instance running
                if "409" in error_str or "Conflict" in error_str:
                    self._log(f"⚠️  409 Conflict: Another bot instance running. Waiting 10s...", level="WARNING")
                    time.sleep(10)
                    continue
                # Handle timeout - network issues
                elif "timeout" in error_str.lower():
                    self._log(f"⏱️  Network timeout. Retrying in 5s...", level="WARNING")
                    time.sleep(5)
                    continue
                else:
                    self._log(f"❌ Error in main loop: {exc}", level="ERROR")
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
