#!/usr/bin/env python3
"""
Scalp Signal Service Management Tool

This script allows you to:
- Start scalp signal services
- Stop scalp signal services
- Check scalp service status
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils.log_utils import cleanup_old_logs


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INSTRUMENTS = ["NIFTY", "BANKNIFTY", "SENSEX"]
STATE_FILE = REPO_ROOT / "data" / "run_scalp_state.json"


class ScalpServiceManager:
    def __init__(self, python_executable=None):
        self.python_executable = python_executable or sys.executable
        self.processes = []
        cleanup_old_logs(retention_days=7)

    def _load_state(self):
        if not STATE_FILE.exists():
            return {"services": []}
        try:
            with STATE_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"services": []}

    def _save_state(self):
        state = {
            "services": [
                {
                    "instrument": entry["instrument"],
                    "pid": entry["process"].pid,
                }
                for entry in self.processes
                if entry["process"].poll() is None
            ]
        }
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)

    def _clear_state(self):
        if STATE_FILE.exists():
            STATE_FILE.unlink()

    @staticmethod
    def _pid_is_running(pid):
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _load_processes_from_state(self):
        state = self._load_state()
        loaded = []
        stale_found = False
        for service in state.get("services", []):
            pid = service.get("pid")
            instrument = service.get("instrument")
            if not pid or not instrument:
                stale_found = True
                continue
            if not self._pid_is_running(pid):
                stale_found = True
                continue
            loaded.append({"instrument": instrument, "process": None, "pid": pid})

        if stale_found:
            if loaded:
                with STATE_FILE.open("w", encoding="utf-8") as f:
                    json.dump(
                        {"services": [{"instrument": e["instrument"], "pid": e["pid"]} for e in loaded]},
                        f,
                        indent=2,
                        sort_keys=True,
                    )
            else:
                self._clear_state()
        return loaded

    def _service_path(self):
        return REPO_ROOT / "services" / "scalp_signal_service.py"

    def start(self, instruments):
        existing = self._load_processes_from_state()
        if existing:
            print("[Scalp Manager] Scalp services are already running:")
            for entry in existing:
                print(f"[Scalp Manager] {entry['instrument']} (pid={entry['pid']})")
            print("[Scalp Manager] Stop them first if you want a fresh restart.")
            return

        normalized_instruments = [instrument.upper() for instrument in instruments]
        print(f"[Scalp Manager] Starting scalp services for: {', '.join(normalized_instruments)}")

        for instrument in normalized_instruments:
            log_file = REPO_ROOT / "logs" / time.strftime("%Y%m%d") / f"scalp_{instrument.lower()}.log"
            log_file.parent.mkdir(parents=True, exist_ok=True)

            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"\n{'=' * 50}\n")
                f.write(f"Starting Scalp Service - {time.strftime('%H:%M:%S')}\n")
                f.write(f"{'=' * 50}\n\n")

            command = [
                self.python_executable,
                "-u",
                str(self._service_path()),
                "--instrument",
                instrument,
            ]
            log_handle = open(log_file, "a", encoding="utf-8")
            process = subprocess.Popen(
                command,
                cwd=str(REPO_ROOT),
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
                close_fds=True,
                start_new_session=True,
            )
            log_handle.close()
            self.processes.append({"instrument": instrument, "process": process, "pid": process.pid})
            print(f"[Scalp Manager] {instrument} started (pid={process.pid}) | log={log_file}")
            time.sleep(0.3)

        self._save_state()
        print("[Scalp Manager] All scalp services started")

    def stop(self):
        loaded_processes = self.processes
        if not loaded_processes:
            loaded_processes = self._load_processes_from_state()
            if loaded_processes:
                self.processes = loaded_processes

        if not self.processes:
            print("[Scalp Manager] No scalp services running")
            return

        print("[Scalp Manager] Stopping scalp services...")
        for entry in self.processes:
            process = entry.get("process")
            pid = entry.get("pid") or (process.pid if process else None)
            if process and process.poll() is not None:
                continue
            try:
                print(f"  • {entry['instrument']} (pid={pid})")
                if process:
                    process.terminate()
                elif pid:
                    os.kill(pid, signal.SIGTERM)
            except Exception as e:
                print(f"  ! Error stopping {entry['instrument']}: {e}")

        deadline = time.time() + 10
        while time.time() < deadline:
            if all(
                (entry.get("process") and entry["process"].poll() is not None)
                or (entry.get("pid") and not self._pid_is_running(entry["pid"]))
                for entry in self.processes
            ):
                break
            time.sleep(0.25)

        for entry in self.processes:
            process = entry.get("process")
            pid = entry.get("pid") or (process.pid if process else None)
            still_running = False
            if process:
                still_running = process.poll() is None
            elif pid:
                still_running = self._pid_is_running(pid)
            if still_running:
                print(f"  • Force killing {entry['instrument']} (pid={pid})")
                if process:
                    process.kill()
                elif pid:
                    os.kill(pid, signal.SIGKILL)

        self.processes = []
        self._clear_state()
        print("[Scalp Manager] All scalp services stopped")

    def status(self):
        entries = self.processes or self._load_processes_from_state()
        if not entries:
            print("[Scalp Manager] No scalp services running")
            return

        for entry in entries:
            process = entry.get("process")
            pid = entry.get("pid") or (process.pid if process else None)
            running = process.poll() is None if process else self._pid_is_running(pid)
            if running:
                print(f"[Scalp Manager] {entry['instrument']} is running (pid={pid})")
            else:
                print(f"[Scalp Manager] {entry['instrument']} is not running (pid={pid})")


def main():
    parser = argparse.ArgumentParser(description="Scalp Signal Service Manager")
    parser.add_argument("command", choices=["start", "stop", "status"])
    parser.add_argument(
        "--instruments",
        nargs="+",
        default=DEFAULT_INSTRUMENTS,
        help=f"Instruments to trade (default: {' '.join(DEFAULT_INSTRUMENTS)})",
    )
    args = parser.parse_args()

    manager = ScalpServiceManager()
    if args.command == "start":
        manager.start(args.instruments)
    elif args.command == "stop":
        manager.stop()
    else:
        manager.status()


if __name__ == "__main__":
    main()
