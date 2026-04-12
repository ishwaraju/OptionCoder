"""
Launch data_collector and oi_collector for multiple instruments.

Signal service is intentionally excluded so it can be started and stopped
independently during manual trading.
"""

import argparse
import json
import signal
import subprocess
import sys
import time
import os
from pathlib import Path

# Add current directory to Python path (same as other services)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils.time_utils import TimeUtils


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INSTRUMENTS = ["NIFTY", "BANKNIFTY", "SENSEX"]
SERVICE_ORDER = ("data_collector", "oi_collector")
STATE_FILE = REPO_ROOT / "data" / "run_collectors_state.json"


class CollectorLauncher:
    def __init__(self, instruments, stagger_seconds=2.0, python_executable=None, skip_market_wait=False):
        self.instruments = [instrument.upper() for instrument in instruments]
        self.stagger_seconds = float(stagger_seconds)
        self.python_executable = python_executable or sys.executable
        self.skip_market_wait = bool(skip_market_wait)
        self.processes = []
        self.running = False
        self.time_utils = TimeUtils()

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
                    "service": entry["service"],
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

    def _pid_is_running(self, pid):
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
            service_name = service.get("service")
            if not pid or not instrument or not service_name:
                stale_found = True
                continue
            if not self._pid_is_running(pid):
                stale_found = True
                continue
            loaded.append(
                {
                    "service": service_name,
                    "instrument": instrument,
                    "process": None,
                    "pid": pid,
                }
            )

        if stale_found:
            if loaded:
                with STATE_FILE.open("w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "services": [
                                {
                                    "service": entry["service"],
                                    "instrument": entry["instrument"],
                                    "pid": entry["pid"],
                                }
                                for entry in loaded
                            ]
                        },
                        f,
                        indent=2,
                        sort_keys=True,
                    )
            else:
                self._clear_state()
        return loaded

    def _service_path(self, service_name):
        return REPO_ROOT / "services" / f"{service_name}.py"

    def _spawn(self, service_name, instrument):
        command = [
            self.python_executable,
            str(self._service_path(service_name)),
            "--instrument",
            instrument,
        ]
        process = subprocess.Popen(command, cwd=str(REPO_ROOT))
        self.processes.append(
            {
                "service": service_name,
                "instrument": instrument,
                "process": process,
                "pid": process.pid,
            }
        )
        print(
            f"[Launcher] Started {service_name} for {instrument} "
            f"(pid={process.pid})"
        )

    def start(self):
        existing = self._load_processes_from_state()
        if existing:
            print("[Launcher] Collector services are already running:")
            for entry in existing:
                print(
                    f"[Launcher] {entry['service']}:{entry['instrument']} "
                    f"(pid={entry['pid']})"
                )
            print("[Launcher] Stop them first if you want a fresh restart.")
            return

        self.running = True
        print("[Launcher] Starting collectors for:", ", ".join(self.instruments))
        print("[Launcher] Signal service is intentionally not started here.")

        # Wait for market to open if before market hours
        if not self.skip_market_wait:
            self._wait_for_market_open()
        else:
            print("[Launcher] Skipping market-open wait (forced start).")

        for instrument in self.instruments:
            for service_name in SERVICE_ORDER:
                self._spawn(service_name, instrument)
                time.sleep(self.stagger_seconds)

        print("[Launcher] All collector services started.")
        print("[Launcher] Press Ctrl+C to stop all collectors.")
        self._save_state()

    def _wait_for_market_open(self):
        """Wait for market to open at 9:14 AM"""
        while not self.time_utils.is_market_open():
            current_time = self.time_utils.now_ist()
            current_time_str = current_time.strftime('%H:%M:%S')
            market_open_time = current_time.replace(hour=9, minute=14, second=0, microsecond=0)
            market_open_str = market_open_time.strftime('%H:%M:%S')
            
            print(f"[Launcher] Current time: {current_time_str} IST")
            print(f"[Launcher] Market opens at: {market_open_str} IST")
            print(f"[Launcher] Waiting for market to open...")
            
            # Check if market opens in next minute
            time_to_open = (market_open_time - current_time).total_seconds()
            if time_to_open > 0 and time_to_open <= 60:
                print(f"[Launcher] Market opening in {int(time_to_open)} seconds...")
                time.sleep(min(time_to_open, 5))
            else:
                time.sleep(5)

    def _terminate_process(self, entry, force=False):
        process = entry["process"]
        if process.poll() is not None:
            return

        label = f"{entry['service']}:{entry['instrument']}"
        try:
            if force:
                print(f"[Launcher] Killing {label} (pid={process.pid})")
                process.kill()
            else:
                print(f"[Launcher] Stopping {label} (pid={process.pid})")
                process.terminate()
        except ProcessLookupError:
            return

    def stop(self):
        loaded_processes = self.processes
        if not loaded_processes:
            loaded_processes = self._load_processes_from_state()
            if loaded_processes:
                self.processes = loaded_processes

        if not self.processes:
            print("[Launcher] No collector services running.")
            return

        print("[Launcher] Shutting down collector services...")
        for entry in self.processes:
            process = entry.get("process")
            pid = entry.get("pid") or (process.pid if process else None)
            if process:
                self._terminate_process(entry, force=False)
                continue
            if pid and self._pid_is_running(pid):
                try:
                    print(
                        f"[Launcher] Stopping {entry['service']}:{entry['instrument']} "
                        f"(pid={pid})"
                    )
                    os.kill(pid, signal.SIGTERM)
                except OSError:
                    pass

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
            if still_running and process:
                self._terminate_process(entry, force=True)
            elif still_running and pid:
                try:
                    print(
                        f"[Launcher] Killing {entry['service']}:{entry['instrument']} "
                        f"(pid={pid})"
                    )
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass

        self.processes = []
        self._clear_state()
        print("[Launcher] All collectors stopped.")

    def status(self):
        entries = self.processes or self._load_processes_from_state()
        if not entries:
            print("[Launcher] No collector services running.")
            return

        for entry in entries:
            process = entry.get("process")
            pid = entry.get("pid") or (process.pid if process else None)
            running = process.poll() is None if process else self._pid_is_running(pid)
            state = "running" if running else "not running"
            print(
                f"[Launcher] {entry['service']}:{entry['instrument']} is {state} "
                f"(pid={pid})"
            )

    def monitor(self):
        try:
            while self.running:
                active_entries = []
                for entry in self.processes:
                    return_code = entry["process"].poll()
                    if return_code is None:
                        active_entries.append(entry)
                        continue

                    label = f"{entry['service']}:{entry['instrument']}"
                    if entry["service"] == "data_collector" and return_code == 0:
                        print(
                            f"[Launcher] {label} exited normally with code 0. "
                            f"Keeping remaining collector services running."
                        )
                        self._save_state()
                        continue

                    print(
                        f"[Launcher] {label} exited unexpectedly "
                        f"with code {return_code}."
                    )
                    self.running = False
                    self._save_state()
                    self.stop()
                    raise SystemExit(return_code or 1)

                self.processes = active_entries
                if not self.processes:
                    self.running = False
                    self._clear_state()
                    print("[Launcher] No active collector services remaining.")
                    return

                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[Launcher] Ctrl+C received.")
            self.running = False
            self.stop()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run data_collector and oi_collector for multiple instruments."
    )
    parser.add_argument(
        "action",
        nargs="?",
        default="start",
        choices=["start", "stop", "status", "monitor"],
        help="Action to perform. Default: start",
    )
    parser.add_argument(
        "--instruments",
        nargs="+",
        default=DEFAULT_INSTRUMENTS,
        help="Instrument list, e.g. NIFTY BANKNIFTY SENSEX",
    )
    parser.add_argument(
        "--stagger-seconds",
        type=float,
        default=2.0,
        help="Delay between starting child services.",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable to use for child services.",
    )
    parser.add_argument(
        "--skip-market-wait",
        action="store_true",
        help="Start collectors immediately without waiting for market open.",
    )
    return parser.parse_args()


def _raise_keyboard_interrupt(*_args):
    raise KeyboardInterrupt


def main():
    args = parse_args()
    launcher = CollectorLauncher(
        instruments=args.instruments,
        stagger_seconds=args.stagger_seconds,
        python_executable=args.python,
        skip_market_wait=args.skip_market_wait,
    )

    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)

    if args.action == "start":
        launcher.start()
        launcher.monitor()
    elif args.action == "stop":
        launcher.stop()
    elif args.action == "status":
        launcher.status()
    elif args.action == "monitor":
        launcher.monitor()


if __name__ == "__main__":
    main()
