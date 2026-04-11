#!/usr/bin/env python3
"""
Launch signal service with all collectors for complete trading system.

This script starts:
- signal_service (main trading signals)
- data_collector (candle data)
- oi_collector (OI data + option bands)

All services are coordinated and monitored together.
"""

import argparse
import signal
import subprocess
import sys
import time
from pathlib import Path

# Add repo root to Python path
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from shared.utils.time_utils import TimeUtils

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INSTRUMENTS = ["NIFTY", "BANKNIFTY", "SENSEX"]
SERVICE_ORDER = ("data_collector", "oi_collector", "signal_service")


class UnifiedServiceLauncher:
    def __init__(self, instruments, stagger_seconds=2.0, python_executable=None):
        self.instruments = [instrument.upper() for instrument in instruments]
        self.stagger_seconds = float(stagger_seconds)
        self.python_executable = python_executable or sys.executable
        self.processes = []
        self.running = False
        self.time_utils = TimeUtils()

    def _service_path(self, service_name):
        return REPO_ROOT / "services" / f"{service_name}.py"

    def _spawn(self, service_name, instrument=None):
        command = [
            self.python_executable,
            str(self._service_path(service_name)),
        ]
        
        if instrument:
            command.extend(["--instrument", instrument])
        
        process = subprocess.Popen(command, cwd=str(REPO_ROOT))
        self.processes.append(
            {
                "service": service_name,
                "instrument": instrument,
                "process": process,
            }
        )
        
        if instrument:
            print(
                f"[Launcher] Started {service_name} for {instrument} "
                f"(pid={process.pid})"
            )
        else:
            print(
                f"[Launcher] Started {service_name} "
                f"(pid={process.pid})"
            )

    def start(self):
        self.running = True
        print("[Launcher] Starting complete trading system...")
        print(f"[Launcher] Instruments: {', '.join(self.instruments)}")
        print("[Launcher] Services: data_collector, oi_collector, signal_service")

        # Wait for market to open if before market hours
        self._wait_for_market_open()

        # Start data collectors first
        print("[Launcher] Starting data collectors...")
        for instrument in self.instruments:
            for service_name in ["data_collector", "oi_collector"]:
                self._spawn(service_name, instrument)
                time.sleep(self.stagger_seconds)

        # Start signal service (no instrument parameter)
        print("[Launcher] Starting signal service...")
        time.sleep(2.0)  # Brief delay before signal service
        self._spawn("signal_service")

        print("[Launcher] All services started successfully!")
        print("[Launcher] Press Ctrl+C to stop all services.")

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

        label = f"{entry['service']}"
        if entry.get("instrument"):
            label += f":{entry['instrument']}"
        
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
        if not self.processes:
            return

        print("[Launcher] Shutting down all services...")
        for entry in self.processes:
            self._terminate_process(entry, force=False)

        deadline = time.time() + 10
        while time.time() < deadline:
            if all(entry["process"].poll() is not None for entry in self.processes):
                break
            time.sleep(0.25)

        for entry in self.processes:
            if entry["process"].poll() is None:
                self._terminate_process(entry, force=True)

        print("[Launcher] All services stopped.")

    def monitor(self):
        try:
            while self.running:
                for entry in self.processes:
                    return_code = entry["process"].poll()
                    if return_code is None:
                        continue

                    label = f"{entry['service']}"
                    if entry.get("instrument"):
                        label += f":{entry['instrument']}"
                    
                    print(
                        f"[Launcher] {label} exited unexpectedly "
                        f"with code {return_code}."
                    )
                    self.running = False
                    self.stop()
                    raise SystemExit(return_code or 1)

                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[Launcher] Ctrl+C received.")
            self.running = False
            self.stop()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run signal service with all collectors for complete trading system."
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
    return parser.parse_args()


def _raise_keyboard_interrupt(*_args):
    raise KeyboardInterrupt


def main():
    args = parse_args()
    launcher = UnifiedServiceLauncher(
        instruments=args.instruments,
        stagger_seconds=args.stagger_seconds,
        python_executable=args.python,
    )

    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)

    launcher.start()
    launcher.monitor()


if __name__ == "__main__":
    main()
