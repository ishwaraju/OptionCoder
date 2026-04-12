#!/usr/bin/env python3
"""
Signal Service Management Tool

This script allows you to:
- Start signal service
- Stop signal service
- Check signal service status
- Monitor signal service health

Useful for manual signal service management during trading.
"""

import argparse
import json
import signal
import subprocess
import sys
import time
import os
from pathlib import Path

# Add current directory to Python path (same as other tools)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils.time_utils import TimeUtils

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INSTRUMENTS = ["NIFTY", "BANKNIFTY", "SENSEX"]
STATE_FILE = REPO_ROOT / "data" / "run_signals_state.json"


class SignalServiceManager:
    def __init__(self, python_executable=None):
        self.python_executable = python_executable or sys.executable
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
            if not pid or not instrument:
                stale_found = True
                continue
            if not self._pid_is_running(pid):
                stale_found = True
                continue
            loaded.append(
                {
                    "instrument": instrument,
                    "process": None,
                    "pid": pid,
                }
            )

        if stale_found:
            if loaded:
                with STATE_FILE.open("w", encoding="utf-8") as f:
                    json.dump({"services": [{"instrument": e["instrument"], "pid": e["pid"]} for e in loaded]}, f, indent=2, sort_keys=True)
            else:
                self._clear_state()
        return loaded

    def _service_path(self):
        return REPO_ROOT / "services" / "signal_service.py"

    def _heartbeat_path(self, instrument):
        return REPO_ROOT / "data" / f"signal_service_{instrument.lower()}_heartbeat.json"

    def _load_heartbeat(self, instrument):
        heartbeat_path = self._heartbeat_path(instrument)
        if not heartbeat_path.exists():
            return None
        try:
            with heartbeat_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def start(self, instruments):
        """Start one or more signal service processes."""
        existing = self._load_processes_from_state()
        if existing:
            print("[Signal Manager] Signal services are already running:")
            for entry in existing:
                print(f"[Signal Manager] {entry['instrument']} (pid={entry['pid']})")
            print("[Signal Manager] Stop them first if you want a fresh restart.")
            return

        self.running = True
        normalized_instruments = [instrument.upper() for instrument in instruments]
        print(
            "[Signal Manager] Starting signal service(s) for:",
            ", ".join(normalized_instruments),
        )

        for instrument in normalized_instruments:
            command = [
                self.python_executable,
                str(self._service_path()),
                "--instrument",
                instrument,
            ]
            print(f"[Signal Manager] Command: {' '.join(command)}")
            process = subprocess.Popen(command, cwd=str(REPO_ROOT))
            self.processes.append(
                {
                    "instrument": instrument,
                    "process": process,
                    "pid": process.pid,
                }
            )
            print(
                f"[Signal Manager] Signal service started for {instrument} "
                f"(pid={process.pid})"
            )

        print("[Signal Manager] Press Ctrl+C to stop all signal services")
        self._save_state()

    def stop(self):
        """Stop signal service"""
        loaded_processes = self.processes
        if not loaded_processes:
            loaded_processes = self._load_processes_from_state()
            if loaded_processes:
                self.processes = loaded_processes

        if not self.processes:
            print("⚠️ [Signal Manager] No signal services running")
            return
        
        print("🛑 [Signal Manager] Stopping signal service(s)...")
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
                print(f"  ❌ Error stopping {entry['instrument']}: {e}")

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
                print(f"  💀 Force killing {entry['instrument']} (pid={pid})")
                if process:
                    process.kill()
                elif pid:
                    os.kill(pid, signal.SIGKILL)
        
        self.processes = []
        self._clear_state()
        print("✅ [Signal Manager] All signal services stopped")

    def status(self):
        """Check signal service status"""
        entries = self.processes or self._load_processes_from_state()
        if not entries:
            print("[Signal Manager] No signal services running")
            return
        
        for entry in entries:
            process = entry.get("process")
            pid = entry.get("pid") or (process.pid if process else None)
            running = process.poll() is None if process else self._pid_is_running(pid)
            if running:
                print(
                    f"[Signal Manager] {entry['instrument']} is running "
                    f"(pid={pid})"
                )
            else:
                print(
                    f"[Signal Manager] {entry['instrument']} is not running "
                    f"(pid={pid})"
                )

    def monitor(self):
        """Monitor signal service"""
        if not self.processes:
            state_entries = self._load_processes_from_state()
            if state_entries:
                try:
                    while True:
                        print("\033[2J\033[H", end="")
                        print("[Signal Monitor] Live Signal Service Health")
                        print("[Signal Monitor] Press Ctrl+C to stop monitoring\n")
                        now = time.time()
                        for entry in state_entries:
                            heartbeat = self._load_heartbeat(entry["instrument"])
                            hb_age = "missing"
                            phase = "unknown"
                            reason = None
                            if heartbeat:
                                epoch = heartbeat.get("epoch")
                                hb_age = "unknown" if epoch is None else f"{max(0.0, now - epoch):.1f}s"
                                status = heartbeat.get("status", {})
                                phase = status.get("phase", "unknown")
                                reason = status.get("reason")
                            print(
                                f"{entry['instrument']} | pid={entry['pid']} | "
                                f"heartbeat={hb_age} | phase={phase}"
                            )
                            if reason:
                                print(f"  reason: {reason}")
                        time.sleep(3)
                except KeyboardInterrupt:
                    print("\n[Signal Monitor] Stopped")
            else:
                print("[Signal Manager] No signal services to monitor")
            return
        
        try:
            while self.running:
                for entry in self.processes:
                    return_code = entry["process"].poll()
                    if return_code is None:
                        continue

                    print(
                        f"[Signal Manager] {entry['instrument']} signal service "
                        f"exited with code {return_code}"
                    )
                    self.running = False
                    self._save_state()
                    self.stop()
                    return

                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n[Signal Manager] Ctrl+C received.")
            self.running = False
            self.stop()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Signal Service Management Tool"
    )
    parser.add_argument(
        "action",
        nargs="?",
        default="start",
        choices=["start", "stop", "status", "monitor"],
        help="Action to perform. Default: start"
    )
    parser.add_argument(
        "--instrument",
        help="Instrument for signal service",
        default=None
    )
    parser.add_argument(
        "--instruments",
        nargs="+",
        help="Run signal services for multiple instruments",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable to use"
    )
    return parser.parse_args()


def _raise_keyboard_interrupt(*_args):
    raise KeyboardInterrupt


def main():
    args = parse_args()
    manager = SignalServiceManager(python_executable=args.python)
    
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
    
    if args.action == "start":
        instruments = args.instruments or ([args.instrument] if args.instrument else DEFAULT_INSTRUMENTS)
        manager.start(instruments)
        manager.monitor()
    elif args.action == "stop":
        manager.stop()
    elif args.action == "status":
        manager.status()
    elif args.action == "monitor":
        manager.monitor()
    else:
        print(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()
