"""
Shortcut launcher for the Telegram read-only command service.
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.utils.log_utils import build_log_path, cleanup_old_logs


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INSTRUMENTS = ["NIFTY", "BANKNIFTY", "SENSEX"]


class TelegramLauncher:
    def __init__(self, instruments=None, python_executable=None):
        self.instruments = [instrument.upper() for instrument in (instruments or DEFAULT_INSTRUMENTS)]
        self.python_executable = python_executable or sys.executable
        self.process = None
        self.running = False

    def _service_path(self):
        return REPO_ROOT / "services" / "telegram_bot_service.py"

    def start(self):
        command = [
            self.python_executable,
            "-u",
            str(self._service_path()),
            "--instruments",
            *self.instruments,
        ]
        cleanup_old_logs(retention_days=7)
        log_path = build_log_path("telegram_bot_service")
        log_handle = open(log_path, "a", encoding="utf-8")
        print("[Telegram Launcher] Starting Telegram command service...")
        print(f"[Telegram Launcher] Command: {' '.join(command)}")
        self.process = subprocess.Popen(
            command,
            cwd=str(REPO_ROOT),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        log_handle.close()
        self.running = True
        print(f"[Telegram Launcher] Started (pid={self.process.pid}) | log={log_path}")
        print("[Telegram Launcher] Press Ctrl+C to stop")

    def stop(self):
        if not self.process:
            return
        if self.process.poll() is None:
            print(f"[Telegram Launcher] Stopping (pid={self.process.pid})")
            self.process.terminate()
            deadline = time.time() + 10
            while time.time() < deadline:
                if self.process.poll() is not None:
                    break
                time.sleep(0.25)
            if self.process.poll() is None:
                print(f"[Telegram Launcher] Force killing (pid={self.process.pid})")
                self.process.kill()
        print("[Telegram Launcher] Stopped")

    def monitor(self):
        if not self.process:
            return
        try:
            while self.running:
                return_code = self.process.poll()
                if return_code is None:
                    time.sleep(1)
                    continue
                print(f"[Telegram Launcher] Service exited with code {return_code}")
                self.running = False
                return
        except KeyboardInterrupt:
            print("\n[Telegram Launcher] Ctrl+C received.")
            self.running = False
            self.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="Run Telegram read-only command service.")
    parser.add_argument(
        "--instruments",
        nargs="+",
        default=DEFAULT_INSTRUMENTS,
        help="Instrument list exposed in /signals",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable to use",
    )
    return parser.parse_args()


def _raise_keyboard_interrupt(*_args):
    raise KeyboardInterrupt


def main():
    args = parse_args()
    launcher = TelegramLauncher(
        instruments=args.instruments,
        python_executable=args.python,
    )
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
    launcher.start()
    launcher.monitor()


if __name__ == "__main__":
    main()
