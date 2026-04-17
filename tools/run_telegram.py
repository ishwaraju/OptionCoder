"""
Shortcut launcher for the Telegram read-only command service.
"""

import argparse
import json
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
STATE_FILE = REPO_ROOT / "data" / "run_telegram_state.json"


class TelegramLauncher:
    def __init__(self, instruments=None, python_executable=None):
        self.instruments = [instrument.upper() for instrument in (instruments or DEFAULT_INSTRUMENTS)]
        self.python_executable = python_executable or sys.executable
        self.process = None

    def _service_path(self):
        return REPO_ROOT / "services" / "telegram_bot_service.py"

    def _load_state(self):
        if not STATE_FILE.exists():
            return {}
        try:
            with STATE_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_state(self, pid):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump({"pid": pid, "instruments": self.instruments}, f, indent=2, sort_keys=True)

    def _clear_state(self):
        if STATE_FILE.exists():
            STATE_FILE.unlink()

    def _pid_is_running(self, pid):
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def start(self):
        state = self._load_state()
        existing_pid = state.get("pid")
        if existing_pid and self._pid_is_running(existing_pid):
            print(f"[Telegram Launcher] Telegram service already running (pid={existing_pid})")
            return

        cleanup_old_logs(retention_days=7)
        log_path = build_log_path("telegram_bot_service")
        command = [
            self.python_executable,
            "-u",
            str(self._service_path()),
            "--instruments",
            *self.instruments,
        ]
        print("[Telegram Launcher] Starting Telegram command service...")
        print(f"[Telegram Launcher] Command: {' '.join(command)}")

        log_handle = open(log_path, "a", encoding="utf-8")
        self.process = subprocess.Popen(
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
        self._save_state(self.process.pid)
        print(f"[Telegram Launcher] Started (pid={self.process.pid}) | log={log_path}")

    def stop(self):
        state = self._load_state()
        pid = state.get("pid")
        if not pid:
            print("[Telegram Launcher] Telegram service is not running")
            return

        if not self._pid_is_running(pid):
            self._clear_state()
            print("[Telegram Launcher] Telegram service is not running")
            return

        print(f"[Telegram Launcher] Stopping (pid={pid})")
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            self._clear_state()
            print("[Telegram Launcher] Telegram service already stopped")
            return

        deadline = time.time() + 10
        while time.time() < deadline:
            if not self._pid_is_running(pid):
                self._clear_state()
                print("[Telegram Launcher] Stopped")
                return
            time.sleep(0.25)

        print(f"[Telegram Launcher] Force killing (pid={pid})")
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
        self._clear_state()
        print("[Telegram Launcher] Stopped")

    def status(self):
        state = self._load_state()
        pid = state.get("pid")
        if not pid or not self._pid_is_running(pid):
            if pid and not self._pid_is_running(pid):
                self._clear_state()
            print("[Telegram Launcher] Telegram service is not running")
            return
        print(f"[Telegram Launcher] Telegram service is running (pid={pid})")


def parse_args():
    parser = argparse.ArgumentParser(description="Run Telegram read-only command service.")
    parser.add_argument(
        "action",
        nargs="?",
        default="start",
        choices=["start", "stop", "status"],
        help="Action to perform. Default: start",
    )
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


def main():
    args = parse_args()
    launcher = TelegramLauncher(
        instruments=args.instruments,
        python_executable=args.python,
    )
    if args.action == "start":
        launcher.start()
    elif args.action == "stop":
        launcher.stop()
    elif args.action == "status":
        launcher.status()


if __name__ == "__main__":
    main()
