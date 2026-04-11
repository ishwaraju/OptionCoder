import json
import os
import threading
import time
from datetime import datetime

import pytz


class RuntimeWatchdog:
    def __init__(
            self,
            heartbeat_file,
            stale_after_seconds=90,
            check_interval=10,
            state_file=None,
            max_restarts=5,
            restart_window_seconds=1800,
    ):
        self.heartbeat_file = heartbeat_file
        self.stale_after_seconds = stale_after_seconds
        self.check_interval = check_interval
        self.state_file = state_file
        self.max_restarts = max_restarts
        self.restart_window_seconds = restart_window_seconds
        self.last_touch_monotonic = time.monotonic()
        self.last_payload = {}
        self.lock = threading.Lock()
        self.running = False
        self.monitor_thread = None

    def touch(self, payload=None):
        now_ist = datetime.now(pytz.timezone("Asia/Kolkata"))
        data = {
            "timestamp": now_ist.isoformat(),
            "epoch": time.time(),
            "status": payload or {},
        }
        with self.lock:
            self.last_touch_monotonic = time.monotonic()
            self.last_payload = data

        os.makedirs(os.path.dirname(self.heartbeat_file), exist_ok=True)
        with open(self.heartbeat_file, "w") as f:
            json.dump(data, f, indent=2, sort_keys=True)

    def start(self, on_stale):
        if self.running:
            return

        self.running = True

        def monitor():
            while self.running:
                time.sleep(self.check_interval)
                with self.lock:
                    age = time.monotonic() - self.last_touch_monotonic
                    payload = dict(self.last_payload)

                if age > self.stale_after_seconds:
                    on_stale(age, payload)
                    return

        self.monitor_thread = threading.Thread(target=monitor, daemon=True)
        self.monitor_thread.start()

    def _load_state(self):
        if not self.state_file or not os.path.exists(self.state_file):
            return {"restart_epochs": []}
        try:
            with open(self.state_file, "r") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return {"restart_epochs": []}
            data.setdefault("restart_epochs", [])
            return data
        except Exception:
            return {"restart_epochs": []}

    def _save_state(self, state):
        if not self.state_file:
            return
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=2, sort_keys=True)

    def can_restart_now(self):
        now = time.time()
        state = self._load_state()
        recent = [
            epoch for epoch in state.get("restart_epochs", [])
            if now - epoch <= self.restart_window_seconds
        ]
        return len(recent) < self.max_restarts, len(recent)

    def record_restart(self, meta=None):
        now = time.time()
        state = self._load_state()
        recent = [
            epoch for epoch in state.get("restart_epochs", [])
            if now - epoch <= self.restart_window_seconds
        ]
        recent.append(now)
        payload = {
            "restart_epochs": recent,
            "last_restart_epoch": now,
            "last_restart_meta": meta or {},
        }
        self._save_state(payload)
        return len(recent)

    def stop(self):
        self.running = False
