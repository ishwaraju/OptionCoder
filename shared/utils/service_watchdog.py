import os
import sys
import time

from config import Config
from shared.utils.runtime_watchdog import RuntimeWatchdog


class ServiceWatchdog:
    def __init__(self, service_name, instrument):
        self.service_name = service_name
        self.instrument = instrument
        base_name = f"{service_name.lower()}_{instrument.lower()}"
        self.heartbeat_file = os.path.join("data", "heartbeat", f"{base_name}.json")
        self.state_file = os.path.join("data", "watchdog", f"{base_name}_state.json")
        self.watchdog = None

        if Config.ENABLE_WATCHDOG:
            self.watchdog = RuntimeWatchdog(
                heartbeat_file=self.heartbeat_file,
                stale_after_seconds=Config.WATCHDOG_STALE_SECONDS,
                check_interval=Config.WATCHDOG_CHECK_INTERVAL,
                state_file=self.state_file,
                max_restarts=Config.WATCHDOG_MAX_RESTARTS,
                restart_window_seconds=Config.WATCHDOG_RESTART_WINDOW_SECONDS,
            )

    def start(self, initial_payload=None):
        if not self.watchdog:
            return
        self.touch(initial_payload or {"phase": "boot"})
        self.watchdog.start(self._restart_process)

    def touch(self, payload=None):
        if not self.watchdog:
            return
        enriched = {"service": self.service_name, "instrument": self.instrument}
        if payload:
            enriched.update(payload)
        self.watchdog.touch(enriched)

    def stop(self):
        if self.watchdog:
            self.watchdog.stop()

    def _restart_process(self, age, payload):
        can_restart, recent_restarts = self.watchdog.can_restart_now()
        if not can_restart:
            print(
                f"[{self.service_name}] Watchdog restart limit reached "
                f"({recent_restarts}/{Config.WATCHDOG_MAX_RESTARTS}). Last payload: {payload}"
            )
            self.touch({"phase": "watchdog_restart_limit_reached", "payload": payload})
            return

        restart_count = self.watchdog.record_restart({"age_seconds": age, "payload": payload})
        print(
            f"[{self.service_name}] Watchdog detected stale service for {age:.1f}s. "
            f"Restarting ({restart_count}/{Config.WATCHDOG_MAX_RESTARTS})..."
        )
        time.sleep(2)
        os.execv(sys.executable, [sys.executable] + sys.argv)
