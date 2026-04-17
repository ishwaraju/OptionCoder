"""
Option Data Cache - Shared option-chain snapshots between services.
OI Collector writes fresh snapshots, Signal Service reads them first.
"""

import json
import os
from datetime import datetime
from threading import Lock


class OptionDataCache:
    """Simple file-based cache for inter-process option-chain sharing."""

    def __init__(self):
        self.cache_dir = ".option_data_cache"
        os.makedirs(self.cache_dir, exist_ok=True)
        self.lock = Lock()

    def _get_file_path(self, instrument):
        return os.path.join(self.cache_dir, f"{instrument.lower()}_option_data.json")

    def set(self, instrument, option_data, timestamp=None):
        """Persist latest option data for an instrument."""
        if not option_data:
            return

        payload = dict(option_data)
        payload["instrument"] = instrument
        payload["snapshot_ts"] = (timestamp or datetime.now()).isoformat()

        temp_path = self._get_file_path(instrument) + ".tmp"
        final_path = self._get_file_path(instrument)
        with self.lock:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=True, default=str)
            os.replace(temp_path, final_path)

    def get(self, instrument):
        """Read latest cached option data for an instrument."""
        try:
            with self.lock:
                with open(self._get_file_path(instrument), "r", encoding="utf-8") as f:
                    data = json.load(f)
            return data if isinstance(data, dict) else None
        except (FileNotFoundError, json.JSONDecodeError):
            return None
