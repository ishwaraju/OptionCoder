"""
Volume Cache - Shared volume data between services
OI Collector writes futures volume, Data Collector reads it
"""

import json
import os
from datetime import datetime
from threading import Lock

class VolumeCache:
    """Simple file-based volume cache for inter-process communication"""
    
    def __init__(self):
        self.cache_dir = ".volume_cache"
        os.makedirs(self.cache_dir, exist_ok=True)
        self.lock = Lock()
    
    def _get_file_path(self, instrument):
        return os.path.join(self.cache_dir, f"{instrument.lower()}_volume.json")
    
    def set(self, instrument, volume):
        """Save volume for instrument"""
        with self.lock:
            data = {
                "instrument": instrument,
                "volume": volume,
                "timestamp": datetime.now().isoformat()
            }
            with open(self._get_file_path(instrument), "w") as f:
                json.dump(data, f)
    
    def get(self, instrument):
        """Get volume for instrument"""
        try:
            with self.lock:
                with open(self._get_file_path(instrument), "r") as f:
                    data = json.load(f)
                    return data.get("volume", 0)
        except (FileNotFoundError, json.JSONDecodeError):
            return 0
    
    def get_all(self):
        """Get all volumes"""
        result = {}
        try:
            for filename in os.listdir(self.cache_dir):
                if filename.endswith("_volume.json"):
                    instrument = filename.replace("_volume.json", "").upper()
                    result[instrument] = self.get(instrument)
        except Exception:
            pass
        return result
