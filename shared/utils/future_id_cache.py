"""
Generic future security-id cache for multi-instrument support.
"""

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
FUTURE_IDS_FILE = DATA_DIR / "future_ids.json"


class FutureIdCache:
    """Read and write cached future security IDs per instrument."""

    def __init__(self, path=None):
        self.path = Path(path) if path else FUTURE_IDS_FILE

    def _ensure_parent(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load_all(self):
        if not self.path.exists():
            return {}
        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return {
                str(instrument).upper(): int(security_id)
                for instrument, security_id in data.items()
                if security_id is not None
            }
        except Exception:
            return {}

    def save_all(self, mapping):
        self._ensure_parent()
        normalized = {
            str(instrument).upper(): int(security_id)
            for instrument, security_id in mapping.items()
            if security_id is not None
        }
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(normalized, f, indent=2, sort_keys=True)

    def get(self, instrument, default=None):
        return self.load_all().get(str(instrument).upper(), default)

    def set(self, instrument, security_id):
        mapping = self.load_all()
        mapping[str(instrument).upper()] = int(security_id)
        self.save_all(mapping)
        return int(security_id)

    def delete(self, instrument):
        mapping = self.load_all()
        mapping.pop(str(instrument).upper(), None)
        self.save_all(mapping)
