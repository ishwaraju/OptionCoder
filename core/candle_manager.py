from collections import deque
from datetime import timedelta

from config import Config
from utils.time_utils import TimeUtils


class CandleManager:
    MAX_1M_HISTORY = 600
    MAX_5M_HISTORY = 300

    def __init__(self):
        self.time_utils = TimeUtils()
        self.timeframe = Config.VWAP_TIMEFRAME  # default 5 min

        # In-progress state
        self.current_minute = None
        self.current_1m_candle = None
        self.current_5m_candle = None
        self.last_5m_slot = None
        self.last_tick_time = None

        # Bounded history buffers
        self.minute_candles = deque(maxlen=self.MAX_1M_HISTORY)
        self.five_min_candles = deque(maxlen=self.MAX_5M_HISTORY)

    # =========================
    # Internal Helpers
    # =========================
    def _minute_slot(self, dt):
        """Return minute-aligned timestamp (sec/microsec = 0)."""
        return dt.replace(second=0, microsecond=0)

    def _next_minute_slot(self, dt):
        """Return next minute slot timestamp."""
        base = self._minute_slot(dt)
        return base + timedelta(minutes=1)

    def _get_5min_slot(self, dt):
        minute = (dt.minute // self.timeframe) * self.timeframe
        return dt.replace(minute=minute, second=0, microsecond=0)

    def _start_1m_candle(self, minute_dt, price, volume):
        minute_key = minute_dt.strftime("%Y-%m-%d %H:%M")
        self.current_minute = minute_key
        self.current_1m_candle = {
            "datetime": minute_dt,
            "close_time": None,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume,
            "tick_count": 1,
        }
        return self.current_1m_candle

    def _finalize_current_1m(self, next_minute_dt):
        if not self.current_1m_candle:
            return None

        completed = dict(self.current_1m_candle)
        completed["close_time"] = next_minute_dt
        self.minute_candles.append(completed)
        return completed

    def _start_5m_candle(self, slot, minute_candle):
        self.current_5m_candle = {
            "time": slot,
            "close_time": None,
            "open": minute_candle["open"],
            "high": minute_candle["high"],
            "low": minute_candle["low"],
            "close": minute_candle["close"],
            "volume": minute_candle["volume"],
            "tick_count": minute_candle.get("tick_count", 0),
        }
        self.last_5m_slot = slot
        return self.current_5m_candle

    def ingest_completed_5m(self, candle):
        self.five_min_candles.append(dict(candle))

    # =========================
    # Tick → 1-min candle
    # =========================
    def add_tick(self, price, volume):
        now = self.time_utils.now_ist()
        self.last_tick_time = now
        minute_dt = self._minute_slot(now)

        # New minute candle
        if self.current_minute != minute_dt.strftime("%Y-%m-%d %H:%M"):
            self._finalize_current_1m(minute_dt)
            return self._start_1m_candle(minute_dt, price, volume), True

        # Update current candle
        self.current_1m_candle["high"] = max(self.current_1m_candle["high"], price)
        self.current_1m_candle["low"] = min(self.current_1m_candle["low"], price)
        self.current_1m_candle["close"] = price
        self.current_1m_candle["volume"] += volume
        self.current_1m_candle["tick_count"] += 1

        return self.current_1m_candle, False

    # =========================
    # 1-min → 5-min candle
    # =========================
    def add_minute_candle(self, minute_candle):
        dt = minute_candle["datetime"]
        slot = self._get_5min_slot(dt)

        # First 5-min candle
        if self.current_5m_candle is None:
            self._start_5m_candle(slot, minute_candle)
            return None

        # Same 5-min slot → update
        if slot == self.last_5m_slot:
            self.current_5m_candle["high"] = max(self.current_5m_candle["high"], minute_candle["high"])
            self.current_5m_candle["low"] = min(self.current_5m_candle["low"], minute_candle["low"])
            self.current_5m_candle["close"] = minute_candle["close"]
            self.current_5m_candle["volume"] += minute_candle["volume"]
            self.current_5m_candle["tick_count"] += minute_candle.get("tick_count", 0)
            return None

        # 5-min candle completed
        completed = dict(self.current_5m_candle)
        completed["close_time"] = slot
        self.ingest_completed_5m(completed)

        # Start new 5-min candle
        self._start_5m_candle(slot, minute_candle)

        return completed

    # =========================
    # Flush helpers (important on shutdown)
    # =========================
    def finalize_current_1m(self):
        """
        Return current open 1m candle as completed snapshot (without mutating main state too much).
        Useful during graceful shutdown.
        """
        if not self.current_1m_candle:
            return None

        snapshot = dict(self.current_1m_candle)
        if snapshot["close_time"] is None:
            snapshot["close_time"] = self._next_minute_slot(snapshot["datetime"])
        return snapshot

    def finalize_current_5m(self):
        """
        Return current open 5m candle snapshot with computed close_time.
        Useful during graceful shutdown.
        """
        if not self.current_5m_candle:
            return None

        snapshot = dict(self.current_5m_candle)
        if snapshot["close_time"] is None:
            snapshot["close_time"] = snapshot["time"] + timedelta(minutes=self.timeframe)
        return snapshot

    def reset_incomplete_candles(self):
        """
        Drop in-progress candles when the live feed goes stale so we do not
        stretch flat/stale prices across reconnect gaps.
        """
        self.current_minute = None
        self.current_1m_candle = None
        self.current_5m_candle = None
        self.last_5m_slot = None

    # =========================
    # Getters
    # =========================
    def get_last_1min_candle(self):
        if self.minute_candles:
            return self.minute_candles[-1]
        return None

    def get_all_1min_candles(self):
        return list(self.minute_candles)

    def get_last_5min_candle(self):
        if self.five_min_candles:
            return self.five_min_candles[-1]
        return None

    def get_all_5min_candles(self):
        return list(self.five_min_candles)

    def get_last_candle_time(self):
        """Get the timestamp of the last completed 1m candle"""
        if self.minute_candles:
            return self.minute_candles[-1]["datetime"]
        return None

    def add_historical_candle(self, candle):
        """Add a historical candle (for backfill) without affecting current state"""
        if "datetime" not in candle:
            return None

        snapshot = dict(candle)
        self.minute_candles.append(snapshot)
        return snapshot
