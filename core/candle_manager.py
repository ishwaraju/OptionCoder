from config import Config
from utils.time_utils import TimeUtils


class CandleManager:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.timeframe = Config.VWAP_TIMEFRAME  # default 5 min

        # 1-min candle
        self.current_minute = None
        self.current_1m_candle = None
        self.minute_candles = []

        # 5-min candle
        self.current_5m_candle = None
        self.five_min_candles = []
        self.last_5m_slot = None

    # =========================
    # Internal Helpers
    # =========================
    def _minute_slot(self, dt):
        """Return minute-aligned timestamp (sec/microsec = 0)."""
        return dt.replace(second=0, microsecond=0)

    def _next_minute_slot(self, dt):
        """Return next minute slot timestamp."""
        base = self._minute_slot(dt)
        return base.replace(minute=base.minute) + __import__("datetime").timedelta(minutes=1)

    def _get_5min_slot(self, dt):
        minute = (dt.minute // self.timeframe) * self.timeframe
        return dt.replace(minute=minute, second=0, microsecond=0)

    # =========================
    # Tick → 1-min candle
    # =========================
    def add_tick(self, price, volume):
        now = self.time_utils.now_ist()
        minute_dt = self._minute_slot(now)
        minute_key = minute_dt.strftime("%Y-%m-%d %H:%M")

        # New minute candle
        if self.current_minute != minute_key:
            if self.current_1m_candle:
                # mark completed candle close time as next minute slot
                self.current_1m_candle["close_time"] = minute_dt
                self.minute_candles.append(self.current_1m_candle)

            self.current_minute = minute_key
            self.current_1m_candle = {
                "datetime": minute_dt,       # candle start time
                "close_time": None,          # filled when candle closes
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume
            }
            return self.current_1m_candle, True

        # Update current candle
        self.current_1m_candle["high"] = max(self.current_1m_candle["high"], price)
        self.current_1m_candle["low"] = min(self.current_1m_candle["low"], price)
        self.current_1m_candle["close"] = price
        self.current_1m_candle["volume"] += volume

        return self.current_1m_candle, False

    # =========================
    # 1-min → 5-min candle
    # =========================
    def add_minute_candle(self, minute_candle):
        dt = minute_candle["datetime"]
        slot = self._get_5min_slot(dt)

        # First 5-min candle
        if self.current_5m_candle is None:
            self.current_5m_candle = {
                "time": slot,  # 5m start time
                "close_time": None,
                "open": minute_candle["open"],
                "high": minute_candle["high"],
                "low": minute_candle["low"],
                "close": minute_candle["close"],
                "volume": minute_candle["volume"]
            }
            self.last_5m_slot = slot
            return None

        # Same 5-min slot → update
        if slot == self.last_5m_slot:
            self.current_5m_candle["high"] = max(self.current_5m_candle["high"], minute_candle["high"])
            self.current_5m_candle["low"] = min(self.current_5m_candle["low"], minute_candle["low"])
            self.current_5m_candle["close"] = minute_candle["close"]
            self.current_5m_candle["volume"] += minute_candle["volume"]
            return None

        # 5-min candle completed
        completed = self.current_5m_candle
        completed["close_time"] = slot
        self.five_min_candles.append(completed)

        # Start new 5-min candle
        self.current_5m_candle = {
            "time": slot,
            "close_time": None,
            "open": minute_candle["open"],
            "high": minute_candle["high"],
            "low": minute_candle["low"],
            "close": minute_candle["close"],
            "volume": minute_candle["volume"]
        }
        self.last_5m_slot = slot

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
            snapshot["close_time"] = snapshot["time"] + __import__("datetime").timedelta(minutes=self.timeframe)
        return snapshot

    # =========================
    # Getters
    # =========================
    def get_last_1min_candle(self):
        if self.minute_candles:
            return self.minute_candles[-1]
        return None

    def get_all_1min_candles(self):
        return self.minute_candles

    def get_last_5min_candle(self):
        if self.five_min_candles:
            return self.five_min_candles[-1]
        return None

    def get_all_5min_candles(self):
        return self.five_min_candles
