from datetime import datetime
from config import Config
from utils.time_utils import TimeUtils


class CandleManager:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.timeframe = Config.VWAP_TIMEFRAME  # 5 min

        # 1-min candle
        self.current_minute = None
        self.current_1m_candle = None
        self.minute_candles = []

        # 5-min candle
        self.current_5m_candle = None
        self.five_min_candles = []
        self.last_5m_slot = None

    # =========================
    # Tick → 1-min candle
    # =========================
    def add_tick(self, price, volume):
        now = self.time_utils.current_time()
        minute_key = now.strftime("%Y-%m-%d %H:%M")

        # New minute candle
        if self.current_minute != minute_key:
            if self.current_1m_candle:
                self.minute_candles.append(self.current_1m_candle)

            self.current_minute = minute_key
            self.current_1m_candle = {
                "datetime": now,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume
            }
            return self.current_1m_candle, True

        else:
            # Update current candle
            self.current_1m_candle["high"] = max(self.current_1m_candle["high"], price)
            self.current_1m_candle["low"] = min(self.current_1m_candle["low"], price)
            self.current_1m_candle["close"] = price
            self.current_1m_candle["volume"] += volume

        return self.current_1m_candle, False

    # =========================
    # 1-min → 5-min candle
    # =========================
    def _get_5min_slot(self, dt):
        minute = (dt.minute // self.timeframe) * self.timeframe
        return dt.replace(minute=minute, second=0, microsecond=0)

    def add_minute_candle(self, minute_candle):
        dt = minute_candle["datetime"]
        slot = self._get_5min_slot(dt)

        # First 5-min candle
        if self.current_5m_candle is None:
            self.current_5m_candle = {
                "time": slot,
                "open": minute_candle["open"],
                "high": minute_candle["high"],
                "low": minute_candle["low"],
                "close": minute_candle["close"],
                "volume": minute_candle["volume"]
            }
            self.last_5m_slot = slot
            return None

        # Same 5-min candle → update
        if slot == self.last_5m_slot:
            self.current_5m_candle["high"] = max(self.current_5m_candle["high"], minute_candle["high"])
            self.current_5m_candle["low"] = min(self.current_5m_candle["low"], minute_candle["low"])
            self.current_5m_candle["close"] = minute_candle["close"]
            self.current_5m_candle["volume"] += minute_candle["volume"]
            return None

        # 5-min candle completed
        completed = self.current_5m_candle
        self.five_min_candles.append(completed)

        # Start new 5-min candle
        self.current_5m_candle = {
            "time": slot,
            "open": minute_candle["open"],
            "high": minute_candle["high"],
            "low": minute_candle["low"],
            "close": minute_candle["close"],
            "volume": minute_candle["volume"]
        }
        self.last_5m_slot = slot

        return completed

    # =========================
    # Getters
    # =========================
    def get_last_5min_candle(self):
        if self.five_min_candles:
            return self.five_min_candles[-1]
        return None

    def get_all_5min_candles(self):
        return self.five_min_candles