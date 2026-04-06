from datetime import time
from utils.time_utils import TimeUtils


class ORB:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.orb_high = None
        self.orb_low = None
        self.orb_candles = []
        self.orb_ready = False

    def add_candle(self, candle):
        """
        Add 5-min candle to ORB calculation
        Normal ORB: 9:15–9:30 IST
        """

        if self.orb_ready:
            return

        candle_time = candle.get("time")
        if candle_time is None:
            return

        candle_clock = candle_time.time()
        if time(9, 15) <= candle_clock < time(9, 30):
            self.orb_candles.append(candle)

    def calculate_orb(self):
        """Calculate ORB high & low"""
        if len(self.orb_candles) < 3:
            return None, None

        highs = [c["high"] for c in self.orb_candles]
        lows = [c["low"] for c in self.orb_candles]

        self.orb_high = max(highs)
        self.orb_low = min(lows)
        self.orb_ready = True

        print("ORB Calculated → High:", self.orb_high, "Low:", self.orb_low)

        return self.orb_high, self.orb_low

    def get_orb_levels(self):
        return self.orb_high, self.orb_low

    def is_orb_ready(self):
        return self.orb_ready

    def get_fallback_levels(self, recent_candles):
        """
        Use last 3 completed 5-min candles as fallback ORB when official
        9:15-9:30 ORB is unavailable (for example, bot started late).
        """
        if not recent_candles or len(recent_candles) < 3:
            return None, None

        fallback_window = recent_candles[-3:]
        highs = [candle["high"] for candle in fallback_window]
        lows = [candle["low"] for candle in fallback_window]

        return max(highs), min(lows)
