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
        Fallback ORB: First 3 candles if bot started late
        """

        if self.orb_ready:
            return

        # Candle time IST me convert
        current_time = self.time_utils.current_time()

        # Normal ORB time (IST)
        if time(9, 15) <= current_time <= time(9, 30):
            self.orb_candles.append(candle)

        # Fallback ORB (if bot started after 9:30 IST)
        elif current_time > time(9, 30) and len(self.orb_candles) < 3:
            print("Using fallback ORB candle")
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