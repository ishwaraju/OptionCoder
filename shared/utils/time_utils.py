from datetime import datetime, time
import pytz
from config import Config


class TimeUtils:
    def __init__(self):
        self.ist = pytz.timezone('Asia/Kolkata')

    @staticmethod
    def _parse_clock(value):
        return datetime.strptime(value, "%H:%M").time()

    def now_ist(self):
        return datetime.now(self.ist)

    def today_str(self):
        return self.now_ist().strftime('%Y-%m-%d')

    def current_time(self):
        return self.now_ist().time()

    def current_time_str(self):
        return self.now_ist().time()

    def is_market_open(self):
        now = self.current_time()
        return self._parse_clock(Config.ORB_START) <= now <= time(15, 30)

    def is_orb_time(self):
        now = self.current_time()
        return self._parse_clock(Config.ORB_START) <= now <= self._parse_clock(Config.ORB_END)

    def can_trade(self):
        now = self.current_time()
        trade_start = self._parse_clock(Config.TRADE_START_TIME)
        trade_end = self._parse_clock(Config.TRADE_END_TIME)
        no_trade_start = self._parse_clock(Config.NO_TRADE_START)
        no_trade_end = self._parse_clock(Config.NO_TRADE_END)

        # Trading window
        if trade_start <= now <= trade_end:
            # No trade zone
            if no_trade_start <= now <= no_trade_end:
                return False
            return True
        return False

    def force_exit_time(self):
        now = self.current_time()
        return now >= self._parse_clock(Config.FORCE_EXIT_TIME)

    # =============================
    # Time comparison functions
    # =============================
    def is_after(self, time_str):
        now = self.current_time()
        t = datetime.strptime(time_str, "%H:%M").time()
        return now >= t

    def is_before(self, time_str):
        now = self.current_time()
        t = datetime.strptime(time_str, "%H:%M").time()
        return now <= t

    def is_between(self, start, end):
        now = self.current_time()
        s = datetime.strptime(start, "%H:%M").time()
        e = datetime.strptime(end, "%H:%M").time()
        return s <= now <= e
