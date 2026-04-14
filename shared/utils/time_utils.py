from datetime import datetime, time
import pytz
from config import Config


class TimeUtils:
    # NSE/BSE Market Holidays 2026
    MARKET_HOLIDAYS_2026 = [
        "2026-01-26",  # Republic Day
        "2026-03-25",  # Holi
        "2026-04-14",  # Ambedkar Jayanti / Dr. Baba Saheb Ambedkar Jayanti
        "2026-05-01",  # Maharashtra Day
        "2026-08-15",  # Independence Day
        "2026-10-02",  # Gandhi Jayanti
        "2026-11-05",  # Diwali Laxmi Puja
    ]

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
        """Check if market is open (weekday, not holiday, market hours)"""
        if not self.is_trading_day():
            return False
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

    def is_weekday(self):
        """Check if today is a weekday (Monday=0 to Friday=4)"""
        return self.now_ist().weekday() < 5

    def is_market_holiday(self):
        """Check if today is a market holiday"""
        today_str = self.today_str()
        return today_str in self.MARKET_HOLIDAYS_2026

    def is_trading_day(self):
        """Check if today is a trading day (weekday AND not holiday)"""
        return self.is_weekday() and not self.is_market_holiday()
