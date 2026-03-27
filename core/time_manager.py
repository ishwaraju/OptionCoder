from datetime import datetime, time
import pytz
from config import Config


class TimeManager:
    def __init__(self):
        self.ist = pytz.timezone('Asia/Kolkata')

    def now_ist(self):
        """Get current IST time"""
        return datetime.now(self.ist)

    def is_market_open(self):
        now = self.now_ist().time()
        market_start = time(9, 15)
        market_end = time(15, 30)
        return market_start <= now <= market_end

    def can_take_new_trade(self):
        now = self.now_ist().time()

        trade_start = time(9, 30)
        trade_end = time(14, 30)

        no_trade_start = time(11, 30)
        no_trade_end = time(12, 30)

        if trade_start <= now <= trade_end:
            if no_trade_start <= now <= no_trade_end:
                return False
            return True
        return False

    def is_force_exit_time(self):
        now = self.now_ist().time()
        force_exit = time(14, 59)
        return now >= force_exit