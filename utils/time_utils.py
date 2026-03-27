from datetime import datetime, time
import pytz


class TimeUtils:
    def __init__(self):
        self.ist = pytz.timezone('Asia/Kolkata')

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
        return time(9, 15) <= now <= time(15, 30)

    def is_orb_time(self):
        now = self.current_time()
        return time(9, 15) <= now <= time(9, 30)

    def can_trade(self):
        now = self.current_time()

        # Trading window
        if time(9, 30) <= now <= time(14, 30):
            # No trade zone
            if time(11, 30) <= now <= time(12, 30):
                return False
            return True
        return False

    def force_exit_time(self):
        now = self.current_time()
        return now >= time(14, 59)

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