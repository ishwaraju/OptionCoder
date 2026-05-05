from datetime import date, datetime, timedelta

from strategies.banknifty.expiry_profile import EXPIRY_PROFILE as BANKNIFTY_EXPIRY_PROFILE
from strategies.nifty.expiry_profile import EXPIRY_PROFILE as NIFTY_EXPIRY_PROFILE
from strategies.sensex.expiry_profile import EXPIRY_PROFILE as SENSEX_EXPIRY_PROFILE


def get_expiry_profile(instrument):
    symbol = (instrument or "NIFTY").upper()
    profiles = {
        "NIFTY": NIFTY_EXPIRY_PROFILE,
        "BANKNIFTY": BANKNIFTY_EXPIRY_PROFILE,
        "SENSEX": SENSEX_EXPIRY_PROFILE,
    }
    return profiles.get(symbol, NIFTY_EXPIRY_PROFILE)


class ExpirySessionContext:
    def __init__(self, current_date, instrument, expiry_date=None):
        self.current_date = current_date
        self.instrument = (instrument or "NIFTY").upper()
        self.expiry_date = expiry_date
        self.profile = get_expiry_profile(self.instrument)

    @staticmethod
    def _business_shift(start_date, delta_days):
        cursor = start_date
        step = 1 if delta_days >= 0 else -1
        remaining = abs(delta_days)
        while remaining > 0:
            cursor += timedelta(days=step)
            if cursor.weekday() < 5:
                remaining -= 1
        return cursor

    def session_mode(self):
        if self.expiry_date and self.expiry_date == self.current_date:
            return "EXPIRY_DAY"

        # Skip weekly expiry logic for instruments without weekly expiry
        if not self.profile.get("has_weekly_expiry", True):
            return "NORMAL"

        weekly_expiry_weekday = int(self.profile.get("weekly_expiry_weekday", 1))

        if self.current_date.weekday() == weekly_expiry_weekday:
            return "EXPIRY_DAY"

        if self.current_date == self._business_shift(
            self._nearest_weekly_expiry_date(),
            -1,
        ):
            return "PRE_EXPIRY_POSITIONING"

        if self.current_date == self._business_shift(
            self._nearest_weekly_expiry_date(),
            1,
        ):
            return "POST_EXPIRY_REBUILD"

        return "NORMAL"

    def _nearest_weekly_expiry_date(self):
        weekday = int(self.profile.get("weekly_expiry_weekday", 1))
        days_ahead = weekday - self.current_date.weekday()
        if days_ahead < 0:
            days_ahead += 7
        return self.current_date + timedelta(days=days_ahead)
