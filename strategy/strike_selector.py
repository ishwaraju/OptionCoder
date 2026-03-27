from utils.time_utils import TimeUtils
from config import Config


class StrikeSelector:
    def __init__(self):
        self.time_utils = TimeUtils()

    def get_atm_strike(self, price):
        strike_gap = Config.STRIKE_GAP
        return round(price / strike_gap) * strike_gap

    def get_itm_strike(self, price, option_type):
        strike_gap = Config.STRIKE_GAP
        atm = self.get_atm_strike(price)

        if option_type == "CE":
            return atm - strike_gap
        else:
            return atm + strike_gap

    def select_strike(self, price, signal, volume_signal):
        """
        Decide which strike to trade
        """

        current_time = self.time_utils.current_time()

        # Afternoon → ITM
        if current_time.hour >= 13:
            return self.get_itm_strike(price, signal)

        # Low volume → ITM
        if volume_signal == "WEAK":
            return self.get_itm_strike(price, signal)

        # Otherwise ATM
        return self.get_atm_strike(price)