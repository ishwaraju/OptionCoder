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

    def get_deeper_itm_strike(self, price, option_type, steps=2):
        strike_gap = Config.STRIKE_GAP * steps
        atm = self.get_atm_strike(price)

        if option_type == "CE":
            return atm - strike_gap
        return atm + strike_gap

    def select_strike(self, price, signal, volume_signal, strategy_score=0, pressure_metrics=None):
        """
        Decide which strike to trade
        """
        strike, _ = self.select_strike_with_reason(
            price=price,
            signal=signal,
            volume_signal=volume_signal,
            strategy_score=strategy_score,
            pressure_metrics=pressure_metrics,
        )
        return strike

    def select_strike_with_reason(self, price, signal, volume_signal, strategy_score=0, pressure_metrics=None):
        """
        Decide which strike to trade and explain why that strike was chosen.
        """

        current_time = self.time_utils.current_time()
        pressure_bias = pressure_metrics.get("pressure_bias") if pressure_metrics else None
        near_call_ratio = pressure_metrics.get("near_call_pressure_ratio", 0) if pressure_metrics else 0
        near_put_ratio = pressure_metrics.get("near_put_pressure_ratio", 0) if pressure_metrics else 0
        strongest_ce_strike = pressure_metrics.get("strongest_ce_strike") if pressure_metrics else None
        strongest_pe_strike = pressure_metrics.get("strongest_pe_strike") if pressure_metrics else None
        atm = self.get_atm_strike(price)

        if signal == "CE":
            aligned_pressure = pressure_bias == "BULLISH" and near_put_ratio >= 1.2
            strongest_nearby = strongest_pe_strike in {atm - Config.STRIKE_GAP, atm, atm + Config.STRIKE_GAP}
        else:
            aligned_pressure = pressure_bias == "BEARISH" and near_call_ratio >= 1.2
            strongest_nearby = strongest_ce_strike in {atm - Config.STRIKE_GAP, atm, atm + Config.STRIKE_GAP}

        if strategy_score >= 85 and volume_signal == "STRONG" and aligned_pressure and strongest_nearby:
            return atm, "ATM because score, volume, and nearby pressure are strongly aligned"

        if current_time.hour >= 13 or strategy_score < 60:
            return self.get_deeper_itm_strike(price, signal, steps=2), "Deeper ITM because session is late or conviction is weak"

        if volume_signal == "WEAK" or not aligned_pressure:
            return self.get_itm_strike(price, signal), "ITM because volume or pressure confirmation is not strong enough"

        if strategy_score < 75:
            return self.get_itm_strike(price, signal), "ITM because score is moderate and setup is not top-tier"

        return atm, "ATM because conviction is good and pressure context is acceptable"
