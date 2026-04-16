from datetime import datetime, time
from config import Config


class ExpiryDayRules:
    """
    Expiry-day specific filters and guidance.
    Keeps expiry logic separate from the base strategy so normal sessions stay clean.
    """

    def __init__(self, time_utils):
        self.time_utils = time_utils

    @staticmethod
    def _parse_expiry(expiry_value):
        if not expiry_value:
            return None

        if isinstance(expiry_value, datetime):
            return expiry_value.date()

        if isinstance(expiry_value, str):
            try:
                return datetime.strptime(expiry_value, "%Y-%m-%d").date()
            except ValueError:
                return None

        return None

    def is_expiry_day(self, expiry_value):
        expiry_date = self._parse_expiry(expiry_value)
        if expiry_date is None:
            return False
        return self.time_utils.now_ist().date() == expiry_date

    @staticmethod
    def _expiry_vwap_distance_limit():
        symbol = (getattr(Config, "SYMBOL", "") or "").upper()
        if symbol == "SENSEX":
            return 140
        if symbol == "BANKNIFTY":
            return 90
        return 45

    def evaluate(
            self,
            expiry_value,
            score,
            confidence,
            price,
            vwap,
            volume_signal,
            pressure_metrics,
            current_signal,
            blockers,
            cautions,
    ):
        if not self.is_expiry_day(expiry_value):
            return {
                "is_expiry_day": False,
                "allow_trade": True,
                "blockers": list(blockers),
                "cautions": list(cautions),
                "score_floor": 60,
            }

        now = self.time_utils.current_time()
        blockers = list(blockers)
        cautions = list(cautions)
        allow_trade = True
        score_floor = 72
        pressure_bias = pressure_metrics["pressure_bias"] if pressure_metrics else "NEUTRAL"
        opposite_pressure = (
            (current_signal == "CE" and pressure_bias == "BEARISH")
            or (current_signal == "PE" and pressure_bias == "BULLISH")
        )
        high_conviction_expiry_trend = (
            current_signal in {"CE", "PE"}
            and score >= 80
            and confidence in {"MEDIUM", "HIGH"}
            and volume_signal == "STRONG"
            and not opposite_pressure
        )

        if time(9, 15) <= now < time(9, 45):
            blockers.append("expiry_opening_whipsaw_window")
            allow_trade = False

        if time(11, 30) <= now < time(13, 30) and score < 85 and not high_conviction_expiry_trend:
            blockers.append("expiry_midday_chop_window")
            allow_trade = False

        if now >= time(13, 30) and score < 82 and not high_conviction_expiry_trend:
            blockers.append("expiry_late_session_requires_high_score")
            allow_trade = False

        if confidence == "LOW":
            blockers.append("expiry_requires_medium_plus_confidence")
            allow_trade = False

        if volume_signal == "WEAK":
            blockers.append("expiry_weak_volume")
            allow_trade = False

        if vwap is not None and abs(price - vwap) > self._expiry_vwap_distance_limit():
            blockers.append("expiry_too_far_from_vwap")
            allow_trade = False

        if current_signal == "CE" and pressure_bias == "BEARISH":
            blockers.append("expiry_opposite_pressure")
            allow_trade = False
        elif current_signal == "PE" and pressure_bias == "BULLISH":
            blockers.append("expiry_opposite_pressure")
            allow_trade = False

        cautions.append("expiry_day_mode")
        if now >= time(13, 0):
            cautions.append("expiry_fast_decay")

        return {
            "is_expiry_day": True,
            "allow_trade": allow_trade,
            "blockers": blockers,
            "cautions": cautions,
            "score_floor": score_floor,
        }

    def adjust_strike_choice(self, expiry_value, signal, strike, strike_reason, price, strategy_score, confidence):
        if not self.is_expiry_day(expiry_value):
            return strike, strike_reason

        strike_gap = Config.STRIKE_GAP
        atm = round(price / strike_gap) * strike_gap

        if signal == "CE":
            safer_itm = atm - strike_gap
        else:
            safer_itm = atm + strike_gap

        if confidence == "HIGH" and strategy_score >= 88:
            return strike, f"{strike_reason} | Expiry day: conviction strong enough to keep current strike"

        if strike == atm:
            return safer_itm, "Expiry day: shifted 1-step ITM to reduce late premium decay risk"

        return strike, f"{strike_reason} | Expiry day: keeping safer non-ATM selection"
