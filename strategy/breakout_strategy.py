from utils.time_utils import TimeUtils
from config import Config


class BreakoutStrategy:
    def __init__(self):
        self.time_utils = TimeUtils()

    def generate_signal(
            self,
            price,
            orb_high,
            orb_low,
            vwap,
            volume_signal,
            oi_bias,
            oi_trend=None,
            build_up=None,
            support=None,
            resistance=None,
            can_trade=True,
            buffer=0
    ):
        """
        Generate CE/PE signal using:
        VWAP + ORB + Volume + OI Bias + OI Trend + Build-up + Support/Resistance
        """

        # =============================
        # Time filter
        # =============================
        if not can_trade and not Config.TEST_MODE:
            return None, "Trade not allowed (time filter)"

        # =============================
        # VWAP ready check
        # =============================
        if vwap is None:
            return None, "VWAP not ready"

        # =============================
        # TEST MODE LOGIC (NO ORB)
        # =============================
        if Config.TEST_MODE:
            # CE Condition
            if (
                    price > vwap
                    and volume_signal in ["STRONG", "NORMAL"]
                    and oi_bias == "BULLISH"
                    and oi_trend == "BULLISH"
                    and build_up in ["LONG_BUILDUP", "SHORT_COVERING"]
            ):
                return "CE", "VWAP + Volume + OI + Long Build-up"

            # PE Condition
            elif (
                    price < vwap
                    and volume_signal in ["STRONG", "NORMAL"]
                    and oi_bias == "BEARISH"
                    and oi_trend == "BEARISH"
                    and build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]
            ):
                return "PE", "VWAP + Volume + OI + Short Build-up"

            return None, "TEST MODE: No setup"

        # =============================
        # REAL MODE LOGIC (ORB)
        # =============================
        if orb_high is None or orb_low is None:
            return None, "ORB not ready"

        # =============================
        # CE BREAKOUT (Smart Money Confirmation)
        # =============================
        if (
                price > orb_high + buffer
                and price > vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and oi_bias == "BULLISH"
                and oi_trend == "BULLISH"
                and build_up in ["LONG_BUILDUP", "SHORT_COVERING"]
        ):
            return "CE", "ORB Breakout Up + VWAP + Volume + Long Build-up"

        # =============================
        # PE BREAKDOWN (Smart Money Confirmation)
        # =============================
        elif (
                price < orb_low - buffer
                and price < vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and oi_bias == "BEARISH"
                and oi_trend == "BEARISH"
                and build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]
        ):
            return "PE", "ORB Breakdown Down + VWAP + Volume + Short Build-up"

        # =============================
        # Support Bounce Trade
        # =============================
        elif (
                support is not None
                and price <= support + buffer
                and oi_trend == "BULLISH"
                and build_up in ["LONG_BUILDUP", "SHORT_COVERING"]
        ):
            return "CE", "Support Bounce + Bullish OI"

        # =============================
        # Resistance Rejection Trade
        # =============================
        elif (
                resistance is not None
                and price >= resistance - buffer
                and oi_trend == "BEARISH"
                and build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]
        ):
            return "PE", "Resistance Rejection + Bearish OI"

        return None, "No valid setup"