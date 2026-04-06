from utils.time_utils import TimeUtils
from config import Config


class BreakoutStrategy:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.last_score = 0
        self.last_score_components = []

    def _score_signal(
            self,
            price,
            orb_high,
            orb_low,
            vwap,
            volume_signal,
            oi_bias,
            oi_trend,
            build_up,
            pressure_metrics,
            buffer,
    ):
        score = 0
        direction = None
        components = []

        if vwap is None:
            return 0, None, ["VWAP unavailable"]

        if price > vwap:
            score += 15
            direction = "CE"
            components.append("price_above_vwap")
        elif price < vwap:
            score += 15
            direction = "PE"
            components.append("price_below_vwap")

        if orb_high is not None and price > orb_high + buffer:
            score += 20
            direction = "CE"
            components.append("orb_breakout_up")
        elif orb_low is not None and price < orb_low - buffer:
            score += 20
            direction = "PE"
            components.append("orb_breakout_down")

        if volume_signal == "STRONG":
            score += 15
            components.append("strong_volume")
        elif volume_signal == "NORMAL":
            score += 8
            components.append("normal_volume")

        if oi_bias == "BULLISH":
            score += 10
            direction = direction or "CE"
            components.append("bullish_oi_bias")
        elif oi_bias == "BEARISH":
            score += 10
            direction = direction or "PE"
            components.append("bearish_oi_bias")

        if oi_trend == "BULLISH":
            score += 10
            direction = direction or "CE"
            components.append("bullish_oi_trend")
        elif oi_trend == "BEARISH":
            score += 10
            direction = direction or "PE"
            components.append("bearish_oi_trend")

        if build_up in ["LONG_BUILDUP", "SHORT_COVERING"]:
            score += 10
            direction = direction or "CE"
            components.append("bullish_build_up")
        elif build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]:
            score += 10
            direction = direction or "PE"
            components.append("bearish_build_up")

        if pressure_metrics:
            if pressure_metrics["pressure_bias"] == "BULLISH":
                score += 15
                direction = direction or "CE"
                components.append("bullish_pressure")
            elif pressure_metrics["pressure_bias"] == "BEARISH":
                score += 15
                direction = direction or "PE"
                components.append("bearish_pressure")

            if pressure_metrics["atm_ce_concentration"] >= 0.2:
                score += 5
                components.append("atm_ce_concentration")

            if pressure_metrics["atm_pe_concentration"] >= 0.2:
                score += 5
                components.append("atm_pe_concentration")

        return min(score, 100), direction, components

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
            buffer=0,
            pressure_metrics=None,
    ):
        """
        Generate CE/PE signal using:
        VWAP + ORB + Volume + OI Bias + OI Trend + Build-up + Support/Resistance
        """

        # =============================
        # Time filter
        # =============================
        score, scored_direction, components = self._score_signal(
            price=price,
            orb_high=orb_high,
            orb_low=orb_low,
            vwap=vwap,
            volume_signal=volume_signal,
            oi_bias=oi_bias,
            oi_trend=oi_trend,
            build_up=build_up,
            pressure_metrics=pressure_metrics,
            buffer=buffer,
        )
        self.last_score = score
        self.last_score_components = components

        if not can_trade and not Config.TEST_MODE:
            return None, f"Trade not allowed (time filter) | score={score}"

        # =============================
        # VWAP ready check
        # =============================
        if vwap is None:
            return None, f"VWAP not ready | score={score}"

        if pressure_metrics and pressure_metrics["pressure_bias"] == "NEUTRAL" and score < 60:
            return None, f"Pressure not aligned | score={score}"

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
                    and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BULLISH", "NEUTRAL"])
                    and score >= 55
            ):
                return "CE", f"VWAP + Volume + OI + Long Build-up | score={score}"

            # PE Condition
            elif (
                    price < vwap
                    and volume_signal in ["STRONG", "NORMAL"]
                    and oi_bias == "BEARISH"
                    and oi_trend == "BEARISH"
                    and build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]
                    and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BEARISH", "NEUTRAL"])
                    and score >= 55
            ):
                return "PE", f"VWAP + Volume + OI + Short Build-up | score={score}"

            return None, f"TEST MODE: No setup | score={score}"

        # =============================
        # REAL MODE LOGIC (ORB)
        # =============================
        if orb_high is None or orb_low is None:
            return None, f"ORB not ready | score={score}"

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
                and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BULLISH", "NEUTRAL"])
                and score >= 65
        ):
            return "CE", f"ORB Breakout Up + VWAP + Volume + Long Build-up | score={score}"

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
                and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BEARISH", "NEUTRAL"])
                and score >= 65
        ):
            return "PE", f"ORB Breakdown Down + VWAP + Volume + Short Build-up | score={score}"

        # =============================
        # Support Bounce Trade
        # =============================
        elif (
                support is not None
                and price <= support + buffer
                and oi_trend == "BULLISH"
                and build_up in ["LONG_BUILDUP", "SHORT_COVERING"]
                and score >= 55
        ):
            return "CE", f"Support Bounce + Bullish OI | score={score}"

        # =============================
        # Resistance Rejection Trade
        # =============================
        elif (
                resistance is not None
                and price >= resistance - buffer
                and oi_trend == "BEARISH"
                and build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]
                and score >= 55
        ):
            return "PE", f"Resistance Rejection + Bearish OI | score={score}"

        if scored_direction and score >= 60:
            return None, f"Directional context present but filters incomplete | score={score}"

        return None, f"No valid setup | score={score}"
