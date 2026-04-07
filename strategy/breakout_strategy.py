from utils.time_utils import TimeUtils
from config import Config
from strategy.expiry_day_rules import ExpiryDayRules


class BreakoutStrategy:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.expiry_rules = ExpiryDayRules(self.time_utils)
        self.last_score = 0
        self.last_score_components = []
        self.last_blockers = []
        self.last_cautions = []
        self.last_confidence = "LOW"
        self.last_regime = "UNKNOWN"
        self.last_is_expiry_day = False

    def _set_diagnostics(self, blockers=None, cautions=None, confidence=None, regime=None):
        self.last_blockers = blockers or []
        self.last_cautions = cautions or []
        if confidence is not None:
            self.last_confidence = confidence
        if regime is not None:
            self.last_regime = regime

    def _derive_regime(self, price, vwap, atr, volume_signal, candle_range):
        if atr is None:
            if volume_signal == "STRONG":
                return "OPENING_EXPANSION"
            return "EARLY_SESSION"

        vwap_distance = abs(price - vwap) if vwap is not None else 0
        if candle_range >= atr * 1.2 and volume_signal == "STRONG":
            return "EXPANDING"
        if vwap_distance >= atr * 0.8 and volume_signal in ["STRONG", "NORMAL"]:
            return "TRENDING"
        if candle_range <= atr * 0.45 and volume_signal == "WEAK":
            return "RANGING"
        return "CHOPPY"

    def _confidence_from_score(self, score, volume_signal, pressure_metrics, cautions):
        pressure_bias = pressure_metrics["pressure_bias"] if pressure_metrics else "NEUTRAL"
        if score >= 85 and volume_signal == "STRONG" and pressure_bias != "NEUTRAL" and not cautions:
            return "HIGH"
        if score >= 65:
            return "MEDIUM"
        return "LOW"

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
        bullish_score = 0
        bearish_score = 0
        score = 0
        direction = None
        components = []
        neutral_components = []

        if vwap is None:
            return 0, None, ["VWAP unavailable"]

        if price > vwap:
            bullish_score += 15
        elif price < vwap:
            bearish_score += 15

        if orb_high is not None and price > orb_high + buffer:
            bullish_score += 20
        elif orb_low is not None and price < orb_low - buffer:
            bearish_score += 20

        if volume_signal == "STRONG":
            score += 15
            neutral_components.append("strong_volume")
        elif volume_signal == "NORMAL":
            score += 8
            neutral_components.append("normal_volume")

        if oi_bias == "BULLISH":
            bullish_score += 10
        elif oi_bias == "BEARISH":
            bearish_score += 10

        if oi_trend == "BULLISH":
            bullish_score += 10
        elif oi_trend == "BEARISH":
            bearish_score += 10

        if build_up in ["LONG_BUILDUP", "SHORT_COVERING"]:
            bullish_score += 10
        elif build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]:
            bearish_score += 10

        if pressure_metrics:
            if pressure_metrics["pressure_bias"] == "BULLISH":
                bullish_score += 15
            elif pressure_metrics["pressure_bias"] == "BEARISH":
                bearish_score += 15

            if (
                    pressure_metrics["atm_pe_concentration"] >= 0.2
                    and pressure_metrics["atm_pe_concentration"] > pressure_metrics["atm_ce_concentration"]
            ):
                bullish_score += 5

            if (
                    pressure_metrics["atm_ce_concentration"] >= 0.2
                    and pressure_metrics["atm_ce_concentration"] > pressure_metrics["atm_pe_concentration"]
            ):
                bearish_score += 5

        if bullish_score > bearish_score:
            direction = "CE"
            score += bullish_score
            if price > vwap:
                components.append("price_above_vwap")
            if orb_high is not None and price > orb_high + buffer:
                components.append("orb_breakout_up")
            if oi_bias == "BULLISH":
                components.append("bullish_oi_bias")
            if oi_trend == "BULLISH":
                components.append("bullish_oi_trend")
            if build_up in ["LONG_BUILDUP", "SHORT_COVERING"]:
                components.append("bullish_build_up")
            if pressure_metrics and pressure_metrics["pressure_bias"] == "BULLISH":
                components.append("bullish_pressure")
            if pressure_metrics and pressure_metrics["atm_pe_concentration"] >= 0.2:
                components.append("atm_pe_concentration")
        elif bearish_score > bullish_score:
            direction = "PE"
            score += bearish_score
            if price < vwap:
                components.append("price_below_vwap")
            if orb_low is not None and price < orb_low - buffer:
                components.append("orb_breakout_down")
            if oi_bias == "BEARISH":
                components.append("bearish_oi_bias")
            if oi_trend == "BEARISH":
                components.append("bearish_oi_trend")
            if build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]:
                components.append("bearish_build_up")
            if pressure_metrics and pressure_metrics["pressure_bias"] == "BEARISH":
                components.append("bearish_pressure")
            if pressure_metrics and pressure_metrics["atm_ce_concentration"] >= 0.2:
                components.append("atm_ce_concentration")
        else:
            components.append("balanced_directional_signals")

        score = max(0, min(score, 100))
        components.extend(neutral_components)
        return score, direction, components

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
            atr=None,
            expiry=None,
            candle_high=None,
            candle_low=None,
            candle_close=None,
            candle_open=None,
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
        blockers = []
        cautions = []

        if not can_trade and not Config.TEST_MODE:
            blockers.append("time_filter")
            self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime="NO_TRADE_WINDOW")
            return None, f"Trade not allowed (time filter) | score={score}"

        # =============================
        # VWAP ready check
        # =============================
        if vwap is None:
            blockers.append("vwap_unavailable")
            self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime="NO_DATA")
            return None, f"VWAP not ready | score={score}"

        if pressure_metrics and pressure_metrics["pressure_bias"] == "NEUTRAL" and score < 58:
            blockers.append("pressure_neutral")
            self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime="CHOPPY")
            self.last_is_expiry_day = self.expiry_rules.is_expiry_day(expiry)
            return None, f"Pressure not aligned | score={score}"

        candle_range = 0
        candle_body = 0
        if candle_high is not None and candle_low is not None:
            candle_range = max(candle_high - candle_low, 0)
        if candle_close is not None and candle_open is not None:
            candle_body = abs(candle_close - candle_open)

        if candle_high is None or candle_low is None or candle_close is None or candle_open is None:
            breakout_body_ok = True
            breakout_structure_ok = True
        else:
            breakout_body_ok = candle_body >= max(buffer * 0.6, 8)
            breakout_structure_ok = candle_range == 0 or (candle_body / candle_range) >= 0.30

        regime = self._derive_regime(price, vwap, atr, volume_signal, candle_range)

        trade_start = self.time_utils._parse_clock(Config.TRADE_START_TIME)
        current_now = self.time_utils.current_time()
        opening_session = trade_start <= current_now < self.time_utils._parse_clock("09:45")
        if opening_session:
            cautions.append("opening_session")

        if atr is not None and abs(price - vwap) > max(atr * 1.5, buffer * 4):
            cautions.append("far_from_vwap")

        if support is not None and abs(price - support) <= max(buffer * 2, 15):
            cautions.append("near_support")
        if resistance is not None and abs(resistance - price) <= max(buffer * 2, 15):
            cautions.append("near_resistance")

        if pressure_metrics:
            if scored_direction == "CE" and pressure_metrics["pressure_bias"] == "BEARISH":
                cautions.append("opposite_pressure")
            if scored_direction == "PE" and pressure_metrics["pressure_bias"] == "BULLISH":
                cautions.append("opposite_pressure")

        provisional_confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
        expiry_eval = self.expiry_rules.evaluate(
            expiry_value=expiry,
            score=score,
            confidence=provisional_confidence,
            price=price,
            vwap=vwap,
            volume_signal=volume_signal,
            pressure_metrics=pressure_metrics,
            current_signal=scored_direction,
            blockers=blockers,
            cautions=cautions,
        )
        blockers = expiry_eval["blockers"]
        cautions = expiry_eval["cautions"]
        self.last_is_expiry_day = expiry_eval["is_expiry_day"]

        if not expiry_eval["allow_trade"]:
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=self._confidence_from_score(score, volume_signal, pressure_metrics, cautions),
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
            )
            return None, f"Expiry filter blocked trade | score={score}"

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
                confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence=confidence, regime=regime)
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
                confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence=confidence, regime=regime)
                return "PE", f"VWAP + Volume + OI + Short Build-up | score={score}"

            blockers.append("test_mode_filters_incomplete")
            self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime)
            return None, f"TEST MODE: No setup | score={score}"

        # =============================
        # REAL MODE LOGIC (ORB)
        # =============================
        orb_ready = orb_high is not None and orb_low is not None

        if not orb_ready and score < 70:
            blockers.append("orb_not_ready")
            self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime)
            return None, f"ORB not ready | score={score}"

        # =============================
        # CE BREAKOUT (Smart Money Confirmation)
        # =============================
        if (
                orb_ready
                and price > orb_high + buffer
                and price > vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and oi_bias != "BEARISH"
                and oi_trend != "BEARISH"
                and build_up in ["LONG_BUILDUP", "SHORT_COVERING"]
                and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BULLISH", "NEUTRAL"])
                and score >= max(60, expiry_eval["score_floor"])
                and breakout_body_ok
                and breakout_structure_ok
                and (candle_close is None or candle_close > orb_high)
                and not opening_session
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
            )
            return "CE", f"ORB Breakout Up + VWAP + Volume + Long Build-up | score={score}"

        # =============================
        # PE BREAKDOWN (Smart Money Confirmation)
        # =============================
        elif (
                orb_ready
                and price < orb_low - buffer
                and price < vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and oi_bias != "BULLISH"
                and oi_trend != "BULLISH"
                and build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]
                and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BEARISH", "NEUTRAL"])
                and score >= max(60, expiry_eval["score_floor"])
                and breakout_body_ok
                and breakout_structure_ok
                and (candle_close is None or candle_close < orb_low)
                and not opening_session
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
            )
            return "PE", f"ORB Breakdown Down + VWAP + Volume + Short Build-up | score={score}"

        # =============================
        # Support Bounce Trade
        # =============================
        elif (
                support is not None
                and price <= support + buffer
                and oi_trend != "BEARISH"
                and build_up in ["LONG_BUILDUP", "SHORT_COVERING"]
                and score >= 55
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
            )
            return "CE", f"Support Bounce + Bullish OI | score={score}"

        # =============================
        # Resistance Rejection Trade
        # =============================
        elif (
                resistance is not None
                and price >= resistance - buffer
                and oi_trend != "BULLISH"
                and build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]
                and score >= 55
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
            )
            return "PE", f"Resistance Rejection + Bearish OI | score={score}"

        if (
                scored_direction == "CE"
                and price > vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and score >= 72
                and oi_bias in ["BULLISH", "NEUTRAL"]
                and oi_trend in ["BULLISH", "NEUTRAL", None]
                and (orb_high is None or price <= orb_high + (buffer * 3))
                and "far_from_vwap" not in cautions
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
            )
            return "CE", f"High-score bullish continuation | score={score}"

        if (
                scored_direction == "PE"
                and price < vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and score >= 72
                and oi_bias in ["BEARISH", "NEUTRAL"]
                and oi_trend in ["BEARISH", "NEUTRAL", None]
                and (orb_low is None or price >= orb_low - (buffer * 3))
                and "far_from_vwap" not in cautions
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
            )
            return "PE", f"High-score bearish continuation | score={score}"

        if scored_direction and score >= 60:
            blockers.append("direction_present_but_filters_incomplete")
            if opening_session:
                blockers.append("opening_session_confirmation_pending")
            if not breakout_body_ok:
                blockers.append("weak_breakout_body")
            if not breakout_structure_ok:
                blockers.append("breakout_structure_weak")
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=self._confidence_from_score(score, volume_signal, pressure_metrics, cautions),
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
            )
            return None, f"Directional context present but filters incomplete | score={score}"

        blockers.append("no_valid_setup")
        self._set_diagnostics(
            blockers=blockers,
            cautions=cautions,
            confidence=self._confidence_from_score(score, volume_signal, pressure_metrics, cautions),
            regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
        )
        return None, f"No valid setup | score={score}"
