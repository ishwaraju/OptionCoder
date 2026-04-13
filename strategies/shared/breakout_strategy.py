from shared.utils.time_utils import TimeUtils
from config import Config
from strategies.shared.expiry_day_rules import ExpiryDayRules


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
        self.last_time_regime = "UNKNOWN"
        self.last_is_expiry_day = False
        self.last_signal_type = "NONE"
        self.last_signal_grade = "SKIP"
        self.last_heikin_ashi = None
        self.prev_heikin_ashi_open = None
        self.prev_heikin_ashi_close = None
        self.retest_setup = None
        self.confirmation_setup = None
        self.retest_bars_max = 3
        self.last_emitted_signal = None

    def _compute_heikin_ashi(self, candle_open, candle_high, candle_low, candle_close):
        if None in (candle_open, candle_high, candle_low, candle_close):
            return None

        ha_close = (candle_open + candle_high + candle_low + candle_close) / 4.0
        had_prior_context = not (
            self.prev_heikin_ashi_open is None or self.prev_heikin_ashi_close is None
        )
        if not had_prior_context:
            ha_open = (candle_open + candle_close) / 2.0
        else:
            ha_open = (self.prev_heikin_ashi_open + self.prev_heikin_ashi_close) / 2.0

        ha_high = max(candle_high, ha_open, ha_close)
        ha_low = min(candle_low, ha_open, ha_close)
        lower_wick = min(ha_open, ha_close) - ha_low
        upper_wick = ha_high - max(ha_open, ha_close)
        tolerance = max(abs(candle_close - candle_open) * 0.1, 0.5)

        if ha_close > ha_open:
            bias = "BULLISH"
            strength = "BULLISH_STRONG" if lower_wick <= tolerance else "BULLISH"
        elif ha_close < ha_open:
            bias = "BEARISH"
            strength = "BEARISH_STRONG" if upper_wick <= tolerance else "BEARISH"
        else:
            bias = "NEUTRAL"
            strength = "NEUTRAL"

        self.prev_heikin_ashi_open = ha_open
        self.prev_heikin_ashi_close = ha_close
        self.last_heikin_ashi = {
            "open": ha_open,
            "high": ha_high,
            "low": ha_low,
            "close": ha_close,
            "bias": bias,
            "strength": strength,
            "had_prior_context": had_prior_context,
        }
        return self.last_heikin_ashi

    @staticmethod
    def _has_bullish_build_up(build_up):
        return build_up in ["LONG_BUILDUP", "SHORT_COVERING"]

    @staticmethod
    def _has_bearish_build_up(build_up):
        return build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]

    def _reset_retest_setup(self):
        self.retest_setup = None

    def _reset_confirmation_setup(self):
        self.confirmation_setup = None

    def _set_retest_setup(self, direction, level, current_bar_time, score):
        self.retest_setup = {
            "direction": direction,
            "level": level,
            "bars_remaining": self.retest_bars_max,
            "session_day": current_bar_time.date() if current_bar_time is not None else self.time_utils.now_ist().date(),
            "score": score,
        }

    def _set_confirmation_setup(self, direction, level, current_bar_time, score):
        self.confirmation_setup = {
            "direction": direction,
            "level": level,
            "bars_remaining": 2,
            "session_day": current_bar_time.date() if current_bar_time is not None else self.time_utils.now_ist().date(),
            "score": score,
        }

    def _should_suppress_duplicate(self, direction, signal_type, current_bar_time, level=None):
        if current_bar_time is None or signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST"}:
            return False

        last = self.last_emitted_signal
        if not last:
            return False

        if last["session_day"] != current_bar_time.date():
            return False

        if last["direction"] != direction or last["signal_type"] != signal_type:
            return False

        bars_apart = int((current_bar_time - last["time"]).total_seconds() // 300)
        if bars_apart > 2:
            return False

        if level is None or last["level"] is None:
            return True

        return abs(level - last["level"]) <= max(last["buffer"] * 2, 10)

    def _mark_signal_emitted(self, direction, signal_type, current_bar_time, level=None, buffer=0):
        if current_bar_time is None:
            return

        self.last_emitted_signal = {
            "direction": direction,
            "signal_type": signal_type,
            "time": current_bar_time,
            "session_day": current_bar_time.date(),
            "level": level,
            "buffer": buffer or 0,
        }

    def _update_retest_setup(self, current_bar_time):
        if not self.retest_setup:
            return

        session_day = current_bar_time.date() if current_bar_time is not None else self.time_utils.now_ist().date()
        if self.retest_setup["session_day"] != session_day:
            self._reset_retest_setup()
            return

        self.retest_setup["bars_remaining"] -= 1
        if self.retest_setup["bars_remaining"] <= 0:
            self._reset_retest_setup()

    def _update_confirmation_setup(self, current_bar_time):
        if not self.confirmation_setup:
            return

        session_day = current_bar_time.date() if current_bar_time is not None else self.time_utils.now_ist().date()
        if self.confirmation_setup["session_day"] != session_day:
            self._reset_confirmation_setup()
            return

        self.confirmation_setup["bars_remaining"] -= 1
        if self.confirmation_setup["bars_remaining"] <= 0:
            self._reset_confirmation_setup()

    def _grade_signal(self, score, confidence, cautions, blockers, signal_type):
        if signal_type in [None, "NONE"]:
            return "SKIP"

        blocker_penalty = len(blockers or [])
        caution_penalty = len(cautions or [])

        if score >= 85 and confidence == "HIGH" and blocker_penalty == 0 and caution_penalty == 0:
            return "A+"
        if score >= 75 and confidence in ["HIGH", "MEDIUM"] and blocker_penalty == 0 and caution_penalty <= 1:
            return "A"
        if score >= 65 and confidence in ["HIGH", "MEDIUM"]:
            return "B"
        return "WATCH"

    def _set_diagnostics(self, blockers=None, cautions=None, confidence=None, regime=None, signal_type=None):
        self.last_blockers = blockers or []
        self.last_cautions = cautions or []
        if confidence is not None:
            self.last_confidence = confidence
        if regime is not None:
            self.last_regime = regime
        if signal_type is not None:
            self.last_signal_type = signal_type
            self.last_signal_grade = self._grade_signal(
                score=self.last_score,
                confidence=self.last_confidence,
                cautions=self.last_cautions,
                blockers=self.last_blockers,
                signal_type=signal_type,
            )

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

    def _derive_time_regime(self, current_now):
        if current_now < self.time_utils._parse_clock("09:40"):
            return "OPENING"
        if current_now < self.time_utils._parse_clock("11:30"):
            return "MID_MORNING"
        if current_now < self.time_utils._parse_clock("13:30"):
            return "MIDDAY"
        if current_now < self.time_utils._parse_clock("14:45"):
            return "LATE_DAY"
        return "ENDGAME"

    def _get_time_regime_thresholds(self, time_regime, fallback_mode):
        thresholds = {
            "opening_drive_min_score": 72,
            "breakout_min_score": Config.MIN_SCORE_THRESHOLD,
            "confirm_min_score": 62,
            "continuation_min_score": 60 if fallback_mode else 65,
            "high_continuation_min_score": 75,
            "retest_min_score": Config.MIN_SCORE_THRESHOLD,
            "reversal_min_score": 58 if fallback_mode else 55,
            "allow_continuation": True,
            "allow_fallback_continuation": True,
        }

        if time_regime == "OPENING":
            thresholds["breakout_min_score"] = max(thresholds["breakout_min_score"], 60 if fallback_mode else 58)
            thresholds["continuation_min_score"] = max(thresholds["continuation_min_score"], 68)
        elif time_regime == "MIDDAY":
            thresholds["breakout_min_score"] = max(thresholds["breakout_min_score"], 63 if fallback_mode else 60)
            thresholds["confirm_min_score"] = max(thresholds["confirm_min_score"], 64)
            thresholds["continuation_min_score"] = max(thresholds["continuation_min_score"], 72)
            thresholds["high_continuation_min_score"] = max(thresholds["high_continuation_min_score"], 80)
            thresholds["allow_fallback_continuation"] = False
        elif time_regime == "LATE_DAY":
            thresholds["breakout_min_score"] = max(thresholds["breakout_min_score"], 60 if fallback_mode else 58)
            thresholds["continuation_min_score"] = max(thresholds["continuation_min_score"], 62 if fallback_mode else 65)
        elif time_regime == "ENDGAME":
            thresholds["breakout_min_score"] = max(thresholds["breakout_min_score"], 65)
            thresholds["confirm_min_score"] = max(thresholds["confirm_min_score"], 66)
            thresholds["continuation_min_score"] = max(thresholds["continuation_min_score"], 78)
            thresholds["high_continuation_min_score"] = max(thresholds["high_continuation_min_score"], 82)
            thresholds["retest_min_score"] = max(thresholds["retest_min_score"], 60)
            thresholds["allow_continuation"] = False
            thresholds["allow_fallback_continuation"] = False

        return thresholds

    def _confidence_from_score(self, score, volume_signal, pressure_metrics, cautions):
        pressure_bias = pressure_metrics["pressure_bias"] if pressure_metrics else "NEUTRAL"
        if score >= 85 and volume_signal == "STRONG" and pressure_bias != "NEUTRAL" and not cautions:
            return "HIGH"
        if score >= 65:
            return "MEDIUM"
        return "LOW"

    def _is_invalid_candle(self, candle_open, candle_high, candle_low, candle_close, candle_volume):
        if None in (candle_open, candle_high, candle_low, candle_close):
            return False
        if candle_volume == 0:
            return True
        return candle_open == candle_high == candle_low == candle_close

    def _analyze_options_volume(self, atm_ce_volume, atm_pe_volume, signal_direction):
        """
        Analyze ATM options volume for additional confirmation
        Returns: (options_volume_signal, boost_score, reason)
        """
        if atm_ce_volume is None or atm_pe_volume is None:
            return "NEUTRAL", 0, "no_options_volume_data"
        
        total_volume = atm_ce_volume + atm_pe_volume
        if total_volume == 0:
            return "NEUTRAL", 0, "zero_options_volume"
        
        ce_ratio = atm_ce_volume / total_volume
        pe_ratio = atm_pe_volume / total_volume
        
        # CE Signal: Need strong CE volume
        if signal_direction == "CE":
            if ce_ratio >= 0.6 and total_volume > 100000:  # CE dominating with high volume
                return "STRONG", 8, f"ce_volume_dominant({atm_ce_volume:,})"
            elif ce_ratio >= 0.55:  # CE slightly dominating
                return "NORMAL", 4, f"ce_volume_lead({atm_ce_volume:,})"
            elif pe_ratio >= 0.6:  # PE dominating - opposite
                return "WEAK", -5, f"pe_volume_opposite({atm_pe_volume:,})"
        
        # PE Signal: Need strong PE volume
        elif signal_direction == "PE":
            if pe_ratio >= 0.6 and total_volume > 100000:  # PE dominating with high volume
                return "STRONG", 8, f"pe_volume_dominant({atm_pe_volume:,})"
            elif pe_ratio >= 0.55:  # PE slightly dominating
                return "NORMAL", 4, f"pe_volume_lead({atm_pe_volume:,})"
            elif ce_ratio >= 0.6:  # CE dominating - opposite
                return "WEAK", -5, f"ce_volume_opposite({atm_ce_volume:,})"
        
        return "NEUTRAL", 0, "balanced_options_volume"

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
            candle_tick_count=None,
            candle_time=None,
            candle_volume=None,
            atm_ce_volume=None,
            atm_pe_volume=None,
    ):
        """
        Generate CE/PE signal using:
        VWAP + ORB + Volume + OI Bias + OI Trend + Build-up + Support/Resistance + ATM Options Volume
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
        self.last_signal_type = "NONE"
        self.last_signal_grade = "SKIP"
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

        if self._is_invalid_candle(candle_open, candle_high, candle_low, candle_close, candle_volume):
            blockers.append("invalid_candle_data")
            self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime="NO_DATA", signal_type="NONE")
            return None, f"Invalid candle data | score={score}"

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
        candle_liquidity_ok = candle_tick_count is None or candle_tick_count >= 3
        heikin_ashi = self._compute_heikin_ashi(candle_open, candle_high, candle_low, candle_close)
        ha_bias = heikin_ashi["bias"] if heikin_ashi else "NEUTRAL"
        ha_strength = heikin_ashi["strength"] if heikin_ashi else "NEUTRAL"
        ha_has_context = heikin_ashi["had_prior_context"] if heikin_ashi else False
        bullish_ha_ok = (ha_bias == "BULLISH") if ha_has_context else True
        bearish_ha_ok = (ha_bias == "BEARISH") if ha_has_context else True
        strong_bullish_ha = ha_strength == "BULLISH_STRONG"
        strong_bearish_ha = ha_strength == "BEARISH_STRONG"

        if scored_direction == "CE" and ha_has_context and ha_strength == "BEARISH_STRONG":
            cautions.append("heikin_ashi_strong_opposite")
        elif scored_direction == "PE" and ha_has_context and ha_strength == "BULLISH_STRONG":
            cautions.append("heikin_ashi_strong_opposite")

        bullish_buildups = ["LONG_BUILDUP", "SHORT_COVERING"]
        bearish_buildups = ["SHORT_BUILDUP", "LONG_UNWINDING"]
        fallback_mode = pressure_metrics is None
        bullish_build_up_ok = self._has_bullish_build_up(build_up) or (
            fallback_mode and oi_bias == "BULLISH" and oi_trend in ["BULLISH", "NEUTRAL"] and score >= 60
        )
        bearish_build_up_ok = self._has_bearish_build_up(build_up) or (
            fallback_mode and oi_bias == "BEARISH" and oi_trend in ["BEARISH", "NEUTRAL"] and score >= 60
        )

        self._update_retest_setup(candle_time)
        self._update_confirmation_setup(candle_time)

        regime = self._derive_regime(price, vwap, atr, volume_signal, candle_range)
        breakout_regime_ok = regime in ["TRENDING", "EXPANDING", "OPENING_EXPANSION"] or (
            regime == "RANGING" and score >= 72 and volume_signal == "STRONG"
        )
        continuation_regime_ok = regime in ["TRENDING", "EXPANDING"] or (
            regime == "RANGING" and score >= 78 and volume_signal == "STRONG"
        )
        retest_regime_ok = regime in ["TRENDING", "EXPANDING", "RANGING"] and not (
            regime == "RANGING" and score < 68
        )
        reversal_regime_ok = regime in ["RANGING", "CHOPPY"]

        trade_start = self.time_utils._parse_clock(Config.TRADE_START_TIME)
        current_now = candle_time.time() if candle_time is not None else self.time_utils.current_time()
        opening_session = trade_start <= current_now < self.time_utils._parse_clock("09:45")
        opening_drive_window = trade_start <= current_now < self.time_utils._parse_clock("09:40")
        time_regime = self._derive_time_regime(current_now)
        self.last_time_regime = time_regime
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
        time_thresholds = self._get_time_regime_thresholds(time_regime, fallback_mode)
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
                signal_type="NONE",
            )
            return None, f"Expiry filter blocked trade | score={score}"

        # =============================
        # TEST MODE LOGIC (NO ORB)
        # =============================
        # Analyze ATM options volume for both directions
        ce_options_vol, ce_vol_boost, ce_vol_reason = self._analyze_options_volume(atm_ce_volume, atm_pe_volume, "CE")
        pe_options_vol, pe_vol_boost, pe_vol_reason = self._analyze_options_volume(atm_ce_volume, atm_pe_volume, "PE")
        
        if Config.TEST_MODE:
            # CE Condition with Options Volume
            ce_options_ok = ce_options_vol in ["STRONG", "NORMAL", "NEUTRAL"]  # Don't block if WEAK
            if ce_options_vol == "WEAK":
                cautions.append(f"ce_{ce_vol_reason}")
            
            if (
                    price > vwap
                    and volume_signal in ["STRONG", "NORMAL"]
                    and oi_bias == "BULLISH"
                    and oi_trend == "BULLISH"
                    and build_up in ["LONG_BUILDUP", "SHORT_COVERING"]
                    and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BULLISH", "NEUTRAL"])
                    and score >= 55
                    and ce_options_ok
            ):
                adjusted_score = score + ce_vol_boost
                confidence = self._confidence_from_score(adjusted_score, volume_signal, pressure_metrics, cautions)
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence=confidence, regime=regime, signal_type="CONTINUATION")
                return "CE", f"VWAP + Vol + OI + OptVol({ce_vol_reason}) | score={adjusted_score}"

            # PE Condition with Options Volume
            pe_options_ok = pe_options_vol in ["STRONG", "NORMAL", "NEUTRAL"]
            if pe_options_vol == "WEAK":
                cautions.append(f"pe_{pe_vol_reason}")
            
            elif (
                    price < vwap
                    and volume_signal in ["STRONG", "NORMAL"]
                    and oi_bias == "BEARISH"
                    and oi_trend == "BEARISH"
                    and build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]
                    and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BEARISH", "NEUTRAL"])
                    and score >= 55
                    and pe_options_ok
            ):
                adjusted_score = score + pe_vol_boost
                confidence = self._confidence_from_score(adjusted_score, volume_signal, pressure_metrics, cautions)
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence=confidence, regime=regime, signal_type="CONTINUATION")
                return "PE", f"VWAP + Vol + OI + OptVol({pe_vol_reason}) | score={adjusted_score}"

            blockers.append("test_mode_filters_incomplete")
            self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
            return None, f"TEST MODE: No setup | score={score}"

        # =============================
        # REAL MODE LOGIC (ORB)
        # =============================
        orb_ready = orb_high is not None and orb_low is not None

        if not orb_ready and score < 70:
            blockers.append("orb_not_ready")
            self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
            return None, f"ORB not ready | score={score}"

        retest_zone = max(buffer * 1.2, 10)
        active_retest = self.retest_setup
        active_confirmation = self.confirmation_setup

        if (
                opening_drive_window
                and orb_ready
                and price > orb_high + buffer
                and price > vwap
                and volume_signal == "STRONG"
                and oi_bias == "BULLISH"
                and oi_trend in ["BULLISH", "NEUTRAL", None]
                and bullish_build_up_ok
                and pressure_metrics
                and pressure_metrics["pressure_bias"] == "BULLISH"
                and score >= time_thresholds["opening_drive_min_score"]
                and breakout_body_ok
                and breakout_structure_ok
                and candle_liquidity_ok
                and bullish_ha_ok
                and "opposite_pressure" not in cautions
                and "far_from_vwap" not in cautions
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="OPENING_DRIVE",
            )
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            return "CE", f"Opening drive breakout up | score={score}"

        if (
                opening_drive_window
                and orb_ready
                and price < orb_low - buffer
                and price < vwap
                and volume_signal == "STRONG"
                and oi_bias == "BEARISH"
                and oi_trend in ["BEARISH", "NEUTRAL", None]
                and bearish_build_up_ok
                and pressure_metrics
                and pressure_metrics["pressure_bias"] == "BEARISH"
                and score >= time_thresholds["opening_drive_min_score"]
                and breakout_body_ok
                and breakout_structure_ok
                and candle_liquidity_ok
                and bearish_ha_ok
                and "opposite_pressure" not in cautions
                and "far_from_vwap" not in cautions
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="OPENING_DRIVE",
            )
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            return "PE", f"Opening drive breakdown down | score={score}"

        if (
                active_confirmation
                and active_confirmation["direction"] == "CE"
                and price > vwap
                and candle_close is not None
                and candle_close > active_confirmation["level"]
                and candle_high is not None
                and candle_high >= active_confirmation["level"] + max(buffer * 0.4, 5)
                and volume_signal in ["NORMAL", "STRONG"]
                and oi_bias in ["BULLISH", "NEUTRAL"]
                and oi_trend in ["BULLISH", "NEUTRAL", None]
                and bullish_build_up_ok
                and "opposite_pressure" not in cautions
                and candle_liquidity_ok
                and continuation_regime_ok
                and score >= time_thresholds["confirm_min_score"]
        ):
            if self._should_suppress_duplicate("CE", "BREAKOUT_CONFIRM", candle_time, active_confirmation["level"]):
                blockers.append("duplicate_signal_suppressed")
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
                return None, f"Duplicate breakout confirmation suppressed | score={score}"
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="BREAKOUT_CONFIRM",
            )
            level = active_confirmation["level"]
            self._reset_confirmation_setup()
            self._mark_signal_emitted("CE", "BREAKOUT_CONFIRM", candle_time, level=level, buffer=buffer)
            return "CE", f"Breakout confirmation above {level} | score={score}"

        if (
                active_confirmation
                and active_confirmation["direction"] == "PE"
                and price < vwap
                and candle_close is not None
                and candle_close < active_confirmation["level"]
                and candle_low is not None
                and candle_low <= active_confirmation["level"] - max(buffer * 0.4, 5)
                and volume_signal in ["NORMAL", "STRONG"]
                and oi_bias in ["BEARISH", "NEUTRAL"]
                and oi_trend in ["BEARISH", "NEUTRAL", None]
                and bearish_build_up_ok
                and "opposite_pressure" not in cautions
                and candle_liquidity_ok
                and continuation_regime_ok
                and score >= time_thresholds["confirm_min_score"]
        ):
            if self._should_suppress_duplicate("PE", "BREAKOUT_CONFIRM", candle_time, active_confirmation["level"]):
                blockers.append("duplicate_signal_suppressed")
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
                return None, f"Duplicate breakdown confirmation suppressed | score={score}"
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="BREAKOUT_CONFIRM",
            )
            level = active_confirmation["level"]
            self._reset_confirmation_setup()
            self._mark_signal_emitted("PE", "BREAKOUT_CONFIRM", candle_time, level=level, buffer=buffer)
            return "PE", f"Breakdown confirmation below {level} | score={score}"

        if (
                active_retest
                and active_retest["direction"] == "CE"
                and candle_low is not None
                and candle_close is not None
                and price > vwap
                and candle_low <= active_retest["level"] + retest_zone
                and candle_close >= active_retest["level"]
                and volume_signal in ["NORMAL", "STRONG"]
                and oi_bias in ["BULLISH", "NEUTRAL"]
                and oi_trend in ["BULLISH", "NEUTRAL", None]
                and bullish_build_up_ok
                and "opposite_pressure" not in cautions
                and candle_liquidity_ok
                and score >= time_thresholds["retest_min_score"]
                and not opening_session
                and retest_regime_ok
        ):
            if self._should_suppress_duplicate("CE", "RETEST", candle_time, active_retest["level"]):
                blockers.append("duplicate_signal_suppressed")
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
                return None, f"Duplicate retest suppressed | score={score}"
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="RETEST",
            )
            level = active_retest["level"]
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            self._mark_signal_emitted("CE", "RETEST", candle_time, level=level, buffer=buffer)
            return "CE", f"Breakout retest support entry above {level} | score={score}"

        if (
                active_retest
                and active_retest["direction"] == "PE"
                and candle_high is not None
                and candle_close is not None
                and price < vwap
                and candle_high >= active_retest["level"] - retest_zone
                and candle_close <= active_retest["level"]
                and volume_signal in ["NORMAL", "STRONG"]
                and oi_bias in ["BEARISH", "NEUTRAL"]
                and oi_trend in ["BEARISH", "NEUTRAL", None]
                and bearish_build_up_ok
                and "opposite_pressure" not in cautions
                and candle_liquidity_ok
                and score >= time_thresholds["retest_min_score"]
                and not opening_session
                and retest_regime_ok
        ):
            if self._should_suppress_duplicate("PE", "RETEST", candle_time, active_retest["level"]):
                blockers.append("duplicate_signal_suppressed")
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
                return None, f"Duplicate retest suppressed | score={score}"
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="RETEST",
            )
            level = active_retest["level"]
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            self._mark_signal_emitted("PE", "RETEST", candle_time, level=level, buffer=buffer)
            return "PE", f"Breakdown retest resistance entry below {level} | score={score}"

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
                and bullish_build_up_ok
                and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BULLISH", "NEUTRAL"])
                and score >= max(time_thresholds["breakout_min_score"], expiry_eval["score_floor"], 60 if fallback_mode else 0)
                and breakout_body_ok
                and breakout_structure_ok
                and candle_liquidity_ok
                and bullish_ha_ok
                and (candle_close is None or candle_close > orb_high)
                and not opening_session
                and breakout_regime_ok
        ):
            if fallback_mode and time_regime in ["MIDDAY", "LATE_DAY"] and volume_signal != "STRONG":
                blockers.append("fallback_volume_not_strong")
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
                return None, f"Fallback breakout needs stronger volume | score={score}"
            if self._should_suppress_duplicate("CE", "BREAKOUT", candle_time, orb_high):
                blockers.append("duplicate_signal_suppressed")
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
                return None, f"Duplicate breakout suppressed | score={score}"
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="BREAKOUT",
            )
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            self._mark_signal_emitted("CE", "BREAKOUT", candle_time, level=orb_high, buffer=buffer)
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
                and bearish_build_up_ok
                and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BEARISH", "NEUTRAL"])
                and score >= max(time_thresholds["breakout_min_score"], expiry_eval["score_floor"], 60 if fallback_mode else 0)
                and breakout_body_ok
                and breakout_structure_ok
                and candle_liquidity_ok
                and bearish_ha_ok
                and (candle_close is None or candle_close < orb_low)
                and not opening_session
                and breakout_regime_ok
        ):
            if fallback_mode and time_regime in ["MIDDAY", "LATE_DAY"] and volume_signal != "STRONG":
                blockers.append("fallback_volume_not_strong")
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
                return None, f"Fallback breakdown needs stronger volume | score={score}"
            if self._should_suppress_duplicate("PE", "BREAKOUT", candle_time, orb_low):
                blockers.append("duplicate_signal_suppressed")
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
                return None, f"Duplicate breakdown suppressed | score={score}"
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="BREAKOUT",
            )
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            self._mark_signal_emitted("PE", "BREAKOUT", candle_time, level=orb_low, buffer=buffer)
            return "PE", f"ORB Breakdown Down + VWAP + Volume + Short Build-up | score={score}"

        # =============================
        # Support Bounce Trade
        # =============================
        elif (
                support is not None
                and price <= support + buffer
                and oi_trend != "BEARISH"
                and bullish_build_up_ok
                and score >= time_thresholds["reversal_min_score"]
                and reversal_regime_ok
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="REVERSAL",
            )
            self._reset_confirmation_setup()
            return "CE", f"Support Bounce + Bullish OI | score={score}"

        # =============================
        # Resistance Rejection Trade
        # =============================
        elif (
                resistance is not None
                and price >= resistance - buffer
                and oi_trend != "BULLISH"
                and bearish_build_up_ok
                and score >= time_thresholds["reversal_min_score"]
                and reversal_regime_ok
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="REVERSAL",
            )
            self._reset_confirmation_setup()
            return "PE", f"Resistance Rejection + Bearish OI | score={score}"

        if (
                Config.AGGRESSIVE_MODE
                and time_thresholds["allow_fallback_continuation"]
                and scored_direction == "CE"
                and price > vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and score >= 50
                and oi_bias in ["BULLISH", "NEUTRAL"]
                and oi_trend in ["BULLISH", "NEUTRAL", None]
                and bullish_build_up_ok
                and candle_liquidity_ok
                and breakout_body_ok
                and breakout_structure_ok
                and bullish_ha_ok
                and "opposite_pressure" not in cautions
                and "far_from_vwap" not in cautions
                and not opening_session
                and regime in ["TRENDING", "EXPANDING", "OPENING_EXPANSION"]
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="AGGRESSIVE_CONTINUATION",
            )
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            return "CE", f"Aggressive bullish continuation | score={score}"

        if (
                Config.AGGRESSIVE_MODE
                and time_thresholds["allow_fallback_continuation"]
                and scored_direction == "PE"
                and price < vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and score >= 50
                and oi_bias in ["BEARISH", "NEUTRAL"]
                and oi_trend in ["BEARISH", "NEUTRAL", None]
                and bearish_build_up_ok
                and candle_liquidity_ok
                and breakout_body_ok
                and breakout_structure_ok
                and bearish_ha_ok
                and "opposite_pressure" not in cautions
                and "far_from_vwap" not in cautions
                and not opening_session
                and regime in ["TRENDING", "EXPANDING", "OPENING_EXPANSION"]
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="AGGRESSIVE_CONTINUATION",
            )
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            return "PE", f"Aggressive bearish continuation | score={score}"

        if (
                scored_direction == "CE"
                and time_thresholds["allow_continuation"]
                and price > vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and score >= time_thresholds["high_continuation_min_score"]
                and oi_bias in ["BULLISH", "NEUTRAL"]
                and oi_trend in ["BULLISH", "NEUTRAL", None]
                and bullish_build_up_ok
                and (orb_high is None or price <= orb_high + (buffer * 3))
                and candle_liquidity_ok
                and breakout_body_ok
                and breakout_structure_ok
                and bullish_ha_ok
                and (
                    "far_from_vwap" not in cautions
                    or (
                        score >= 82
                        and volume_signal == "STRONG"
                        and pressure_metrics
                        and pressure_metrics["pressure_bias"] == "BULLISH"
                    )
                )
                and "opposite_pressure" not in cautions
                and continuation_regime_ok
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="CONTINUATION",
            )
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            return "CE", f"High-score bullish continuation | score={score}"

        if (
                scored_direction == "CE"
                and time_thresholds["allow_fallback_continuation"]
                and price > vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and score >= time_thresholds["continuation_min_score"]
                and oi_bias in ["BULLISH", "NEUTRAL"]
                and oi_trend in ["BULLISH", "NEUTRAL", None]
                and bullish_build_up_ok
                and candle_liquidity_ok
                and breakout_body_ok
                and breakout_structure_ok
                and bullish_ha_ok
                and "opposite_pressure" not in cautions
                and "far_from_vwap" not in cautions
                and not opening_session
                and continuation_regime_ok
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="CONTINUATION",
            )
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            return "CE", f"Continuation follow-through setup | score={score}"

        if (
                scored_direction == "PE"
                and time_thresholds["allow_continuation"]
                and price < vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and score >= time_thresholds["high_continuation_min_score"]
                and oi_bias in ["BEARISH", "NEUTRAL"]
                and oi_trend in ["BEARISH", "NEUTRAL", None]
                and bearish_build_up_ok
                and (orb_low is None or price >= orb_low - (buffer * 3))
                and candle_liquidity_ok
                and breakout_body_ok
                and breakout_structure_ok
                and bearish_ha_ok
                and (
                    "far_from_vwap" not in cautions
                    or (
                        score >= 82
                        and volume_signal == "STRONG"
                        and pressure_metrics
                        and pressure_metrics["pressure_bias"] == "BEARISH"
                    )
                )
                and "opposite_pressure" not in cautions
                and continuation_regime_ok
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="CONTINUATION",
            )
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            return "PE", f"High-score bearish continuation | score={score}"

        if (
                scored_direction == "PE"
                and time_thresholds["allow_fallback_continuation"]
                and price < vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and score >= time_thresholds["continuation_min_score"]
                and oi_bias in ["BEARISH", "NEUTRAL"]
                and oi_trend in ["BEARISH", "NEUTRAL", None]
                and bearish_build_up_ok
                and candle_liquidity_ok
                and breakout_body_ok
                and breakout_structure_ok
                and bearish_ha_ok
                and "opposite_pressure" not in cautions
                and "far_from_vwap" not in cautions
                and not opening_session
                and continuation_regime_ok
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="CONTINUATION",
            )
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            return "PE", f"Continuation follow-through setup | score={score}"

        if scored_direction and score >= Config.MIN_SCORE_THRESHOLD:
            if (
                    orb_ready
                    and scored_direction == "CE"
                    and price > orb_high
                    and price > vwap
                    and volume_signal in ["NORMAL", "STRONG"]
                    and oi_bias in ["BULLISH", "NEUTRAL"]
                    and oi_trend in ["BULLISH", "NEUTRAL", None]
                    and bullish_build_up_ok
                    and "opposite_pressure" not in cautions
                    and candle_liquidity_ok
                    and not opening_session
                    and continuation_regime_ok
                    and (not breakout_body_ok or not breakout_structure_ok)
            ):
                self._set_confirmation_setup("CE", orb_high, candle_time, score)
                cautions = cautions + ["confirmation_watch_active"]

            if (
                    orb_ready
                    and scored_direction == "PE"
                    and price < orb_low
                    and price < vwap
                    and volume_signal in ["NORMAL", "STRONG"]
                    and oi_bias in ["BEARISH", "NEUTRAL"]
                    and oi_trend in ["BEARISH", "NEUTRAL", None]
                    and bearish_build_up_ok
                    and "opposite_pressure" not in cautions
                    and candle_liquidity_ok
                    and not opening_session
                    and continuation_regime_ok
                    and (not breakout_body_ok or not breakout_structure_ok)
            ):
                self._set_confirmation_setup("PE", orb_low, candle_time, score)
                cautions = cautions + ["confirmation_watch_active"]

            if (
                    orb_ready
                    and scored_direction == "CE"
                    and price > orb_high
                    and price > vwap
                    and volume_signal in ["NORMAL", "STRONG"]
                    and oi_bias in ["BULLISH", "NEUTRAL"]
                    and oi_trend in ["BULLISH", "NEUTRAL", None]
                    and bullish_build_up_ok
                    and "opposite_pressure" not in cautions
                    and candle_liquidity_ok
                    and not opening_session
                    and retest_regime_ok
            ):
                self._set_retest_setup("CE", orb_high, candle_time, score)
                cautions = cautions + ["retest_watch_active"]

            if (
                    orb_ready
                    and scored_direction == "PE"
                    and price < orb_low
                    and price < vwap
                    and volume_signal in ["NORMAL", "STRONG"]
                    and oi_bias in ["BEARISH", "NEUTRAL"]
                    and oi_trend in ["BEARISH", "NEUTRAL", None]
                    and bearish_build_up_ok
                    and "opposite_pressure" not in cautions
                    and candle_liquidity_ok
                    and not opening_session
                    and retest_regime_ok
            ):
                self._set_retest_setup("PE", orb_low, candle_time, score)
                cautions = cautions + ["retest_watch_active"]

            if scored_direction and score >= Config.MIN_SCORE_THRESHOLD and not any([breakout_regime_ok, continuation_regime_ok, retest_regime_ok, reversal_regime_ok]):
                blockers.append("regime_filter")

            blockers.append("direction_present_but_filters_incomplete")
            if opening_session:
                blockers.append("opening_session_confirmation_pending")
            if not breakout_body_ok:
                blockers.append("weak_breakout_body")
            if not breakout_structure_ok:
                blockers.append("breakout_structure_weak")
            if not candle_liquidity_ok:
                blockers.append("low_tick_density")
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=self._confidence_from_score(score, volume_signal, pressure_metrics, cautions),
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="NONE",
            )
            return None, f"Directional context present but filters incomplete | score={score}"

        blockers.append("no_valid_setup")
        if fallback_mode:
            blockers.append("oi_only_context")
        if score < Config.MIN_SCORE_THRESHOLD:
            blockers.append("score_below_threshold")
        if scored_direction is None:
            blockers.append("direction_unresolved")
        if volume_signal == "WEAK":
            blockers.append("volume_weak")
        if not candle_liquidity_ok:
            blockers.append("low_tick_density")
        if scored_direction == "CE":
            if price <= vwap:
                blockers.append("vwap_not_supportive")
            if oi_bias == "BEARISH" or oi_trend == "BEARISH":
                blockers.append("oi_conflict")
            if not bullish_build_up_ok and not fallback_mode:
                blockers.append("build_up_missing")
            elif build_up not in bullish_buildups:
                blockers.append("build_up_inferred")
            if "opposite_pressure" in cautions or (pressure_metrics and pressure_metrics["pressure_bias"] == "BEARISH"):
                blockers.append("pressure_conflict")
            if orb_high is not None and price <= orb_high:
                blockers.append("orb_breakout_missing")
            if orb_high is not None and price > orb_high + (buffer * 3):
                blockers.append("orb_extension_too_far")
        elif scored_direction == "PE":
            if price >= vwap:
                blockers.append("vwap_not_supportive")
            if oi_bias == "BULLISH" or oi_trend == "BULLISH":
                blockers.append("oi_conflict")
            if not bearish_build_up_ok and not fallback_mode:
                blockers.append("build_up_missing")
            elif build_up not in bearish_buildups:
                blockers.append("build_up_inferred")
            if "opposite_pressure" in cautions or (pressure_metrics and pressure_metrics["pressure_bias"] == "BULLISH"):
                blockers.append("pressure_conflict")
            if orb_low is not None and price >= orb_low:
                blockers.append("orb_breakout_missing")
            if orb_low is not None and price < orb_low - (buffer * 3):
                blockers.append("orb_extension_too_far")
        self._set_diagnostics(
            blockers=blockers,
            cautions=cautions,
            confidence=self._confidence_from_score(score, volume_signal, pressure_metrics, cautions),
            regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
            signal_type="NONE",
        )
        return None, f"No valid setup | score={score}"
