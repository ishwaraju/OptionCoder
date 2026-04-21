from shared.utils.time_utils import TimeUtils
from config import Config
from strategies.shared.expiry_day_rules import ExpiryDayRules

# New option buyer protection indicators
from shared.indicators.adx_calculator import ADXCalculator, OPTION_BUYER_MIN_ADX
from shared.indicators.volume_spike_detector import VolumeSpikeDetector, is_volume_confirmed_for_option_buying
from shared.indicators.session_rules import SessionRules, is_optimal_trading_time
from shared.indicators.oi_buildup_analyzer import OIBuildupAnalyzer, is_oi_confirming_trend
from shared.indicators.multi_timeframe_trend import MultiTimeframeTrend, quick_trend_alignment_check


class BreakoutStrategy:
    def __init__(self, for_option_buyer=True, instrument="NIFTY"):
        self.time_utils = TimeUtils()
        self.expiry_rules = ExpiryDayRules(self.time_utils)
        self.for_option_buyer = for_option_buyer  # Enable strict mode for option buyers
        self.instrument = (instrument or "NIFTY").upper()

        # New option buyer protection indicators
        self.adx_calc = ADXCalculator(period=14)
        self.volume_detector = VolumeSpikeDetector(
            period=14,
            spike_threshold=1.5,
            option_buyer_threshold=1.8 if for_option_buyer else 1.5
        )
        self.session_rules = SessionRules(for_option_buyer=for_option_buyer)
        self.oi_analyzer = OIBuildupAnalyzer(for_option_buyer=for_option_buyer)
        self.multi_tf_trend = MultiTimeframeTrend()

        # Tracking variables
        self.last_score = 0
        self.last_context_score = 0
        self.last_entry_score = 0
        self.last_score_components = []
        self.last_blockers = []
        self.last_cautions = []
        self.last_confidence = "LOW"
        self.last_regime = "UNKNOWN"
        self.last_time_regime = "UNKNOWN"
        self.last_decision_state = "IGNORE"
        self.last_watch_bucket = "NONE"
        self.last_pressure_conflict_level = "NONE"
        self.last_confidence_summary = None
        self.last_entry_plan = {}
        self.last_is_expiry_day = False
        self.last_signal_type = "NONE"
        self.last_signal_grade = "SKIP"
        self.last_heikin_ashi = None
        self.prev_heikin_ashi_open = None
        self.prev_heikin_ashi_close = None
        self.retest_setup = None
        self.confirmation_setup = None
        self.breakout_memory = None
        self.retest_bars_max = 3
        self.last_emitted_signal = None

        # Option buyer specific settings
        self.option_buyer_min_adx = OPTION_BUYER_MIN_ADX  # 28
        self.min_volume_spike = 1.8 if for_option_buyer else 1.5

    def _instrument_tuning(self):
        tuning = {
            "NIFTY": {
                "far_vwap_atr_mult": 1.55,
                "body_buffer_mult": 0.55,
                "body_floor": 8,
                "retest_zone_floor": 10,
                "watch_realert_move_mult": 0.6,
            },
            "BANKNIFTY": {
                "far_vwap_atr_mult": 1.85,
                "body_buffer_mult": 0.5,
                "body_floor": 12,
                "retest_zone_floor": 18,
                "watch_realert_move_mult": 0.8,
            },
            "SENSEX": {
                "far_vwap_atr_mult": 1.95,
                "body_buffer_mult": 0.48,
                "body_floor": 10,
                "retest_zone_floor": 16,
                "watch_realert_move_mult": 0.75,
            },
        }
        return tuning.get(self.instrument, tuning["NIFTY"])

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

    # ============================
    # OPTION BUYER PROTECTION FILTERS
    # ============================

    def _check_adx_filter(self, candles_5m, signal):
        """ADX filter - avoid sideways markets for option buyers"""
        if not self.for_option_buyer:
            return True, 0, "ADX check skipped (not option buyer mode)"

        if not candles_5m or len(candles_5m) < 15:
            return False, 0, "Insufficient candles for ADX"

        # Update ADX calculator
        for candle in candles_5m[-15:]:
            self.adx_calc.update(
                candle.get('high', candle[2] if isinstance(candle, (list, tuple)) else 0),
                candle.get('low', candle[3] if isinstance(candle, (list, tuple)) else 0),
                candle.get('close', candle[4] if isinstance(candle, (list, tuple)) else 0)
            )

        adx_data = self.adx_calc.get_current()
        if adx_data is None:
            return False, 0, "ADX calculation failed"

        adx_value = adx_data['adx']
        di_plus = adx_data['di_plus']
        di_minus = adx_data['di_minus']

        # Check minimum ADX
        if adx_value < self.option_buyer_min_adx:
            return False, 0, f"ADX {adx_value} < {self.option_buyer_min_adx} (sideways market)"

        # Check trend alignment
        if signal == "CE" and di_plus <= di_minus:
            return False, adx_value, f"ADX {adx_value} OK but DI+ {di_plus} <= DI- {di_minus}"

        if signal == "PE" and di_minus <= di_plus:
            return False, adx_value, f"ADX {adx_value} OK but DI- {di_minus} <= DI+ {di_plus}"

        # ADX contribution to score
        adx_score = 20 if adx_value >= 35 else 15

        return True, adx_score, f"ADX {adx_value} confirmed with trend alignment"

    def _check_volume_filter(self, current_volume):
        """Volume spike filter - confirm breakout with volume"""
        if not self.for_option_buyer:
            return True, 0, "Volume check skipped (not option buyer mode)"

        if current_volume is None or current_volume <= 0:
            return False, 0, "No volume data"

        confirmed, ratio, reason = self.volume_detector.is_breakout_confirmed(
            volume=current_volume,
            for_option_buyer=True
        )

        # Score calculation based on ratio
        if ratio >= 2.5:
            score = 25
        elif ratio >= 1.8:
            score = 20
        elif ratio >= 1.5:
            score = 15
        else:
            score = 0

        if not confirmed:
            return False, score, f"Volume weak: {reason}"

        return True, score, f"Volume confirmed: {reason}"

    def _check_session_filter(self, timestamp=None):
        """Session filter - avoid bad trading times"""
        if not self.for_option_buyer:
            return True, 0, "Session check skipped (not option buyer mode)"

        is_good, score, reason = self.session_rules.is_tradable(timestamp)

        if not is_good:
            return False, score, f"Bad session: {reason}"

        return True, score, f"Good session: {reason}"

    def _check_oi_filter(self, oi_data, price, signal):
        """OI buildup filter - confirm institutional participation"""
        if not self.for_option_buyer:
            return True, 0, "OI check skipped (not option buyer mode)"

        if oi_data is None:
            return False, 0, "No OI data"

        # Update OI analyzer
        current_oi = oi_data.get('current_oi', oi_data.get('oi', 0))
        self.oi_analyzer.update(current_oi, price)

        confirmed, score, reason = self.oi_analyzer.confirm_signal(signal)

        if not confirmed:
            return False, score, f"OI not confirming: {reason}"

        return True, score, f"OI confirming: {reason}"

    def _check_multi_timeframe_filter(self, trend_15m, signal):
        """Multi-timeframe filter - higher timeframe confirmation"""
        if not self.for_option_buyer:
            return True, 0, "Multi-TF check skipped (not option buyer mode)"

        if trend_15m in [None, "NEUTRAL", "UNKNOWN", "INSUFFICIENT_DATA"]:
            return True, 0, "15m trend unavailable"

        self.multi_tf_trend.update_trends("BULLISH" if signal == "CE" else "BEARISH", trend_15m)
        aligned, strength, reason = self.multi_tf_trend.check_alignment(signal)

        if not aligned:
            return False, strength, f"15m against: {reason}"

        return True, strength, f"Timeframes aligned: {reason}"

    def _apply_option_buyer_filters(self, signal, candles_5m, current_volume,
                                      oi_data, price, trend_15m=None, timestamp=None):
        """
        Apply all option buyer protection filters
        Returns: (pass_all, total_score, blockers)
        """
        if not self.for_option_buyer:
            return True, 0, []

        blockers = []
        total_score = 0

        # 1. Session Filter (Most Important - avoids time decay in bad periods)
        session_ok, session_score, session_reason = self._check_session_filter(timestamp)
        if not session_ok:
            blockers.append(f"SESSION: {session_reason}")
        else:
            total_score += session_score

        # 2. ADX Filter (Critical - avoids sideways market losses)
        adx_ok, adx_score, adx_reason = self._check_adx_filter(candles_5m, signal)
        if not adx_ok:
            blockers.append(f"ADX: {adx_reason}")
        else:
            total_score += adx_score

        # 3. Volume Filter (Important - confirms real breakout)
        vol_ok, vol_score, vol_reason = self._check_volume_filter(current_volume)
        if not vol_ok:
            blockers.append(f"VOLUME: {vol_reason}")
        else:
            total_score += vol_score

        # 4. OI Filter (Good to have - institutional confirmation)
        oi_ok, oi_score, oi_reason = self._check_oi_filter(oi_data, price, signal)
        if not oi_ok:
            blockers.append(f"OI: {oi_reason}")
        else:
            total_score += oi_score

        # 5. Multi-Timeframe Filter
        tf_ok, tf_score, tf_reason = self._check_multi_timeframe_filter(trend_15m, signal)
        if not tf_ok:
            blockers.append(f"TIMEFRAME: {tf_reason}")
        else:
            total_score += tf_score

        pass_all = len(blockers) == 0

        return pass_all, total_score, blockers

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

    def _calculate_entry_score(
        self,
        score,
        breakout_body_ok,
        breakout_structure_ok,
        candle_liquidity_ok,
        volume_signal,
        cautions,
        blockers,
        adx_trade_ok,
        mtf_trade_ok,
        pressure_conflict_level="NONE",
    ):
        entry_score = min(int(score), 100)
        if breakout_body_ok:
            entry_score += 8
        else:
            entry_score -= 12
        if breakout_structure_ok:
            entry_score += 8
        else:
            entry_score -= 10
        if candle_liquidity_ok:
            entry_score += 4
        else:
            entry_score -= 10
        if volume_signal == "STRONG":
            entry_score += 8
        elif volume_signal == "WEAK":
            entry_score -= 8
        if not adx_trade_ok:
            entry_score -= 8
        if not mtf_trade_ok:
            entry_score -= 6
        if pressure_conflict_level == "MILD":
            entry_score -= 6
        elif pressure_conflict_level == "MODERATE":
            entry_score -= 11
        elif pressure_conflict_level == "HARD":
            entry_score -= 16
        elif "opposite_pressure" in (cautions or []):
            entry_score -= 12
        if "far_from_vwap" in (cautions or []):
            entry_score -= 6
        if "near_resistance" in (cautions or []) or "near_support" in (cautions or []):
            entry_score -= 4
        entry_score -= min(len(blockers or []) * 4, 20)
        entry_score -= min(len(cautions or []) * 2, 12)
        return max(0, min(entry_score, 100))

    def _derive_decision_state(self, signal_type, signal, score, entry_score, confidence, blockers, cautions):
        if signal and signal_type not in {None, "NONE"}:
            if confidence in {"MEDIUM", "HIGH"} and entry_score >= 70 and len(blockers or []) == 0:
                return "ACTION"
            return "WATCH"

        if score >= max(Config.MIN_SCORE_THRESHOLD, 60) and confidence in {"MEDIUM", "HIGH"}:
            watch_markers = {
                "direction_present_but_filters_incomplete",
                "weak_breakout_body",
                "breakout_structure_weak",
                "pressure_conflict",
                "oi_conflict",
                "build_up_missing",
                "orb_breakout_missing",
                "orb_extension_too_far",
                "adx_not_confirmed",
                "higher_tf_not_aligned",
            }
            if any(marker in (blockers or []) for marker in watch_markers) or any(
                marker in (cautions or []) for marker in {"opposite_pressure", "adx_not_confirmed", "far_from_vwap"}
            ):
                return "WATCH"
        return "IGNORE"

    @staticmethod
    def _pressure_conflict(cautions, pressure_metrics, direction):
        if "opposite_pressure" in (cautions or []):
            return True
        if not pressure_metrics:
            return False
        if direction == "CE":
            return pressure_metrics["pressure_bias"] == "BEARISH"
        if direction == "PE":
            return pressure_metrics["pressure_bias"] == "BULLISH"
        return False

    def _reversal_confirmation_ready(self, direction, candle_open, candle_close, breakout_body_ok, ha_strength):
        if None in (candle_open, candle_close):
            return breakout_body_ok
        if direction == "CE":
            return candle_close > candle_open and breakout_body_ok and ha_strength != "BEARISH_STRONG"
        if direction == "PE":
            return candle_close < candle_open and breakout_body_ok and ha_strength != "BULLISH_STRONG"
        return False

    @staticmethod
    def _watch_signal_type(cautions, fallback_signal_type="NONE"):
        cautions = cautions or []
        if "confirmation_watch_active" in cautions:
            return "BREAKOUT_CONFIRM"
        if "retest_watch_active" in cautions:
            return "RETEST"
        return fallback_signal_type

    @staticmethod
    def _soft_conflict_count(cautions):
        soft_conflicts = {
            "opposite_pressure",
            "far_from_vwap",
            "adx_not_confirmed",
        }
        return sum(1 for caution in (cautions or []) if caution in soft_conflicts)

    def _pressure_conflict_level(self, pressure_metrics, direction, cautions):
        if not pressure_metrics or direction not in {"CE", "PE"}:
            return "NONE"

        opposite_bias = (
            direction == "CE" and pressure_metrics["pressure_bias"] == "BEARISH"
        ) or (
            direction == "PE" and pressure_metrics["pressure_bias"] == "BULLISH"
        )
        if not opposite_bias and "opposite_pressure" not in (cautions or []):
            return "NONE"

        same_side_concentration = (
            pressure_metrics.get("atm_pe_concentration", 0)
            if direction == "CE"
            else pressure_metrics.get("atm_ce_concentration", 0)
        )
        opposite_concentration = (
            pressure_metrics.get("atm_ce_concentration", 0)
            if direction == "CE"
            else pressure_metrics.get("atm_pe_concentration", 0)
        )

        if opposite_concentration >= max(0.28, same_side_concentration + 0.08):
            return "HARD"
        if opposite_concentration >= max(0.18, same_side_concentration):
            return "MODERATE"
        return "MILD"

    @staticmethod
    def _watch_bucket(signal_type, blockers, cautions):
        signal_type = (signal_type or "NONE").upper()
        blockers = blockers or []
        cautions = cautions or []
        if signal_type in {"BREAKOUT_CONFIRM", "RETEST", "REVERSAL"}:
            return "WATCH_CONFIRMATION_PENDING"
        if "direction_present_but_filters_incomplete" in blockers:
            return "WATCH_SETUP"
        if any(flag in cautions for flag in {"confirmation_watch_active", "retest_watch_active"}):
            return "WATCH_CONFIRMATION_PENDING"
        return "WATCH_CONTEXT"

    def _remember_breakout_context(self, direction, signal_type, candle_time, level, score):
        if direction not in {"CE", "PE"} or candle_time is None:
            return
        if signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM", "OPENING_DRIVE"}:
            return
        self.breakout_memory = {
            "direction": direction,
            "signal_type": signal_type,
            "time": candle_time,
            "level": level,
            "score": score,
            "session_day": candle_time.date(),
        }

    def _recent_breakout_context(self, direction, candle_time, price, vwap, buffer):
        if not self.breakout_memory or candle_time is None or direction not in {"CE", "PE"}:
            return False
        memory = self.breakout_memory
        if memory["session_day"] != candle_time.date():
            return False
        if memory["direction"] != direction:
            return False
        minutes_apart = int((candle_time - memory["time"]).total_seconds() // 60)
        if minutes_apart < 0 or minutes_apart > 45:
            return False
        level = memory.get("level")
        if level is None or vwap is None:
            return False
        if direction == "CE":
            return price >= max(level - max(buffer, 5), vwap)
        return price <= min(level + max(buffer, 5), vwap)

    def _confidence_summary(self, confidence, score, entry_score, signal_type, blockers, cautions):
        blocker_count = len(blockers or [])
        caution_count = len(cautions or [])
        setup_label = (signal_type or "NONE").replace("_", " ").title()
        if score >= 80 and entry_score >= 72 and blocker_count == 0 and caution_count <= 1:
            return f"Strong {setup_label.lower()} setup, low conflict"
        if blocker_count == 0 and caution_count <= 2:
            return f"Moderate {setup_label.lower()} setup, confirmation pending"
        if caution_count > 2:
            return f"{setup_label} visible, but conflicts abhi zyada hain"
        return f"{setup_label} building hai, entry quality abhi incomplete hai"

    def _build_entry_plan(self, direction, signal_type, trigger_price, invalidate_price, atr, support, resistance):
        plan = {
            "entry_above": None,
            "entry_below": None,
            "invalidate_price": None,
            "first_target_price": None,
        }
        signal_type = (signal_type or "NONE").upper()
        if direction not in {"CE", "PE"}:
            return plan

        step = max((atr or 0) * 0.6, 8 if self.instrument == "NIFTY" else 15)
        if direction == "CE":
            plan["entry_above"] = trigger_price
            plan["invalidate_price"] = invalidate_price or support
            base_target = trigger_price or invalidate_price
            if base_target is not None:
                plan["first_target_price"] = round(base_target + step, 2)
        else:
            plan["entry_below"] = trigger_price
            plan["invalidate_price"] = invalidate_price or resistance
            base_target = trigger_price or invalidate_price
            if base_target is not None:
                plan["first_target_price"] = round(base_target - step, 2)

        if signal_type == "RETEST" and plan["invalidate_price"] is None and trigger_price is not None:
            offset = max((atr or 0) * 0.4, 6 if self.instrument == "NIFTY" else 12)
            plan["invalidate_price"] = round(trigger_price - offset, 2) if direction == "CE" else round(trigger_price + offset, 2)

        return plan

    def _strong_context_soft_entry_ready(
        self,
        score,
        entry_score,
        volume_signal,
        candle_liquidity_ok,
        breakout_structure_ok,
        regime_ok,
        cautions,
        direction_ok,
    ):
        if not direction_ok or not regime_ok:
            return False
        if volume_signal != "STRONG":
            return False
        if not candle_liquidity_ok or not breakout_structure_ok:
            return False
        if score < 78 or entry_score < 64:
            return False
        return self._soft_conflict_count(cautions) <= 2

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
                score=self.last_entry_score or self.last_score,
                confidence=self.last_confidence,
                cautions=self.last_cautions,
                blockers=self.last_blockers,
                signal_type=signal_type,
            )
            self.last_decision_state = self._derive_decision_state(
                signal_type=signal_type,
                signal=None if signal_type in {None, "NONE"} else "PRESENT",
                score=self.last_context_score,
                entry_score=self.last_entry_score,
                confidence=self.last_confidence,
                blockers=self.last_blockers,
                cautions=self.last_cautions,
            )
            self.last_watch_bucket = self._watch_bucket(signal_type, self.last_blockers, self.last_cautions)
            self.last_confidence_summary = self._confidence_summary(
                confidence=self.last_confidence,
                score=self.last_context_score,
                entry_score=self.last_entry_score,
                signal_type=signal_type,
                blockers=self.last_blockers,
                cautions=self.last_cautions,
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

        if self.instrument == "BANKNIFTY":
            thresholds["retest_min_score"] = max(thresholds["retest_min_score"], 58)
            thresholds["confirm_min_score"] = max(thresholds["confirm_min_score"], 60)
        elif self.instrument == "SENSEX":
            thresholds["breakout_min_score"] = max(thresholds["breakout_min_score"], 57)
            thresholds["confirm_min_score"] = max(thresholds["confirm_min_score"], 61)
            thresholds["retest_min_score"] = max(thresholds["retest_min_score"], 57)
        elif self.instrument == "NIFTY":
            thresholds["continuation_min_score"] = max(thresholds["continuation_min_score"], 64)

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
            thresholds["retest_min_score"] = max(thresholds["retest_min_score"], 60 if self.instrument == "BANKNIFTY" else 58)
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
            recent_candles_5m=None,
            trend_15m=None,
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
        self.last_context_score = score
        self.last_entry_score = score
        self.last_score_components = components
        self.last_decision_state = "IGNORE"
        self.last_watch_bucket = "NONE"
        self.last_pressure_conflict_level = "NONE"
        self.last_confidence_summary = None
        self.last_entry_plan = {}
        self.last_signal_type = "NONE"
        self.last_signal_grade = "SKIP"
        blockers = []
        cautions = []
        tuning = self._instrument_tuning()

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
            breakout_body_ok = candle_body >= max(buffer * tuning["body_buffer_mult"], tuning["body_floor"])
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

        opening_direction_pressure_aligned = (
            pressure_metrics
            and (
                (scored_direction == "CE" and pressure_metrics["pressure_bias"] == "BULLISH")
                or (scored_direction == "PE" and pressure_metrics["pressure_bias"] == "BEARISH")
            )
        )
        opening_breakout_override = (
            opening_session
            and scored_direction in {"CE", "PE"}
            and score >= 90
            and volume_signal == "STRONG"
            and breakout_body_ok
            and breakout_structure_ok
            and candle_liquidity_ok
            and opening_direction_pressure_aligned
        )
        opening_far_vwap_override = opening_breakout_override and score >= 95

        if atr is not None and abs(price - vwap) > max(atr * tuning["far_vwap_atr_mult"], buffer * 4):
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
        pressure_conflict_level = self._pressure_conflict_level(pressure_metrics, scored_direction, cautions)
        self.last_pressure_conflict_level = pressure_conflict_level

        provisional_confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
        time_thresholds = self._get_time_regime_thresholds(time_regime, fallback_mode)
        adx_trade_ok = True
        mtf_trade_ok = True
        if scored_direction and recent_candles_5m and len(recent_candles_5m) >= 15:
            adx_ok, _, _ = self._check_adx_filter(recent_candles_5m, scored_direction)
            adx_trade_ok = adx_ok or (score >= 85 and volume_signal == "STRONG")
            if not adx_ok:
                cautions.append("adx_not_confirmed")
        if scored_direction and trend_15m is not None:
            tf_ok, _, _ = self._check_multi_timeframe_filter(trend_15m, scored_direction)
            mtf_trade_ok = tf_ok or (score >= 80 and volume_signal == "STRONG")
            if not tf_ok:
                cautions.append("higher_tf_not_aligned")

        self.last_entry_score = self._calculate_entry_score(
            score=score,
            breakout_body_ok=breakout_body_ok,
            breakout_structure_ok=breakout_structure_ok,
            candle_liquidity_ok=candle_liquidity_ok,
            volume_signal=volume_signal,
            cautions=cautions,
            blockers=blockers,
            adx_trade_ok=adx_trade_ok,
            mtf_trade_ok=mtf_trade_ok,
            pressure_conflict_level=pressure_conflict_level,
        )
        recent_breakout_context = self._recent_breakout_context(
            scored_direction,
            candle_time,
            price,
            vwap,
            buffer,
        )

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

        retest_zone = max(buffer * 1.2, tuning["retest_zone_floor"])
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
                and ("far_from_vwap" not in cautions or opening_far_vwap_override)
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="OPENING_DRIVE",
            )
            self._remember_breakout_context("CE", "OPENING_DRIVE", candle_time, orb_high, score)
            self.last_entry_plan = self._build_entry_plan("CE", "OPENING_DRIVE", orb_high, orb_low, atr, support, resistance)
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
                and ("far_from_vwap" not in cautions or opening_far_vwap_override)
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="OPENING_DRIVE",
            )
            self._remember_breakout_context("PE", "OPENING_DRIVE", candle_time, orb_low, score)
            self.last_entry_plan = self._build_entry_plan("PE", "OPENING_DRIVE", orb_low, orb_high, atr, support, resistance)
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
            self._remember_breakout_context("CE", "BREAKOUT_CONFIRM", candle_time, level, score)
            self.last_entry_plan = self._build_entry_plan("CE", "BREAKOUT_CONFIRM", level, orb_low, atr, support, resistance)
            self._reset_confirmation_setup()
            self._mark_signal_emitted("CE", "BREAKOUT_CONFIRM", candle_time, level=level, buffer=buffer)
            return "CE", f"Breakout confirmation above {level} | score={score}"

        if (
                active_confirmation
                and active_confirmation["direction"] == "CE"
                and price > vwap
                and candle_close is not None
                and candle_close > active_confirmation["level"]
                and candle_high is not None
                and candle_high >= active_confirmation["level"] + max(buffer * 0.6, 8)
                and volume_signal == "STRONG"
                and oi_bias in ["BULLISH", "NEUTRAL"]
                and oi_trend in ["BULLISH", "NEUTRAL", None]
                and bullish_build_up_ok
                and candle_liquidity_ok
                and breakout_structure_ok
                and bullish_ha_ok
                and continuation_regime_ok
                and score >= max(time_thresholds["confirm_min_score"] + 10, 74)
                and self._strong_context_soft_entry_ready(
                    score=score,
                    entry_score=self.last_entry_score,
                    volume_signal=volume_signal,
                    candle_liquidity_ok=candle_liquidity_ok,
                    breakout_structure_ok=breakout_structure_ok,
                    regime_ok=continuation_regime_ok,
                    cautions=cautions,
                    direction_ok=bullish_ha_ok and bullish_build_up_ok,
                )
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
            self._remember_breakout_context("CE", "BREAKOUT_CONFIRM", candle_time, level, score)
            self.last_entry_plan = self._build_entry_plan("CE", "BREAKOUT_CONFIRM", level, orb_low, atr, support, resistance)
            self._reset_confirmation_setup()
            self._mark_signal_emitted("CE", "BREAKOUT_CONFIRM", candle_time, level=level, buffer=buffer)
            return "CE", f"Strong-context breakout confirmation above {level} | score={score}"

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
            self._remember_breakout_context("PE", "BREAKOUT_CONFIRM", candle_time, level, score)
            self.last_entry_plan = self._build_entry_plan("PE", "BREAKOUT_CONFIRM", level, orb_high, atr, support, resistance)
            self._reset_confirmation_setup()
            self._mark_signal_emitted("PE", "BREAKOUT_CONFIRM", candle_time, level=level, buffer=buffer)
            return "PE", f"Breakdown confirmation below {level} | score={score}"

        if (
                active_confirmation
                and active_confirmation["direction"] == "PE"
                and price < vwap
                and candle_close is not None
                and candle_close < active_confirmation["level"]
                and candle_low is not None
                and candle_low <= active_confirmation["level"] - max(buffer * 0.6, 8)
                and volume_signal == "STRONG"
                and oi_bias in ["BEARISH", "NEUTRAL"]
                and oi_trend in ["BEARISH", "NEUTRAL", None]
                and bearish_build_up_ok
                and candle_liquidity_ok
                and breakout_structure_ok
                and bearish_ha_ok
                and continuation_regime_ok
                and score >= max(time_thresholds["confirm_min_score"] + 10, 74)
                and self._strong_context_soft_entry_ready(
                    score=score,
                    entry_score=self.last_entry_score,
                    volume_signal=volume_signal,
                    candle_liquidity_ok=candle_liquidity_ok,
                    breakout_structure_ok=breakout_structure_ok,
                    regime_ok=continuation_regime_ok,
                    cautions=cautions,
                    direction_ok=bearish_ha_ok and bearish_build_up_ok,
                )
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
            self._remember_breakout_context("PE", "BREAKOUT_CONFIRM", candle_time, level, score)
            self.last_entry_plan = self._build_entry_plan("PE", "BREAKOUT_CONFIRM", level, orb_high, atr, support, resistance)
            self._reset_confirmation_setup()
            self._mark_signal_emitted("PE", "BREAKOUT_CONFIRM", candle_time, level=level, buffer=buffer)
            return "PE", f"Strong-context breakdown confirmation below {level} | score={score}"

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
            self.last_entry_plan = self._build_entry_plan("CE", "RETEST", level, support, atr, support, resistance)
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            self._mark_signal_emitted("CE", "RETEST", candle_time, level=level, buffer=buffer)
            return "CE", f"Breakout retest support entry above {level} | score={score}"

        if (
                active_retest
                and active_retest["direction"] == "CE"
                and candle_low is not None
                and candle_close is not None
                and price > vwap
                and candle_low <= active_retest["level"] + retest_zone
                and candle_close >= active_retest["level"]
                and volume_signal == "STRONG"
                and oi_bias in ["BULLISH", "NEUTRAL"]
                and oi_trend in ["BULLISH", "NEUTRAL", None]
                and bullish_build_up_ok
                and candle_liquidity_ok
                and breakout_structure_ok
                and score >= max(time_thresholds["retest_min_score"] + 8, 72)
                and not opening_session
                and retest_regime_ok
                and self._strong_context_soft_entry_ready(
                    score=score,
                    entry_score=self.last_entry_score,
                    volume_signal=volume_signal,
                    candle_liquidity_ok=candle_liquidity_ok,
                    breakout_structure_ok=breakout_structure_ok,
                    regime_ok=retest_regime_ok,
                    cautions=cautions,
                    direction_ok=bullish_build_up_ok,
                )
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
            self.last_entry_plan = self._build_entry_plan("CE", "RETEST", level, support, atr, support, resistance)
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            self._mark_signal_emitted("CE", "RETEST", candle_time, level=level, buffer=buffer)
            return "CE", f"Strong-context retest support entry above {level} | score={score}"

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
            self.last_entry_plan = self._build_entry_plan("PE", "RETEST", level, resistance, atr, support, resistance)
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            self._mark_signal_emitted("PE", "RETEST", candle_time, level=level, buffer=buffer)
            return "PE", f"Breakdown retest resistance entry below {level} | score={score}"

        if (
                active_retest
                and active_retest["direction"] == "PE"
                and candle_high is not None
                and candle_close is not None
                and price < vwap
                and candle_high >= active_retest["level"] - retest_zone
                and candle_close <= active_retest["level"]
                and volume_signal == "STRONG"
                and oi_bias in ["BEARISH", "NEUTRAL"]
                and oi_trend in ["BEARISH", "NEUTRAL", None]
                and bearish_build_up_ok
                and candle_liquidity_ok
                and breakout_structure_ok
                and score >= max(time_thresholds["retest_min_score"] + 8, 72)
                and not opening_session
                and retest_regime_ok
                and self._strong_context_soft_entry_ready(
                    score=score,
                    entry_score=self.last_entry_score,
                    volume_signal=volume_signal,
                    candle_liquidity_ok=candle_liquidity_ok,
                    breakout_structure_ok=breakout_structure_ok,
                    regime_ok=retest_regime_ok,
                    cautions=cautions,
                    direction_ok=bearish_build_up_ok,
                )
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
            self.last_entry_plan = self._build_entry_plan("PE", "RETEST", level, resistance, atr, support, resistance)
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            self._mark_signal_emitted("PE", "RETEST", candle_time, level=level, buffer=buffer)
            return "PE", f"Strong-context retest resistance entry below {level} | score={score}"

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
                and (not opening_session or opening_breakout_override)
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
            self._remember_breakout_context("CE", "BREAKOUT", candle_time, orb_high, score)
            self.last_entry_plan = self._build_entry_plan("CE", "BREAKOUT", orb_high, orb_low, atr, support, resistance)
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
                and (not opening_session or opening_breakout_override)
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
            self._remember_breakout_context("PE", "BREAKOUT", candle_time, orb_low, score)
            self.last_entry_plan = self._build_entry_plan("PE", "BREAKOUT", orb_low, orb_high, atr, support, resistance)
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
                and not opening_session
                and volume_signal in ["NORMAL", "STRONG"]
                and oi_trend != "BEARISH"
                and bullish_build_up_ok
                and score >= max(time_thresholds["reversal_min_score"], 62)
                and pressure_metrics
                and pressure_metrics["pressure_bias"] == "BULLISH"
                and breakout_structure_ok
                and reversal_regime_ok
                and self._reversal_confirmation_ready("CE", candle_open, candle_close, breakout_body_ok, ha_strength)
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="REVERSAL",
            )
            self.last_entry_plan = self._build_entry_plan("CE", "REVERSAL", support, support - max(buffer, 5) if support is not None else None, atr, support, resistance)
            self._reset_confirmation_setup()
            return "CE", f"Support Bounce + Bullish OI | score={score}"

        # =============================
        # Resistance Rejection Trade
        # =============================
        elif (
                resistance is not None
                and price >= resistance - buffer
                and not opening_session
                and volume_signal in ["NORMAL", "STRONG"]
                and oi_trend != "BULLISH"
                and bearish_build_up_ok
                and score >= max(time_thresholds["reversal_min_score"], 62)
                and pressure_metrics
                and pressure_metrics["pressure_bias"] == "BEARISH"
                and breakout_structure_ok
                and reversal_regime_ok
                and self._reversal_confirmation_ready("PE", candle_open, candle_close, breakout_body_ok, ha_strength)
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="REVERSAL",
            )
            self.last_entry_plan = self._build_entry_plan("PE", "REVERSAL", resistance, resistance + max(buffer, 5) if resistance is not None else None, atr, support, resistance)
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
                and adx_trade_ok
                and mtf_trade_ok
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="AGGRESSIVE_CONTINUATION",
            )
            self.last_entry_plan = self._build_entry_plan("CE", "AGGRESSIVE_CONTINUATION", price, vwap, atr, support, resistance)
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
                and adx_trade_ok
                and mtf_trade_ok
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="AGGRESSIVE_CONTINUATION",
            )
            self.last_entry_plan = self._build_entry_plan("PE", "AGGRESSIVE_CONTINUATION", price, vwap, atr, support, resistance)
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
                and adx_trade_ok
                and mtf_trade_ok
                and (pressure_conflict_level in {"NONE", "MILD"} or recent_breakout_context)
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="CONTINUATION",
            )
            self.last_entry_plan = self._build_entry_plan("CE", "CONTINUATION", price, vwap, atr, support, resistance)
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
                and (adx_trade_ok or recent_breakout_context)
                and (mtf_trade_ok or recent_breakout_context)
                and (recent_breakout_context or pressure_conflict_level == "NONE")
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="CONTINUATION",
            )
            self.last_entry_plan = self._build_entry_plan("CE", "CONTINUATION", price, vwap, atr, support, resistance)
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
                and adx_trade_ok
                and mtf_trade_ok
                and (pressure_conflict_level in {"NONE", "MILD"} or recent_breakout_context)
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="CONTINUATION",
            )
            self.last_entry_plan = self._build_entry_plan("PE", "CONTINUATION", price, vwap, atr, support, resistance)
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
                and (adx_trade_ok or recent_breakout_context)
                and (mtf_trade_ok or recent_breakout_context)
                and (recent_breakout_context or pressure_conflict_level == "NONE")
        ):
            confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="CONTINUATION",
            )
            self.last_entry_plan = self._build_entry_plan("PE", "CONTINUATION", price, vwap, atr, support, resistance)
            self._reset_retest_setup()
            self._reset_confirmation_setup()
            return "PE", f"Continuation follow-through setup | score={score}"

        if scored_direction and score >= Config.MIN_SCORE_THRESHOLD:
            pressure_conflict_ce = self._pressure_conflict(cautions, pressure_metrics, "CE")
            pressure_conflict_pe = self._pressure_conflict(cautions, pressure_metrics, "PE")
            if (
                    orb_ready
                    and scored_direction == "CE"
                    and price > orb_high
                    and price > vwap
                    and volume_signal in ["NORMAL", "STRONG"]
                    and oi_bias in ["BULLISH", "NEUTRAL"]
                    and oi_trend in ["BULLISH", "NEUTRAL", None]
                    and bullish_build_up_ok
                    and candle_liquidity_ok
                    and not opening_session
                    and continuation_regime_ok
                    and (
                        (not breakout_body_ok or not breakout_structure_ok)
                        or pressure_conflict_ce
                    )
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
                    and candle_liquidity_ok
                    and not opening_session
                    and continuation_regime_ok
                    and (
                        (not breakout_body_ok or not breakout_structure_ok)
                        or pressure_conflict_pe
                    )
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
                    and not (orb_high is not None and price > orb_high + (buffer * 3))
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
                    and not (orb_low is not None and price < orb_low - (buffer * 3))
            ):
                self._set_retest_setup("PE", orb_low, candle_time, score)
                cautions = cautions + ["retest_watch_active"]

            if (
                    orb_ready
                    and scored_direction == "CE"
                    and price > orb_high + (buffer * 3)
                    and price > vwap
                    and volume_signal in ["NORMAL", "STRONG"]
                    and bullish_build_up_ok
                    and candle_liquidity_ok
                    and not opening_session
                    and retest_regime_ok
            ):
                self._set_retest_setup("CE", orb_high, candle_time, score)
                cautions = cautions + ["retest_watch_active"]

            if (
                    orb_ready
                    and scored_direction == "PE"
                    and price < orb_low - (buffer * 3)
                    and price < vwap
                    and volume_signal in ["NORMAL", "STRONG"]
                    and bearish_build_up_ok
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
                if not opening_breakout_override:
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
                signal_type=self._watch_signal_type(cautions, "NONE"),
            )
            self.last_decision_state = self._derive_decision_state(
                signal_type=self._watch_signal_type(cautions, "NONE"),
                signal=None,
                score=self.last_context_score,
                entry_score=self.last_entry_score,
                confidence=self.last_confidence,
                blockers=self.last_blockers,
                cautions=self.last_cautions,
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
            signal_type=self._watch_signal_type(cautions, "NONE"),
        )
        self.last_decision_state = self._derive_decision_state(
            signal_type=self._watch_signal_type(cautions, "NONE"),
            signal=None,
            score=self.last_context_score,
            entry_score=self.last_entry_score,
            confidence=self.last_confidence,
            blockers=self.last_blockers,
            cautions=self.last_cautions,
        )
        return None, f"No valid setup | score={score}"
