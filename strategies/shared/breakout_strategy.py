from shared.utils.time_utils import TimeUtils
from config import Config
from strategies.shared.expiry_day_rules import ExpiryDayRules

# New option buyer protection indicators
from shared.indicators.adx_calculator import ADXCalculator, OPTION_BUYER_MIN_ADX
from shared.indicators.volume_spike_detector import VolumeSpikeDetector, is_volume_confirmed_for_option_buying
from shared.indicators.session_rules import SessionRules, is_optimal_trading_time
from shared.indicators.oi_buildup_analyzer import OIBuildupAnalyzer, is_oi_confirming_trend
from shared.indicators.multi_timeframe_trend import MultiTimeframeTrend, quick_trend_alignment_check
from strategies.shared.confirmation_retest_evaluator import confirmation_ready, retest_ready
from strategies.shared.continuation_evaluator import fallback_continuation_ready, high_score_continuation_ready
from strategies.shared.breakout_daystate import (
    apply_day_state_adjustment,
    derive_active_day_state,
    derive_opening_bias,
)
from strategies.shared.breakout_scoring import score_signal_components
from strategies.shared.breakout_confirmation_engine import evaluate_confirmation_and_retest
from strategies.shared.breakout_continuation_engine import (
    evaluate_aggressive_continuations,
    evaluate_core_continuations,
)
from strategies.shared.breakout_orb_engine import evaluate_manual_confirmations, evaluate_orb_breakouts
from strategies.shared.breakout_reversal_engine import evaluate_reversal_setups
from strategies.shared.breakout_watch_engine import finalize_watch_state
from strategies.shared.decision_rules import calculate_entry_score, derive_decision_state, grade_signal
from strategies.shared.setup_helpers import (
    build_entry_plan,
    early_impulse_breakout_ready,
    entry_too_extended,
    price_led_hybrid_fallback_ready,
    recent_breakout_context,
    sensex_hybrid_fallback_ready,
    sensex_volume_flexible,
    watch_bucket,
)
from strategies.shared.time_regime_thresholds import build_time_regime_thresholds


class BreakoutStrategy:
    def __init__(self, for_option_buyer=True, instrument="NIFTY"):
        self.time_utils = TimeUtils()
        self.instrument = (instrument or "NIFTY").upper()
        self.expiry_rules = ExpiryDayRules(self.time_utils, instrument=self.instrument)
        self.for_option_buyer = for_option_buyer  # Enable strict mode for option buyers

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
        self.last_price_structure_score = 0
        self.last_option_flow_score = 0
        self.last_oi_structure_score = 0
        self.last_contract_quality_score = 0
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
        self.last_participation_metrics = None
        self.last_is_expiry_day = False
        self.last_signal_type = "NONE"
        self.last_signal_grade = "SKIP"
        self.last_heikin_ashi = None
        self.prev_heikin_ashi_open = None
        self.prev_heikin_ashi_close = None
        self.last_opening_bias = "UNKNOWN"
        self.last_active_day_state = "UNKNOWN"
        self.last_day_state_direction = "NONE"
        self.last_day_state_detail = ""
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
                "extension_buffer_mult": 3.0,
            },
            "BANKNIFTY": {
                "far_vwap_atr_mult": 1.85,
                "body_buffer_mult": 0.5,
                "body_floor": 12,
                "retest_zone_floor": 18,
                "watch_realert_move_mult": 0.8,
                "extension_buffer_mult": 3.2,
            },
            "SENSEX": {
                "far_vwap_atr_mult": 1.95,
                "body_buffer_mult": 0.48,
                "body_floor": 10,
                "retest_zone_floor": 16,
                "watch_realert_move_mult": 0.75,
                "extension_buffer_mult": 4.0,
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
        bars_remaining = 3 if (score or 0) >= 68 else 2
        self.confirmation_setup = {
            "direction": direction,
            "level": level,
            "bars_remaining": bars_remaining,
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
        return grade_signal(
            score=score,
            confidence=confidence,
            cautions=cautions,
            blockers=blockers,
            signal_type=signal_type,
        )

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
        price_structure_score=None,
        option_flow_score=None,
        oi_structure_score=None,
        contract_quality_score=None,
    ):
        return calculate_entry_score(
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
            price_structure_score=price_structure_score,
            option_flow_score=option_flow_score,
            oi_structure_score=oi_structure_score,
            contract_quality_score=contract_quality_score,
        )

    def _derive_decision_state(self, signal_type, signal, score, entry_score, confidence, blockers, cautions):
        return derive_decision_state(
            signal_type=signal_type,
            signal=signal,
            score=score,
            entry_score=entry_score,
            confidence=confidence,
            blockers=blockers,
            cautions=cautions,
        )

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

    def _reversal_setup_ready(
        self,
        direction,
        price,
        vwap,
        support,
        resistance,
        prev_high,
        prev_low,
        prev_close,
        candle_open,
        candle_high,
        candle_low,
        candle_close,
        buffer,
        volume_signal,
        score,
        entry_score,
        pressure_metrics,
        pressure_conflict_level,
        breakout_body_ok,
        breakout_structure_ok,
        candle_liquidity_ok,
        ha_strength,
        build_up_ok,
        regime_ok,
        time_regime,
        cautions,
    ):
        if direction not in {"CE", "PE"}:
            return False
        if not regime_ok or not candle_liquidity_ok:
            return False
        if volume_signal not in {"NORMAL", "STRONG"}:
            return False
        if score < 60 or entry_score < 48:
            return False
        if not build_up_ok:
            return False
        if pressure_conflict_level == "HARD":
            return False
        if self._soft_conflict_count(cautions) > 3:
            return False

        vwap_cross_ok = False
        sr_reclaim_ok = False
        rejection_wick_ok = False

        if direction == "CE":
            vwap_cross_ok = self._crossed_from_below_to_above(prev_low, prev_close, candle_close, vwap)
            sr_reclaim_ok = self._crossed_from_below_to_above(prev_low, prev_close, candle_close, support)
            if candle_low is not None and candle_close is not None and candle_open is not None:
                rejection_wick_ok = candle_low <= (support + max(buffer, 5)) and candle_close >= max(candle_open, support or candle_close)
            pressure_ok = not pressure_metrics or pressure_metrics["pressure_bias"] in {"BULLISH", "NEUTRAL"}
            if time_regime == "MIDDAY" and pressure_conflict_level == "MODERATE":
                pressure_ok = pressure_ok and score >= 72
        else:
            vwap_cross_ok = self._crossed_from_above_to_below(prev_high, prev_close, candle_close, vwap)
            sr_reclaim_ok = self._crossed_from_above_to_below(prev_high, prev_close, candle_close, resistance)
            if candle_high is not None and candle_close is not None and candle_open is not None:
                rejection_wick_ok = candle_high >= (resistance - max(buffer, 5)) and candle_close <= min(candle_open, resistance or candle_close)
            pressure_ok = not pressure_metrics or pressure_metrics["pressure_bias"] in {"BEARISH", "NEUTRAL"}
            if time_regime == "MIDDAY" and pressure_conflict_level == "MODERATE":
                pressure_ok = pressure_ok and score >= 72

        confirmation_ok = self._reversal_confirmation_ready(
            direction, candle_open, candle_close, breakout_body_ok or rejection_wick_ok, ha_strength
        )
        structure_ok = breakout_structure_ok or rejection_wick_ok
        reclaim_ok = vwap_cross_ok or sr_reclaim_ok
        return pressure_ok and confirmation_ok and structure_ok and reclaim_ok

    def _neutral_pressure_reversal_ready(
        self,
        scored_direction,
        score,
        entry_score,
        pressure_metrics,
        prev_high,
        prev_low,
        prev_close,
        candle_close,
        candle_high,
        candle_low,
        vwap,
        support,
        resistance,
        breakout_body_ok,
        breakout_structure_ok,
        candle_liquidity_ok,
        cautions,
    ):
        if scored_direction not in {"CE", "PE"}:
            return False
        if not pressure_metrics or pressure_metrics.get("pressure_bias") != "NEUTRAL":
            return False
        if score < 70 or entry_score < 58:
            return False
        if not candle_liquidity_ok:
            return False
        if self._soft_conflict_count(cautions) > 3:
            return False

        if scored_direction == "CE":
            reclaim_ok = self._crossed_from_below_to_above(prev_low, prev_close, candle_close, vwap) or \
                self._crossed_from_below_to_above(prev_low, prev_close, candle_close, support)
            wick_ok = (
                candle_low is not None
                and support is not None
                and candle_close is not None
                and candle_low <= support
                and candle_close >= support
            )
        else:
            reclaim_ok = self._crossed_from_above_to_below(prev_high, prev_close, candle_close, vwap) or \
                self._crossed_from_above_to_below(prev_high, prev_close, candle_close, resistance)
            wick_ok = (
                candle_high is not None
                and resistance is not None
                and candle_close is not None
                and candle_high >= resistance
                and candle_close <= resistance
            )

        return reclaim_ok and (breakout_body_ok or breakout_structure_ok or wick_ok)

    def _reversal_trap_context(
        self,
        prev_high,
        prev_low,
        prev_close,
        candle_open,
        candle_high,
        candle_low,
        candle_close,
        vwap,
        support,
        resistance,
        volume_signal,
        candle_liquidity_ok,
    ):
        result = {
            "CE": {"ready": False, "score_boost": 0},
            "PE": {"ready": False, "score_boost": 0},
        }
        if candle_close is None or not candle_liquidity_ok or volume_signal not in {"NORMAL", "STRONG"}:
            return result

        bullish_vwap_reclaim = self._crossed_from_below_to_above(prev_low, prev_close, candle_close, vwap)
        bullish_support_reclaim = self._crossed_from_below_to_above(prev_low, prev_close, candle_close, support)
        bearish_vwap_reject = self._crossed_from_above_to_below(prev_high, prev_close, candle_close, vwap)
        bearish_resistance_reject = self._crossed_from_above_to_below(prev_high, prev_close, candle_close, resistance)

        bullish_wick = (
            candle_low is not None
            and support is not None
            and candle_open is not None
            and candle_low <= support
            and candle_close >= max(candle_open, support)
        )
        bearish_wick = (
            candle_high is not None
            and resistance is not None
            and candle_open is not None
            and candle_high >= resistance
            and candle_close <= min(candle_open, resistance)
        )

        if bullish_vwap_reclaim or bullish_support_reclaim or bullish_wick:
            result["CE"]["ready"] = True
            result["CE"]["score_boost"] = 8
            if bullish_vwap_reclaim and bullish_support_reclaim:
                result["CE"]["score_boost"] += 4
            if bullish_wick:
                result["CE"]["score_boost"] += 2

        if bearish_vwap_reject or bearish_resistance_reject or bearish_wick:
            result["PE"]["ready"] = True
            result["PE"]["score_boost"] = 8
            if bearish_vwap_reject and bearish_resistance_reject:
                result["PE"]["score_boost"] += 4
            if bearish_wick:
                result["PE"]["score_boost"] += 2

        return result

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
            "pressure_neutral",
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

    def _strong_option_sweep_trade_ready(
        self,
        option_sweep_context,
        direction,
        price,
        vwap,
        candle_close,
        trigger_level,
        atr,
        buffer,
        candle_liquidity_ok,
        score,
        entry_score,
        pressure_conflict_level,
    ):
        if (
            not option_sweep_context
            or direction not in {"CE", "PE"}
            or option_sweep_context.get("direction") != direction
            or option_sweep_context.get("quality") != "STRONG"
            or not option_sweep_context.get("micro_confirmed")
            or option_sweep_context.get("persistence_pairs", 0) < 3
        ):
            return False
        if not candle_liquidity_ok:
            return False
        if float(score or 0) < 78 or float(entry_score or 0) < 70:
            return False
        if pressure_conflict_level not in {"NONE", "MILD"}:
            return False
        if direction == "CE" and not (price is not None and vwap is not None and price > vwap):
            return False
        if direction == "PE" and not (price is not None and vwap is not None and price < vwap):
            return False

        return True

    def _late_day_price_expansion_ready(
        self,
        *,
        current_now,
        scored_direction,
        price,
        vwap,
        prev_low,
        prev_high,
        candle_open,
        candle_high,
        candle_low,
        candle_close,
        candle_range,
        candle_body,
        atr,
        buffer,
        volume_signal,
        candle_liquidity_ok,
        pressure_conflict_level,
        support=None,
        resistance=None,
        wall_break_alert=None,
        support_wall_state=None,
        resistance_wall_state=None,
    ):
        if current_now is None or current_now < self.time_utils._parse_clock("14:25"):
            return None
        if scored_direction not in {"CE", "PE"} or not candle_liquidity_ok:
            return None
        if volume_signal not in {"NORMAL", "STRONG"}:
            return None
        if pressure_conflict_level == "HARD":
            return None
        if None in (price, vwap, candle_open, candle_high, candle_low, candle_close):
            return None

        if scored_direction == "PE":
            if prev_low is None or not (price < vwap and candle_close < candle_open):
                return None
            level = prev_low
            if support is not None and support < prev_low and candle_close < support:
                level = support
            trigger_touched = candle_low
            trigger_close_ok = candle_close <= level - max(buffer * 0.25, 3)
            trigger_wick_ok = candle_low <= level - max(buffer * 0.25, 3)
            invalidate_price = max(
                value for value in (prev_high, vwap, candle_high) if value is not None
            )
        else:
            if prev_high is None or not (price > vwap and candle_close > candle_open):
                return None
            level = prev_high
            if resistance is not None and resistance > prev_high and candle_close > resistance:
                level = resistance
            trigger_touched = candle_high
            trigger_close_ok = candle_close >= level + max(buffer * 0.25, 3)
            trigger_wick_ok = candle_high >= level + max(buffer * 0.25, 3)
            invalidate_price = min(
                value for value in (prev_low, vwap, candle_low) if value is not None
            )

        body_ratio = candle_body / candle_range if candle_range else 0
        min_range = max((atr or 0) * 0.35, buffer * 1.2, 8)

        if not trigger_close_ok:
            return None
        if not trigger_wick_ok:
            return None
        if body_ratio < 0.38 or candle_range < min_range:
            return None

        wall_aligned = (
            (scored_direction == "CE" and wall_break_alert == "RESISTANCE_BREAK_RISK")
            or (scored_direction == "PE" and wall_break_alert == "SUPPORT_BREAK_RISK")
            or (scored_direction == "CE" and resistance_wall_state == "WEAKENING")
            or (scored_direction == "PE" and support_wall_state == "WEAKENING")
        )

        return {
            "level": level,
            "trigger_touched": trigger_touched,
            "invalidate_price": invalidate_price,
            "wall_aligned": wall_aligned,
        }

    @staticmethod
    def _watch_bucket(signal_type, blockers, cautions):
        return watch_bucket(signal_type, blockers, cautions)

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
        return recent_breakout_context(self.breakout_memory, direction, candle_time, price, vwap, buffer)

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
        return build_entry_plan(
            self.instrument,
            direction,
            signal_type,
            trigger_price,
            invalidate_price,
            atr,
            support,
            resistance,
        )

    def _entry_too_extended(self, direction, close_price, trigger_level, atr, buffer):
        return entry_too_extended(self.instrument, direction, close_price, trigger_level, atr, buffer)

    def _early_impulse_breakout_ready(
        self,
        direction,
        score,
        volume_signal,
        candle_liquidity_ok,
        breakout_body_ok,
        breakout_structure_ok,
        pressure_conflict_level,
        time_regime,
        close_price,
        trigger_level,
        atr,
        buffer,
    ):
        return early_impulse_breakout_ready(
            self.instrument,
            direction,
            score,
            volume_signal,
            candle_liquidity_ok,
            breakout_body_ok,
            breakout_structure_ok,
            pressure_conflict_level,
            time_regime,
            close_price,
            trigger_level,
            atr,
            buffer,
        )

    def _sensex_volume_flexible(self, volume_signal, score, time_regime, candle_range, atr):
        return sensex_volume_flexible(self.instrument, volume_signal, score, time_regime, candle_range, atr)

    def _sensex_hybrid_fallback_ready(
        self,
        direction,
        score,
        time_regime,
        price,
        vwap,
        orb_high,
        orb_low,
        candle_close,
        candle_range,
        atr,
        candle_liquidity_ok,
        breakout_body_ok,
        breakout_structure_ok,
        ha_ok,
        pressure_conflict_level,
    ):
        return sensex_hybrid_fallback_ready(
            self.instrument,
            direction,
            score,
            time_regime,
            price,
            vwap,
            orb_high,
            orb_low,
            candle_close,
            candle_range,
            atr,
            candle_liquidity_ok,
            breakout_body_ok,
            breakout_structure_ok,
            ha_ok,
            pressure_conflict_level,
        )

    def _price_led_hybrid_fallback_ready(
        self,
        direction,
        score,
        entry_score,
        time_regime,
        price,
        vwap,
        orb_high,
        orb_low,
        candle_close,
        candle_range,
        atr,
        candle_liquidity_ok,
        breakout_body_ok,
        breakout_structure_ok,
        ha_ok,
        pressure_conflict_level,
        volume_signal,
        trend_day_context=False,
    ):
        return price_led_hybrid_fallback_ready(
            self.instrument,
            direction,
            score,
            entry_score,
            time_regime,
            price,
            vwap,
            orb_high,
            orb_low,
            candle_close,
            candle_range,
            atr,
            candle_liquidity_ok,
            breakout_body_ok,
            breakout_structure_ok,
            ha_ok,
            pressure_conflict_level,
            volume_signal,
            trend_day_context=trend_day_context,
        )

    @staticmethod
    def _candle_value(candle, key, index):
        if candle is None:
            return None
        if isinstance(candle, dict):
            return candle.get(key)
        if isinstance(candle, (list, tuple)) and len(candle) > index:
            return candle[index]
        return None

    def _recent_price_sequence_ready(self, recent_candles_5m, direction):
        if not recent_candles_5m or len(recent_candles_5m) < 3:
            return False
        candles = recent_candles_5m[-3:]
        closes = [self._candle_value(c, "close", 4) for c in candles]
        opens = [self._candle_value(c, "open", 1) for c in candles]
        highs = [self._candle_value(c, "high", 2) for c in candles]
        lows = [self._candle_value(c, "low", 3) for c in candles]
        if any(v is None for v in closes + opens + highs + lows):
            return False

        if direction == "CE":
            higher_closes = closes[0] < closes[1] < closes[2]
            higher_highs = highs[0] <= highs[1] <= highs[2]
            supportive_bodies = sum(1 for o, c in zip(opens, closes) if c >= o) >= 2
            return higher_closes and higher_highs and supportive_bodies

        lower_closes = closes[0] > closes[1] > closes[2]
        lower_lows = lows[0] >= lows[1] >= lows[2]
        supportive_bodies = sum(1 for o, c in zip(opens, closes) if c <= o) >= 2
        return lower_closes and lower_lows and supportive_bodies

    def _nifty_trend_day_context_ready(
        self,
        direction,
        price,
        vwap,
        orb_high,
        orb_low,
        candle_close,
        candle_range,
        atr,
        time_regime,
        volume_signal,
        pressure_conflict_level,
        ha_strength,
        recent_candles_5m,
    ):
        if self.instrument not in {"NIFTY", "BANKNIFTY", "SENSEX"} or direction not in {"CE", "PE"}:
            return False

        profile = {
            "NIFTY": {
                "allowed_regimes": {"OPENING", "MID_MORNING", "MIDDAY"},
                "weak_volume_range_atr": 0.65,
                "max_extension_floor": 22,
                "max_extension_atr_mult": 1.35,
            },
            "BANKNIFTY": {
                "allowed_regimes": {"OPENING", "MID_MORNING", "MIDDAY"},
                "weak_volume_range_atr": 0.72,
                "max_extension_floor": 38,
                "max_extension_atr_mult": 1.2,
            },
            "SENSEX": {
                "allowed_regimes": {"OPENING", "MID_MORNING", "MIDDAY", "LATE_DAY"},
                "weak_volume_range_atr": 0.68,
                "max_extension_floor": 28,
                "max_extension_atr_mult": 1.25,
            },
        }[self.instrument]

        if time_regime not in profile["allowed_regimes"]:
            return False
        if pressure_conflict_level not in {"NONE", "MILD", "MODERATE"}:
            return False
        if volume_signal == "WEAK":
            if atr is None or candle_range < atr * profile["weak_volume_range_atr"]:
                return False
        elif volume_signal not in {"NORMAL", "STRONG"}:
            return False
        if not self._recent_price_sequence_ready(recent_candles_5m, direction):
            return False
        if direction == "CE":
            if price is None or vwap is None or price <= vwap:
                return False
            if orb_high is None or price <= orb_high:
                return False
            if ha_strength not in {"BULLISH", "BULLISH_STRONG"}:
                return False
            trigger_level = orb_high
        else:
            if price is None or vwap is None or price >= vwap:
                return False
            if orb_low is None or price >= orb_low:
                return False
            if ha_strength not in {"BEARISH", "BEARISH_STRONG"}:
                return False
            trigger_level = orb_low
        if self._entry_too_extended(
            direction,
            candle_close or price,
            trigger_level,
            atr,
            max((atr or 0) * profile["max_extension_atr_mult"], profile["max_extension_floor"]),
        ):
            return False
        return True

    def _nifty_post_expiry_continuation_ready(
        self,
        direction,
        score,
        entry_score,
        expiry_session_mode,
        time_regime,
        price,
        vwap,
        orb_high,
        orb_low,
        candle_close,
        candle_range,
        atr,
        candle_liquidity_ok,
        breakout_body_ok,
        breakout_structure_ok,
        ha_ok,
        pressure_conflict_level,
        volume_signal,
        recent_breakout_context,
    ):
        if self.instrument != "NIFTY" or expiry_session_mode != "POST_EXPIRY_REBUILD":
            return False
        if direction not in {"CE", "PE"}:
            return False
        if time_regime not in {"MID_MORNING", "MIDDAY"}:
            return False
        if score < 68 or entry_score < 62:
            return False
        if volume_signal not in {"NORMAL", "STRONG"}:
            return False
        if not candle_liquidity_ok or not ha_ok:
            return False
        if pressure_conflict_level not in {"NONE", "MILD"}:
            return False
        if not (breakout_body_ok or breakout_structure_ok):
            return False
        if atr is not None and candle_range < atr * 0.55:
            return False
        if direction == "CE":
            trigger_level = orb_high
            if price is None or vwap is None or price <= vwap:
                return False
            if trigger_level is None or price <= trigger_level:
                return False
        else:
            trigger_level = orb_low
            if price is None or vwap is None or price >= vwap:
                return False
            if trigger_level is None or price >= trigger_level:
                return False
        if self._entry_too_extended(
            direction,
            candle_close or price,
            trigger_level,
            atr,
            max((atr or 0) * 0.16, 5),
        ):
            return False
        return recent_breakout_context or score >= 72

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

    @staticmethod
    def _crossed_from_below_to_above(prev_low, prev_close, current_close, level):
        if level is None or current_close is None:
            return False
        prev_below = (
            (prev_low is not None and prev_low < level)
            or (prev_close is not None and prev_close < level)
        )
        return prev_below and current_close > level

    @staticmethod
    def _crossed_from_above_to_below(prev_high, prev_close, current_close, level):
        if level is None or current_close is None:
            return False
        prev_above = (
            (prev_high is not None and prev_high > level)
            or (prev_close is not None and prev_close > level)
        )
        return prev_above and current_close < level

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

    def _get_time_regime_thresholds(self, time_regime, fallback_mode, market_regime=None):
        return build_time_regime_thresholds(
            instrument=self.instrument,
            time_regime=time_regime,
            fallback_mode=fallback_mode,
            market_regime=market_regime,
        )

    def _derive_opening_bias(self, recent_candles_5m, vwap, atr):
        return derive_opening_bias(
            recent_candles_5m=recent_candles_5m,
            vwap=vwap,
            atr=atr,
            trade_start=self.time_utils._parse_clock(Config.TRADE_START_TIME),
            first_30_end=self.time_utils._parse_clock("09:45"),
            first_60_end=self.time_utils._parse_clock("10:15"),
        )

    def _derive_active_day_state(self, recent_candles_5m, price, vwap, atr, pressure_metrics, opening_bias, time_regime):
        return derive_active_day_state(
            recent_candles_5m=recent_candles_5m,
            price=price,
            vwap=vwap,
            atr=atr,
            pressure_metrics=pressure_metrics,
            opening_bias=opening_bias,
            time_regime=time_regime,
        )

    def _apply_day_state_adjustment(self, score, scored_direction, cautions, components, day_state):
        return apply_day_state_adjustment(
            score=score,
            scored_direction=scored_direction,
            cautions=cautions,
            components=components,
            day_state=day_state,
            append_cautions=self._append_cautions,
        )

    def _sensex_late_day_guard(self, current_now, score, entry_score, confidence, volume_signal, pressure_conflict_level):
        return self._sensex_late_day_guard_with_context(
            current_now=current_now,
            score=score,
            entry_score=entry_score,
            confidence=confidence,
            volume_signal=volume_signal,
            pressure_conflict_level=pressure_conflict_level,
            option_sweep_context=None,
            direction=None,
        )

    def _sensex_late_day_guard_with_context(
        self,
        current_now,
        score,
        entry_score,
        confidence,
        volume_signal,
        pressure_conflict_level,
        option_sweep_context=None,
        direction=None,
    ):
        if self.instrument != "SENSEX" or current_now is None:
            return None

        confidence = (confidence or "LOW").upper()
        pressure_conflict_level = (pressure_conflict_level or "NONE").upper()
        sweep_override = (
            option_sweep_context
            and option_sweep_context.get("direction") == direction
            and option_sweep_context.get("quality") == "STRONG"
            and option_sweep_context.get("micro_confirmed")
            and option_sweep_context.get("persistence_pairs", 0) >= 3
            and float(score or 0) >= 84
            and float(entry_score or 0) >= 84
            and pressure_conflict_level in {"NONE", "MILD"}
        )

        if current_now >= self.time_utils._parse_clock("14:25"):
            if sweep_override:
                return None
            if volume_signal not in {"NORMAL", "STRONG"}:
                return "sensex_late_day_requires_strong_volume"
            if confidence not in {"MEDIUM", "HIGH"}:
                return "sensex_late_day_requires_high_confidence"
            if pressure_conflict_level not in {"NONE", "MILD"}:
                return "sensex_late_day_pressure_conflict"
            if float(score or 0) < 80 or float(entry_score or 0) < 82:
                return "sensex_late_day_requires_elite_score"

        return None

    @staticmethod
    def _direction_vwap_aligned(direction, price, vwap):
        if direction == "CE":
            return price is not None and vwap is not None and price > vwap
        if direction == "PE":
            return price is not None and vwap is not None and price < vwap
        return False

    @staticmethod
    def _previous_candle(recent_candles_5m):
        if not recent_candles_5m or len(recent_candles_5m) < 2:
            return None
        return recent_candles_5m[-2]

    @staticmethod
    def _append_cautions(existing, *new_flags):
        merged = list(existing or [])
        for flag in new_flags:
            if flag and flag not in merged:
                merged.append(flag)
        return merged

    @staticmethod
    def _adverse_sr_cautions(direction, price, support, resistance, buffer):
        cautions = []
        zone = max(buffer * 2, 15)
        near_support = support is not None and abs(price - support) <= zone
        near_resistance = resistance is not None and abs(resistance - price) <= zone

        if direction == "PE" and near_support:
            cautions.append("near_support")
        elif direction == "CE" and near_resistance:
            cautions.append("near_resistance")
        elif direction not in {"CE", "PE"}:
            if near_support:
                cautions.append("near_support")
            if near_resistance:
                cautions.append("near_resistance")

        return cautions

    def _confidence_from_score(self, score, volume_signal, pressure_metrics, cautions):
        pressure_bias = pressure_metrics["pressure_bias"] if pressure_metrics else "NEUTRAL"
        if score >= 85 and volume_signal == "STRONG" and pressure_bias != "NEUTRAL" and not cautions:
            return "HIGH"
        if score >= 65:
            return "MEDIUM"
        return "LOW"

    def _evaluate_confirmation_and_retest(self, ctx):
        return evaluate_confirmation_and_retest(self, ctx)

    def _evaluate_core_continuations(self, ctx):
        return evaluate_core_continuations(self, ctx)

    def _evaluate_opening_drive(self, ctx):
        opening_drive_window = ctx["opening_drive_window"]
        orb_ready = ctx["orb_ready"]
        price = ctx["price"]
        orb_high = ctx["orb_high"]
        orb_low = ctx["orb_low"]
        buffer = ctx["buffer"]
        vwap = ctx["vwap"]
        volume_signal = ctx["volume_signal"]
        oi_bias = ctx["oi_bias"]
        oi_trend = ctx["oi_trend"]
        bullish_build_up_ok = ctx["bullish_build_up_ok"]
        bearish_build_up_ok = ctx["bearish_build_up_ok"]
        pressure_metrics = ctx["pressure_metrics"]
        score = ctx["score"]
        time_thresholds = ctx["time_thresholds"]
        breakout_body_ok = ctx["breakout_body_ok"]
        breakout_structure_ok = ctx["breakout_structure_ok"]
        candle_liquidity_ok = ctx["candle_liquidity_ok"]
        bullish_ha_ok = ctx["bullish_ha_ok"]
        bearish_ha_ok = ctx["bearish_ha_ok"]
        cautions = ctx["cautions"]
        blockers = ctx["blockers"]
        expiry_eval = ctx["expiry_eval"]
        regime = ctx["regime"]
        candle_time = ctx["candle_time"]
        atr = ctx["atr"]
        support = ctx["support"]
        resistance = ctx["resistance"]
        opening_far_vwap_override = ctx["opening_far_vwap_override"]

        if (
            opening_drive_window and orb_ready and price > orb_high + buffer and price > vwap
            and volume_signal == "STRONG" and oi_bias == "BULLISH"
            and oi_trend in ["BULLISH", "NEUTRAL", None]
            and bullish_build_up_ok and pressure_metrics and pressure_metrics["pressure_bias"] == "BULLISH"
            and score >= time_thresholds["opening_drive_min_score"]
            and breakout_body_ok and breakout_structure_ok and candle_liquidity_ok and bullish_ha_ok
            and "opposite_pressure" not in cautions
            and ("far_from_vwap" not in cautions or opening_far_vwap_override)
        ):
            return self._emit_trade_signal(
                "CE", "OPENING_DRIVE", score, volume_signal, pressure_metrics, cautions,
                blockers=blockers, regime=self._effective_signal_regime(expiry_eval, regime),
                candle_time=candle_time, message="Opening drive breakout up",
                trigger_price=orb_high, invalidate_price=orb_low, atr=atr, support=support, resistance=resistance,
                remember_level=orb_high, reset_retest=True, reset_confirmation=True,
            )

        if (
            opening_drive_window and orb_ready and price < orb_low - buffer and price < vwap
            and volume_signal == "STRONG" and oi_bias == "BEARISH"
            and oi_trend in ["BEARISH", "NEUTRAL", None]
            and bearish_build_up_ok and pressure_metrics and pressure_metrics["pressure_bias"] == "BEARISH"
            and score >= time_thresholds["opening_drive_min_score"]
            and breakout_body_ok and breakout_structure_ok and candle_liquidity_ok and bearish_ha_ok
            and "opposite_pressure" not in cautions
            and ("far_from_vwap" not in cautions or opening_far_vwap_override)
        ):
            return self._emit_trade_signal(
                "PE", "OPENING_DRIVE", score, volume_signal, pressure_metrics, cautions,
                blockers=blockers, regime=self._effective_signal_regime(expiry_eval, regime),
                candle_time=candle_time, message="Opening drive breakdown down",
                trigger_price=orb_low, invalidate_price=orb_high, atr=atr, support=support, resistance=resistance,
                remember_level=orb_low, reset_retest=True, reset_confirmation=True,
            )

        return None

    def _evaluate_hybrid_continuations(self, ctx):
        score = ctx["score"]
        volume_signal = ctx["volume_signal"]
        pressure_metrics = ctx["pressure_metrics"]
        cautions = ctx["cautions"]
        blockers = ctx["blockers"]
        expiry_eval = ctx["expiry_eval"]
        regime = ctx["regime"]
        candle_time = ctx["candle_time"]
        price = ctx["price"]
        vwap = ctx["vwap"]
        atr = ctx["atr"]
        support = ctx["support"]
        resistance = ctx["resistance"]
        orb_high = ctx["orb_high"]
        orb_low = ctx["orb_low"]
        candle_close = ctx["candle_close"]
        candle_range = ctx["candle_range"]
        candle_liquidity_ok = ctx["candle_liquidity_ok"]
        breakout_body_ok = ctx["breakout_body_ok"]
        breakout_structure_ok = ctx["breakout_structure_ok"]
        bullish_ha_ok = ctx["bullish_ha_ok"]
        bearish_ha_ok = ctx["bearish_ha_ok"]
        pressure_conflict_level = ctx["pressure_conflict_level"]
        time_regime = ctx["time_regime"]
        oi_bias = ctx["oi_bias"]
        bullish_build_up_ok = ctx["bullish_build_up_ok"]
        bearish_build_up_ok = ctx["bearish_build_up_ok"]
        nifty_trend_day_context = ctx["nifty_trend_day_context"]
        expiry_session_mode = ctx["expiry_session_mode"]
        scored_direction = ctx["scored_direction"]
        opening_session = ctx["opening_session"]

        if (
            self._sensex_hybrid_fallback_ready(
                direction="CE", score=score, time_regime=time_regime, price=price, vwap=vwap,
                orb_high=orb_high, orb_low=orb_low, candle_close=candle_close, candle_range=candle_range,
                atr=atr, candle_liquidity_ok=candle_liquidity_ok, breakout_body_ok=breakout_body_ok,
                breakout_structure_ok=breakout_structure_ok, ha_ok=bullish_ha_ok,
                pressure_conflict_level=pressure_conflict_level,
            )
            and oi_bias in {"BULLISH", "NEUTRAL"}
            and pressure_conflict_level in {"NONE", "MILD", "MODERATE"}
        ):
            hybrid_cautions = self._append_cautions(cautions, "hybrid_price_led_setup")
            return self._emit_trade_signal(
                "CE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=self._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="SENSEX hybrid price-led continuation up", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        if (
            self.instrument == "NIFTY" and expiry_session_mode == "POST_EXPIRY_REBUILD" and nifty_trend_day_context
            and scored_direction == "CE" and price > vwap and oi_bias in {"BULLISH", "NEUTRAL"}
            and ctx["oi_trend"] in {"BULLISH", "NEUTRAL", None} and bullish_build_up_ok and score >= 68
            and self.last_entry_score >= 58 and pressure_conflict_level in {"NONE", "MILD", "MODERATE"} and not opening_session
        ):
            hybrid_cautions = self._append_cautions(cautions, "trend_day_price_override", "hybrid_price_led_setup")
            return self._emit_trade_signal(
                "CE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=self._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="NIFTY trend-day continuation up", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        if (
            self._price_led_hybrid_fallback_ready(
                direction="CE", score=score, entry_score=self.last_entry_score, time_regime=time_regime,
                price=price, vwap=vwap, orb_high=orb_high, orb_low=orb_low, candle_close=candle_close,
                candle_range=candle_range, atr=atr, candle_liquidity_ok=candle_liquidity_ok,
                breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok,
                ha_ok=bullish_ha_ok, pressure_conflict_level=pressure_conflict_level,
                volume_signal=volume_signal, trend_day_context=nifty_trend_day_context,
            )
            and oi_bias in {"BULLISH", "NEUTRAL"} and bullish_build_up_ok
        ):
            hybrid_cautions = self._append_cautions(cautions, "hybrid_price_led_setup")
            return self._emit_trade_signal(
                "CE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=self._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="Hybrid price-led continuation up", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        if (
            self._sensex_hybrid_fallback_ready(
                direction="PE", score=score, time_regime=time_regime, price=price, vwap=vwap,
                orb_high=orb_high, orb_low=orb_low, candle_close=candle_close, candle_range=candle_range,
                atr=atr, candle_liquidity_ok=candle_liquidity_ok, breakout_body_ok=breakout_body_ok,
                breakout_structure_ok=breakout_structure_ok, ha_ok=bearish_ha_ok,
                pressure_conflict_level=pressure_conflict_level,
            )
            and oi_bias in {"BEARISH", "NEUTRAL"}
            and pressure_conflict_level in {"NONE", "MILD", "MODERATE"}
        ):
            hybrid_cautions = self._append_cautions(cautions, "hybrid_price_led_setup")
            return self._emit_trade_signal(
                "PE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=self._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="SENSEX hybrid price-led continuation down", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        if (
            self.instrument == "NIFTY" and expiry_session_mode == "POST_EXPIRY_REBUILD" and nifty_trend_day_context
            and scored_direction == "PE" and price < vwap and oi_bias in {"BEARISH", "NEUTRAL"}
            and ctx["oi_trend"] in {"BEARISH", "NEUTRAL", None} and bearish_build_up_ok and score >= 68
            and self.last_entry_score >= 58 and pressure_conflict_level in {"NONE", "MILD", "MODERATE"} and not opening_session
        ):
            hybrid_cautions = self._append_cautions(cautions, "trend_day_price_override", "hybrid_price_led_setup")
            return self._emit_trade_signal(
                "PE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=self._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="NIFTY trend-day continuation down", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        if (
            self._price_led_hybrid_fallback_ready(
                direction="PE", score=score, entry_score=self.last_entry_score, time_regime=time_regime,
                price=price, vwap=vwap, orb_high=orb_high, orb_low=orb_low, candle_close=candle_close,
                candle_range=candle_range, atr=atr, candle_liquidity_ok=candle_liquidity_ok,
                breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok,
                ha_ok=bearish_ha_ok, pressure_conflict_level=pressure_conflict_level,
                volume_signal=volume_signal, trend_day_context=nifty_trend_day_context,
            )
            and oi_bias in {"BEARISH", "NEUTRAL"} and bearish_build_up_ok
        ):
            hybrid_cautions = self._append_cautions(cautions, "hybrid_price_led_setup")
            return self._emit_trade_signal(
                "PE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=self._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="Hybrid price-led continuation down", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        return None

    def _evaluate_manual_confirmations(self, ctx):
        return evaluate_manual_confirmations(self, ctx)

    def _evaluate_reversal_setups(self, ctx):
        return evaluate_reversal_setups(self, ctx)

    def _evaluate_orb_breakouts(self, ctx):
        return evaluate_orb_breakouts(self, ctx)

    def _evaluate_aggressive_continuations(self, ctx):
        return evaluate_aggressive_continuations(self, ctx)

    def _finalize_watch_state(self, ctx):
        return finalize_watch_state(self, ctx)

    @staticmethod
    def _effective_signal_regime(expiry_eval, regime):
        return "EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime

    def _emit_duplicate_signal(self, blockers, cautions, regime, score, message):
        blockers.append("duplicate_signal_suppressed")
        self._set_diagnostics(
            blockers=blockers,
            cautions=cautions,
            confidence="LOW",
            regime=regime,
            signal_type="NONE",
        )
        return None, f"{message} | score={score}"

    def _emit_trade_signal(
        self,
        direction,
        signal_type,
        score,
        volume_signal,
        pressure_metrics,
        cautions,
        blockers,
        regime,
        candle_time,
        message,
        trigger_price,
        invalidate_price,
        atr,
        support,
        resistance,
        remember_level=None,
        emitted_level=None,
        buffer=0,
        reset_retest=False,
        reset_confirmation=False,
        mark_emitted=False,
    ):
        confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
        self._set_diagnostics(
            blockers=blockers,
            cautions=cautions,
            confidence=confidence,
            regime=regime,
            signal_type=signal_type,
        )
        if remember_level is not None:
            self._remember_breakout_context(direction, signal_type, candle_time, remember_level, score)
        self.last_entry_plan = self._build_entry_plan(
            direction,
            signal_type,
            trigger_price,
            invalidate_price,
            atr,
            support,
            resistance,
        )
        if reset_retest:
            self._reset_retest_setup()
        if reset_confirmation:
            self._reset_confirmation_setup()
        if mark_emitted:
            self._mark_signal_emitted(direction, signal_type, candle_time, level=emitted_level, buffer=buffer)
        return direction, f"{message} | score={score}"

    @staticmethod
    def _directional_participation(participation_metrics, direction):
        if not participation_metrics or direction not in {"CE", "PE"}:
            return None
        return participation_metrics.get(direction)

    def _apply_participation_adjustment(self, score, direction, cautions, components, participation_metrics):
        directional = self._directional_participation(participation_metrics, direction)
        self.last_participation_metrics = participation_metrics
        if not directional:
            return score, cautions

        adjusted_score = max(0, min(100, score + int(directional.get("score_boost", 0))))
        quality = directional.get("quality")
        flags = directional.get("flags") or []

        if quality == "STRONG":
            components.append("participation_strong")
        elif quality == "MODERATE":
            components.append("participation_moderate")
        elif quality == "WEAK":
            components.append("participation_weak")

        for flag in flags[:3]:
            components.append(f"participation_{flag}")

        updated_cautions = list(cautions or [])
        if quality == "WEAK" and "participation_weak" not in updated_cautions:
            updated_cautions.append("participation_weak")
        if "atm_spread_wide" in flags and "participation_spread_wide" not in updated_cautions:
            updated_cautions.append("participation_spread_wide")
        if "same_side_volume_delta_missing" in flags and "participation_delta_missing" not in updated_cautions:
            updated_cautions.append("participation_delta_missing")
        if "same_side_vs_rolling_avg_missing" in flags and "participation_baseline_weak" not in updated_cautions:
            updated_cautions.append("participation_baseline_weak")
        return adjusted_score, updated_cautions

    def _is_invalid_candle(self, candle_open, candle_high, candle_low, candle_close, candle_volume):
        if None in (candle_open, candle_high, candle_low, candle_close):
            return False
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

    def _score_signal_components(
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
        return score_signal_components(
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
        scorecard = self._score_signal_components(
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
        self.last_price_structure_score = scorecard["price_structure_score"]
        self.last_option_flow_score = scorecard["option_flow_score"]
        self.last_oi_structure_score = scorecard["oi_structure_score"]
        self.last_contract_quality_score = scorecard["contract_quality_score"]
        directional_total = max(scorecard["bullish_total"], scorecard["bearish_total"])
        context_score = (
            directional_total * 0.82
            + scorecard["contract_quality_score"] * 0.18
        )
        score = max(0, min(int(round(context_score)), 100))
        return score, scorecard["direction"], scorecard["components"]

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
            participation_metrics=None,
            oi_ladder_data=None,
            option_sweep_context=None,
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
        self.last_participation_metrics = participation_metrics
        self.last_option_sweep_context = option_sweep_context
        self.last_signal_type = "NONE"
        self.last_signal_grade = "SKIP"
        self.last_opening_bias = "UNKNOWN"
        self.last_active_day_state = "UNKNOWN"
        self.last_day_state_direction = "NONE"
        self.last_day_state_detail = ""
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

        score, cautions = self._apply_participation_adjustment(
            score=score,
            direction=scored_direction,
            cautions=cautions,
            components=components,
            participation_metrics=participation_metrics,
        )
        if (
            option_sweep_context
            and option_sweep_context.get("direction") == scored_direction
        ):
            score = min(100, score + int(option_sweep_context.get("score_boost", 0) or 0))
            components.append(
                f"option_sweep_{(option_sweep_context.get('quality') or 'moderate').lower()}"
            )
            if option_sweep_context.get("micro_confirmed"):
                components.append("option_sweep_micro_confirmed")
            if option_sweep_context.get("persistence_pairs", 0) >= 3:
                components.append("option_sweep_persistent")
            if option_sweep_context.get("quality") == "STRONG":
                cautions = [
                    caution
                    for caution in cautions
                    if caution not in {"participation_weak", "participation_delta_missing"}
                ]
        self.last_score = score
        self.last_context_score = score
        self.last_score_components = components

        current_now = candle_time.time() if candle_time is not None else self.time_utils.current_time()
        time_regime = self._derive_time_regime(current_now)
        self.last_time_regime = time_regime
        opening_bias, opening_detail = self._derive_opening_bias(recent_candles_5m, vwap, atr)
        day_state = self._derive_active_day_state(
            recent_candles_5m=recent_candles_5m,
            price=price,
            vwap=vwap,
            atr=atr,
            pressure_metrics=pressure_metrics,
            opening_bias=opening_bias,
            time_regime=time_regime,
        )
        self.last_opening_bias = opening_bias
        self.last_active_day_state = day_state.get("state") or "UNKNOWN"
        self.last_day_state_direction = day_state.get("direction") or "NONE"
        self.last_day_state_detail = day_state.get("detail") or opening_detail
        score, cautions = self._apply_day_state_adjustment(
            score=score,
            scored_direction=scored_direction,
            cautions=cautions,
            components=components,
            day_state=day_state,
        )
        self.last_score = score
        self.last_context_score = score
        self.last_entry_score = score
        self.last_score_components = components

        oi_divergence = (oi_ladder_data or {}).get("price_vs_oi_divergence")
        wall_break_alert = (oi_ladder_data or {}).get("wall_break_alert")
        support_wall_state = (oi_ladder_data or {}).get("support_wall_state")
        resistance_wall_state = (oi_ladder_data or {}).get("resistance_wall_state")
        divergence_against_direction = (
            (scored_direction == "CE" and oi_divergence == "BEARISH_DIVERGENCE")
            or (scored_direction == "PE" and oi_divergence == "BULLISH_DIVERGENCE")
        )
        divergence_supports_direction = (
            (scored_direction == "CE" and oi_divergence == "BULLISH_DIVERGENCE")
            or (scored_direction == "PE" and oi_divergence == "BEARISH_DIVERGENCE")
        )
        wall_break_supports_direction = (
            (scored_direction == "CE" and wall_break_alert == "RESISTANCE_BREAK_RISK")
            or (scored_direction == "PE" and wall_break_alert == "SUPPORT_BREAK_RISK")
        )
        wall_strength_supports_direction = (
            (scored_direction == "CE" and resistance_wall_state == "WEAKENING")
            or (scored_direction == "PE" and support_wall_state == "WEAKENING")
        )
        if divergence_against_direction:
            score = max(0, score - 8)
            cautions = self._append_cautions(cautions, "oi_divergence_against")
            components.append("oi_divergence_against")
        elif divergence_supports_direction:
            score = min(100, score + 4)
            components.append("oi_divergence_support")
        if wall_break_supports_direction:
            score = min(100, score + 6)
            components.append("oi_wall_break_risk_support")
        elif wall_strength_supports_direction:
            score = min(100, score + 3)
            components.append("oi_wall_strength_support")
        self.last_score = score
        self.last_context_score = score
        self.last_score_components = components

        bullish_buildups = ["LONG_BUILDUP", "SHORT_COVERING"]
        bearish_buildups = ["SHORT_BUILDUP", "LONG_UNWINDING"]
        fallback_mode = pressure_metrics is None
        bullish_build_up_ok = self._has_bullish_build_up(build_up) or (
            fallback_mode and oi_bias == "BULLISH" and oi_trend in ["BULLISH", "NEUTRAL"] and score >= 60
        )
        bearish_build_up_ok = self._has_bearish_build_up(build_up) or (
            fallback_mode and oi_bias == "BEARISH" and oi_trend in ["BEARISH", "NEUTRAL"] and score >= 60
        )
        if self.instrument == "SENSEX":
            bullish_build_up_ok = bullish_build_up_ok or (
                oi_bias == "BULLISH" and oi_trend in ["BULLISH", "NEUTRAL", None] and score >= 56
            )
            bearish_build_up_ok = bearish_build_up_ok or (
                oi_bias == "BEARISH" and oi_trend in ["BEARISH", "NEUTRAL", None] and score >= 56
            )

        previous_candle = self._previous_candle(recent_candles_5m)
        prev_high = previous_candle.get("high") if previous_candle else None
        prev_low = previous_candle.get("low") if previous_candle else None
        prev_close = previous_candle.get("close") if previous_candle else None
        reversal_trap_context = self._reversal_trap_context(
            prev_high=prev_high,
            prev_low=prev_low,
            prev_close=prev_close,
            candle_open=candle_open,
            candle_high=candle_high,
            candle_low=candle_low,
            candle_close=candle_close,
            vwap=vwap,
            support=support,
            resistance=resistance,
            volume_signal=volume_signal,
            candle_liquidity_ok=candle_liquidity_ok,
        )
        if reversal_trap_context["CE"]["ready"] and scored_direction == "CE":
            score = min(100, score + reversal_trap_context["CE"]["score_boost"])
            components.append("reversal_trap_reclaim")
        elif reversal_trap_context["PE"]["ready"] and scored_direction == "PE":
            score = min(100, score + reversal_trap_context["PE"]["score_boost"])
            components.append("reversal_trap_reclaim")

        nifty_trend_day_context = self._nifty_trend_day_context_ready(
            direction=scored_direction,
            price=price,
            vwap=vwap,
            orb_high=orb_high,
            orb_low=orb_low,
            candle_close=candle_close,
            candle_range=candle_range,
            atr=atr,
            time_regime=self._derive_time_regime(
                candle_time.time() if candle_time is not None else self.time_utils.current_time()
            ),
            volume_signal=volume_signal,
            pressure_conflict_level=self._pressure_conflict_level(pressure_metrics, scored_direction, cautions),
            ha_strength=ha_strength,
            recent_candles_5m=recent_candles_5m,
        )
        if nifty_trend_day_context:
            score = min(100, score + (10 if volume_signal == "WEAK" else 8))
            components.append("nifty_trend_day_context")
        self.last_score = score
        self.last_context_score = score
        self.last_score_components = components

        self._update_retest_setup(candle_time)
        self._update_confirmation_setup(candle_time)

        strong_sweep_trade_ready = False
        regime = self._derive_regime(price, vwap, atr, volume_signal, candle_range)
        if day_state.get("state") in {"BULL_TREND_ACTIVE", "BEAR_TREND_ACTIVE"} and regime in {"CHOPPY", "RANGING", "EARLY_SESSION"}:
            regime = "TRENDING"
            components.append("day_state_regime_trending")
        elif day_state.get("state") == "RANGE_ACTIVE" and regime == "TRENDING" and score < 82:
            regime = "RANGING"
            components.append("day_state_regime_range")
        if nifty_trend_day_context and regime in {"CHOPPY", "RANGING", "EARLY_SESSION"}:
            regime = "TRENDING"
            components.append("nifty_trend_day_regime_override")
        breakout_regime_ok = regime in ["TRENDING", "EXPANDING", "OPENING_EXPANSION"] or (
            regime == "RANGING" and score >= 72 and volume_signal == "STRONG"
        )
        continuation_regime_ok = regime in ["TRENDING", "EXPANDING"] or (
            regime == "RANGING" and score >= 78 and volume_signal == "STRONG"
        )
        if strong_sweep_trade_ready and regime in {"CHOPPY", "RANGING", "EARLY_SESSION"}:
            breakout_regime_ok = True
            continuation_regime_ok = True
            components.append("option_sweep_regime_override")
        retest_regime_ok = regime in ["TRENDING", "EXPANDING", "RANGING"] and not (
            regime == "RANGING" and score < 68
        )
        reversal_regime_ok = regime in ["RANGING", "CHOPPY"] or (
            scored_direction == "CE" and reversal_trap_context["CE"]["ready"]
        ) or (
            scored_direction == "PE" and reversal_trap_context["PE"]["ready"]
        )

        trade_start = self.time_utils._parse_clock(Config.TRADE_START_TIME)
        opening_session = trade_start <= current_now < self.time_utils._parse_clock("09:45")
        opening_drive_window = trade_start <= current_now < self.time_utils._parse_clock("09:40")
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

        cautions = self._append_cautions(
            cautions,
            *self._adverse_sr_cautions(scored_direction, price, support, resistance, buffer),
        )

        sweep_pressure_override = (
            option_sweep_context
            and option_sweep_context.get("direction") == scored_direction
            and option_sweep_context.get("quality") == "STRONG"
            and option_sweep_context.get("micro_confirmed")
            and option_sweep_context.get("persistence_pairs", 0) >= 3
        )
        if pressure_metrics:
            if scored_direction == "CE" and pressure_metrics["pressure_bias"] == "BEARISH":
                if not sweep_pressure_override:
                    cautions.append("opposite_pressure")
                else:
                    cautions = self._append_cautions(cautions, "option_sweep_pressure_override")
            if scored_direction == "PE" and pressure_metrics["pressure_bias"] == "BULLISH":
                if not sweep_pressure_override:
                    cautions.append("opposite_pressure")
                else:
                    cautions = self._append_cautions(cautions, "option_sweep_pressure_override")
        pressure_conflict_level = self._pressure_conflict_level(pressure_metrics, scored_direction, cautions)
        if sweep_pressure_override and pressure_conflict_level == "MODERATE":
            pressure_conflict_level = "MILD"
        if (
            day_state.get("state") == "REVERSAL_UNDERWAY"
            and day_state.get("direction") == scored_direction
            and pressure_conflict_level == "MODERATE"
        ):
            pressure_conflict_level = "MILD"
        self.last_pressure_conflict_level = pressure_conflict_level
        late_day_expansion = self._late_day_price_expansion_ready(
            current_now=current_now,
            scored_direction=scored_direction,
            price=price,
            vwap=vwap,
            prev_low=prev_low,
            prev_high=prev_high,
            candle_open=candle_open,
            candle_high=candle_high,
            candle_low=candle_low,
            candle_close=candle_close,
            candle_range=candle_range,
            candle_body=candle_body,
            atr=atr,
            buffer=buffer,
            volume_signal=volume_signal,
            candle_liquidity_ok=candle_liquidity_ok,
            pressure_conflict_level=pressure_conflict_level,
            support=support,
            resistance=resistance,
            wall_break_alert=wall_break_alert,
            support_wall_state=support_wall_state,
            resistance_wall_state=resistance_wall_state,
        )
        if late_day_expansion:
            late_day_label = "breakout" if scored_direction == "CE" else "breakdown"
            cautions = self._append_cautions(
                cautions,
                f"late_day_{late_day_label}_watch",
                "theta_fast_exit_required",
            )
            if late_day_expansion["wall_aligned"]:
                cautions = self._append_cautions(cautions, "oi_wall_break_confirmed")
                components.append("late_day_oi_wall_confirmed")
            else:
                cautions = self._append_cautions(cautions, "oi_wall_not_confirmed")
            aligned_floor = 72 if volume_signal == "STRONG" else 68
            unconfirmed_floor = 70 if volume_signal == "STRONG" else 66
            score = max(
                score,
                aligned_floor if late_day_expansion["wall_aligned"] else unconfirmed_floor,
            )
            self.last_score = score
            self.last_context_score = score
            self.last_score_components = components
            if self.last_entry_score <= 0:
                self.last_entry_score = max(62, score - 4)
            return self._emit_trade_signal(
                scored_direction,
                "BREAKOUT_CONFIRM",
                score,
                volume_signal,
                pressure_metrics,
                cautions,
                blockers=blockers,
                regime=f"LATE_DAY_{late_day_label.upper()}",
                candle_time=candle_time,
                message=f"Late-day {'resistance breakout above' if scored_direction == 'CE' else 'support breakdown below'} {round(late_day_expansion['level'], 2)}",
                trigger_price=late_day_expansion["level"],
                invalidate_price=late_day_expansion["invalidate_price"],
                atr=atr,
                support=support,
                resistance=resistance,
                remember_level=late_day_expansion["level"],
                emitted_level=late_day_expansion["level"],
                buffer=buffer,
                reset_retest=True,
                reset_confirmation=True,
                mark_emitted=True,
            )
        strong_sweep_trade_ready = self._strong_option_sweep_trade_ready(
            option_sweep_context=option_sweep_context,
            direction=scored_direction,
            price=price,
            vwap=vwap,
            candle_close=candle_close,
            trigger_level=(
                prev_high if scored_direction == "CE" and prev_high is not None
                else prev_low if scored_direction == "PE" and prev_low is not None
                else orb_high if scored_direction == "CE"
                else orb_low
            ),
            atr=atr,
            buffer=buffer,
            candle_liquidity_ok=candle_liquidity_ok,
            score=score,
            entry_score=self.last_entry_score,
            pressure_conflict_level=pressure_conflict_level,
        )
        if strong_sweep_trade_ready and regime in {"CHOPPY", "RANGING", "EARLY_SESSION"}:
            breakout_regime_ok = True
            continuation_regime_ok = True
            if "option_sweep_regime_override" not in components:
                components.append("option_sweep_regime_override")
        if nifty_trend_day_context:
            if scored_direction == "CE" and oi_bias in {"BULLISH", "NEUTRAL"} and oi_trend in {"BULLISH", "NEUTRAL", None}:
                bullish_build_up_ok = True
                if pressure_conflict_level == "MODERATE":
                    cautions = self._append_cautions(cautions, "trend_day_price_override")
            elif scored_direction == "PE" and oi_bias in {"BEARISH", "NEUTRAL"} and oi_trend in {"BEARISH", "NEUTRAL", None}:
                bearish_build_up_ok = True
                if pressure_conflict_level == "MODERATE":
                    cautions = self._append_cautions(cautions, "trend_day_price_override")
        provisional_confidence = self._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
        time_thresholds = self._get_time_regime_thresholds(time_regime, fallback_mode, market_regime=regime)
        neutral_pressure_soft_watch = (
            pressure_metrics
            and pressure_metrics["pressure_bias"] == "NEUTRAL"
            and scored_direction in {"CE", "PE"}
            and self._direction_vwap_aligned(scored_direction, price, vwap)
            and volume_signal in ["NORMAL", "STRONG"]
            and score >= max(time_thresholds["breakout_min_score"] - 2, 52)
        )
        neutral_pressure_reversal_ok = self._neutral_pressure_reversal_ready(
            scored_direction=scored_direction,
            score=score,
            entry_score=score,
            pressure_metrics=pressure_metrics,
            prev_high=prev_high,
            prev_low=prev_low,
            prev_close=prev_close,
            candle_close=candle_close,
            candle_high=candle_high,
            candle_low=candle_low,
            vwap=vwap,
            support=support,
            resistance=resistance,
            breakout_body_ok=breakout_body_ok,
            breakout_structure_ok=breakout_structure_ok,
            candle_liquidity_ok=candle_liquidity_ok,
            cautions=cautions,
        )
        if pressure_metrics and pressure_metrics["pressure_bias"] == "NEUTRAL":
            if neutral_pressure_reversal_ok:
                cautions.append("pressure_neutral")
            elif neutral_pressure_soft_watch:
                cautions.append("pressure_neutral")
            else:
                blockers.append("pressure_neutral")
                self._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime="CHOPPY")
                self.last_is_expiry_day = self.expiry_rules.is_expiry_day(expiry)
                return None, f"Pressure not aligned | score={score}"
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
            price_structure_score=self.last_price_structure_score,
            option_flow_score=self.last_option_flow_score,
            oi_structure_score=self.last_oi_structure_score,
            contract_quality_score=self.last_contract_quality_score,
        )
        if self.instrument == "SENSEX" and Config.FOCUSED_MANUAL_MODE and self.last_entry_score > 0:
            self.last_entry_score = min(100, self.last_entry_score + 6)
        if wall_break_supports_direction and self.last_entry_score > 0:
            self.last_entry_score = min(100, self.last_entry_score + 4)
        if divergence_against_direction and self.last_entry_score > 0:
            self.last_entry_score = max(0, self.last_entry_score - 6)
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
        adaptive_expiry_continuation_mode = expiry_eval.get("adaptive_continuation_mode", False)
        soften_build_up_requirement = expiry_eval.get("soften_build_up_requirement", False)
        soften_pressure_conflict = expiry_eval.get("soften_pressure_conflict", False)
        expiry_session_mode = expiry_eval.get("session_mode", "NORMAL")

        if not expiry_eval["allow_trade"]:
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=self._confidence_from_score(score, volume_signal, pressure_metrics, cautions),
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="NONE",
            )
            return None, f"Expiry filter blocked trade | score={score}"

        sensex_late_day_block = self._sensex_late_day_guard_with_context(
            current_now=current_now,
            score=score,
            entry_score=self.last_entry_score,
            confidence=provisional_confidence,
            volume_signal=volume_signal,
            pressure_conflict_level=pressure_conflict_level,
            option_sweep_context=option_sweep_context,
            direction=scored_direction,
        )
        if sensex_late_day_block:
            blockers.append(sensex_late_day_block)
            self._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=provisional_confidence,
                regime=regime,
                signal_type="NONE",
            )
            return None, f"SENSEX late-day guard blocked trade | score={score}"

        if soften_build_up_requirement:
            if scored_direction == "CE" and oi_bias == "BULLISH" and score >= max(expiry_eval["score_floor"], 64):
                bullish_build_up_ok = True
            elif scored_direction == "PE" and oi_bias == "BEARISH" and score >= max(expiry_eval["score_floor"], 64):
                bearish_build_up_ok = True

        adaptive_expiry_continuation_ok = (
            adaptive_expiry_continuation_mode
            and scored_direction in {"CE", "PE"}
            and volume_signal in {"NORMAL", "STRONG"}
            and candle_liquidity_ok
            and (breakout_body_ok or breakout_structure_ok)
            and pressure_conflict_level in {"NONE", "MILD"}
            and self.last_entry_score >= 60
            and (
                (scored_direction == "CE" and bullish_ha_ok and price > vwap)
                or (scored_direction == "PE" and bearish_ha_ok and price < vwap)
            )
        )
        nifty_post_expiry_continuation_ok = self._nifty_post_expiry_continuation_ready(
            direction=scored_direction,
            score=score,
            entry_score=self.last_entry_score,
            expiry_session_mode=expiry_session_mode,
            time_regime=time_regime,
            price=price,
            vwap=vwap,
            orb_high=orb_high,
            orb_low=orb_low,
            candle_close=candle_close,
            candle_range=candle_range,
            atr=atr,
            candle_liquidity_ok=candle_liquidity_ok,
            breakout_body_ok=breakout_body_ok,
            breakout_structure_ok=breakout_structure_ok,
            ha_ok=bullish_ha_ok if scored_direction == "CE" else bearish_ha_ok,
            pressure_conflict_level=pressure_conflict_level,
            volume_signal=volume_signal,
            recent_breakout_context=recent_breakout_context,
        )
        continuation_override_ok = adaptive_expiry_continuation_ok or nifty_post_expiry_continuation_ok or nifty_trend_day_context

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
        section_ctx = locals().copy()

        section_result = self._evaluate_opening_drive(section_ctx)
        if section_result is not None:
            return section_result

        section_result = self._evaluate_confirmation_and_retest(section_ctx)
        if section_result is not None:
            return section_result

        section_result = self._evaluate_manual_confirmations(section_ctx)
        if section_result is not None:
            return section_result

        section_result = self._evaluate_orb_breakouts(section_ctx)
        if section_result is not None:
            return section_result
        else:
            section_result = self._evaluate_reversal_setups(section_ctx)
            if section_result is not None:
                return section_result

        section_result = self._evaluate_aggressive_continuations(section_ctx)
        if section_result is not None:
            return section_result

        section_result = self._evaluate_core_continuations(section_ctx)
        if section_result is not None:
            return section_result

        section_result = self._evaluate_hybrid_continuations(section_ctx)
        if section_result is not None:
            return section_result

        section_result = self._finalize_watch_state(section_ctx)
        if section_result is not None:
            return section_result

        blockers.append("no_valid_setup")
        if fallback_mode:
            blockers.append("oi_only_context")
        if score < Config.MIN_SCORE_THRESHOLD:
            blockers.append("score_below_threshold")
        if scored_direction is None:
            blockers.append("direction_unresolved")
        if volume_signal == "WEAK":
            if not (
                self.instrument == "SENSEX"
                and score >= 54
                and scored_direction in {"CE", "PE"}
                and self._direction_vwap_aligned(scored_direction, price, vwap)
                and candle_liquidity_ok
                and time_regime in {"OPENING", "MID_MORNING"}
            ):
                blockers.append("volume_weak")
            else:
                cautions = self._append_cautions(cautions, "volume_weak")
        if not candle_liquidity_ok:
            blockers.append("low_tick_density")
        if divergence_against_direction and (
            volume_signal != "STRONG"
            or self.last_entry_score < 66
            or pressure_conflict_level not in {"NONE", "MILD"}
        ):
            blockers.append("oi_divergence_conflict")
        if scored_direction == "CE":
            if price <= vwap:
                blockers.append("vwap_not_supportive")
            if oi_bias == "BEARISH" or oi_trend == "BEARISH":
                blockers.append("oi_conflict")
            if not bullish_build_up_ok and not fallback_mode:
                if soften_build_up_requirement and expiry_session_mode == "POST_EXPIRY_REBUILD" and score >= expiry_eval["score_floor"]:
                    cautions = self._append_cautions(cautions, "build_up_missing")
                else:
                    blockers.append("build_up_missing")
            elif build_up not in bullish_buildups:
                blockers.append("build_up_inferred")
            if "opposite_pressure" in cautions or (pressure_metrics and pressure_metrics["pressure_bias"] == "BEARISH"):
                if not (
                    self.instrument == "SENSEX"
                    and pressure_conflict_level == "MILD"
                    and score >= 56
                    and time_regime in {"OPENING", "MID_MORNING"}
                ) and not (
                    wall_break_supports_direction
                    and self.last_entry_score >= 68
                    and pressure_conflict_level == "MILD"
                ) and not (
                    soften_pressure_conflict
                    and pressure_conflict_level == "MILD"
                    and score >= expiry_eval["score_floor"]
                    and self.last_entry_score >= 60
                ):
                    blockers.append("pressure_conflict")
                else:
                    cautions = self._append_cautions(cautions, "pressure_conflict")
            if orb_high is not None and price <= orb_high:
                blockers.append("orb_breakout_missing")
            if orb_high is not None and price > orb_high + (buffer * tuning["extension_buffer_mult"]):
                blockers.append("orb_extension_too_far")
        elif scored_direction == "PE":
            if price >= vwap:
                blockers.append("vwap_not_supportive")
            if oi_bias == "BULLISH" or oi_trend == "BULLISH":
                blockers.append("oi_conflict")
            if not bearish_build_up_ok and not fallback_mode:
                if soften_build_up_requirement and expiry_session_mode == "POST_EXPIRY_REBUILD" and score >= expiry_eval["score_floor"]:
                    cautions = self._append_cautions(cautions, "build_up_missing")
                else:
                    blockers.append("build_up_missing")
            elif build_up not in bearish_buildups:
                blockers.append("build_up_inferred")
            if "opposite_pressure" in cautions or (pressure_metrics and pressure_metrics["pressure_bias"] == "BULLISH"):
                if not (
                    self.instrument == "SENSEX"
                    and pressure_conflict_level == "MILD"
                    and score >= 56
                    and time_regime in {"OPENING", "MID_MORNING"}
                ) and not (
                    wall_break_supports_direction
                    and self.last_entry_score >= 68
                    and pressure_conflict_level == "MILD"
                ) and not (
                    soften_pressure_conflict
                    and pressure_conflict_level == "MILD"
                    and score >= expiry_eval["score_floor"]
                    and self.last_entry_score >= 60
                ):
                    blockers.append("pressure_conflict")
                else:
                    cautions = self._append_cautions(cautions, "pressure_conflict")
            if orb_low is not None and price >= orb_low:
                blockers.append("orb_breakout_missing")
            if orb_low is not None and price < orb_low - (buffer * tuning["extension_buffer_mult"]):
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
