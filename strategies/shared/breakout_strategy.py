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
from strategies.shared.breakout import (
    apply_day_state_adjustment,
    apply_option_buyer_filters,
    build_trade_signal_context,
    check_adx_filter,
    check_multi_timeframe_filter,
    check_oi_filter,
    check_session_filter,
    check_volume_filter,
    derive_active_day_state,
    derive_opening_bias,
    evaluate_aggressive_continuations,
    evaluate_confirmation_and_retest,
    evaluate_core_continuations,
    evaluate_hybrid_continuations,
    evaluate_manual_confirmations,
    evaluate_opening_drive,
    evaluate_orb_breakouts,
    evaluate_reversal_setups,
    finalize_no_setup,
    finalize_watch_state,
    mark_signal_emitted,
    reset_confirmation_setup,
    reset_retest_setup,
    score_signal_components,
    set_confirmation_setup,
    set_retest_setup,
    should_suppress_duplicate,
    update_confirmation_setup,
    update_retest_setup,
)
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


class BreakoutSignalStrategy:
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
        self.last_trend_leg_stage = "NEUTRAL"
        self.last_session_map_phase = "UNKNOWN"
        self.last_futures_acceptance = None
        self.last_futures_acceptance_score = 0.0
        self.last_initiative_strength_score = 0.0
        self.last_price_action_watch_ready = False
        self.last_signal_family = "UNKNOWN"
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
        return check_adx_filter(self, candles_5m, signal)

    def _check_volume_filter(self, current_volume):
        return check_volume_filter(self, current_volume)

    def _check_session_filter(self, timestamp=None):
        return check_session_filter(self, timestamp)

    def _check_oi_filter(self, oi_data, price, signal):
        return check_oi_filter(self, oi_data, price, signal)

    def _check_multi_timeframe_filter(self, trend_15m, signal):
        return check_multi_timeframe_filter(self, trend_15m, signal)

    def _apply_option_buyer_filters(self, signal, candles_5m, current_volume,
                                      oi_data, price, trend_15m=None, timestamp=None):
        return apply_option_buyer_filters(self, signal, candles_5m, current_volume, oi_data, price, trend_15m=trend_15m, timestamp=timestamp)

    def _reset_retest_setup(self):
        return reset_retest_setup(self)

    def _reset_confirmation_setup(self):
        return reset_confirmation_setup(self)

    def _set_retest_setup(self, direction, level, current_bar_time, score):
        return set_retest_setup(self, direction, level, current_bar_time, score)

    def _set_confirmation_setup(self, direction, level, current_bar_time, score):
        return set_confirmation_setup(self, direction, level, current_bar_time, score)

    def _should_suppress_duplicate(self, direction, signal_type, current_bar_time, level=None):
        return should_suppress_duplicate(self, direction, signal_type, current_bar_time, level=level)

    def _mark_signal_emitted(self, direction, signal_type, current_bar_time, level=None, buffer=0):
        return mark_signal_emitted(self, direction, signal_type, current_bar_time, level=level, buffer=buffer)

    def _update_retest_setup(self, current_bar_time):
        return update_retest_setup(self, current_bar_time)

    def _update_confirmation_setup(self, current_bar_time):
        return update_confirmation_setup(self, current_bar_time)

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
        return evaluate_opening_drive(self, ctx)

    def _evaluate_hybrid_continuations(self, ctx):
        return evaluate_hybrid_continuations(self, ctx)

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

    def _build_trade_signal_context(self, **kwargs):
        return build_trade_signal_context(self, **kwargs)

    def _finalize_no_setup(self, ctx):
        return finalize_no_setup(self, ctx)

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
        phase = directional.get("participation_phase")
        same_side_breadth = int(directional.get("same_side_breadth") or 0)
        opposite_side_breadth = int(directional.get("opposite_side_breadth") or 0)
        oi_delta_same = float(directional.get("same_side_oi_delta") or 0.0)
        oi_delta_opp = float(directional.get("opposite_side_oi_delta") or 0.0)
        price_led_participation = (
            phase in {"MID_MORNING", "MIDDAY", "LATE"}
            and same_side_breadth >= max(opposite_side_breadth, 1)
            and oi_delta_same >= oi_delta_opp
        )

        if quality == "STRONG":
            components.append("participation_strong")
        elif quality == "MODERATE":
            components.append("participation_moderate")
        elif quality == "WEAK":
            components.append("participation_weak")

        for flag in flags[:3]:
            components.append(f"participation_{flag}")

        updated_cautions = list(cautions or [])
        if quality == "WEAK" and not price_led_participation and "participation_weak" not in updated_cautions:
            updated_cautions.append("participation_weak")
        if "atm_spread_wide" in flags and "participation_spread_wide" not in updated_cautions:
            updated_cautions.append("participation_spread_wide")
        if (
            "same_side_volume_delta_missing" in flags
            and "same_side_breadth_missing" in flags
            and not price_led_participation
            and "participation_delta_missing" not in updated_cautions
        ):
            updated_cautions.append("participation_delta_missing")
        if (
            "same_side_vs_rolling_avg_missing" in flags
            and not price_led_participation
            and "participation_baseline_weak" not in updated_cautions
        ):
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

    def generate_trade_signal(
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
        early_result, section_ctx = self._build_trade_signal_context(
            price=price,
            orb_high=orb_high,
            orb_low=orb_low,
            vwap=vwap,
            volume_signal=volume_signal,
            oi_bias=oi_bias,
            oi_trend=oi_trend,
            build_up=build_up,
            support=support,
            resistance=resistance,
            can_trade=can_trade,
            buffer=buffer,
            pressure_metrics=pressure_metrics,
            atr=atr,
            expiry=expiry,
            candle_high=candle_high,
            candle_low=candle_low,
            candle_close=candle_close,
            candle_open=candle_open,
            candle_tick_count=candle_tick_count,
            candle_time=candle_time,
            candle_volume=candle_volume,
            atm_ce_volume=atm_ce_volume,
            atm_pe_volume=atm_pe_volume,
            recent_candles_5m=recent_candles_5m,
            trend_15m=trend_15m,
            participation_metrics=participation_metrics,
            oi_ladder_data=oi_ladder_data,
            option_sweep_context=option_sweep_context,
        )
        if early_result is not None:
            return early_result

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
        return self._finalize_no_setup(section_ctx)

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
        """Backward-compatible alias. Prefer `generate_trade_signal()`."""
        return self.generate_trade_signal(
            price=price,
            orb_high=orb_high,
            orb_low=orb_low,
            vwap=vwap,
            volume_signal=volume_signal,
            oi_bias=oi_bias,
            oi_trend=oi_trend,
            build_up=build_up,
            support=support,
            resistance=resistance,
            can_trade=can_trade,
            buffer=buffer,
            pressure_metrics=pressure_metrics,
            atr=atr,
            expiry=expiry,
            candle_high=candle_high,
            candle_low=candle_low,
            candle_close=candle_close,
            candle_open=candle_open,
            candle_tick_count=candle_tick_count,
            candle_time=candle_time,
            candle_volume=candle_volume,
            atm_ce_volume=atm_ce_volume,
            atm_pe_volume=atm_pe_volume,
            recent_candles_5m=recent_candles_5m,
            trend_15m=trend_15m,
            participation_metrics=participation_metrics,
            oi_ladder_data=oi_ladder_data,
            option_sweep_context=option_sweep_context,
        )


BreakoutStrategy = BreakoutSignalStrategy
