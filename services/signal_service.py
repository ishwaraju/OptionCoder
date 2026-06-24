"""
Signal Service - Can Start/Stop Anytime
Responsible for:
- Reading data from database
- Strategy analysis
- Signal generation
- Notifications
- Manual trading signals
"""

import time as time_module
import sys
import os
import argparse
import threading
import traceback
from collections import deque
from datetime import timedelta, time, datetime

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils.time_utils import TimeUtils
from shared.utils.log_utils import log_with_timestamp
from config import Config, get_config_for_instrument, get_risk_profile_matrix
from shared.db.reader import DBReader
from shared.db.writer import DBWriter
from shared.indicators.vwap import VWAPCalculator
from shared.indicators.orb import ORB
from shared.indicators.atr import ATRCalculator
from shared.indicators.volume_analyzer import VolumeAnalyzer
from shared.market.oi_analyzer import OIAnalyzer
from shared.market.option_chain import OptionChain
from shared.market.oi_ladder import OILadder
from shared.market.pressure_analyzer import PressureAnalyzer
from shared.market.spread_filter import SpreadFilter
from shared.market.oi_quote_confirmation import OIQuoteConfirmation
from shared.market.option_spike_detector import OptionSpikeDetector
from shared.indicators.multi_timeframe_trend import calculate_trend_from_candles
from strategies.shared.breakout_strategy import BreakoutSignalStrategy
from strategies.shared.one_minute_momentum import OneMinuteMomentumQuality
from strategies.shared.option_buyer_score import calculate_option_buyer_entry_score, option_buyer_action
from strategies.shared.strike_selector import StrikeSelector
from strategies.shared.actionable_rules import InstrumentActionableRules
from shared.utils.logger import TradeLogger
from shared.utils.notifier import Notifier
from shared.utils.instrument_profile import get_instrument_profile
from shared.utils.runtime_gap_detector import RuntimeGapDetector
from shared.utils.service_watchdog import ServiceWatchdog
from shared.utils.option_data_cache import OptionDataCache
from shared.utils.option_greeks import enrich_contract_greeks, format_greek_summary, project_option_price
from services.signal_pending_watch import (
    PendingEntryWatchEvaluator,
    PendingEntryWatchStateManager,
    PendingEntryWatchTriggerEngine,
    PendingEntryWatchUtils,
)
from services.signal_service_support import (
    OptionSignalGuard,
    PendingEntryWatchPolicy,
    RuntimeGapManager,
    TradeMonitorSupport,
)
from services.trade_monitor import (
    TradeMonitorDispatcher,
    TradeMonitorEvaluator,
    TradeMonitorStateManager,
    TradeMonitorUtils,
)

# ML Signal Enhancement (FREE - scikit-learn)
try:
    from shared.ml.feature_extractor import MLFeatureExtractor
    from shared.ml.signal_filter import MLSignalFilter
    ML_ENABLED = True
except ImportError as e:
    print(f"[Signal Service] ML modules not available: {e}")
    ML_ENABLED = False


class SignalService:
    RISK_PROFILE_MATRIX = get_risk_profile_matrix()

    @staticmethod
    def _coerce_comparable_datetimes(left_dt, right_dt):
        """Align tz-awareness before subtracting or comparing DB timestamps."""
        if left_dt is None or right_dt is None:
            return left_dt, right_dt

        left_has_tz = getattr(left_dt, "tzinfo", None) is not None
        right_has_tz = getattr(right_dt, "tzinfo", None) is not None
        if left_has_tz == right_has_tz:
            return left_dt, right_dt
        if left_has_tz and not right_has_tz:
            return left_dt, right_dt.replace(tzinfo=left_dt.tzinfo)
        if right_has_tz and not left_has_tz:
            return left_dt.replace(tzinfo=right_dt.tzinfo), right_dt
        return left_dt, right_dt

    def _run_async_notification(self, callback, payload):
        """Fire notification without blocking the signal path."""
        def _runner():
            try:
                callback(payload)
            except Exception as e:
                self._log(f"Async notification error: {e}")

        thread = threading.Thread(target=_runner, daemon=True)
        thread.start()
        return thread

    def _send_action_notification(self, callback, payload, label="action signal"):
        """Send trade-critical notifications synchronously so DB delivery state is real."""
        try:
            delivered = bool(callback(payload))
            if Config.ENABLE_ALERTS and not delivered:
                self._log(f"Telegram delivery failed for {label}")
            return delivered
        except Exception as e:
            self._log(f"Telegram delivery exception for {label}: {e}")
            return False

    def __init__(self, instrument=None):
        self.time_utils = TimeUtils()
        self.profile = get_instrument_profile(instrument)
        self.instrument = self.profile["instrument"]
        self.watchdog = ServiceWatchdog("signal_service", self.instrument)
        
        # Get instrument-specific config
        self.config = get_config_for_instrument(self.instrument)
        self.current_expectancy_profile = None
        
        # Database access
        self.db_reader = DBReader()
        self.db_writer = DBWriter()
        
        # Indicators (read from DB)
        self.vwap = VWAPCalculator()
        self.orb = ORB()
        self.atr = ATRCalculator()
        self.volume = VolumeAnalyzer()
        self.oi = OIAnalyzer()
        
        # Market data
        self.option_chain = OptionChain(self.instrument)
        self.option_data_cache = OptionDataCache()
        self.oi_ladder = OILadder()
        self.pressure = PressureAnalyzer()
        self.spread_filter = SpreadFilter()
        self.oi_quote_confirmation = OIQuoteConfirmation()
        self.option_spike_detector = OptionSpikeDetector(self.profile.get("strike_step"))
        
        # Strategy
        self.strategy = BreakoutSignalStrategy(instrument=self.instrument)
        self.one_minute_momentum = OneMinuteMomentumQuality(min_score=30)
        self.strike_selector = StrikeSelector(self.instrument)
        
        # Logging and notifications
        self.audit_logger = TradeLogger()
        self.notifier = Notifier()
        
        # ML Components (FREE - scikit-learn)
        if ML_ENABLED:
            self.ml_feature_extractor = MLFeatureExtractor()
            self.ml_filter = MLSignalFilter(threshold=0.55)
            self._log("[ML] Signal filter initialized (FREE - scikit-learn)")
        else:
            self.ml_feature_extractor = None
            self.ml_filter = None
        
        # State tracking
        self.running = False
        self.last_option_fetch = 0
        self.last_signal_time = 0
        self.signal_cooldown_remaining = 0
        self.last_emitted_signal = None
        self.last_watch_alert_key = None
        self.last_watch_alert_time = 0
        self.watch_alert_state = {}
        self.session_decisions = []
        self.pending_entry_watch = None
        self.pending_spike_watch = None
        self.option_sweep_context = None
        self._current_risk_option_contract = None
        self._current_risk_reference_contract = None
        self._current_market_iv_regime = "NORMAL"
        self._current_market_iv_context = None
        self.participation_history = {
            "CE": deque(maxlen=12),
            "PE": deque(maxlen=12),
        }
        self.last_participation_history_key = None
        self.day_high_5m = None
        self.day_low_5m = None
        self.opening_range_30_high = None
        self.opening_range_30_low = None
        
        # Status tracking
        self.last_heartbeat = 0
        self.heartbeat_interval = 30  # Every 30 seconds
        self.last_market_status_check = 0
        self.market_status_interval = 60  # Every minute
        self.last_market_status = "UNKNOWN"
        self.candles_processed = 0
        self.signals_generated = 0
        self.last_processed_5m_ts = None
        self.last_monitor_check_minute = None
        self.active_trade_monitor = None
        self.data_pause_active = False
        self.last_data_pause_reason = None
        self.last_data_health = None
        self.last_data_health_alert_time = 0
        self.last_data_health_alert_reason = None
        self.pending_pullback_watch_plan = None
        self._suppress_live_actions = False
        self._stale_backlog_replay = False
        
        # Runtime gap detection
        self.runtime_gap_detector = RuntimeGapDetector(
            threshold_seconds=90,
            sleep_confirmation_seconds=20,
        )
        
        # Previous values for OI build-up
        self.prev_price = None
        self.prev_total_oi = None
        self.option_data = None
        self.option_data_ts = None
        self.option_data_source = None
        self.last_oi_fallback_notice = 0
        
        # Initialize with historical data
        self._restore_indicator_state()

    @staticmethod
    def _effective_candle_close_time(candle):
        """Return the effective close time for a 5m candle."""
        close_time = candle.get("close_time")
        if close_time:
            return close_time
        return candle["time"] + timedelta(minutes=5)

    @staticmethod
    def _build_signal_key(ts, instrument, signal, strike):
        strike_part = strike if strike is not None else "NA"
        return f"{instrument}:{signal}:{strike_part}:{ts.isoformat()}"

    def _log(self, message):
        """Log with HH:mm:ss IST timestamp prefix"""
        ts = self.time_utils.now_ist().strftime('%H:%M:%S')
        print(f"[{ts}] [Signal Service] {message}")

    def _handle_runtime_gap_recovery(self, reason_label):
        return RuntimeGapManager.handle_runtime_gap_recovery(self, reason_label)

    def _handle_runtime_gap(self, gap_event):
        return RuntimeGapManager.handle_runtime_gap(self, gap_event)

    def _start_trade_monitor(self, signal, candle_5m, price, balanced_pro, selected_strike, entry_time=None):
        return TradeMonitorStateManager.start_trade_monitor(
            self,
            signal,
            candle_5m,
            price,
            balanced_pro,
            selected_strike,
            entry_time=entry_time,
        )

    @staticmethod
    def _option_pnl_percent(entry_price, option_price):
        return TradeMonitorUtils.option_pnl_percent(entry_price, option_price)

    @staticmethod
    def _drawdown_from_peak_percent(peak_price, option_price):
        return TradeMonitorUtils.drawdown_from_peak_percent(peak_price, option_price)

    @staticmethod
    def _spread_widening_percent(entry_spread, current_spread):
        return TradeMonitorUtils.spread_widening_percent(entry_spread, current_spread)

    def _option_three_bar_momentum(self, recent_1m_candles, strike, signal):
        return TradeMonitorSupport.option_three_bar_momentum(self, recent_1m_candles, strike, signal)

    @staticmethod
    def _estimate_live_atr(recent_5m_candles, fallback=None):
        return TradeMonitorUtils.estimate_live_atr(recent_5m_candles, fallback)

    def _get_atm_reference_option_contract(self, signal, before_ts=None):
        atm_strike = (self.option_data or {}).get("atm")
        if atm_strike is None:
            return None
        try:
            return self._get_option_contract_snapshot(atm_strike, signal, before_ts=before_ts)
        except Exception:
            return None

    @staticmethod
    def _dynamic_trail_percent(base_trail_pct, setup_bucket, live_atr, underlying_price, time_regime=None):
        return TradeMonitorUtils.dynamic_trail_percent(
            base_trail_pct,
            setup_bucket,
            live_atr,
            underlying_price,
            time_regime,
        )

    @staticmethod
    def _update_psar_style_level(signal, existing_level, latest_1m, previous_1m, live_atr=None):
        return TradeMonitorUtils.update_psar_style_level(
            signal,
            existing_level,
            latest_1m,
            previous_1m,
            live_atr,
        )

    @staticmethod
    def _entry_decision_label(signal):
        signal = (signal or "").upper()
        if signal in {"CE", "PE"}:
            return f"CONFIRMED_{signal}_ENTRY"
        return "CONFIRMED_ENTRY"

    @staticmethod
    def _flip_watch_label(direction):
        direction = (direction or "").upper()
        if direction in {"CE", "PE"}:
            return f"WATCH_{direction}_FLIP"
        return "WATCH_FLIP"

    @staticmethod
    def _flip_confirmed_label(direction):
        direction = (direction or "").upper()
        if direction in {"CE", "PE"}:
            return f"CONFIRMED_{direction}_FLIP"
        return "CONFIRMED_FLIP"

    def _infer_monitor_flip_signal(self, current_signal, latest_1m, previous_1m, micro_high, micro_low, vwap_break, structure_break):
        return TradeMonitorSupport.infer_monitor_flip_signal(
            self,
            current_signal,
            latest_1m,
            previous_1m,
            micro_high,
            micro_low,
            vwap_break,
            structure_break,
        )

    def _resolve_trade_risk_profile(self, setup_type=None, quality=None, confidence=None, cautions=None):
        return TradeMonitorSupport.resolve_trade_risk_profile(
            self,
            setup_type=setup_type,
            quality=quality,
            confidence=confidence,
            cautions=cautions,
        )

    @staticmethod
    def _resolve_session_bucket(cautions):
        cautions = {str(item).lower() for item in (cautions or []) if item}
        if "expiry_day_mode" in cautions:
            return "EXPIRY"
        if "pre_expiry_positioning_mode" in cautions:
            return "PRE_EXPIRY"
        if "post_expiry_rebuild_mode" in cautions:
            return "POST_EXPIRY"
        return "NON_EXPIRY"

    @staticmethod
    def _resolve_iv_bucket(selected_option_contract=None, reference_option_contract=None):
        try:
            selected_iv = float((selected_option_contract or {}).get("iv") or 0)
            reference_iv = float((reference_option_contract or {}).get("iv") or 0)
        except Exception:
            return "NORMAL"

        if selected_iv <= 0 or reference_iv <= 0:
            return "NORMAL"

        iv_markup_pct = ((selected_iv - reference_iv) / reference_iv) * 100.0
        if iv_markup_pct <= -8.0:
            return "CHEAP"
        if iv_markup_pct >= 18.0:
            return "EXTREME"
        if iv_markup_pct >= 8.0:
            return "RICH"
        return "NORMAL"

    @staticmethod
    def _resolve_time_stop_minutes(session_bucket, setup_bucket, quality, confidence, iv_bucket):
        if session_bucket == "EXPIRY":
            time_warn, time_exit = 2, 4
        elif session_bucket in {"PRE_EXPIRY", "POST_EXPIRY"}:
            time_warn, time_exit = 3, 5
        else:
            time_warn, time_exit = 3, 5

        if setup_bucket == "CONTINUATION":
            time_exit = min(time_exit, 4)
        elif setup_bucket == "REVERSAL" and session_bucket != "EXPIRY":
            time_exit = max(time_exit, 5)

        if iv_bucket == "EXTREME":
            time_warn = max(2, time_warn - 1)
            time_exit = max(3, time_exit - 1)
        elif iv_bucket == "CHEAP" and session_bucket != "EXPIRY":
            time_exit += 1

        if quality == "C" or confidence == "LOW":
            time_exit = min(time_exit, 4)

        return time_warn, time_exit

    def _resolve_market_iv_regime(self, reference_option_contract=None, option_type=None, before_ts=None):
        option_type = option_type or ((reference_option_contract or {}).get("option_type")) or "CE"
        try:
            current_atm_iv = float((reference_option_contract or {}).get("iv") or 0)
        except Exception:
            current_atm_iv = 0.0

        if current_atm_iv <= 0:
            self._current_market_iv_context = {
                "current_atm_iv": None,
                "baseline_atm_iv": None,
                "iv_deviation_pct": None,
            }
            self._current_market_iv_regime = "NORMAL"
            return "NORMAL"

        history = []
        try:
            history = self.db_reader.fetch_recent_atm_iv_series(
                instrument=self.instrument,
                option_type=option_type,
                before_ts=before_ts,
                limit=20,
            )
        except Exception:
            history = []

        iv_values = [float(item["iv"]) for item in history if item.get("iv")]
        if iv_values and abs(iv_values[0] - current_atm_iv) < 1e-9:
            iv_values = iv_values[1:]

        if not iv_values:
            baseline_atm_iv = current_atm_iv
            deviation_pct = 0.0
            regime = "NORMAL"
        else:
            sample = iv_values[: min(len(iv_values), 12)]
            baseline_atm_iv = sum(sample) / len(sample)
            deviation_pct = ((current_atm_iv - baseline_atm_iv) / baseline_atm_iv) * 100.0 if baseline_atm_iv > 0 else 0.0
            if deviation_pct >= 20.0:
                regime = "EVENT"
            elif deviation_pct >= 10.0:
                regime = "HIGH"
            elif deviation_pct <= -10.0:
                regime = "LOW"
            else:
                regime = "NORMAL"

        self._current_market_iv_context = {
            "current_atm_iv": round(current_atm_iv, 2),
            "baseline_atm_iv": round(baseline_atm_iv, 2) if baseline_atm_iv else None,
            "iv_deviation_pct": round(deviation_pct, 2),
        }
        self._current_market_iv_regime = regime
        return regime

    @staticmethod
    def _apply_market_iv_regime_adjustments(hard_stop, target_pct, trail_pct, time_warn, time_exit, market_iv_regime, session_bucket):
        hard_stop = float(hard_stop)
        target_pct = float(target_pct)
        trail_pct = float(trail_pct)

        if market_iv_regime == "LOW":
            hard_stop += 1.0
            target_pct += 3.0 if session_bucket != "EXPIRY" else 2.0
        elif market_iv_regime == "HIGH":
            hard_stop = max(5.0, hard_stop - 1.0)
            target_pct = max(hard_stop + 2.0, target_pct - 3.0)
            trail_pct = max(7.0, trail_pct - 0.5)
        elif market_iv_regime == "EVENT":
            hard_stop = max(4.0, hard_stop - 2.0)
            target_pct = max(hard_stop + 2.0, target_pct - 6.0)
            trail_pct = max(7.0, trail_pct - 1.0)
            time_warn = max(1, time_warn - 1)
            time_exit = max(3, time_exit - 1)

        return round(hard_stop, 2), round(target_pct, 2), round(trail_pct, 2), int(time_warn), int(time_exit)

    @staticmethod
    def _runner_profile_for_setup(setup_bucket, session_bucket):
        setup_bucket = (setup_bucket or "BREAKOUT").upper()
        session_bucket = (session_bucket or "NON_EXPIRY").upper()

        profile = {
            "partial_trigger_pct": 20.0,
            "runner_trigger_pct": 28.0,
            "runner_trail_bonus": 1.5,
            "allow_endgame_runner": False,
            "time_extension_minutes": 0,
        }

        if setup_bucket == "BREAKOUT":
            profile.update(
                {
                    "partial_trigger_pct": 24.0,
                    "runner_trigger_pct": 32.0,
                    "runner_trail_bonus": 2.0,
                    "allow_endgame_runner": True,
                    "time_extension_minutes": 3,
                }
            )
        elif setup_bucket == "CONTINUATION":
            profile.update(
                {
                    "partial_trigger_pct": 22.0,
                    "runner_trigger_pct": 30.0,
                    "runner_trail_bonus": 2.5,
                    "allow_endgame_runner": True,
                    "time_extension_minutes": 2,
                }
            )
        elif setup_bucket == "REVERSAL":
            profile.update(
                {
                    "partial_trigger_pct": 18.0,
                    "runner_trigger_pct": 26.0,
                    "runner_trail_bonus": 1.0,
                    "allow_endgame_runner": False,
                    "time_extension_minutes": 0,
                }
            )

        if session_bucket == "EXPIRY":
            profile["partial_trigger_pct"] = max(14.0, profile["partial_trigger_pct"] - 4.0)
            profile["runner_trigger_pct"] = max(20.0, profile["runner_trigger_pct"] - 4.0)
            profile["runner_trail_bonus"] = max(0.5, profile["runner_trail_bonus"] - 0.5)

        return profile

    @staticmethod
    def _option_expansion_metrics(entry_option_price, option_price, entry_underlying_price, underlying_price, entry_delta=None):
        return TradeMonitorSupport.option_expansion_metrics(
            entry_option_price,
            option_price,
            entry_underlying_price,
            underlying_price,
            entry_delta,
        )

    @staticmethod
    def _classify_trade_run_profile(
        pnl_percent,
        expansion_metrics,
        minutes_active,
        momentum_strong,
        pressure_flip_exit,
        drawdown_from_peak_pct,
        setup_bucket,
        session_bucket,
    ):
        return TradeMonitorSupport.classify_trade_run_profile(
            pnl_percent,
            expansion_metrics,
            minutes_active,
            momentum_strong,
            pressure_flip_exit,
            drawdown_from_peak_pct,
            setup_bucket,
            session_bucket,
        )

    def _session_start_for(self, candle_time):
        return candle_time.replace(hour=9, minute=15, second=0, microsecond=0)

    def _assess_raw_feed_health(self, candle_time):
        return OptionSignalGuard.assess_raw_feed_health(self, candle_time)

    @staticmethod
    def _derive_option_volume_signal(option_data):
        return OptionSignalGuard.derive_option_volume_signal(option_data)

    def _evaluate_oi_wall_guard(self, signal, price, oi_ladder_data=None, pressure_metrics=None):
        return OptionSignalGuard.evaluate_oi_wall_guard(self, signal, price, oi_ladder_data, pressure_metrics)

    def _should_soften_option_sweep_filters(self, signal):
        return OptionSignalGuard.should_soften_option_sweep_filters(self, signal)

    def _evaluate_premium_quality_guard(self, signal, selected_option_contract, candle_time):
        return OptionSignalGuard.evaluate_premium_quality_guard(self, signal, selected_option_contract, candle_time)

    def _assess_high_expectancy_profile(
        self,
        signal,
        candle_time,
        balanced_pro=None,
        selected_option_contract=None,
        premium_guard=None,
        risk_profile=None,
        price=None,
    ):
        return OptionSignalGuard.assess_high_expectancy(
            self,
            signal,
            candle_time,
            balanced_pro=balanced_pro,
            selected_option_contract=selected_option_contract,
            premium_guard=premium_guard,
            risk_profile=risk_profile,
            price=price,
        )

    def _high_probability_action_gate(self, signal, expectancy_profile, premium_guard, feed_health, selected_option_contract=None):
        if not bool(getattr(Config, "HIGH_PROB_ACTION_ONLY", True)):
            return True, "high_probability_gate_disabled"
        signal = (signal or "").upper()
        if signal not in {"CE", "PE"}:
            return False, "invalid_signal"

        profile = expectancy_profile or {}
        premium_guard = premium_guard or {}
        feed_label = ((feed_health or {}).get("label") or "GOOD").upper()
        pro_check = profile.get("pro_check") or {}
        quality_tag = (profile.get("quality_tag") or "").upper()
        signal_family = (profile.get("signal_family") or "").upper()
        confidence = (getattr(self.strategy, "last_confidence", "") or "").upper()
        signal_grade = (getattr(self.strategy, "last_signal_grade", "") or "").upper()
        pressure_conflict = (getattr(self.strategy, "last_pressure_conflict_level", "NONE") or "NONE").upper()
        cautions = set(getattr(self.strategy, "last_cautions", []) or [])
        blockers = set(getattr(self.strategy, "last_blockers", []) or [])
        score = float(getattr(self.strategy, "last_score", 0) or 0)
        entry_score = float(getattr(self.strategy, "last_entry_score", score) or score)
        min_score = float(getattr(Config, "HIGH_PROB_MIN_CONTEXT_SCORE", 88.0) or 88.0)
        min_entry = float(getattr(Config, "HIGH_PROB_MIN_ENTRY_SCORE", 86.0) or 86.0)
        min_pro = float(getattr(Config, "HIGH_PROB_MIN_PRO_SCORE", 75.0) or 75.0)
        starter_profile = quality_tag == "PA_STRONG_ENTER_SMALL"
        spread_pct = premium_guard.get("spread_pct")
        if spread_pct is None and selected_option_contract:
            spread_pct = self._spread_percent(selected_option_contract)

        reasons = []
        if feed_label == "REJECT":
            reasons.append("feed_reject")
        if feed_label == "RISKY":
            reasons.append("feed_risky")
        if signal_grade not in {"A", "A+"} and not (starter_profile and signal_grade == "B" and score >= 96 and entry_score >= 92):
            reasons.append(f"grade_{signal_grade or 'missing'}")
        if confidence not in {"HIGH", "MEDIUM"}:
            reasons.append(f"confidence_{confidence or 'missing'}")
        if score < min_score:
            reasons.append(f"context_score<{min_score:.0f}")
        if entry_score < min_entry:
            reasons.append(f"entry_score<{min_entry:.0f}")
        if pressure_conflict not in {"NONE", "MILD", ""}:
            reasons.append(f"pressure_conflict_{pressure_conflict}")
        if blockers.intersection({"time_filter", "institutional_confluence_reject", "feed_quality_reject"}):
            reasons.append("hard_blocker_present")
        if cautions.intersection({"participation_weak", "participation_delta_missing"}):
            reasons.append("participation_not_clean")
        if quality_tag not in {"HQ", "TQ_CLEAN", "RQ", "PA_STRONG_ENTER_SMALL"}:
            reasons.append(f"quality_{quality_tag or 'missing'}")
        if signal_family == "LATE_EXTENSION":
            reasons.append("late_extension")
        if bool(getattr(Config, "HIGH_PROB_REQUIRE_PREMIUM_CONFIRMED", True)) and not profile.get("premium_confirmed") and not starter_profile:
            reasons.append("premium_not_confirmed")
        if (premium_guard.get("label") or "").upper() != "PREMIUM_OK":
            reasons.append(f"premium_{premium_guard.get('label') or 'missing'}")
        if spread_pct is not None and float(spread_pct) > float(getattr(Config, "PRO_TRADER_MAX_ACTION_SPREAD_PCT", 3.8) or 3.8):
            reasons.append("spread_too_wide")
        if (pro_check.get("label") or "").upper() != "PRO_PASS":
            reasons.append(f"pro_{pro_check.get('label') or 'missing'}")
        if float(pro_check.get("score") or 0.0) < min_pro:
            reasons.append(f"pro_score<{min_pro:.0f}")

        if reasons:
            return False, ";".join(reasons)
        return True, "high_probability_pass"

    def _compute_flip_score(self, direction, structure_break, vwap_break, latest_1m=None, previous_1m=None):
        return OptionSignalGuard.compute_flip_score(self, direction, structure_break, vwap_break, latest_1m, previous_1m)

    def _classify_no_trade_zone(self, balanced_pro, signal=None, selected_option_contract=None):
        return OptionSignalGuard.classify_no_trade_zone(self, balanced_pro, signal, selected_option_contract)

    @staticmethod
    def _build_journal_note(decision_label, action_text, extra=None):
        return TradeMonitorSupport.build_journal_note(decision_label, action_text, extra)

    def _build_trade_thesis(self, signal, balanced_pro=None, premium_guard=None):
        signal = (signal or "").upper()
        direction_text = "CE" if signal == "CE" else "PE" if signal == "PE" else "setup"
        futures_acceptance = getattr(self.strategy, "last_futures_acceptance", None) or {}
        session_phase = getattr(self.strategy, "last_session_map_phase", "UNKNOWN")
        pressure_summary = (balanced_pro or {}).get("pressure_summary") or {}
        pressure_bias = pressure_summary.get("bias")
        active_day_state = getattr(self.strategy, "last_active_day_state", "UNKNOWN")
        premium_guard = premium_guard or {}
        premium_confirmed = (premium_guard.get("label") or "").upper() == "PREMIUM_OK"
        volume_supporting = premium_guard.get("volume_supporting")
        spread_pct = premium_guard.get("spread_pct")

        bits = []
        if futures_acceptance.get("accepted"):
            bits.append(f"Price action accepted in {session_phase.lower().replace('_', ' ')}")
        elif getattr(self.strategy, "last_price_action_watch_ready", False):
            bits.append("Price action strong hai")
        if active_day_state and active_day_state != "UNKNOWN":
            bits.append(active_day_state.replace("_", " ").title())
        if pressure_bias in {"BULLISH", "BEARISH"}:
            bits.append(f"pressure {pressure_bias.lower()}")
        if premium_confirmed:
            premium_line = f"{direction_text} premium confirmed"
            if volume_supporting:
                premium_line += " with volume"
            if spread_pct is not None:
                premium_line += f" and spread {round(float(spread_pct), 2)}%"
            bits.append(premium_line)
        elif signal in {"CE", "PE"}:
            bits.append(f"{direction_text} premium confirmation abhi pending hai")

        return ". ".join(bits[:2]) if bits else None

    def _classify_action_trade_type(self, setup_type=None, high_expectancy_profile=None, premium_guard=None):
        setup_type = (setup_type or getattr(self.strategy, "last_signal_type", None) or "").upper()
        profile = high_expectancy_profile or {}
        quality_tag = (profile.get("quality_tag") or "").upper()
        signal_family = (profile.get("signal_family") or "").upper()
        premium_guard = premium_guard or {}
        momentum = premium_guard.get("premium_momentum_pct")
        likely_runner = bool(profile.get("likely_runner"))

        if quality_tag == "LQ" or signal_family == "LATE_EXTENSION":
            return "LATE CHASE - AVOID"
        if likely_runner or (momentum is not None and float(momentum) >= 2.5 and setup_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "CONTINUATION"}):
            return "RUNNER CANDIDATE"
        if setup_type in {"BREAKOUT_CONFIRM", "RETEST", "CONTINUATION", "AGGRESSIVE_CONTINUATION"}:
            return "CLEAN CONTINUATION"
        if setup_type in {"REVERSAL", "TRAP_REVERSAL"}:
            return "SCALP"
        return "SCALP"

    @staticmethod
    def _action_exit_if_line(signal, trigger_price=None, invalidate_price=None, premium_sl=None):
        signal = (signal or "").upper()
        if signal not in {"CE", "PE"}:
            return None
        level = invalidate_price if invalidate_price is not None else trigger_price
        side = "below" if signal == "CE" else "above"
        bits = []
        if level is not None:
            bits.append(f"Exit if next 1m closes {side} {round(float(level), 2)}")
        if premium_sl is not None:
            bits.append(f"or premium loses {round(float(premium_sl), 2)}")
        return " ".join(bits) if bits else None

    def _evaluate_1m_execution_gate(self, signal, trigger_price=None, signal_entry_time=None):
        if not bool(getattr(Config, "REQUIRE_1M_EXECUTION_FOR_ACTION", True)):
            return True, "1m execution gate disabled", None
        if signal not in {"CE", "PE"}:
            return True, "No option direction", None
        recent_1m = self.db_reader.fetch_recent_candles_1m(self.instrument, limit=3)
        if not recent_1m:
            return False, "1m execution data unavailable", None
        latest_1m = recent_1m[-1]
        latest_time = latest_1m.get("time")
        reference_time = signal_entry_time or latest_time
        if latest_time and reference_time:
            reference_time_cmp, latest_time_cmp = self._coerce_comparable_datetimes(reference_time, latest_time)
            age_seconds = abs((latest_time_cmp - reference_time_cmp).total_seconds())
            max_age = int(getattr(Config, "ONE_MIN_EXECUTION_MAX_AGE_SECONDS", 120) or 120)
            if age_seconds > max_age:
                return False, f"1m execution candle stale ({int(age_seconds)}s old)", latest_1m

        close_price = float(latest_1m.get("close") or 0.0)
        open_price = float(latest_1m.get("open") or close_price)
        high_price = float(latest_1m.get("high") or close_price)
        low_price = float(latest_1m.get("low") or close_price)
        candle_range = max(high_price - low_price, 0.01)
        body = abs(close_price - open_price)
        min_body = float(getattr(Config, "ENTRY_TRIGGER_MIN_BODY", 5) or 5)
        body_ok = body >= min(min_body, max(candle_range * 0.35, 1.0))
        if signal == "CE":
            trigger_ok = trigger_price is None or close_price >= float(trigger_price)
            direction_ok = close_price >= open_price
            side_text = "above"
        else:
            trigger_ok = trigger_price is None or close_price <= float(trigger_price)
            direction_ok = close_price <= open_price
            side_text = "below"
        if not trigger_ok:
            return False, f"1m close trigger ke {side_text} confirm nahi hua", latest_1m
        if not direction_ok or not body_ok:
            return False, "1m execution candle decisive nahi hai", latest_1m
        return True, "1m execution confirmed", latest_1m

    def _optimized_spread_short_strike(self, signal, long_strike, balanced_pro=None, risk_profile=None):
        return OptionSignalGuard.optimized_spread_short_strike(self, signal, long_strike, balanced_pro, risk_profile)

    def _build_option_structure_suggestion(self, signal, selected_strike, selected_option_contract, balanced_pro=None, risk_profile=None):
        return OptionSignalGuard.build_option_structure_suggestion(
            self,
            signal,
            selected_strike,
            selected_option_contract,
            balanced_pro,
            risk_profile,
        )

    @staticmethod
    def _spread_percent(option_row):
        return OptionSignalGuard.spread_percent(option_row)

    def _get_option_contract_snapshot(self, strike, option_type, before_ts=None):
        if strike is None or option_type not in {"CE", "PE"}:
            return None

        band_rows = (self.option_data or {}).get("band_snapshots") or []
        if before_ts is None and band_rows:
            for row in band_rows:
                if row.get("strike") == strike and row.get("option_type") == option_type:
                    return dict(row)

        return self.db_reader.fetch_option_contract_snapshot(
            instrument=self.instrument,
            strike=strike,
            option_type=option_type,
            before_ts=before_ts,
        )

    def _greek_enriched_option_contract(self, option_contract, option_type, underlying_price, before_ts=None):
        if not option_contract or option_type not in {"CE", "PE"}:
            return option_contract

        expiry_value = (
            option_contract.get("expiry")
            or (self.option_data or {}).get("expiry")
        )
        current_dt = before_ts or self.time_utils.now_ist()
        return enrich_contract_greeks(
            option_contract=option_contract,
            underlying_price=underlying_price,
            option_type=option_type,
            current_dt=current_dt,
            expiry_value=expiry_value,
        )

    def _project_premium_levels_for_spot_map(self, option_contract, option_type, spot_sl, spot_t1, before_ts=None):
        if not option_contract or option_type not in {"CE", "PE"}:
            return None, None

        strike = option_contract.get("strike")
        iv = option_contract.get("iv")
        expiry_value = option_contract.get("expiry") or (self.option_data or {}).get("expiry")
        current_dt = before_ts or self.time_utils.now_ist()

        projected_sl = None
        projected_t1 = None
        if spot_sl is not None:
            projected_sl = project_option_price(
                underlying_price=spot_sl,
                strike=strike,
                option_type=option_type,
                implied_volatility=iv,
                current_dt=current_dt,
                expiry_value=expiry_value,
            )
        if spot_t1 is not None:
            projected_t1 = project_option_price(
                underlying_price=spot_t1,
                strike=strike,
                option_type=option_type,
                implied_volatility=iv,
                current_dt=current_dt,
                expiry_value=expiry_value,
            )
        return projected_sl, projected_t1

    def _prime_oi_quote_confirmation(self, timestamp, price):
        if not self.option_data:
            return

        self.oi_quote_confirmation.add_oi_snapshot(
            timestamp=timestamp,
            ce_oi=self.option_data.get("ce_oi"),
            pe_oi=self.option_data.get("pe_oi"),
            ce_volume=self.option_data.get("ce_volume"),
            pe_volume=self.option_data.get("pe_volume"),
            price=price,
        )
        self.oi_quote_confirmation.add_quote_snapshot(
            timestamp=timestamp,
            option_data={
                "ce_spread": self.option_data.get("ce_spread"),
                "pe_spread": self.option_data.get("pe_spread"),
                "ce_top_bid_quantity": self.option_data.get("ce_top_bid_quantity"),
                "ce_top_ask_quantity": self.option_data.get("ce_top_ask_quantity"),
                "pe_top_bid_quantity": self.option_data.get("pe_top_bid_quantity"),
                "pe_top_ask_quantity": self.option_data.get("pe_top_ask_quantity"),
                "atm": self.option_data.get("atm"),
                "underlying_price": self.option_data.get("underlying_price"),
            },
        )

    def _confirm_signal_microstructure(self, signal, selected_strike, timestamp, price, strict=False):
        if signal not in {"CE", "PE"} or selected_strike is None or not self.option_data:
            return (not strict), "Microstructure data unavailable", None, None

        self._prime_oi_quote_confirmation(timestamp, price)

        should_filter, filter_reason, alternative_strike = self.spread_filter.should_filter_signal(
            self.option_data,
            signal,
            selected_strike,
        )
        if should_filter:
            return False, filter_reason, None, alternative_strike

        threshold = 0.68 if strict else 0.58
        confirmed, oi_reason, confidence, metrics = self.oi_quote_confirmation.confirm_signal_with_oi(
            signal,
            confidence_threshold=threshold,
        )
        if not confirmed:
            sweep_ctx = getattr(self, "option_sweep_context", None) or {}
            setup_type = (getattr(self.strategy, "last_signal_type", None) or "NONE").upper()
            futures_acceptance = getattr(self.strategy, "last_futures_acceptance", None) or {}
            initiative_strength_score = float(getattr(self.strategy, "last_initiative_strength_score", 0) or 0)
            sweep_micro_override = (
                sweep_ctx.get("direction") == signal
                and sweep_ctx.get("quality") == "STRONG"
                and sweep_ctx.get("micro_confirmed")
                and sweep_ctx.get("persistence_pairs", 0) >= 3
                and setup_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "REVERSAL", "TRAP_REVERSAL"}
                and float(getattr(self.strategy, "last_entry_score", 0) or 0) >= (95 if setup_type == "BREAKOUT" else 88)
                and getattr(self.strategy, "last_pressure_conflict_level", "NONE") in {"NONE", "MILD"}
                and oi_reason in {"Insufficient OI data", "Microstructure data unavailable"}
            )
            if sweep_micro_override:
                override_metrics = dict(metrics or {})
                override_metrics["override"] = "option_sweep_microstructure"
                override_metrics["base_reason"] = oi_reason
                return True, "Option sweep microstructure override", override_metrics, None
            price_action_micro_override = (
                oi_reason in {"Insufficient OI data", "Microstructure data unavailable"}
                and setup_type in {
                    "BREAKOUT",
                    "BREAKOUT_CONFIRM",
                    "CONTINUATION",
                    "AGGRESSIVE_CONTINUATION",
                    "RETEST",
                    "OPENING_DRIVE",
                    "REVERSAL",
                    "TRAP_REVERSAL",
                }
                and getattr(self.strategy, "last_pressure_conflict_level", "NONE") in {"NONE", "MILD"}
                and bool(futures_acceptance.get("accepted"))
                and float(futures_acceptance.get("score") or 0) >= 62
                and initiative_strength_score >= 32
                and float(getattr(self.strategy, "last_entry_score", 0) or 0) >= (
                    92 if setup_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "CONTINUATION", "AGGRESSIVE_CONTINUATION", "RETEST", "OPENING_DRIVE"} else 88
                )
            )
            if price_action_micro_override:
                override_metrics = dict(metrics or {})
                override_metrics["override"] = "price_action_microstructure"
                override_metrics["base_reason"] = oi_reason
                override_metrics["futures_acceptance_score"] = float(futures_acceptance.get("score") or 0)
                override_metrics["initiative_strength_score"] = initiative_strength_score
                return True, "Price-action microstructure override", override_metrics, None
            return False, oi_reason, metrics, None

        return True, oi_reason, metrics, None

    def _score_option_candidate(self, row, direction, preferred_strike, underlying_price):
        return OptionSignalGuard.score_option_candidate(self, row, direction, preferred_strike, underlying_price)

    def _build_option_candidates(self, underlying_price, preferred_strikes=None, signal_direction=None, balanced_pro=None):
        return OptionSignalGuard.build_option_candidates(
            self,
            underlying_price,
            preferred_strikes,
            signal_direction,
            balanced_pro,
        )

    def _clear_pending_entry_watch(self):
        self.pending_entry_watch = None

    def _clear_pending_spike_watch(self):
        self.pending_spike_watch = None

    def _preserve_existing_pending_watch(self, candle_5m):
        return PendingEntryWatchPolicy.preserve_existing_pending_watch(self, candle_5m)

    def _pending_watch_is_recoverable(self, pending, current_time=None):
        return PendingEntryWatchPolicy.pending_watch_is_recoverable(self, pending, current_time)

    def _active_trade_monitor_is_recoverable(self, current_time=None):
        return PendingEntryWatchPolicy.active_trade_monitor_is_recoverable(self, current_time)

    def _collect_unprocessed_5m_candles(self, recent_candles, current_time):
        return PendingEntryWatchPolicy.collect_unprocessed_5m_candles(self, recent_candles, current_time)

    @staticmethod
    def _one_minute_trigger_volume_ok(recent_1m_candles):
        return PendingEntryWatchPolicy.one_minute_trigger_volume_ok(recent_1m_candles)

    @staticmethod
    def _pending_watch_risk_reward_ok(pending):
        return PendingEntryWatchPolicy.pending_watch_risk_reward_ok(pending)

    def _rebalance_pending_watch_plan(self, pending, candle_5m):
        return PendingEntryWatchPolicy.rebalance_pending_watch_plan(self, pending, candle_5m)

    @staticmethod
    def _pending_watch_not_too_late(pending, latest_price):
        return PendingEntryWatchPolicy.pending_watch_not_too_late(pending, latest_price)

    def _pending_watch_max_minutes(self, pending):
        return PendingEntryWatchUtils.pending_watch_max_minutes(self.instrument, pending)

    @staticmethod
    def _pending_watch_retrigger_eligible(pending, latest, previous):
        return PendingEntryWatchUtils.pending_watch_retrigger_eligible(pending, latest, previous)

    def _rearm_pending_entry_watch(self, latest, reason):
        return PendingEntryWatchPolicy.rearm_pending_entry_watch(self, latest, reason)

    @staticmethod
    def _pending_watch_elite_ready(pending):
        return PendingEntryWatchUtils.pending_watch_elite_ready(pending)

    @staticmethod
    def _candle_close_strength(candle, direction):
        return PendingEntryWatchUtils.candle_close_strength(candle, direction)

    @staticmethod
    def _candle_body_ratio(candle):
        return PendingEntryWatchUtils.candle_body_ratio(candle)

    def _pending_watch_quality_ok(self, pending, latest, previous):
        return PendingEntryWatchPolicy.pending_watch_quality_ok(self, pending, latest, previous)

    @staticmethod
    def _pending_watch_spike_micro_override_ready(pending, latest):
        return PendingEntryWatchPolicy.pending_watch_spike_micro_override_ready(pending, latest)

    @staticmethod
    def _pending_watch_has_caution(pending, caution):
        return PendingEntryWatchPolicy.pending_watch_has_caution(pending, caution)

    def _pending_watch_conflicts_too_high(self, pending):
        return PendingEntryWatchPolicy.pending_watch_conflicts_too_high(self, pending)

    def _set_pending_entry_watch(self, watch_payload, balanced_pro, candle_5m):
        return PendingEntryWatchStateManager.set_pending_entry_watch(
            self,
            watch_payload,
            balanced_pro,
            candle_5m,
        )

    def _evaluate_pending_entry_watch(self, recent_1m_candles):
        return PendingEntryWatchEvaluator.evaluate_pending_entry_watch(self, recent_1m_candles)

    def _maybe_fire_pending_entry_watch(self, latest_5m_candle):
        return PendingEntryWatchTriggerEngine.maybe_fire_pending_entry_watch(self, latest_5m_candle)
        self._start_trade_monitor(signal, latest_5m_candle, evaluation["price"], balanced_pro, strike)
        self.signals_generated += 1
        self._clear_pending_entry_watch()

    def _evaluate_trade_monitor(self, recent_1m_candles, recent_5m_candles):
        return TradeMonitorEvaluator.evaluate_trade_monitor(self, recent_1m_candles, recent_5m_candles)

    def _maybe_send_trade_monitor_update(self, latest_5m_candle):
        return TradeMonitorDispatcher.maybe_send_trade_monitor_update(self, latest_5m_candle)

    @staticmethod
    def _monitor_alert_is_high_priority(guidance):
        return TradeMonitorDispatcher.monitor_alert_is_high_priority(guidance)

    def _should_send_trade_monitor_alert(self, monitor_data, minute_key):
        return TradeMonitorDispatcher.should_send_trade_monitor_alert(self, monitor_data, minute_key)

    def _classify_base_bias(self, price, vwap_value, oi_bias, oi_ladder_data):
        """Balanced Pro layer 1: derive directional bias."""
        if vwap_value is None:
            return "NEUTRAL"

        oi_trend = oi_ladder_data["trend"] if oi_ladder_data else None
        build_up = oi_ladder_data["build_up"] if oi_ladder_data else None
        bullish_pressure_score = float((oi_ladder_data or {}).get("bullish_pressure_score") or 0)
        bearish_pressure_score = float((oi_ladder_data or {}).get("bearish_pressure_score") or 0)
        support_wall_state = (oi_ladder_data or {}).get("support_wall_state")
        resistance_wall_state = (oi_ladder_data or {}).get("resistance_wall_state")

        bullish_votes = 0
        bearish_votes = 0

        if price > vwap_value:
            bullish_votes += 1
        elif price < vwap_value:
            bearish_votes += 1

        if oi_bias == "BULLISH":
            bullish_votes += 1
        elif oi_bias == "BEARISH":
            bearish_votes += 1

        if oi_trend == "BULLISH":
            bullish_votes += 1
        elif oi_trend == "BEARISH":
            bearish_votes += 1

        if build_up in ["LONG_BUILDUP", "SHORT_COVERING"]:
            bullish_votes += 1
        elif build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]:
            bearish_votes += 1

        if bullish_pressure_score >= bearish_pressure_score * 1.2 and bullish_pressure_score > 0:
            bullish_votes += 1
        elif bearish_pressure_score >= bullish_pressure_score * 1.2 and bearish_pressure_score > 0:
            bearish_votes += 1

        if support_wall_state == "STRENGTHENING":
            bullish_votes += 1
        if resistance_wall_state == "STRENGTHENING":
            bearish_votes += 1

        if bullish_votes >= bearish_votes + 2:
            return "BULLISH"
        if bearish_votes >= bullish_votes + 2:
            return "BEARISH"
        return "NEUTRAL"

    def _classify_signal_quality(self, fallback_context, pressure_metrics):
        """Balanced Pro layer 3: grade signal quality based on data richness."""
        if pressure_metrics and not (fallback_context and fallback_context["fallback_used"]):
            return "A"
        if fallback_context and fallback_context["fallback_used"]:
            return "B"
        return "C"

    def _is_option_buyer_actionable(self, signal, candle_time=None):
        if not signal:
            return False

        sweep_ctx = getattr(self, "option_sweep_context", None) or {}
        signal_type = (self.strategy.last_signal_type or "NONE").upper()
        signal_grade = (self.strategy.last_signal_grade or "SKIP").upper()
        confidence = (self.strategy.last_confidence or "LOW").upper()
        regime = (self.strategy.last_regime or "UNKNOWN").upper()
        current_cautions = set(getattr(self.strategy, "last_cautions", []) or [])
        score = float(
            getattr(self.strategy, "last_entry_score", None)
            or getattr(self.strategy, "last_score", 0)
            or 0
        )
        entry_score = float(
            getattr(self.strategy, "last_entry_score", getattr(self.strategy, "last_score", 0))
            or 0
        )
        sweep_override = (
            sweep_ctx.get("direction") == signal
            and sweep_ctx.get("quality") == "STRONG"
            and sweep_ctx.get("micro_confirmed")
            and sweep_ctx.get("persistence_pairs", 0) >= 3
            and score >= 82
            and getattr(self.strategy, "last_pressure_conflict_level", "NONE") in {"NONE", "MILD"}
        )
        instrument_rule_allows = InstrumentActionableRules.should_allow_signal(
            instrument=self.instrument,
            signal_type=signal_type,
            signal_grade=signal_grade,
            confidence=confidence,
            regime=regime,
            candle_time=candle_time,
            score=getattr(self.strategy, "last_score", score),
            entry_score=entry_score,
            pressure_conflict_level=getattr(self.strategy, "last_pressure_conflict_level", "NONE"),
        )
        opening_breakout_window = (
            candle_time is not None
            and (candle_time.hour, candle_time.minute) <= (11, 0)
        )
        weak_participation_flags = {
            "participation_weak",
            "participation_delta_missing",
            "participation_baseline_weak",
        }
        late_chase_window = (
            candle_time is not None
            and (candle_time.hour, candle_time.minute) >= (14, 45)
        )
        late_chase_flags = {
            "far_from_vwap",
            "theta_fast_exit_required",
            "late_day_breakdown_watch",
        }

        if signal_type == "CONTINUATION" and not Config.ALLOW_CONTINUATION_ENTRY:
            breakout_memory = getattr(self.strategy, "breakout_memory", None) or {}
            recent_breakout = bool(
                breakout_memory
                and candle_time is not None
                and breakout_memory.get("session_day") == candle_time.date()
                and int((candle_time - breakout_memory.get("time")).total_seconds() // 60) <= 45
            )
            continuation_override = (
                instrument_rule_allows
                and confidence in {"MEDIUM", "HIGH"}
                and score >= 70
                and getattr(self.strategy, "last_pressure_conflict_level", "NONE") in {"NONE", "MILD"}
            )
            if not sweep_override and not continuation_override and not (
                recent_breakout and signal_grade in {"A", "A+"} and confidence == "HIGH" and score >= 78
            ):
                return False
        if sweep_override and signal_type in {"REVERSAL", "BREAKOUT_CONFIRM", "CONTINUATION", "RETEST", "BREAKOUT"}:
            return True
        if (
            signal in {"CE", "PE"}
            and opening_breakout_window
            and signal_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "OPENING_DRIVE"}
            and len(current_cautions.intersection(weak_participation_flags)) >= 2
            and signal_grade not in {"A+"}
        ):
            return False
        if (
            signal in {"CE", "PE"}
            and late_chase_window
            and signal_type in {"BREAKOUT_CONFIRM", "RETEST", "CONTINUATION"}
            and len(current_cautions.intersection(late_chase_flags)) >= 2
            and signal_grade not in {"A+"}
            and entry_score < 92
        ):
            return False
        if (
            signal in {"CE", "PE"}
            and candle_time is not None
            and (candle_time.hour, candle_time.minute) >= (14, 50)
            and signal_type in {"BREAKOUT_CONFIRM", "RETEST", "CONTINUATION"}
            and signal_grade not in {"A+"}
            and current_cautions.intersection({"far_from_vwap", "adx_not_confirmed", "theta_fast_exit_required"})
        ):
            return False
        if self.instrument == "SENSEX":
            return instrument_rule_allows
        late_reversal_window = (
            candle_time is not None
            and candle_time.time() >= datetime.strptime("13:30", "%H:%M").time()
        )
        if (
            self.instrument == "NIFTY"
            and signal_type in {"REVERSAL", "TRAP_REVERSAL"}
            and signal_grade in {"A", "A+"}
            and confidence == "HIGH"
            and getattr(self.strategy, "last_pressure_conflict_level", "NONE") == "NONE"
            and float(getattr(self.strategy, "last_score", 0) or 0) >= 88
            and float(getattr(self.strategy, "last_entry_score", getattr(self.strategy, "last_score", 0)) or 0) >= 84
            and not late_reversal_window
        ):
            return True
        if signal_type not in Config.OPTION_BUYER_ALERT_TYPES and not instrument_rule_allows:
            return False
        if self.instrument in {"NIFTY", "BANKNIFTY"} and instrument_rule_allows:
            return True
        if signal_type == "BREAKOUT_CONFIRM" and signal_grade == "B" and confidence in {"MEDIUM", "HIGH"} and score >= 80:
            return True
        if signal_grade not in Config.OPTION_BUYER_ALERT_GRADES:
            return False
        if confidence not in {"MEDIUM", "HIGH"}:
            return False
        critical_cautions = {
            "participation_spread_wide",
            "participation_weak",
            "adx_not_confirmed",
            "higher_tf_not_aligned",
            "oi_divergence_against",
        }
        caution_hits = current_cautions.intersection(critical_cautions)
        if len(caution_hits) >= 2 and score < 90:
            return False
        if caution_hits and score < 84 and signal_type not in {"BREAKOUT_CONFIRM", "RETEST"}:
            return False
        return True

    def _build_balanced_pro_summary(self, bias, signal, fallback_context, pressure_metrics, actionable_signal=False):
        """Balanced Pro output summary for logs and saved reasons."""
        setup = self.strategy.last_signal_type or "NONE"
        quality = self._classify_signal_quality(fallback_context, pressure_metrics)
        decision_state = (self.strategy.last_decision_state or "IGNORE").upper()
        if actionable_signal:
            tradability = "ACTION"
        elif decision_state == "ACTION":
            tradability = "WATCH"
        elif decision_state == "WATCH":
            tradability = "WATCH"
        else:
            tradability = "NO_TRADE"
        return {
            "bias": bias,
            "setup": setup,
            "quality": quality,
            "tradability": tradability,
            "time_regime": self.strategy.last_time_regime,
            "opening_bias": getattr(self.strategy, "last_opening_bias", "UNKNOWN"),
            "active_day_state": getattr(self.strategy, "last_active_day_state", "UNKNOWN"),
            "day_state_direction": getattr(self.strategy, "last_day_state_direction", "NONE"),
            "day_state_detail": getattr(self.strategy, "last_day_state_detail", ""),
            "context_score": self.strategy.last_context_score,
            "entry_score": self.strategy.last_entry_score,
            "decision_state": decision_state,
            "watch_bucket": self.strategy.last_watch_bucket,
            "pressure_conflict_level": self.strategy.last_pressure_conflict_level,
            "confidence_summary": self.strategy.last_confidence_summary,
            "session_map_phase": getattr(self.strategy, "last_session_map_phase", "UNKNOWN"),
            "futures_acceptance_score": getattr(self.strategy, "last_futures_acceptance_score", 0.0),
            "initiative_strength_score": getattr(self.strategy, "last_initiative_strength_score", 0.0),
            "signal_family": getattr(self.strategy, "last_signal_family", "UNKNOWN"),
            "pressure_summary": None,
        }

    @staticmethod
    def _safe_ratio(numerator, denominator):
        if denominator in (None, 0):
            return 0.0
        return float(numerator) / float(denominator)

    @staticmethod
    def _option_row_key(row):
        return (row.get("strike"), row.get("option_type"))

    @staticmethod
    def _participation_row_weight(distance_from_atm):
        distance = abs(distance_from_atm if distance_from_atm is not None else 99)
        weights = {
            0: 1.8,
            1: 1.25,
            2: 0.75,
        }
        return weights.get(distance, 0.35)

    def _build_option_sweep_context(self, candle_time, price, atr, recent_candles_5m=None):
        if not candle_time or not getattr(self, "db_reader", None):
            return None

        effective_close = candle_time + timedelta(minutes=5)
        snapshot_groups = self.db_reader.fetch_recent_option_band_snapshots(
            self.instrument,
            before_ts=effective_close,
            limit=3,
        )
        if len(snapshot_groups) < 2:
            return None

        latest_rows = snapshot_groups[-1]
        prev_rows = snapshot_groups[-2]
        older_rows = snapshot_groups[-3] if len(snapshot_groups) >= 3 else None
        latest_ts = latest_rows[0]["ts"] if latest_rows else None
        prev_ts = prev_rows[0]["ts"] if prev_rows else None
        if latest_ts is None or prev_ts is None:
            return None

        latest_map = {self._option_row_key(row): row for row in latest_rows}
        prev_map = {self._option_row_key(row): row for row in prev_rows}
        older_map = {self._option_row_key(row): row for row in (older_rows or [])}
        current_atm = next((row.get("atm_strike") for row in latest_rows if row.get("atm_strike") is not None), None)
        if current_atm is None:
            return None

        strike_step = int(self.profile.get("strike_step") or 100)
        scoped_offsets = range(-5, 6)
        scoped_strikes = {int(current_atm + (offset * strike_step)) for offset in scoped_offsets}
        strike_count = len(scoped_strikes)

        def summarize(direction):
            same_side = "CE" if direction == "CE" else "PE"
            opposite_side = "PE" if direction == "CE" else "CE"
            pair_support = 0
            price_breadth = 0
            price_failures = 0
            volume_breadth = 0
            weighted_edge = 0.0
            oi_support = 0
            impulse_examples = []

            for strike in sorted(scoped_strikes):
                same_now = latest_map.get((strike, same_side))
                same_prev = prev_map.get((strike, same_side))
                opp_now = latest_map.get((strike, opposite_side))
                opp_prev = prev_map.get((strike, opposite_side))
                if not all([same_now, same_prev, opp_now, opp_prev]):
                    continue

                same_price_delta = float((same_now.get("ltp") or 0) - (same_prev.get("ltp") or 0))
                opp_price_delta = float((opp_now.get("ltp") or 0) - (opp_prev.get("ltp") or 0))
                same_vol_delta = int((same_now.get("volume") or 0) - (same_prev.get("volume") or 0))
                opp_vol_delta = int((opp_now.get("volume") or 0) - (opp_prev.get("volume") or 0))
                same_oi_delta = int((same_now.get("oi") or 0) - (same_prev.get("oi") or 0))
                opp_oi_delta = int((opp_now.get("oi") or 0) - (opp_prev.get("oi") or 0))

                if direction == "CE":
                    directional_price_ok = same_price_delta > 0 and opp_price_delta < 0
                    oi_ok = same_oi_delta <= 0 or opp_oi_delta >= 0
                else:
                    directional_price_ok = same_price_delta > 0 and opp_price_delta < 0
                    oi_ok = same_oi_delta <= 0 or opp_oi_delta >= 0

                if directional_price_ok:
                    price_breadth += 1
                else:
                    price_failures += 1

                if same_vol_delta > 0:
                    volume_breadth += 1

                if directional_price_ok and same_vol_delta > 0:
                    pair_support += 1
                    weighted_edge += self._participation_row_weight(
                        same_now.get("distance_from_atm")
                    )
                    if oi_ok:
                        oi_support += 1
                    if len(impulse_examples) < 3:
                        impulse_examples.append(
                            f"{strike}{same_side}:{round(same_price_delta, 2)}/{same_vol_delta}"
                        )

            return {
                "pair_support": pair_support,
                "price_breadth": price_breadth,
                "price_failures": price_failures,
                "volume_breadth": volume_breadth,
                "weighted_edge": round(weighted_edge, 2),
                "oi_support": oi_support,
                "examples": impulse_examples,
            }

        def summarize_persistence(direction):
            if not older_rows:
                return 0
            same_side = "CE" if direction == "CE" else "PE"
            opposite_side = "PE" if direction == "CE" else "CE"
            persistent_pairs = 0
            for strike in sorted(scoped_strikes):
                newer = prev_map.get((strike, same_side))
                older = older_map.get((strike, same_side))
                opp_newer = prev_map.get((strike, opposite_side))
                opp_older = older_map.get((strike, opposite_side))
                if not all([newer, older, opp_newer, opp_older]):
                    continue
                same_delta = float((newer.get("ltp") or 0) - (older.get("ltp") or 0))
                opp_delta = float((opp_newer.get("ltp") or 0) - (opp_older.get("ltp") or 0))
                if same_delta > 0 and opp_delta < 0:
                    persistent_pairs += 1
            return persistent_pairs

        def micro_trend(direction):
            recent_1m = self.db_reader.fetch_recent_candles_1m(
                self.instrument,
                limit=4,
                before_ts=effective_close,
            )
            if len(recent_1m) < 3:
                return False
            closes = [float(candle.get("close") or 0.0) for candle in recent_1m[-3:]]
            if direction == "CE":
                return closes[-1] > closes[-2] > closes[-3]
            return closes[-1] < closes[-2] < closes[-3]

        ce_summary = summarize("CE")
        pe_summary = summarize("PE")
        ce_persistence = summarize_persistence("CE")
        pe_persistence = summarize_persistence("PE")
        ce_micro_ok = micro_trend("CE")
        pe_micro_ok = micro_trend("PE")

        direction = None
        dominant = None
        if (
            ce_summary["pair_support"] >= 6
            and ce_summary["price_breadth"] >= 7
            and ce_summary["weighted_edge"] >= 5.2
        ):
            direction = "CE"
            dominant = ce_summary
        elif (
            pe_summary["pair_support"] >= 6
            and pe_summary["price_breadth"] >= 7
            and pe_summary["weighted_edge"] >= 5.2
        ):
            direction = "PE"
            dominant = pe_summary

        if not direction or not dominant:
            return None

        persistence_pairs = ce_persistence if direction == "CE" else pe_persistence
        micro_ok = ce_micro_ok if direction == "CE" else pe_micro_ok
        five_min_ok = False
        if recent_candles_5m and len(recent_candles_5m) >= 2:
            last_two = recent_candles_5m[-2:]
            if direction == "CE":
                five_min_ok = last_two[-1]["close"] >= last_two[0]["close"]
            else:
                five_min_ok = last_two[-1]["close"] <= last_two[0]["close"]

        quality = "MODERATE"
        if (
            dominant["pair_support"] >= 7
            and dominant["price_breadth"] >= 8
            and persistence_pairs >= 5
            and micro_ok
        ):
            quality = "STRONG"

        score_boost = 4 if quality == "MODERATE" else 10
        if persistence_pairs >= 4:
            score_boost += 2

        return {
            "direction": direction,
            "quality": quality,
            "score_boost": score_boost,
            "pair_support": dominant["pair_support"],
            "price_breadth": dominant["price_breadth"],
            "price_failures": dominant["price_failures"],
            "volume_breadth": dominant["volume_breadth"],
            "weighted_edge": dominant["weighted_edge"],
            "oi_support": dominant["oi_support"],
            "persistence_pairs": persistence_pairs,
            "micro_confirmed": micro_ok,
            "five_min_confirmed": five_min_ok,
            "trigger_ready": quality == "STRONG" and micro_ok and persistence_pairs >= 3,
            "summary": (
                f"{direction} sweep {quality.lower()} | breadth {dominant['pair_support']}/{strike_count}"
                f" | persist {persistence_pairs} | micro {'yes' if micro_ok else 'no'}"
            ),
            "examples": dominant["examples"],
            "latest_ts": latest_ts,
            "previous_ts": prev_ts,
        }

    def _participation_phase(self, candle_time):
        current_now = candle_time.time() if candle_time is not None else self.time_utils.current_time()
        if current_now < self.time_utils._parse_clock("09:45"):
            return "OPENING"
        if current_now < self.time_utils._parse_clock("11:30"):
            return "MID_MORNING"
        if current_now < self.time_utils._parse_clock("13:30"):
            return "MIDDAY"
        return "LATE"

    def _participation_rolling_average(self, direction):
        history = list(self.participation_history.get(direction) or [])
        if not history:
            return 0.0
        return sum(history) / len(history)

    def _update_participation_history(self, history_key, metrics):
        if history_key is None or history_key == self.last_participation_history_key or not metrics:
            return

        for direction in ("CE", "PE"):
            directional = metrics.get(direction) or {}
            self.participation_history[direction].append(float(directional.get("weighted_volume_delta", 0.0) or 0.0))
        self.last_participation_history_key = history_key

    def _build_option_participation_metrics(self, candle_time=None):
        if not self.option_data or not self.option_data.get("band_snapshots"):
            return None

        band_rows = self.option_data.get("band_snapshots") or []
        ce_rows = [row for row in band_rows if row.get("option_type") == "CE"]
        pe_rows = [row for row in band_rows if row.get("option_type") == "PE"]
        participation_phase = self._participation_phase(candle_time)

        def summarize(rows, atm_distance=2):
            scoped = [row for row in rows if abs(row.get("distance_from_atm", 99)) <= atm_distance]
            atm_row = next((row for row in rows if row.get("distance_from_atm") == 0), None)
            volume_total = sum(int(row.get("volume", 0) or 0) for row in scoped)
            volume_delta = sum(
                max(int(row.get("volume", 0) or 0) - int(row.get("previous_volume", 0) or 0), 0)
                for row in scoped
                if row.get("previous_volume") is not None
            )
            weighted_volume_delta = sum(
                max(int(row.get("volume", 0) or 0) - int(row.get("previous_volume", 0) or 0), 0)
                * self._participation_row_weight(row.get("distance_from_atm"))
                for row in scoped
                if row.get("previous_volume") is not None
            )
            oi_delta = sum(
                int(row.get("oi", 0) or 0) - int(row.get("previous_oi", 0) or 0)
                for row in scoped
                if row.get("previous_oi") is not None
            )
            active_breadth = sum(
                1
                for row in scoped
                if (
                    (int(row.get("volume", 0) or 0) - int(row.get("previous_volume", 0) or 0)) > 0
                    if row.get("previous_volume") is not None else int(row.get("volume", 0) or 0) > 0
                )
            )
            weighted_breadth = sum(
                self._participation_row_weight(row.get("distance_from_atm"))
                for row in scoped
                if (
                    (int(row.get("volume", 0) or 0) - int(row.get("previous_volume", 0) or 0)) > 0
                    if row.get("previous_volume") is not None else int(row.get("volume", 0) or 0) > 0
                )
            )
            spread = atm_row.get("spread") if atm_row else None
            ltp = atm_row.get("ltp") if atm_row else None
            spread_pct = round(self._safe_ratio(spread, ltp) * 100, 2) if spread is not None and ltp else None
            bid_qty = atm_row.get("top_bid_quantity") if atm_row else None
            ask_qty = atm_row.get("top_ask_quantity") if atm_row else None
            quote_ratio = round(self._safe_ratio(bid_qty or 0, ask_qty or 1), 2) if bid_qty is not None or ask_qty is not None else None
            return {
                "volume_total": volume_total,
                "volume_delta": volume_delta,
                "weighted_volume_delta": round(weighted_volume_delta, 2),
                "oi_delta": oi_delta,
                "active_breadth": active_breadth,
                "weighted_breadth": round(weighted_breadth, 2),
                "atm_spread": spread,
                "atm_spread_pct": spread_pct,
                "atm_quote_ratio": quote_ratio,
            }

        ce = summarize(ce_rows)
        pe = summarize(pe_rows)

        def directional_metrics(direction):
            same_side = ce if direction == "CE" else pe
            opposite_side = pe if direction == "CE" else ce
            score_boost = 0
            flags = []
            rolling_avg = self._participation_rolling_average(direction)
            opening_mode = participation_phase == "OPENING"

            opposite_delta_factor = 1.05 if opening_mode else 1.15
            rolling_factor = 0.8 if opening_mode else 1.0
            breadth_edge = 0.2 if opening_mode else 0.45

            same_dominates_delta = same_side["weighted_volume_delta"] > max(
                opposite_side["weighted_volume_delta"] * opposite_delta_factor,
                rolling_avg * rolling_factor,
                0,
            )
            same_dominates_breadth = same_side["weighted_breadth"] >= (opposite_side["weighted_breadth"] + breadth_edge)
            oi_supportive = same_side["oi_delta"] >= opposite_side["oi_delta"]
            spread_ok = same_side["atm_spread_pct"] is None or same_side["atm_spread_pct"] <= Config.MAX_SPREAD_PERCENT
            quote_ok = same_side["atm_quote_ratio"] is None or same_side["atm_quote_ratio"] >= 0.85
            beats_own_baseline = same_side["weighted_volume_delta"] >= max(rolling_avg * (0.95 if opening_mode else 1.1), 0)

            if same_dominates_delta:
                score_boost += 5
                flags.append("same_side_volume_delta")
            else:
                score_boost -= 4
                flags.append("same_side_volume_delta_missing")
            if beats_own_baseline:
                score_boost += 3
                flags.append("same_side_vs_rolling_avg")
            else:
                score_boost -= 2
                flags.append("same_side_vs_rolling_avg_missing")
            if same_dominates_breadth:
                score_boost += 4
                flags.append("same_side_breadth")
            else:
                score_boost -= 3
                flags.append("same_side_breadth_missing")
            if oi_supportive:
                score_boost += 3
                flags.append("same_side_oi_delta")
            else:
                score_boost -= 2
                flags.append("same_side_oi_delta_missing")
            if spread_ok:
                score_boost += 2
                flags.append("atm_spread_ok")
            else:
                score_boost -= 4
                flags.append("atm_spread_wide")
            if quote_ok:
                score_boost += 1
                flags.append("atm_quote_supportive")
            else:
                score_boost -= 2
                flags.append("atm_quote_weak")

            if score_boost >= 9:
                quality = "STRONG"
            elif score_boost >= 3:
                quality = "MODERATE"
            else:
                quality = "WEAK"

            return {
                "quality": quality,
                "score_boost": score_boost,
                "same_side_volume_delta": same_side["volume_delta"],
                "opposite_side_volume_delta": opposite_side["volume_delta"],
                "same_side_weighted_delta": same_side["weighted_volume_delta"],
                "opposite_side_weighted_delta": opposite_side["weighted_volume_delta"],
                "rolling_avg_weighted_delta": round(rolling_avg, 2),
                "same_side_breadth": same_side["active_breadth"],
                "opposite_side_breadth": opposite_side["active_breadth"],
                "same_side_weighted_breadth": same_side["weighted_breadth"],
                "opposite_side_weighted_breadth": opposite_side["weighted_breadth"],
                "same_side_oi_delta": same_side["oi_delta"],
                "opposite_side_oi_delta": opposite_side["oi_delta"],
                "atm_spread_pct": same_side["atm_spread_pct"],
                "atm_quote_ratio": same_side["atm_quote_ratio"],
                "participation_phase": participation_phase,
                "flags": flags,
            }

        metrics = {
            "CE": directional_metrics("CE"),
            "PE": directional_metrics("PE"),
        }
        history_key = candle_time.isoformat() if candle_time is not None else None
        self._update_participation_history(history_key, metrics)
        return metrics

    @staticmethod
    def _summarize_participation_read(participation_metrics, direction):
        if not participation_metrics or direction not in {"CE", "PE"}:
            return None

        directional = participation_metrics.get(direction) or {}
        quality = directional.get("quality")
        same_delta = directional.get("same_side_volume_delta")
        opposite_delta = directional.get("opposite_side_volume_delta")
        same_weighted_delta = directional.get("same_side_weighted_delta")
        rolling_avg = directional.get("rolling_avg_weighted_delta")
        breadth = directional.get("same_side_breadth")
        spread_pct = directional.get("atm_spread_pct")
        phase = directional.get("participation_phase")

        bits = []
        if quality:
            bits.append(f"{quality} participation")
        if phase:
            bits.append(phase.lower())
        if same_delta is not None and opposite_delta is not None:
            bits.append(f"same-side delta {same_delta} vs opp {opposite_delta}")
        if same_weighted_delta is not None and rolling_avg is not None:
            bits.append(f"weighted {same_weighted_delta} vs avg {rolling_avg}")
        if breadth is not None:
            bits.append(f"breadth {breadth}")
        if spread_pct is not None:
            bits.append(f"ATM spread {spread_pct}%")
        return " | ".join(bits) if bits else None

    @staticmethod
    def _pressure_direction_scores(pressure_metrics):
        if not pressure_metrics:
            return None

        flow_bullish = pressure_metrics.get("bullish_pressure_score")
        flow_bearish = pressure_metrics.get("bearish_pressure_score")
        if flow_bullish is not None and flow_bearish is not None:
            return {
                "bullish_score": round(min(max(float(flow_bullish), 0.0), 100.0), 1),
                "bearish_score": round(min(max(float(flow_bearish), 0.0), 100.0), 1),
            }

        bullish_score = 0.0
        bearish_score = 0.0

        near_put = float(pressure_metrics.get("near_put_pressure_ratio") or 0.0)
        near_call = float(pressure_metrics.get("near_call_pressure_ratio") or 0.0)
        full_put = float(pressure_metrics.get("full_put_pressure_ratio") or 0.0)
        full_call = float(pressure_metrics.get("full_call_pressure_ratio") or 0.0)

        bullish_score += min(near_put * 22, 40)
        bullish_score += min(full_put * 16, 28)
        bearish_score += min(near_call * 22, 40)
        bearish_score += min(full_call * 16, 28)

        atm_pe_vol = float(pressure_metrics.get("atm_pe_volume") or 0.0)
        atm_ce_vol = float(pressure_metrics.get("atm_ce_volume") or 0.0)
        atm_pe_oi = float(pressure_metrics.get("atm_pe_oi") or 0.0)
        atm_ce_oi = float(pressure_metrics.get("atm_ce_oi") or 0.0)
        mid_pe_volume = float(pressure_metrics.get("mid_pe_volume") or 0.0)
        mid_ce_volume = float(pressure_metrics.get("mid_ce_volume") or 0.0)
        near_pe_oi = float(pressure_metrics.get("near_pe_oi") or 0.0)
        near_ce_oi = float(pressure_metrics.get("near_ce_oi") or 0.0)

        if atm_pe_vol > atm_ce_vol:
            bullish_score += 8
        elif atm_ce_vol > atm_pe_vol:
            bearish_score += 8

        if atm_pe_oi > atm_ce_oi:
            bullish_score += 6
        elif atm_ce_oi > atm_pe_oi:
            bearish_score += 6

        if mid_pe_volume > mid_ce_volume:
            bullish_score += 6
        elif mid_ce_volume > mid_pe_volume:
            bearish_score += 6

        if near_pe_oi > near_ce_oi:
            bullish_score += 4
        elif near_ce_oi > near_pe_oi:
            bearish_score += 4

        return {
            "bullish_score": round(min(max(bullish_score, 0.0), 100.0), 1),
            "bearish_score": round(min(max(bearish_score, 0.0), 100.0), 1),
        }

    def _build_pressure_summary(self, pressure_metrics, participation_metrics=None, direction=None):
        scores = self._pressure_direction_scores(pressure_metrics)
        if not scores:
            return None

        bullish_score = scores["bullish_score"]
        bearish_score = scores["bearish_score"]
        edge = round(abs(bullish_score - bearish_score), 1)

        if bullish_score >= bearish_score + 8:
            bias = "BULLISH"
        elif bearish_score >= bullish_score + 8:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        strength = "STRONG" if edge >= 18 else "MODERATE" if edge >= 8 else "MIXED"

        bits = [f"Pressure {bias} ({strength})", f"Bull {bullish_score} vs Bear {bearish_score}"]

        if pressure_metrics:
            underlying_delta = pressure_metrics.get("underlying_delta")
            if underlying_delta is not None:
                bits.append(f"spotΔ {underlying_delta}")
            ce_price_delta = pressure_metrics.get("atm_ce_ltp_delta")
            pe_price_delta = pressure_metrics.get("atm_pe_ltp_delta")
            if ce_price_delta is not None and pe_price_delta is not None:
                bits.append(f"ATM CEΔ {ce_price_delta} / PEΔ {pe_price_delta}")
            near_put = pressure_metrics.get("near_put_pressure_ratio")
            near_call = pressure_metrics.get("near_call_pressure_ratio")
            if near_put is not None and near_call is not None:
                bits.append(f"near PE {near_put} / CE {near_call}")
            top_put_wall = pressure_metrics.get("top_put_wall_strike")
            top_call_wall = pressure_metrics.get("top_call_wall_strike")
            if top_put_wall or top_call_wall:
                bits.append(
                    "walls P {put} ({put_ratio}) / C {call} ({call_ratio})".format(
                        put=top_put_wall if top_put_wall is not None else "-",
                        call=top_call_wall if top_call_wall is not None else "-",
                        put_ratio=pressure_metrics.get("put_wall_strength_ratio"),
                        call_ratio=pressure_metrics.get("call_wall_strength_ratio"),
                    )
                )
            flow_notes = pressure_metrics.get("flow_notes") or []
            if flow_notes:
                bits.append("flow " + ", ".join(flow_notes[:4]))

        if direction in {"CE", "PE"} and participation_metrics:
            directional = participation_metrics.get(direction) or {}
            quality = directional.get("quality")
            if quality:
                bits.append(f"{direction} participation {quality}")

        return {
            "bias": bias,
            "strength": strength,
            "bullish_score": bullish_score,
            "bearish_score": bearish_score,
            "edge": edge,
            "summary": " | ".join(bits),
        }

    @staticmethod
    def _derive_15m_trend_from_5m(candles_5m):
        if not candles_5m or len(candles_5m) < 6:
            return None

        grouped = []
        for idx in range(0, len(candles_5m), 3):
            chunk = candles_5m[idx:idx + 3]
            if len(chunk) < 3:
                continue
            grouped.append(
                {
                    "time": chunk[-1]["time"],
                    "open": chunk[0]["open"],
                    "high": max(c["high"] for c in chunk),
                    "low": min(c["low"] for c in chunk),
                    "close": chunk[-1]["close"],
                    "volume": sum(c.get("volume", 0) for c in chunk),
                }
            )

        if len(grouped) < 2:
            return None
        return calculate_trend_from_candles(grouped, lookback=min(5, len(grouped)))

    def _institutional_confluence_gate(
        self,
        signal,
        trend_15m,
        pressure_metrics,
        oi_ladder_data,
        participation_metrics,
        option_sweep_context=None,
    ):
        if signal not in {"CE", "PE"}:
            return True, "no_signal"

        expected_bias = "BULLISH" if signal == "CE" else "BEARISH"
        setup = (getattr(self.strategy, "last_signal_type", None) or "NONE").upper()
        grade = (getattr(self.strategy, "last_signal_grade", None) or "SKIP").upper()
        confidence = (getattr(self.strategy, "last_confidence", None) or "LOW").upper()
        context_score = float(getattr(self.strategy, "last_context_score", 0) or 0)
        entry_score = float(getattr(self.strategy, "last_entry_score", 0) or 0)
        pressure_bias = (pressure_metrics or {}).get("pressure_bias")
        pressure_bias = (pressure_bias or "NEUTRAL").upper()
        flow_edge = float((pressure_metrics or {}).get("smoothed_flow_edge") or (pressure_metrics or {}).get("flow_edge") or 0.0)
        oi_trend = ((oi_ladder_data or {}).get("trend") or "NEUTRAL").upper()
        build_up = ((oi_ladder_data or {}).get("build_up") or "NONE").upper()
        wall_alert = ((oi_ladder_data or {}).get("wall_break_alert") or "NONE").upper()
        directional = (participation_metrics or {}).get(signal) or {}
        participation_quality = (directional.get("quality") or "WEAK").upper()
        participation_delta = float(directional.get("same_side_weighted_delta") or 0.0)
        participation_breadth = float(directional.get("same_side_weighted_breadth") or 0.0)
        opposite_delta = float(directional.get("opposite_side_weighted_delta") or 0.0)
        sweep_quality = ((option_sweep_context or {}).get("quality") or "").upper()
        ict_context = getattr(self.strategy, "last_ict_context", None) or {}
        ict_quality = (ict_context.get("quality") or "NONE").upper()
        ict_action_ready = bool(
            ict_context.get("direction") == signal
            and ict_context.get("action_ready")
            and ict_quality in {"A", "B"}
        )

        trend_text = (trend_15m or "UNKNOWN").upper()
        aligned_trend = (
            trend_text in {"BULLISH", "STRONG_BULLISH", "UP"}
            if signal == "CE"
            else trend_text in {"BEARISH", "STRONG_BEARISH", "DOWN"}
        )
        neutral_trend = trend_text in {"NEUTRAL", "UNKNOWN", "INSUFFICIENT_DATA", "NONE"}
        reversal_setup = setup in {"REVERSAL", "TRAP_REVERSAL"}
        strong_sweep_context = (
            sweep_quality == "STRONG"
            and pressure_bias == expected_bias
            and oi_trend == expected_bias
            and flow_edge >= float(getattr(Config, "INSTITUTIONAL_MIN_FLOW_EDGE", 8.0) or 8.0)
            and setup in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "CONTINUATION", "OPENING_DRIVE"}
            and entry_score >= 88
        )
        if not aligned_trend and not (neutral_trend and (reversal_setup or strong_sweep_context or ict_action_ready)):
            return False, f"15m context not aligned ({trend_text} vs {signal})"

        if context_score < float(getattr(Config, "INSTITUTIONAL_MIN_CONTEXT_SCORE", 88.0) or 88.0):
            return False, f"context score low ({context_score:.0f})"
        if entry_score < float(getattr(Config, "INSTITUTIONAL_MIN_ENTRY_SCORE", 84.0) or 84.0):
            return False, f"entry score low ({entry_score:.0f})"
        if grade not in {"A+", "A", "B"} or confidence not in {"MEDIUM", "HIGH"}:
            return False, f"setup quality weak ({grade}/{confidence})"

        if pressure_bias != expected_bias:
            return False, f"pressure not aligned ({pressure_bias})"
        if flow_edge < float(getattr(Config, "INSTITUTIONAL_MIN_FLOW_EDGE", 8.0) or 8.0):
            return False, f"flow edge weak ({flow_edge:.1f})"
        if oi_trend != expected_bias:
            return False, f"OI trend not aligned ({oi_trend})"

        supportive_build_ups = (
            {"LONG_BUILDUP", "SHORT_COVERING"}
            if signal == "CE"
            else {"SHORT_BUILDUP", "LONG_UNWINDING"}
        )
        squeeze_ok = (
            build_up == "NO_CLEAR_SIGNAL"
            and sweep_quality == "STRONG"
            and pressure_bias == expected_bias
            and oi_trend == expected_bias
            and wall_alert in {"RESISTANCE_BREAK_RISK", "SUPPORT_BREAK_RISK"}
        )
        sweep_continuation_ok = (
            build_up == "NO_CLEAR_SIGNAL"
            and strong_sweep_context
            and participation_quality in {"STRONG", "MODERATE", "WEAK"}
            and participation_breadth >= 1.0
        )
        ict_continuation_ok = (
            build_up == "NO_CLEAR_SIGNAL"
            and ict_action_ready
            and pressure_bias == expected_bias
            and oi_trend in {expected_bias, "NEUTRAL"}
            and participation_breadth >= 1.0
        )
        if build_up not in supportive_build_ups and not squeeze_ok and not sweep_continuation_ok and not ict_continuation_ok:
            return False, f"build-up not supportive ({build_up})"

        wall_ok = (
            wall_alert in {"RESISTANCE_BREAK_RISK", "NONE", ""}
            if signal == "CE"
            else wall_alert in {"SUPPORT_BREAK_RISK", "NONE", ""}
        )
        if not wall_ok:
            return False, f"OI wall against trade ({wall_alert})"

        participation_ok = (
            participation_quality in {"STRONG", "MODERATE"}
            and participation_delta > max(opposite_delta * 0.85, 0.0)
            and participation_breadth >= 1.8
        )
        if not participation_ok and sweep_quality != "STRONG":
            return False, "option participation not broad enough"

        return True, (
            f"institutional confluence ok | 15m={trend_text} | pressure={pressure_bias} "
            f"| oi={oi_trend}/{build_up} | participation={participation_quality}"
            + (f" | {ict_context.get('summary')}" if ict_action_ready else "")
        )

    def _restore_indicator_state(self):
        """Restore indicator state from database"""
        if not self.db_writer.enabled:
            return

        recent_candles = self.db_writer.fetch_recent_candles_5m(
            instrument=self.instrument,
            limit=self.config.STATE_RECOVERY_5M_BARS,
        )
        if not recent_candles:
            self._log("No recent candles found for indicator warmup")
            return

        for candle in recent_candles:
            self.vwap.update(candle)
            self.atr.update(candle)
            self.volume.update(candle)
            self.orb.add_candle(candle)

        if not self.orb.is_orb_ready():
            self.orb.calculate_orb()

        self._log(f"Restored {len(recent_candles)} candles for indicator warmup")

    def _print_startup_status(self):
        """Print startup status"""
        self._log("Started:")
        self._log(f"Instrument: {self.instrument}")
        self._log(f"Strategy: {self.strategy.__class__.__name__}")
        self._log(f"DB Enabled: {self.db_writer.enabled}")
        self._log(f"Notifications: {'ENABLED' if Config.ENABLE_ALERTS else 'DISABLED'}")

    def _is_debug_enabled(self):
        """Check if debug mode is enabled"""
        return Config.DEBUG or Config.CONSOLE_MODE == "DETAILED"

    def _debug_print(self, *args, **kwargs):
        """Print debug messages if enabled"""
        if self._is_debug_enabled():
            print("[Signal Service]", *args, **kwargs)

    def _refresh_option_data_if_due(self):
        """Refresh option data from hybrid shared cache first, DB fallback second."""
        if time_module.time() - self.last_option_fetch <= 5:
            return

        self.last_option_fetch = time_module.time()
        latest_option_data = self._load_cached_option_data()
        source = "CACHE"
        if not latest_option_data:
            latest_option_data = self._load_shared_option_data()
            source = "DB_FALLBACK"
        if latest_option_data:
            self.option_data = latest_option_data
            self.option_data_ts = latest_option_data.get("snapshot_ts")
            self.option_data_source = source
            self._log(f"Option data source: {self.option_data_source} | snapshot_ts={self.option_data_ts}")

    def _load_cached_option_data(self):
        """Load freshest local shared-cache option data when available."""
        cached = self.option_data_cache.get(self.instrument)
        if not cached:
            return None

        snapshot_ts_raw = cached.get("snapshot_ts")
        if not snapshot_ts_raw:
            return None

        try:
            snapshot_ts = datetime.fromisoformat(snapshot_ts_raw)
        except ValueError:
            return None

        now = self.time_utils.now_ist()
        if snapshot_ts.tzinfo is None:
            snapshot_ts = snapshot_ts.replace(tzinfo=now.tzinfo)
        cache_age = max(0.0, (now - snapshot_ts).total_seconds())
        max_age = getattr(self.config, "OPTION_CACHE_MAX_AGE_SECONDS", Config.OPTION_CACHE_MAX_AGE_SECONDS)
        if cache_age > max_age:
            return None

        band_snapshots = cached.get("band_snapshots") or []
        if not band_snapshots:
            return None

        normalized_rows = []
        for row in band_snapshots:
            normalized_rows.append(
                {
                    "atm_strike": int(row["atm_strike"]) if row.get("atm_strike") is not None else None,
                    "strike": int(row["strike"]) if row.get("strike") is not None else None,
                    "distance_from_atm": int(row["distance_from_atm"]) if row.get("distance_from_atm") is not None else None,
                    "option_type": row.get("option_type"),
                    "security_id": row.get("security_id"),
                    "oi": int(row.get("oi", 0) or 0),
                    "volume": int(row.get("volume", 0) or 0),
                    "ltp": float(row.get("ltp", 0) or 0),
                    "iv": float(row.get("iv", 0) or 0),
                    "top_bid_price": float(row["top_bid_price"]) if row.get("top_bid_price") is not None else None,
                    "top_bid_quantity": int(row["top_bid_quantity"]) if row.get("top_bid_quantity") is not None else None,
                    "top_ask_price": float(row["top_ask_price"]) if row.get("top_ask_price") is not None else None,
                    "top_ask_quantity": int(row["top_ask_quantity"]) if row.get("top_ask_quantity") is not None else None,
                    "spread": float(row["spread"]) if row.get("spread") is not None else None,
                    "average_price": float(row["average_price"]) if row.get("average_price") is not None else None,
                    "previous_oi": int(row["previous_oi"]) if row.get("previous_oi") is not None else None,
                    "previous_volume": int(row["previous_volume"]) if row.get("previous_volume") is not None else None,
                    "delta": float(row["delta"]) if row.get("delta") is not None else None,
                    "theta": float(row["theta"]) if row.get("theta") is not None else None,
                    "gamma": float(row["gamma"]) if row.get("gamma") is not None else None,
                    "vega": float(row["vega"]) if row.get("vega") is not None else None,
                }
            )

        ce_rows = [row for row in normalized_rows if row["option_type"] == "CE"]
        pe_rows = [row for row in normalized_rows if row["option_type"] == "PE"]
        total_ce = sum(row["oi"] for row in ce_rows)
        total_pe = sum(row["oi"] for row in pe_rows)
        atm = next((row["atm_strike"] for row in normalized_rows if row.get("atm_strike") is not None), None)
        atm_ce = next((row for row in ce_rows if row.get("distance_from_atm") == 0), None)
        atm_pe = next((row for row in pe_rows if row.get("distance_from_atm") == 0), None)

        return {
            "snapshot_ts": snapshot_ts_raw,
            "instrument": cached.get("instrument"),
            "time": cached.get("time"),
            "expiry": cached.get("expiry"),
            "underlying_price": float(cached["underlying_price"]) if cached.get("underlying_price") is not None else None,
            "atm": atm,
            "pcr": round(total_pe / total_ce, 2) if total_ce else 0.0,
            "ce_oi_ladder": {row["strike"]: row["oi"] for row in ce_rows if row.get("strike") is not None},
            "pe_oi_ladder": {row["strike"]: row["oi"] for row in pe_rows if row.get("strike") is not None},
            "band_snapshots": normalized_rows,
            "max_call_oi_strike": max((row["strike"] for row in ce_rows), key=lambda strike: next(r["oi"] for r in ce_rows if r["strike"] == strike), default=None),
            "max_put_oi_strike": max((row["strike"] for row in pe_rows), key=lambda strike: next(r["oi"] for r in pe_rows if r["strike"] == strike), default=None),
            "atm_ce_security_id": atm_ce.get("security_id") if atm_ce else None,
            "atm_pe_security_id": atm_pe.get("security_id") if atm_pe else None,
            "ce_ltp": atm_ce.get("ltp", 0) if atm_ce else 0,
            "pe_ltp": atm_pe.get("ltp", 0) if atm_pe else 0,
            "ce_oi": atm_ce.get("oi", 0) if atm_ce else 0,
            "pe_oi": atm_pe.get("oi", 0) if atm_pe else 0,
            "ce_volume": atm_ce.get("volume", 0) if atm_ce else 0,
            "pe_volume": atm_pe.get("volume", 0) if atm_pe else 0,
            "ce_volume_band": sum(row["volume"] for row in ce_rows),
            "pe_volume_band": sum(row["volume"] for row in pe_rows),
            "ce_iv": atm_ce.get("iv", 0) if atm_ce else 0,
            "pe_iv": atm_pe.get("iv", 0) if atm_pe else 0,
            "ce_top_bid_price": atm_ce.get("top_bid_price") if atm_ce else None,
            "ce_top_bid_quantity": atm_ce.get("top_bid_quantity") if atm_ce else None,
            "ce_top_ask_price": atm_ce.get("top_ask_price") if atm_ce else None,
            "ce_top_ask_quantity": atm_ce.get("top_ask_quantity") if atm_ce else None,
            "pe_top_bid_price": atm_pe.get("top_bid_price") if atm_pe else None,
            "pe_top_bid_quantity": atm_pe.get("top_bid_quantity") if atm_pe else None,
            "pe_top_ask_price": atm_pe.get("top_ask_price") if atm_pe else None,
            "pe_top_ask_quantity": atm_pe.get("top_ask_quantity") if atm_pe else None,
            "ce_spread": atm_ce.get("spread") if atm_ce else None,
            "pe_spread": atm_pe.get("spread") if atm_pe else None,
            "ce_delta": atm_ce.get("delta") if atm_ce else None,
            "pe_delta": atm_pe.get("delta") if atm_pe else None,
            "ce_theta": atm_ce.get("theta") if atm_ce else None,
            "pe_theta": atm_pe.get("theta") if atm_pe else None,
        }

    def _load_shared_option_data(self):
        """Build option-chain context from the latest shared DB snapshot."""
        before_ts = getattr(self, "_current_candle_time", None)
        band_rows = self.db_reader.fetch_latest_option_band_snapshot(self.instrument, before_ts=before_ts)
        if not band_rows:
            return None

        snapshot_ts = band_rows[0]["ts"]
        ce_rows = [row for row in band_rows if row["option_type"] == "CE"]
        pe_rows = [row for row in band_rows if row["option_type"] == "PE"]
        if not ce_rows and not pe_rows:
            return None

        atm = next((row["atm_strike"] for row in band_rows if row.get("atm_strike") is not None), None)
        ce_oi_ladder = {row["strike"]: row["oi"] for row in ce_rows if row.get("strike") is not None}
        pe_oi_ladder = {row["strike"]: row["oi"] for row in pe_rows if row.get("strike") is not None}
        total_ce = sum(row["oi"] for row in ce_rows)
        total_pe = sum(row["oi"] for row in pe_rows)
        pcr = round(total_pe / total_ce, 2) if total_ce else 0.0

        atm_ce = next((row for row in ce_rows if row.get("distance_from_atm") == 0), None)
        atm_pe = next((row for row in pe_rows if row.get("distance_from_atm") == 0), None)

        latest_oi_snapshot = self.db_reader.fetch_latest_oi_snapshot(self.instrument, before_ts=snapshot_ts)
        underlying_price = (
            latest_oi_snapshot.get("underlying_price")
            if latest_oi_snapshot and latest_oi_snapshot.get("underlying_price") is not None
            else None
        )

        return {
            "snapshot_ts": snapshot_ts,
            "atm": atm,
            "band_snapshots": [
                {key: value for key, value in row.items() if key != "ts"}
                for row in band_rows
            ],
            "ce_oi_ladder": ce_oi_ladder,
            "pe_oi_ladder": pe_oi_ladder,
            "pcr": pcr,
            "ce_oi": atm_ce.get("oi", 0) if atm_ce else 0,
            "pe_oi": atm_pe.get("oi", 0) if atm_pe else 0,
            "ce_volume": atm_ce.get("volume", 0) if atm_ce else 0,
            "pe_volume": atm_pe.get("volume", 0) if atm_pe else 0,
            "ce_ltp": atm_ce.get("ltp", 0) if atm_ce else 0,
            "pe_ltp": atm_pe.get("ltp", 0) if atm_pe else 0,
            "ce_iv": atm_ce.get("iv", 0) if atm_ce else 0,
            "pe_iv": atm_pe.get("iv", 0) if atm_pe else 0,
            "ce_top_bid_price": atm_ce.get("top_bid_price") if atm_ce else None,
            "ce_top_bid_quantity": atm_ce.get("top_bid_quantity") if atm_ce else None,
            "ce_top_ask_price": atm_ce.get("top_ask_price") if atm_ce else None,
            "ce_top_ask_quantity": atm_ce.get("top_ask_quantity") if atm_ce else None,
            "pe_top_bid_price": atm_pe.get("top_bid_price") if atm_pe else None,
            "pe_top_bid_quantity": atm_pe.get("top_bid_quantity") if atm_pe else None,
            "pe_top_ask_price": atm_pe.get("top_ask_price") if atm_pe else None,
            "pe_top_ask_quantity": atm_pe.get("top_ask_quantity") if atm_pe else None,
            "ce_spread": atm_ce.get("spread") if atm_ce else None,
            "pe_spread": atm_pe.get("spread") if atm_pe else None,
            "ce_delta": atm_ce.get("delta") if atm_ce else None,
            "pe_delta": atm_pe.get("delta") if atm_pe else None,
            "ce_theta": atm_ce.get("theta") if atm_ce else None,
            "pe_theta": atm_pe.get("theta") if atm_pe else None,
            "underlying_price": underlying_price,
            "expiry": None,
        }

    def _build_oi_snapshot_fallback_context(self, price, candle_time):
        """Approximate strategy context from latest OI snapshot when band data is unavailable."""
        snapshot = self.db_reader.fetch_latest_oi_snapshot(self.instrument, before_ts=candle_time)
        if not snapshot:
            return {
                "oi_bias": "NEUTRAL",
                "oi_trend": None,
                "build_up": None,
                "support": None,
                "resistance": None,
                "pressure_metrics": None,
                "pcr": None,
                "ce_delta_total": None,
                "pe_delta_total": None,
                "fallback_used": False,
            }

        oi_bias = "NEUTRAL"
        if snapshot["oi_sentiment"] == "BULLISH":
            oi_bias = "BULLISH"
        elif snapshot["oi_sentiment"] == "BEARISH":
            oi_bias = "BEARISH"

        if oi_bias == "BULLISH" or (snapshot["pcr"] > 1 and snapshot["volume_pcr"] >= 1):
            oi_trend = "BULLISH"
        elif oi_bias == "BEARISH" or (snapshot["pcr"] < 1 and snapshot["volume_pcr"] < 1):
            oi_trend = "BEARISH"
        else:
            oi_trend = "NEUTRAL"

        build_up = None
        base_price = snapshot["underlying_price"]
        price_change = 0 if base_price is None else price - base_price
        if price_change > 0:
            if snapshot["pe_oi_change"] > 0:
                build_up = "LONG_BUILDUP"
            elif snapshot["ce_oi_change"] < 0:
                build_up = "SHORT_COVERING"
        elif price_change < 0:
            if snapshot["ce_oi_change"] > 0:
                build_up = "SHORT_BUILDUP"
            elif snapshot["pe_oi_change"] < 0:
                build_up = "LONG_UNWINDING"

        support = snapshot["support_level"] if snapshot["support_level"] and snapshot["support_level"] > 0 else None
        resistance = snapshot["resistance_level"] if snapshot["resistance_level"] and snapshot["resistance_level"] > 0 else None

        now_ts = time_module.time()
        if now_ts - self.last_oi_fallback_notice > 60:
            self._log("OI-only fallback active (option band snapshots unavailable)")
            self.last_oi_fallback_notice = now_ts

        return {
            "oi_bias": oi_bias,
            "oi_trend": oi_trend,
            "build_up": build_up,
            "support": support,
            "resistance": resistance,
            "pressure_metrics": None,
            "pcr": snapshot["pcr"],
            "ce_delta_total": snapshot["ce_oi_change"],
            "pe_delta_total": snapshot["pe_oi_change"],
            "fallback_used": True,
        }

    def _build_oi_ladder_context(self, price):
        """Build OI ladder context for strategy"""
        if not self.option_data:
            self.prev_price = price
            return None

        ce_oi_ladder = self.option_data.get("ce_oi_ladder", {})
        pe_oi_ladder = self.option_data.get("pe_oi_ladder", {})
        band_rows = self.option_data.get("band_snapshots") or []
        ce_volume_delta = {
            row["strike"]: int((row.get("volume") or 0) - (row.get("previous_volume") or 0))
            for row in band_rows
            if row.get("option_type") == "CE" and row.get("strike") is not None
        }
        pe_volume_delta = {
            row["strike"]: int((row.get("volume") or 0) - (row.get("previous_volume") or 0))
            for row in band_rows
            if row.get("option_type") == "PE" and row.get("strike") is not None
        }
        price_change = 0 if self.prev_price is None else price - self.prev_price

        self.prev_price = price
        self.prev_total_oi = sum(ce_oi_ladder.values()) + sum(pe_oi_ladder.values())

        oi_ladder_data = self.oi_ladder.analyze(
            ce_oi_ladder,
            pe_oi_ladder,
            price_change,
            atm=self.option_data.get("atm"),
            price=price,
            ce_volume_delta=ce_volume_delta,
            pe_volume_delta=pe_volume_delta,
        )

        if self._is_debug_enabled():
            print("\n[Signal Service] OI Ladder Analysis:")
            print("Support:", oi_ladder_data["support"])
            print("Resistance:", oi_ladder_data["resistance"])
            print("Trend:", oi_ladder_data["trend"])
            print("Build-up:", oi_ladder_data["build_up"])
            print("OI Summary:", oi_ladder_data.get("oi_summary"))

        return oi_ladder_data

    def _resolve_orb_levels(self, candle_5m):
        """Get ORB levels"""
        if Config.TEST_MODE:
            return candle_5m["high"] + 10, candle_5m["low"] - 10

        self.orb.add_candle(candle_5m)
        if self.orb.is_orb_ready():
            return self.orb.get_orb_levels()

        orb_high, orb_low = self.orb.calculate_orb()
        if orb_high is not None and orb_low is not None:
            return orb_high, orb_low

        recent_candles = self.db_reader.fetch_recent_candles_5m(self.instrument, limit=24)
        if not recent_candles or recent_candles[-1]["time"] != candle_5m["time"]:
            recent_candles = (recent_candles or []) + [candle_5m]

        orb_high, orb_low = self.orb.get_fallback_levels(recent_candles)
        if orb_high is not None and orb_low is not None:
            return orb_high, orb_low
        return None, None

    def _apply_signal_cooldowns(self, signal, reason):
        """Apply signal cooldowns and filters"""
        signal, reason = signal, reason  # No connection manager in signal service

        if signal and not self._is_option_buyer_actionable(signal, candle_time=getattr(self, "_current_candle_time", None)):
            signal = None
            reason = (
                f"Option-buyer filter blocked live alert"
                f" | candidate_type={self.strategy.last_signal_type}"
                f" | candidate_grade={self.strategy.last_signal_grade}"
                f" | score={self.strategy.last_score}"
            )

        if (
                signal
                and self.strategy.last_signal_type == "CONTINUATION"
                and not Config.ALLOW_CONTINUATION_ENTRY
        ):
            signal = None
            reason = f"Continuation watchlist only | score={self.strategy.last_score}"

        if signal and self.last_emitted_signal == signal and self.signal_cooldown_remaining > 0:
            signal = None
            reason = (
                f"Duplicate signal cooldown active ({self.signal_cooldown_remaining} bars left)"
                f" | score={self.strategy.last_score}"
            )
            self.signal_cooldown_remaining -= 1
        elif signal:
            self.last_emitted_signal = signal
            self.signal_cooldown_remaining = Config.SIGNAL_COOLDOWN_BARS
        elif self.signal_cooldown_remaining > 0:
            self.signal_cooldown_remaining -= 1

        return signal, reason

    def _update_intraday_ranges(self, candle_5m):
        """Update intraday ranges"""
        self.day_high_5m = candle_5m["high"] if self.day_high_5m is None else max(self.day_high_5m, candle_5m["high"])
        self.day_low_5m = candle_5m["low"] if self.day_low_5m is None else min(self.day_low_5m, candle_5m["low"])

        if candle_5m["time"].time() >= self.time_utils._parse_clock("09:45"):
            return

        self.opening_range_30_high = (
            candle_5m["high"] if self.opening_range_30_high is None
            else max(self.opening_range_30_high, candle_5m["high"])
        )
        self.opening_range_30_low = (
            candle_5m["low"] if self.opening_range_30_low is None
            else min(self.opening_range_30_low, candle_5m["low"])
        )

    def _safe_save_strategy_decision(self, ts, price, signal, reason, volume_signal, oi_bias, oi_trend, build_up, pressure_metrics, ce_delta_total, pe_delta_total, pcr, orb_high, orb_low, vwap, atr, strike, balanced_pro=None, oi_mode=None):
        """Save strategy decision to database"""
        try:
            actionable_block_reason = None
            if signal is None and reason:
                if reason.startswith("Option-buyer filter blocked live alert"):
                    actionable_block_reason = "option_buyer_filter"
                elif reason.startswith("No-trade zone filtered"):
                    actionable_block_reason = "no_trade_zone"
                elif reason.startswith("Microstructure filtered"):
                    actionable_block_reason = "microstructure"
                elif reason.startswith("Premium quality filtered"):
                    actionable_block_reason = "premium_quality"
                elif reason.startswith("OI wall filtered"):
                    actionable_block_reason = "oi_wall"
                elif reason.startswith("ML Filtered"):
                    actionable_block_reason = "ml_filter"

            row = (
                ts,
                self.instrument,
                float(price) if price is not None else None,
                signal,
                reason,
                int(self.strategy.last_score),
                ", ".join(self.strategy.last_score_components),
                volume_signal,
                oi_bias,
                oi_trend,
                build_up,
                pressure_metrics["pressure_bias"] if pressure_metrics else None,
                int(ce_delta_total) if ce_delta_total is not None else None,
                int(pe_delta_total) if pe_delta_total is not None else None,
                float(pcr) if pcr is not None else None,
                float(orb_high) if orb_high is not None else None,
                float(orb_low) if orb_low is not None else None,
                float(vwap) if vwap is not None else None,
                float(atr) if atr is not None else None,
                int(strike) if strike is not None else None,
                balanced_pro["bias"] if balanced_pro else None,
                balanced_pro["setup"] if balanced_pro else None,
                balanced_pro["quality"] if balanced_pro else None,
                balanced_pro["tradability"] if balanced_pro else None,
                balanced_pro["time_regime"] if balanced_pro else None,
                balanced_pro.get("opening_bias") if balanced_pro else None,
                balanced_pro.get("active_day_state") if balanced_pro else None,
                balanced_pro.get("day_state_direction") if balanced_pro else None,
                balanced_pro.get("day_state_detail") if balanced_pro else None,
                oi_mode,
                list(self.strategy.last_blockers or []),
                list(self.strategy.last_cautions or []),
                self.strategy.last_signal_type,
                self.strategy.last_signal_grade,
                self.strategy.last_confidence,
                actionable_block_reason,
                self.strategy.last_watch_bucket,
                self.strategy.last_pressure_conflict_level,
                self.strategy.last_confidence_summary,
                self.strategy.last_entry_plan.get("entry_above"),
                self.strategy.last_entry_plan.get("entry_below"),
                self.strategy.last_entry_plan.get("invalidate_price"),
                self.strategy.last_entry_plan.get("first_target_price"),
            )
            self.db_writer.insert_strategy_decision_5m(row)
        except Exception as e:
            self._log(f"DB save error (strategy decision): {e}")

    def _safe_save_trade_monitor_event(self, ts, monitor_data):
        """Persist trade monitor guidance to DB."""
        try:
            row = (
                ts,
                self.instrument,
                monitor_data.get("signal"),
                self.active_trade_monitor.get("entry_time") if self.active_trade_monitor else None,
                float(monitor_data.get("entry_price")) if monitor_data.get("entry_price") is not None else None,
                float(monitor_data.get("option_price")) if monitor_data.get("option_price") is not None else None,
                float(monitor_data.get("pnl_points")) if monitor_data.get("pnl_points") is not None else None,
                monitor_data.get("guidance"),
                monitor_data.get("reason"),
                monitor_data.get("structure"),
                monitor_data.get("quality"),
                monitor_data.get("time_regime"),
                monitor_data.get("run_profile"),
                bool(monitor_data.get("runner_mode")),
                float(monitor_data.get("dynamic_trail_pct")) if monitor_data.get("dynamic_trail_pct") is not None else None,
                bool(monitor_data.get("profit_lock_armed")),
            )
            self.db_writer.insert_trade_monitor_event_1m(row)
        except Exception as e:
            self._log(f"DB save error (trade monitor): {e}")

    def _safe_save_option_signal_outcome(self, ts, monitor_data):
        """Persist option-premium outcome snapshots for fired signals."""
        try:
            if not self.active_trade_monitor:
                return
            minutes_since_signal = int(self.active_trade_monitor.get("minutes_active") or 0)
            row = (
                self.active_trade_monitor.get("entry_time"),
                ts,
                self.instrument,
                monitor_data.get("signal"),
                self.active_trade_monitor.get("strike"),
                float(self.active_trade_monitor.get("entry_underlying_price")) if self.active_trade_monitor.get("entry_underlying_price") is not None else None,
                float(monitor_data.get("price")) if monitor_data.get("price") is not None else None,
                float(monitor_data.get("entry_price")) if monitor_data.get("entry_price") is not None else None,
                float(monitor_data.get("option_price")) if monitor_data.get("option_price") is not None else None,
                float(monitor_data.get("option_bid")) if monitor_data.get("option_bid") is not None else None,
                float(monitor_data.get("option_ask")) if monitor_data.get("option_ask") is not None else None,
                float(monitor_data.get("option_spread")) if monitor_data.get("option_spread") is not None else None,
                float(monitor_data.get("pnl_points")) if monitor_data.get("pnl_points") is not None else None,
                float(monitor_data.get("max_favorable_ltp")) if monitor_data.get("max_favorable_ltp") is not None else None,
                float(monitor_data.get("max_adverse_ltp")) if monitor_data.get("max_adverse_ltp") is not None else None,
                minutes_since_signal,
                monitor_data.get("guidance"),
                monitor_data.get("reason"),
                monitor_data.get("run_profile"),
                bool(monitor_data.get("runner_mode")),
            )
            self.db_writer.insert_option_signal_outcome_1m(row)
            self._safe_save_option_signal_horizon_outcome(ts, monitor_data, minutes_since_signal)
        except Exception as e:
            self._log(f"DB save error (option outcome): {e}")

    @staticmethod
    def _classify_option_horizon_outcome(pnl_points, max_favorable_points, max_adverse_points):
        pnl = float(pnl_points or 0)
        max_fav = float(max_favorable_points or 0)
        max_adv = float(max_adverse_points or 0)
        if max_fav >= 20 or pnl >= 12:
            return "WIN"
        if max_adv <= -12 and pnl <= 0:
            return "LOSS"
        if pnl > 0:
            return "POSITIVE"
        if pnl < 0:
            return "NEGATIVE"
        return "FLAT"

    def _safe_save_option_signal_horizon_outcome(self, ts, monitor_data, minutes_since_signal):
        if minutes_since_signal not in {1, 2, 3, 5, 10, 15, 20}:
            return
        try:
            entry_price = monitor_data.get("entry_price")
            option_price = monitor_data.get("option_price")
            pnl_points = monitor_data.get("pnl_points")
            max_fav_ltp = monitor_data.get("max_favorable_ltp")
            max_adv_ltp = monitor_data.get("max_adverse_ltp")
            max_fav_points = (
                float(max_fav_ltp) - float(entry_price)
                if max_fav_ltp is not None and entry_price is not None else pnl_points
            )
            max_adv_points = (
                float(max_adv_ltp) - float(entry_price)
                if max_adv_ltp is not None and entry_price is not None else pnl_points
            )
            pnl_percent = (
                (float(pnl_points) / float(entry_price)) * 100.0
                if pnl_points is not None and entry_price not in {None, 0} else None
            )
            row = (
                self.active_trade_monitor.get("entry_time"),
                minutes_since_signal,
                ts,
                self.instrument,
                monitor_data.get("signal"),
                self.active_trade_monitor.get("strike"),
                float(self.active_trade_monitor.get("entry_underlying_price")) if self.active_trade_monitor.get("entry_underlying_price") is not None else None,
                float(monitor_data.get("price")) if monitor_data.get("price") is not None else None,
                float(entry_price) if entry_price is not None else None,
                float(option_price) if option_price is not None else None,
                float(pnl_points) if pnl_points is not None else None,
                float(pnl_percent) if pnl_percent is not None else None,
                float(max_fav_points) if max_fav_points is not None else None,
                float(max_adv_points) if max_adv_points is not None else None,
                self._classify_option_horizon_outcome(pnl_points, max_fav_points, max_adv_points),
            )
            self.db_writer.insert_option_signal_horizon_outcome(row)
        except Exception as e:
            self._log(f"DB save error (option horizon outcome): {e}")

    @staticmethod
    def _option_contract_spread_percent(option_contract):
        if not option_contract:
            return None
        spread = option_contract.get("spread")
        ltp = option_contract.get("ltp")
        if spread is None:
            bid = option_contract.get("top_bid_price")
            ask = option_contract.get("top_ask_price")
            if bid is not None and ask is not None:
                spread = float(ask) - float(bid)
        if spread is None or not ltp:
            return None
        return (float(spread) / max(float(ltp), 1.0)) * 100.0

    def _build_option_buyer_entry_score(self, pending, evaluation=None, option_contract=None):
        evaluation = evaluation or {}
        momentum = evaluation.get("momentum_quality") or {}
        score = calculate_option_buyer_entry_score(
            base_entry_score=pending.get("entry_score"),
            strategy_score=pending.get("score"),
            momentum_score=momentum.get("score"),
            premium_state=evaluation.get("premium_state"),
            liquidity_quality=evaluation.get("liquidity_quality"),
            spread_percent=self._option_contract_spread_percent(option_contract),
            blockers=pending.get("blockers"),
            cautions=pending.get("cautions"),
            confidence=pending.get("confidence"),
            signal_grade=pending.get("signal_grade"),
        )
        return score, option_buyer_action(score)

    def _sync_ml_outcome_from_monitor(self):
        """Backfill ML outcome from premium-based monitor results for the active signal."""
        if not self.active_trade_monitor:
            return
        signal_ts = self.active_trade_monitor.get("signal_ts") or self.active_trade_monitor.get("entry_time")
        signal = self.active_trade_monitor.get("signal")
        strike = self.active_trade_monitor.get("strike")
        outcome = self.db_reader.fetch_option_outcome_summary(signal_ts, self.instrument, signal, strike)
        if not outcome:
            return

        final_pnl = outcome.get("final_pnl_points")
        max_pnl = outcome.get("max_pnl_points")
        min_pnl = outcome.get("min_pnl_points")
        outcome_label = "BREAKEVEN"
        if final_pnl is not None:
            if final_pnl > 0:
                outcome_label = "PROFIT"
            elif final_pnl < 0:
                outcome_label = "LOSS"
        elif max_pnl is not None and min_pnl is not None:
            if max_pnl > abs(min_pnl):
                outcome_label = "PROFIT"
            elif min_pnl < 0:
                outcome_label = "LOSS"

        try:
            from shared.db.pool import DBPool
            if not DBPool._enabled:
                return
            with DBPool.connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE ml_features_log
                    SET actual_outcome = %s,
                        max_favorable_points = %s,
                        max_adverse_points = %s,
                        close_pnl_points = %s,
                        outcome_tag = %s,
                        updated_at = NOW()
                    WHERE alert_ts = %s
                      AND instrument = %s
                      AND signal_direction = %s;
                    """,
                    (
                        outcome_label,
                        outcome.get("max_pnl_points"),
                        outcome.get("min_pnl_points"),
                        final_pnl,
                        "PREMIUM_MONITOR",
                        signal_ts,
                        self.instrument,
                        signal,
                    ),
                )
                conn.commit()
                cur.close()
        except Exception as e:
            self._log(f"DB save error (ml outcome sync): {e}")

    def _safe_save_signal_issued(
        self,
        ts,
        signal,
        price,
        strike,
        reason,
        balanced_pro,
        oi_mode,
        telegram_sent=True,
        monitor_started=True,
        entry_window_end=None,
        underlying_price=None,
        option_contract=None,
        strike_reason=None,
        option_data_source=None,
    ):
        """Persist only actual fired actionable signals to DB."""
        try:
            atm_strike = None
            distance_from_atm = None
            option_entry_ltp = None
            entry_bid = None
            entry_ask = None
            entry_spread = None
            entry_iv = None
            entry_delta = None
            if option_contract:
                atm_strike = option_contract.get("atm_strike")
                distance_from_atm = option_contract.get("distance_from_atm")
                option_entry_ltp = option_contract.get("ltp")
                entry_bid = option_contract.get("top_bid_price")
                entry_ask = option_contract.get("top_ask_price")
                entry_spread = option_contract.get("spread")
                entry_iv = option_contract.get("iv")
                entry_delta = option_contract.get("delta")
            row = (
                ts,
                self.instrument,
                signal,
                float(price) if price is not None else None,
                float(underlying_price) if underlying_price is not None else None,
                int(strike) if strike is not None else None,
                int(atm_strike) if atm_strike is not None else None,
                int(distance_from_atm) if distance_from_atm is not None else None,
                float(option_entry_ltp) if option_entry_ltp is not None else None,
                float(entry_bid) if entry_bid is not None else None,
                float(entry_ask) if entry_ask is not None else None,
                float(entry_spread) if entry_spread is not None else None,
                float(entry_iv) if entry_iv is not None else None,
                float(entry_delta) if entry_delta is not None else None,
                int(self.strategy.last_entry_score or self.strategy.last_score),
                balanced_pro["quality"] if balanced_pro else None,
                balanced_pro["setup"] if balanced_pro else None,
                balanced_pro["tradability"] if balanced_pro else None,
                balanced_pro["time_regime"] if balanced_pro else None,
                balanced_pro.get("opening_bias") if balanced_pro else None,
                balanced_pro.get("active_day_state") if balanced_pro else None,
                balanced_pro.get("day_state_direction") if balanced_pro else None,
                balanced_pro.get("day_state_detail") if balanced_pro else None,
                oi_mode,
                reason,
                strike_reason,
                option_data_source,
                self.strategy.last_confidence_summary,
                self.strategy.last_entry_plan.get("entry_above"),
                self.strategy.last_entry_plan.get("entry_below"),
                self.strategy.last_entry_plan.get("invalidate_price"),
                self.strategy.last_entry_plan.get("first_target_price"),
                bool(telegram_sent),
                bool(monitor_started),
                entry_window_end,
            )
            self.db_writer.insert_signal_issued(row)
            self.last_signal_time = time_module.time()
            return True
        except Exception as e:
            self._log(f"DB save error (signal issued): {e}")
            return False

    def _safe_save_entry_decision_1m(
        self,
        ts,
        pending,
        decision,
        latest_1m,
        evaluation=None,
        option_contract=None,
        strike=None,
        reason=None,
    ):
        """Persist every 1m option-buyer entry gate decision."""
        try:
            if not pending or not latest_1m:
                return

            option_contract = option_contract or {}
            evaluation = evaluation or {}
            option_buyer_score, option_buyer_decision = self._build_option_buyer_entry_score(
                pending,
                evaluation=evaluation,
                option_contract=option_contract,
            )
            row = (
                ts,
                self.instrument,
                pending.get("created_at"),
                pending.get("direction"),
                decision,
                float(evaluation.get("price") or latest_1m.get("close")) if (evaluation.get("price") or latest_1m.get("close")) is not None else None,
                float(latest_1m.get("open")) if latest_1m.get("open") is not None else None,
                float(latest_1m.get("high")) if latest_1m.get("high") is not None else None,
                float(latest_1m.get("low")) if latest_1m.get("low") is not None else None,
                float(latest_1m.get("close")) if latest_1m.get("close") is not None else None,
                int(latest_1m.get("volume")) if latest_1m.get("volume") is not None else None,
                float(pending.get("trigger_price")) if pending.get("trigger_price") is not None else None,
                float(pending.get("invalidate_price")) if pending.get("invalidate_price") is not None else None,
                float(pending.get("first_target_price")) if pending.get("first_target_price") is not None else None,
                int(strike or evaluation.get("selected_strike")) if (strike or evaluation.get("selected_strike")) is not None else None,
                float(option_contract.get("ltp")) if option_contract.get("ltp") is not None else None,
                float(option_contract.get("top_bid_price")) if option_contract.get("top_bid_price") is not None else None,
                float(option_contract.get("top_ask_price")) if option_contract.get("top_ask_price") is not None else None,
                float(option_contract.get("spread")) if option_contract.get("spread") is not None else None,
                int(pending.get("score")) if pending.get("score") is not None else None,
                int(pending.get("entry_score")) if pending.get("entry_score") is not None else None,
                pending.get("signal_type"),
                pending.get("signal_grade"),
                pending.get("confidence"),
                pending.get("watch_bucket"),
                pending.get("time_regime"),
                int(pending.get("minutes_since_watch")) if pending.get("minutes_since_watch") is not None else None,
                int(option_buyer_score),
                option_buyer_decision,
                reason or evaluation.get("reason") or pending.get("reason"),
                list(pending.get("blockers") or []),
                list(pending.get("cautions") or []),
                getattr(self, "option_data_source", None),
            )
            self.db_writer.insert_entry_decision_1m(row)
        except Exception as e:
            self._log(f"DB save error (entry decision 1m): {e}")

    def _safe_save_ml_features(self, ml_features, ml_prob):
        """Save ML features to database for training."""
        if not ml_features:
            return
        try:
            from shared.db.pool import DBPool
            if not DBPool._enabled:
                return

            columns = [
                "alert_ts", "instrument", "signal_direction", "score", "confidence",
                "adx", "volume_ratio", "oi_change_pct", "vwap_distance", "time_hour",
                "time_regime", "iv_rank", "spread_pct", "atr", "price_momentum",
                "pressure_conflict_level", "oi_bias", "oi_trend", "wall_break_alert",
                "support_wall_state", "resistance_wall_state", "oi_divergence",
                "trend_15m", "trend_5m", "trend_aligned",
                "risk_reward_ratio", "has_hybrid_mode", "signal_type", "signal_grade",
                "entry_score", "context_score", "target_points", "stop_points",
                "ml_predicted_prob", "ml_prediction",
            ]
            values = (
                ml_features.get('alert_ts'),
                ml_features.get('instrument'),
                ml_features.get('signal_direction'),
                ml_features.get('score'),
                ml_features.get('confidence'),
                ml_features.get('adx'),
                ml_features.get('volume_ratio'),
                ml_features.get('oi_change_pct'),
                ml_features.get('vwap_distance'),
                ml_features.get('time_hour'),
                ml_features.get('time_regime'),
                ml_features.get('iv_rank'),
                ml_features.get('spread_pct'),
                ml_features.get('atr'),
                ml_features.get('price_momentum'),
                ml_features.get('pressure_conflict_level'),
                ml_features.get('oi_bias'),
                ml_features.get('oi_trend'),
                ml_features.get('wall_break_alert'),
                ml_features.get('support_wall_state'),
                ml_features.get('resistance_wall_state'),
                ml_features.get('oi_divergence'),
                ml_features.get('trend_15m'),
                ml_features.get('trend_5m'),
                ml_features.get('trend_aligned'),
                ml_features.get('risk_reward_ratio'),
                ml_features.get('has_hybrid_mode'),
                ml_features.get('signal_type'),
                ml_features.get('signal_grade'),
                ml_features.get('entry_score'),
                ml_features.get('context_score'),
                ml_features.get('target_points'),
                ml_features.get('stop_points'),
                round(ml_prob, 4) if ml_prob is not None else None,
                'TAKE' if ml_prob is not None and ml_prob >= 0.55 else 'LOG',
            )
            placeholders = ", ".join(["%s"] * len(values))
            query = f"""
            INSERT INTO ml_features_log (
                {", ".join(columns)}, created_at
            ) VALUES (
                {placeholders}, NOW()
            )
            ON CONFLICT (alert_ts, instrument, signal_direction) DO UPDATE
            SET ml_predicted_prob = EXCLUDED.ml_predicted_prob,
                ml_prediction = EXCLUDED.ml_prediction,
                score = EXCLUDED.score,
                confidence = EXCLUDED.confidence,
                signal_type = EXCLUDED.signal_type,
                signal_grade = EXCLUDED.signal_grade,
                entry_score = EXCLUDED.entry_score,
                context_score = EXCLUDED.context_score,
                updated_at = NOW()
            """
            
            with DBPool.connection() as conn:
                cur = conn.cursor()
                cur.execute(query, values)
                conn.commit()
                cur.close()
        except Exception as e:
            self._log(f"DB save error (ml features): {e}")

    def _extract_watch_direction(self, candidate_signal, balanced_pro):
        if candidate_signal in {"CE", "PE"}:
            return candidate_signal
        bias = (balanced_pro or {}).get("bias")
        if bias == "BULLISH":
            return "CE"
        if bias == "BEARISH":
            return "PE"
        return None

    def _build_manual_watch_payload(self, candle_5m, price, candidate_signal, candidate_reason, balanced_pro, orb_high, orb_low, support, resistance):
        if not balanced_pro or balanced_pro.get("tradability") != "WATCH":
            return None
        if "time_filter" in (self.strategy.last_blockers or []):
            return None
        if (self.strategy.last_context_score or self.strategy.last_score or 0) < 60:
            return None
        if (self.strategy.last_confidence or "LOW").upper() not in {"MEDIUM", "HIGH"}:
            return None

        direction = self._extract_watch_direction(candidate_signal, balanced_pro)
        if not direction:
            return None

        blockers = list(self.strategy.last_blockers or [])
        strong_watch = (
            candidate_signal in {"CE", "PE"}
            or "direction_present_but_filters_incomplete" in blockers
            or (self.strategy.last_signal_type or "NONE") != "NONE"
        )
        if not strong_watch:
            return None

        planned_trigger = (
            self.strategy.last_entry_plan.get("entry_above")
            if direction == "CE"
            else self.strategy.last_entry_plan.get("entry_below")
        )
        trigger_price = planned_trigger if planned_trigger is not None else (orb_high if direction == "CE" else orb_low)
        planned_invalidate = self.strategy.last_entry_plan.get("invalidate_price")
        invalidate_price = planned_invalidate if planned_invalidate is not None else (support if direction == "CE" else resistance)
        setup = self.strategy.last_signal_type or balanced_pro.get("setup") or "WATCH"
        if setup == "NONE":
            return None
        watch_bucket = balanced_pro.get("watch_bucket") or self.strategy.last_watch_bucket or "WATCH_SETUP"
        if watch_bucket == "WATCH_CONTEXT":
            return None
        risk_profile = self._resolve_trade_risk_profile(
            setup_type=setup,
            quality=balanced_pro.get("quality") if balanced_pro else None,
            confidence=getattr(self.strategy, "last_confidence", None),
            cautions=getattr(self.strategy, "last_cautions", None),
        )
        no_trade_zone = self._classify_no_trade_zone(balanced_pro, signal=direction)
        structure_suggestion = self._build_option_structure_suggestion(
            signal=direction,
            selected_strike=self.strike_selector.select_strike(
                price=price,
                signal=direction,
                volume_signal="NORMAL",
                strategy_score=self.strategy.last_score,
                pressure_metrics=None,
                cautions=getattr(self.strategy, "last_cautions", None),
                option_chain_data=self.option_data,
                setup_type=setup,
                time_regime=(balanced_pro or {}).get("time_regime"),
                candle_time=self._current_candle_time,
            ) if self.option_data else None,
            selected_option_contract=None,
            balanced_pro=balanced_pro,
            risk_profile=risk_profile,
        )
        flip_context = self._infer_flip_context(
            direction=direction,
            setup=setup,
            candidate_reason=candidate_reason,
        )

        context_bits = [
            f"Bias={balanced_pro.get('bias')}",
            f"Setup={setup}",
            f"Regime={self.strategy.last_regime}",
            f"Bucket={watch_bucket}",
        ]
        context = " | ".join(bit for bit in context_bits if bit and "None" not in bit)
        if watch_bucket == "PA_STRONG_WAIT_PREMIUM":
            action_hint = "Price action strong hai. Option premium confirm kare tabhi actual entry lena."
        elif candidate_signal in {"CE", "PE"}:
            if flip_context:
                action_hint = (
                    f"{flip_context['failed_side']} thesis weak ho rahi hai. "
                    f"{flip_context['buy_side']} tab socho jab reclaim/rejection candle hold kare."
                )
            else:
                action_hint = "Candidate mila hai. Entry tabhi lena jab candle close hold kare aur chart bhi support kare."
        elif setup == "RETEST":
            action_hint = "Retest hold kare tabhi entry socho. Break ho to skip karo."
        elif setup == "REVERSAL":
            if flip_context:
                action_hint = (
                    f"{flip_context['failed_side']} fail lag raha hai. "
                    f"Agar next candle confirm kare to {flip_context['buy_side']} prefer karo."
                )
            else:
                action_hint = "Reversal hai. Next candle confirm kare tabhi entry lena."
        elif setup == "BREAKOUT_CONFIRM":
            if flip_context:
                action_hint = (
                    f"{flip_context['failed_side']} side ka move reject hua lag raha hai. "
                    f"{flip_context['buy_side']} tabhi lo jab confirm zone hold kare."
                )
            else:
                action_hint = "Breakout confirm zone hold kare to entry socho. Instant chase mat karo."
        elif trigger_price is not None:
            side = "above" if direction == "CE" else "below"
            action_hint = f"Abhi sirf watch karo. Entry se pehle clean 5m close {side} {trigger_price} ka wait karo."
        else:
            action_hint = "Abhi entry mat lo. Price confirmation ka wait karo."

        entry_if = None
        if trigger_price is not None:
            side = "above" if direction == "CE" else "below"
            entry_if = f"Clean 5m close {side} {round(trigger_price, 2)} with follow-through volume"

        avoid_if = None
        if invalidate_price is not None:
            side = "below" if direction == "CE" else "above"
            avoid_if = f"Skip if price closes {side} {round(invalidate_price, 2)}"
        elif blockers:
            avoid_if = "Skip if current blockers clear nahi hote"

        participation_read = self._summarize_participation_read(
            getattr(self.strategy, "last_participation_metrics", None),
            direction,
        )
        pressure_summary = (balanced_pro or {}).get("pressure_summary")
        expectancy_profile = self.current_expectancy_profile or {}
        pullback_plan = self.pending_pullback_watch_plan or {}
        trade_thesis = self._build_trade_thesis(direction, balanced_pro=balanced_pro, premium_guard=None)
        if watch_bucket == "WAIT_PULLBACK" and pullback_plan:
            action_hint = (
                f"Move chase mat karo. {pullback_plan.get('strike')} {direction} premium "
                f"{pullback_plan.get('entry_max')} ke paas/cooldown ke baad hi entry consider karo."
            )
            entry_if = (
                f"Premium <= {pullback_plan.get('entry_max')} ke baad 1m reclaim/follow-through confirm ho"
            )
            avoid_if = (
                f"Skip if premium {pullback_plan.get('current_ltp')} se upar hi bhaagta rahe ya spread wide ho"
            )

        return {
            "instrument": self.instrument,
            "direction": direction,
            "setup": setup,
            "signal_grade": self.strategy.last_signal_grade,
            "confidence": self.strategy.last_confidence,
            "confidence_summary": self.strategy.last_confidence_summary,
            "score": self.strategy.last_context_score or self.strategy.last_score,
            "entry_score": self.strategy.last_entry_score,
            "setup_bucket": risk_profile["setup_bucket"],
            "price": round(price, 2) if price is not None else None,
            "trigger_price": round(trigger_price, 2) if trigger_price is not None else None,
            "invalidate_price": round(invalidate_price, 2) if invalidate_price is not None else None,
            "first_target_price": self.strategy.last_entry_plan.get("first_target_price"),
            "option_stop_loss_pct": risk_profile["hard_premium_stop_pct"],
            "option_target_pct": risk_profile["target_pct"],
            "option_trail_pct": risk_profile["trail_from_peak_pct"],
            "time_stop_warn_minutes": risk_profile["time_stop_warn_minutes"],
            "time_stop_exit_minutes": risk_profile["time_stop_exit_minutes"],
            "risk_note": risk_profile["risk_note"],
            "selected_strike": pullback_plan.get("strike"),
            "wait_pullback": watch_bucket == "WAIT_PULLBACK",
            "premium_pullback_entry_max": pullback_plan.get("entry_max"),
            "premium_chase_ltp": pullback_plan.get("current_ltp"),
            "no_trade_zone": no_trade_zone,
            "structure_suggestion": structure_suggestion,
            "blockers": blockers,
            "cautions": list(self.strategy.last_cautions or []),
            "watch_bucket": watch_bucket,
            "decision_label": (no_trade_zone or {}).get("label") or (self._flip_watch_label(direction) if flip_context else f"WATCH_{direction}_SETUP"),
            "journal_note": self._build_journal_note(
                (no_trade_zone or {}).get("label") or (self._flip_watch_label(direction) if flip_context else f"WATCH_{direction}_SETUP"),
                (no_trade_zone or {}).get("action_text") or action_hint,
                (no_trade_zone or {}).get("reason"),
            ),
            "context": f"{context} | ContextScore={self.strategy.last_context_score} | EntryScore={self.strategy.last_entry_score}",
            "reason": candidate_reason,
            "action_hint": action_hint,
            "action_text": (no_trade_zone or {}).get("action_text") or (structure_suggestion or {}).get("action_text") or action_hint,
            "entry_if": entry_if,
            "avoid_if": avoid_if,
            "participation_read": participation_read,
            "pressure_read": pressure_summary.get("summary") if pressure_summary else None,
            "quality_tag": expectancy_profile.get("quality_tag"),
            "entry_phase": expectancy_profile.get("entry_phase"),
            "premium_confirmed": expectancy_profile.get("premium_confirmed"),
            "path_quality": expectancy_profile.get("path_quality"),
            "signal_family": expectancy_profile.get("signal_family"),
            "session_map_phase": expectancy_profile.get("session_map_phase"),
            "price_action_watch_ready": expectancy_profile.get("price_action_watch_ready"),
            "trade_thesis": trade_thesis,
            "flip_context": flip_context,
            "key": (
                self.instrument,
                direction,
                setup,
            ),
        }

    @staticmethod
    def _infer_flip_context(direction, setup, candidate_reason):
        direction = (direction or "").upper()
        setup = (setup or "").upper()
        reason = (candidate_reason or "").lower()

        if direction not in {"CE", "PE"}:
            return None

        buy_side = direction
        failed_side = "PE" if direction == "CE" else "CE"
        reclaim_words = ("reclaim", "rejection", "reversal", "trap")
        setup_supports_flip = setup in {"REVERSAL", "BREAKOUT_CONFIRM", "TRAP_REVERSAL"}

        if setup_supports_flip or any(word in reason for word in reclaim_words):
            return {
                "buy_side": buy_side,
                "failed_side": failed_side,
                "decision_label": f"WATCH_{buy_side}_FLIP",
            }
        return None

    def _maybe_send_watch_alert(self, watch_payload):
        if not watch_payload or not Config.ENABLE_ALERTS:
            return

        if not self._watch_alert_is_eligible(watch_payload):
            return

        now_ts = time_module.time()
        post_signal_suppress_seconds = int(getattr(Config, "POST_SIGNAL_WATCH_SUPPRESS_SECONDS", 0) or 0)
        if (
            post_signal_suppress_seconds > 0
            and getattr(self, "last_signal_time", 0)
            and now_ts - self.last_signal_time < post_signal_suppress_seconds
        ):
            return

        if self._aggressive_watch_is_eligible(watch_payload):
            watch_payload = dict(watch_payload)
            watch_payload["aggressive_watch"] = True
            watch_payload["setup_bucket"] = "AGGRESSIVE_WATCH"
            direction = (watch_payload.get("direction") or "").upper()
            watch_payload["decision_label"] = f"AGGRESSIVE_WATCH_{direction}"
            watch_payload["action_text"] = (
                "Fast setup hai, safe ACTION nahi. Entry sirf 1m follow-through aur tight risk ke saath lena."
            )
            watch_payload["risk_note"] = (
                "Aggressive watch: half size, fast SL, aur 6-8 minute me follow-through nahi aaye to skip/exit."
            )

        state = self.watch_alert_state.get(watch_payload["key"])
        if state and now_ts - state["time"] < 30 * 60:
            score_improved = (watch_payload.get("score") or 0) >= state.get("score", 0) + 5
            entry_score_improved = (watch_payload.get("entry_score") or 0) >= state.get("entry_score", 0) + 4
            blockers_reduced = len(watch_payload.get("blockers") or []) < state.get("blocker_count", 99)
            if not any([score_improved, entry_score_improved, blockers_reduced]):
                return

        instrument_key = watch_payload.get("instrument") or self.instrument
        direction = (watch_payload.get("direction") or "").upper()
        if instrument_key and direction:
            for key, old_state in self.watch_alert_state.items():
                if (
                    isinstance(key, tuple)
                    and len(key) >= 2
                    and key[0] == instrument_key
                    and key[1] in {"CE", "PE"}
                    and key[1] != direction
                    and now_ts - old_state.get("time", 0) < 15 * 60
                ):
                    score_improved = (watch_payload.get("score") or 0) >= old_state.get("score", 0) + 10
                    entry_score_improved = (watch_payload.get("entry_score") or 0) >= old_state.get("entry_score", 0) + 8
                    if not (score_improved or entry_score_improved):
                        return

        self.notifier.send_watch_notification(watch_payload)
        self.last_watch_alert_key = watch_payload["key"]
        self.last_watch_alert_time = now_ts
        self.watch_alert_state[watch_payload["key"]] = {
            "time": now_ts,
            "score": watch_payload.get("score") or 0,
            "entry_score": watch_payload.get("entry_score") or 0,
            "blocker_count": len(watch_payload.get("blockers") or []),
        }

    @staticmethod
    def _aggressive_watch_is_eligible(watch_payload):
        score = float(watch_payload.get("score") or 0)
        entry_score = float(watch_payload.get("entry_score") or 0)
        confidence = (watch_payload.get("confidence") or "LOW").upper()
        signal_grade = (watch_payload.get("signal_grade") or "SKIP").upper()
        watch_bucket = (watch_payload.get("watch_bucket") or "").upper()
        setup = (watch_payload.get("setup") or "").upper()
        blockers = set(watch_payload.get("blockers") or [])
        cautions = set(watch_payload.get("cautions") or [])
        pressure_conflict_level = (watch_payload.get("pressure_conflict_level") or "NONE").upper()
        reason = (watch_payload.get("reason") or "").lower()

        premium_ok = bool(watch_payload.get("premium_confirmed")) or "premium_guard=premium_ok" in reason
        micro_ok = (
            "micro yes" in reason
            or "microstructure=" in reason
            or "option sweep microstructure override" in reason
        )
        hard_blockers = {
            "time_filter",
            "pre_expiry_requires_medium_plus_confidence",
            "sensex_late_day_requires_elite_score",
        }

        return (
            watch_bucket == "WATCH_CONFIRMATION_PENDING"
            and setup in {"BREAKOUT_CONFIRM", "BREAKOUT", "RETEST", "CONTINUATION", "REVERSAL", "TRAP_REVERSAL"}
            and signal_grade in {"A", "A+", "B"}
            and confidence in {"MEDIUM", "HIGH"}
            and score >= 90
            and entry_score >= 88
            and premium_ok
            and micro_ok
            and pressure_conflict_level in {"NONE", "MILD", ""}
            and not hard_blockers.intersection(blockers)
            and "participation_baseline_weak" not in cautions
            and "participation_weak" not in cautions
        )

    def _watch_alert_is_eligible(self, watch_payload):
        score = float(watch_payload.get("score") or 0)
        entry_score = float(watch_payload.get("entry_score") or 0)
        confidence = (watch_payload.get("confidence") or "LOW").upper()
        signal_grade = (watch_payload.get("signal_grade") or "SKIP").upper()
        watch_bucket = (watch_payload.get("watch_bucket") or "WATCH_CONTEXT").upper()
        setup = (watch_payload.get("setup") or "NONE").upper()
        blockers = set(watch_payload.get("blockers") or [])
        cautions = set(watch_payload.get("cautions") or [])
        spike_context = watch_payload.get("spike_context") or {}
        spike_quality = (spike_context.get("quality") or "").upper()
        pressure_conflict_level = (watch_payload.get("pressure_conflict_level") or "NONE").upper()

        if confidence not in {"MEDIUM", "HIGH"}:
            return False
        if score < 74 or entry_score < 68:
            return False
        if "time_filter" in blockers:
            return False
        if pressure_conflict_level not in {"NONE", "MILD", ""}:
            return False

        if spike_context:
            if spike_quality != "STRONG":
                return False
            if spike_context.get("price_breadth", 0) < 7 or spike_context.get("volume_breadth", 0) < 7:
                return False
            if signal_grade not in {"A", "A+"}:
                return False
            return watch_bucket == "WATCH_CONFIRMATION_PENDING"

        if watch_bucket == "WATCH_CONFIRMATION_PENDING":
            if self._aggressive_watch_is_eligible(watch_payload):
                return True
            if "participation_baseline_weak" in cautions:
                return False
            return (
                signal_grade in {"A", "A+"}
                and score >= 78
                and entry_score >= 72
            )
        if watch_bucket == "PA_STRONG_WAIT_PREMIUM":
            return (
                signal_grade in {"A", "A+", "B"}
                and score >= 78
                and entry_score >= 74
                and "participation_delta_missing" not in cautions
            )
        if watch_bucket == "WAIT_PULLBACK":
            return (
                signal_grade in {"A", "A+", "B"}
                and score >= 76
                and entry_score >= 68
                and "participation_weak" not in cautions
            )
        if watch_bucket == "WATCH_SETUP":
            return (
                setup in {"BREAKOUT_CONFIRM", "RETEST", "REVERSAL", "TRAP_REVERSAL"}
                and signal_grade in {"A", "A+"}
                and score >= 82
                and entry_score >= 76
                and "participation_weak" not in cautions
                and "participation_baseline_weak" not in cautions
            )
        return False

    def _build_option_spike_watch_payload(self, recent_1m_candles):
        if not recent_1m_candles:
            return None

        latest_1m = recent_1m_candles[-1]
        recent_5m = self.db_reader.fetch_recent_candles_5m(self.instrument, limit=9)
        oi_ladder_data = self._build_oi_ladder_context(float(latest_1m.get("close") or 0.0))
        pressure_metrics = None
        if getattr(self, "pressure_analyzer", None) and getattr(self, "option_data", None):
            pressure_metrics = self.pressure_analyzer.analyze(
                self.option_data,
                underlying_price=float(latest_1m.get("close") or 0.0),
                oi_ladder_data=oi_ladder_data,
            )
        snapshot_groups = self.db_reader.fetch_recent_option_band_snapshots(
            self.instrument,
            before_ts=latest_1m["time"],
            limit=3,
        )
        spike = self.option_spike_detector.detect(
            recent_1m_candles,
            snapshot_groups,
            recent_candles_5m=recent_5m,
            oi_ladder_data=oi_ladder_data,
        )
        if not spike:
            spike = self._detect_option_momentum_reentry(
                recent_1m_candles=recent_1m_candles,
                snapshot_groups=snapshot_groups,
                recent_5m=recent_5m,
                oi_ladder_data=oi_ladder_data,
            )
        if not spike:
            return None

        direction = spike["direction"]
        trigger_price = spike.get("trigger_price")
        invalidate_price = spike.get("invalidate_price")
        structure = spike.get("structure") or {}
        score = 86 if spike.get("quality") == "STRONG" else 78
        entry_score = 80 if spike.get("quality") == "STRONG" else 72
        action_text = (
            f"1m spike broad {direction} side par dikh raha hai. "
            "15m structure ko respect karo, 5m watch ready hai, aur next 1m follow-through aaye to fast entry consider karo."
        )
        return {
            "instrument": self.instrument,
            "direction": direction,
            "setup": "BREAKOUT_CONFIRM",
            "signal_grade": "A" if spike.get("quality") == "STRONG" else "B",
            "confidence": "HIGH" if spike.get("quality") == "STRONG" else "MEDIUM",
            "confidence_summary": "Broad 1m option spike detected",
            "score": score,
            "entry_score": entry_score,
            "setup_bucket": "SPIKE_EARLY",
            "price": round(float(latest_1m.get("close") or 0.0), 2),
            "trigger_price": round(float(trigger_price), 2) if trigger_price is not None else None,
            "invalidate_price": round(float(invalidate_price), 2) if invalidate_price is not None else None,
            "first_target_price": (
                round(float(trigger_price) + max(float(latest_1m.get("high") or 0.0) - float(latest_1m.get("low") or 0.0), 1.0) * 2, 2)
                if direction == "CE" and trigger_price is not None
                else round(float(trigger_price) - max(float(latest_1m.get("high") or 0.0) - float(latest_1m.get("low") or 0.0), 1.0) * 2, 2)
                if direction == "PE" and trigger_price is not None
                else None
            ),
            "option_stop_loss_pct": 23,
            "option_target_pct": 38,
            "option_trail_pct": 16,
            "time_stop_warn_minutes": 8,
            "time_stop_exit_minutes": 14,
            "risk_note": "1m spike watch hai; follow-through ke bina chase mat karo.",
            "no_trade_zone": None,
            "structure_suggestion": None,
            "blockers": [],
            "cautions": ["one_minute_spike_watch"],
            "watch_bucket": "WATCH_CONFIRMATION_PENDING",
            "decision_label": f"WATCH_{direction}_SPIKE",
            "journal_note": self._build_journal_note(
                f"WATCH_{direction}_SPIKE",
                action_text,
                spike.get("summary"),
            ),
            "context": f"SpikeWatch | {structure.get('summary') or 'structure'} | {spike.get('summary')}",
            "reason": f"1m option spike watch | {spike.get('summary')}",
            "action_hint": action_text,
            "action_text": action_text,
            "entry_if": (
                f"Next 1m candle {'above' if direction == 'CE' else 'below'} {round(float(trigger_price), 2)} with follow-through"
                if trigger_price is not None else None
            ),
            "avoid_if": (
                f"Skip if next 1m candle reverses {'below' if direction == 'CE' else 'above'} {round(float(invalidate_price), 2)}"
                if invalidate_price is not None else None
            ),
            "participation_read": ", ".join(spike.get("examples") or []),
            "pressure_read": f"{structure.get('summary') or ''} | {spike.get('summary')}".strip(" |"),
            "flip_context": None,
            "key": (self.instrument, direction, "OPTION_SPIKE_1M"),
            "spike_context": {
                **spike,
                "oi_ladder_data": oi_ladder_data,
                "pressure_metrics": pressure_metrics,
            },
        }

    def _detect_option_momentum_reentry(self, recent_1m_candles, snapshot_groups, recent_5m=None, oi_ladder_data=None):
        if not bool(getattr(Config, "ENABLE_OPTION_MOMENTUM_REENTRY", True)):
            return None
        if len(recent_1m_candles or []) < 4 or len(snapshot_groups or []) < 3:
            return None

        latest = recent_1m_candles[-1]
        previous = recent_1m_candles[-2]
        prior_window = recent_1m_candles[-5:-1]
        latest_close = float(latest.get("close") or 0.0)
        previous_close = float(previous.get("close") or latest_close)
        latest_high = float(latest.get("high") or latest_close)
        latest_low = float(latest.get("low") or latest_close)
        body = abs(latest_close - float(latest.get("open") or latest_close))
        candle_range = max(latest_high - latest_low, 0.01)
        body_ratio = body / candle_range
        break_points = float(getattr(Config, "OPTION_REENTRY_MIN_UNDERLYING_BREAK", 4.0) or 4.0)
        prior_high = max(float(c.get("high") or latest_high) for c in prior_window)
        prior_low = min(float(c.get("low") or latest_low) for c in prior_window)

        direction = None
        if latest_close <= prior_low - break_points and latest_close < previous_close and body_ratio >= 0.35:
            direction = "PE"
        elif latest_close >= prior_high + break_points and latest_close > previous_close and body_ratio >= 0.35:
            direction = "CE"
        if direction is None:
            return None
        trend_context = self._trend_day_regime_context(recent_1m_candles, recent_5m, direction)

        latest_rows = snapshot_groups[-1]
        previous_rows = snapshot_groups[-2]
        older_rows = snapshot_groups[0]
        latest_map = {(row.get("strike"), row.get("option_type")): row for row in latest_rows}
        previous_map = {(row.get("strike"), row.get("option_type")): row for row in previous_rows}
        older_map = {(row.get("strike"), row.get("option_type")): row for row in older_rows}

        min_1m_pct = float(getattr(Config, "OPTION_REENTRY_MIN_1M_PREMIUM_PCT", 7.0) or 7.0)
        min_3m_pct = float(getattr(Config, "OPTION_REENTRY_MIN_3M_PREMIUM_PCT", 12.0) or 12.0)
        min_premium = float(
            getattr(
                Config,
                "OPTION_REENTRY_MIN_PREMIUM",
                getattr(Config, "PRO_TRADER_MIN_PREMIUM", 25.0),
            )
            or 50.0
        )
        max_spread = float(getattr(Config, "PRO_TRADER_MAX_ACTION_SPREAD_PCT", 3.8) or 3.8)
        max_otm_distance = int(getattr(Config, "PRO_TRADER_MAX_OTM_DISTANCE", 2) or 2)
        max_itm_distance = int(getattr(Config, "PRO_TRADER_MAX_ITM_DISTANCE", 5) or 5)
        leader = None

        for row in latest_rows:
            if (row.get("option_type") or "").upper() != direction:
                continue
            distance = row.get("distance_from_atm")
            distance = int(distance) if distance not in {None, ""} else 0
            otm_distance = max(distance, 0) if direction == "CE" else max(-distance, 0)
            itm_distance = max(-distance, 0) if direction == "CE" else max(distance, 0)
            if otm_distance > max_otm_distance or itm_distance > max_itm_distance:
                continue
            key = (row.get("strike"), row.get("option_type"))
            prev = previous_map.get(key)
            older = older_map.get(key)
            if not prev or not older:
                continue
            ltp = float(row.get("ltp") or 0.0)
            prev_ltp = float(prev.get("ltp") or 0.0)
            older_ltp = float(older.get("ltp") or 0.0)
            if ltp < min_premium or prev_ltp <= 0 or older_ltp <= 0:
                continue
            spread_pct = self._spread_percent(row)
            if spread_pct is not None and float(spread_pct) > max_spread:
                continue
            premium_1m_pct = ((ltp - prev_ltp) / prev_ltp) * 100.0
            premium_3m_pct = ((ltp - older_ltp) / older_ltp) * 100.0
            volume_delta = int(row.get("volume") or 0) - int(prev.get("volume") or 0)
            if volume_delta <= 0:
                continue
            if premium_1m_pct < min_1m_pct and premium_3m_pct < min_3m_pct:
                continue
            distance_penalty = (otm_distance * 5.0) + max(itm_distance - 2, 0) * 1.5
            premium_quality = min(ltp / 10.0, 14.0)
            score = premium_1m_pct + (premium_3m_pct * 0.6) + min(volume_delta / 100000.0, 12.0) + premium_quality - distance_penalty
            if trend_context.get("active"):
                score += 8.0
            if leader is None or score > leader["score"]:
                leader = {
                    "row": row,
                    "premium_1m_pct": premium_1m_pct,
                    "premium_3m_pct": premium_3m_pct,
                    "volume_delta": volume_delta,
                    "spread_pct": spread_pct,
                    "score": score,
                }

        if not leader:
            return None

        recent_5m = recent_5m or []
        if len(recent_5m) >= 3:
            last_5m = recent_5m[-1]
            prev_5m = recent_5m[-2]
            if direction == "PE" and not (
                float(last_5m.get("close") or 0.0) <= float(prev_5m.get("close") or 0.0)
                or float(last_5m.get("low") or 0.0) <= float(prev_5m.get("low") or 0.0)
            ):
                return None
            if direction == "CE" and not (
                float(last_5m.get("close") or 0.0) >= float(prev_5m.get("close") or 0.0)
                or float(last_5m.get("high") or 0.0) >= float(prev_5m.get("high") or 0.0)
            ):
                return None

        row = leader["row"]
        strike = row.get("strike")
        trigger_price = latest_low if direction == "PE" else latest_high
        invalidate_price = trend_context.get("invalidation") or (latest_high if direction == "PE" else latest_low)
        structure = self.option_spike_detector._derive_structure_context(
            direction=direction,
            recent_candles_5m=recent_5m,
            oi_ladder_data=oi_ladder_data,
        )
        if structure.get("alignment") == "AGAINST" and not trend_context.get("active"):
            return None

        return {
            "direction": direction,
            "quality": "STRONG",
            "stage": "OPTION_LEADER_REENTRY",
            "leader_momentum": True,
            "leader_strike": strike,
            "price_breadth": 1,
            "volume_breadth": 1,
            "opposite_collapse": 0,
            "same_volume_total": leader["volume_delta"],
            "underlying_volume_ratio": 1.0,
            "trend_day_context": trend_context,
            "trigger_price": trigger_price,
            "invalidate_price": invalidate_price,
            "entry_reference": latest_close,
            "summary": (
                f"{direction} leader re-entry | {strike}{direction} "
                f"1m +{leader['premium_1m_pct']:.1f}% | 3m +{leader['premium_3m_pct']:.1f}% "
                f"| vol +{leader['volume_delta']} | spread {float(leader['spread_pct'] or 0):.2f}%"
                f"{' | ' + trend_context.get('summary') if trend_context.get('active') else ''}"
            ),
            "examples": [
                f"{strike}{direction}:1m+{leader['premium_1m_pct']:.1f}%/3m+{leader['premium_3m_pct']:.1f}%"
            ],
            "structure": structure,
        }

    def _trend_day_regime_context(self, recent_1m_candles, recent_5m, direction):
        context = {"active": False, "direction": None}
        if direction not in {"CE", "PE"} or len(recent_5m or []) < 6:
            return context
        latest_1m = recent_1m_candles[-1]
        latest_close = float(latest_1m.get("close") or 0.0)
        vwap_value = None
        if getattr(self, "vwap", None):
            try:
                vwap_value = self.vwap.get_vwap()
            except Exception:
                vwap_value = None
        if vwap_value is None:
            typicals = [
                (float(c.get("high") or 0.0) + float(c.get("low") or 0.0) + float(c.get("close") or 0.0)) / 3.0
                for c in recent_5m
            ]
            vwap_value = sum(typicals) / max(len(typicals), 1)

        window = recent_5m[-7:]
        highs = [float(c.get("high") or 0.0) for c in window]
        lows = [float(c.get("low") or 0.0) for c in window]
        closes = [float(c.get("close") or 0.0) for c in window]
        lower_high_count = sum(1 for left, right in zip(highs, highs[1:]) if right <= left)
        lower_low_count = sum(1 for left, right in zip(lows, lows[1:]) if right <= left)
        higher_low_count = sum(1 for left, right in zip(lows, lows[1:]) if right >= left)
        higher_high_count = sum(1 for left, right in zip(highs, highs[1:]) if right >= left)
        min_vwap_distance = float(getattr(Config, "TREND_DAY_MIN_VWAP_DISTANCE", 18.0) or 18.0)
        min_lh = int(getattr(Config, "TREND_DAY_MIN_LOWER_HIGH_COUNT", 3) or 3)
        min_ll = int(getattr(Config, "TREND_DAY_MIN_LOWER_LOW_COUNT", 2) or 2)

        if direction == "PE":
            below_vwap = latest_close <= float(vwap_value) - min_vwap_distance
            failed_pullback = max(highs[-4:-1]) < float(vwap_value) and closes[-1] <= min(lows[-4:-1])
            if below_vwap and lower_high_count >= min_lh and lower_low_count >= min_ll and failed_pullback:
                return {
                    "active": True,
                    "direction": "PE",
                    "vwap": round(float(vwap_value), 2),
                    "breakdown_level": round(min(lows[-4:-1]), 2),
                    "invalidation": round(max(highs[-3:]), 2),
                    "summary": f"bear trend-day | below VWAP {round(float(vwap_value), 2)} | failed pullback",
                }
        else:
            above_vwap = latest_close >= float(vwap_value) + min_vwap_distance
            failed_pullback = min(lows[-4:-1]) > float(vwap_value) and closes[-1] >= max(highs[-4:-1])
            if above_vwap and higher_high_count >= min_lh and higher_low_count >= min_ll and failed_pullback:
                return {
                    "active": True,
                    "direction": "CE",
                    "vwap": round(float(vwap_value), 2),
                    "breakout_level": round(max(highs[-4:-1]), 2),
                    "invalidation": round(min(lows[-3:]), 2),
                    "summary": f"bull trend-day | above VWAP {round(float(vwap_value), 2)} | failed pullback",
                }
        return context

    def _fast_spike_action_ready(self, watch_payload):
        if not bool(getattr(Config, "ENABLE_FAST_SPIKE_ACTION", True)):
            return False
        spike = watch_payload.get("spike_context") or {}
        direction = (watch_payload.get("direction") or "").upper()
        structure = spike.get("structure") or {}
        if spike.get("leader_momentum"):
            trend_context = spike.get("trend_day_context") or {}
            min_context_score = float(getattr(Config, "FAST_SPIKE_ACTION_MIN_CONTEXT_SCORE", 84.0) or 84.0)
            min_entry_score = float(getattr(Config, "FAST_SPIKE_ACTION_MIN_ENTRY_SCORE", 78.0) or 78.0)
            return (
                direction in {"CE", "PE"}
                and (spike.get("quality") or "").upper() == "STRONG"
                and (structure.get("alignment") or "NEUTRAL").upper() != "AGAINST"
                and (bool(structure.get("five_min_ready")) or bool(trend_context.get("active")))
                and (watch_payload.get("signal_grade") or "").upper() in {"A", "A+"}
                and (watch_payload.get("confidence") or "").upper() == "HIGH"
                and float(watch_payload.get("score") or 0) >= min_context_score
                and float(watch_payload.get("entry_score") or 0) >= min_entry_score
            )
        oi_ladder_data = spike.get("oi_ladder_data") or {}
        pressure_metrics = spike.get("pressure_metrics") or {}
        min_breadth = int(getattr(Config, "FAST_SPIKE_ACTION_MIN_BREADTH", 8) or 8)
        min_flow_edge = float(getattr(Config, "FAST_SPIKE_ACTION_MIN_FLOW_EDGE", 10.0) or 10.0)
        expected_bias = "BULLISH" if direction == "CE" else "BEARISH"
        supportive_build_ups = (
            {"LONG_BUILDUP", "SHORT_COVERING"}
            if direction == "CE"
            else {"SHORT_BUILDUP", "LONG_UNWINDING"}
        )
        pressure_bias = (pressure_metrics.get("pressure_bias") or "").upper()
        flow_edge = float(pressure_metrics.get("smoothed_flow_edge") or pressure_metrics.get("flow_edge") or 0.0)
        oi_trend = (oi_ladder_data.get("trend") or "").upper()
        build_up = (oi_ladder_data.get("build_up") or "").upper()
        wall_alert = (oi_ladder_data.get("wall_break_alert") or "").upper()
        sweep_quality = (spike.get("quality") or "").upper()
        wall_ok = (
            wall_alert in {"RESISTANCE_BREAK_RISK", "NONE", ""}
            if direction == "CE"
            else wall_alert in {"SUPPORT_BREAK_RISK", "NONE", ""}
        )
        min_context_score = float(getattr(Config, "FAST_SPIKE_ACTION_MIN_CONTEXT_SCORE", 84.0) or 84.0)
        min_entry_score = float(getattr(Config, "FAST_SPIKE_ACTION_MIN_ENTRY_SCORE", 78.0) or 78.0)
        squeeze_ok = (
            build_up == "NO_CLEAR_SIGNAL"
            and sweep_quality == "STRONG"
            and pressure_bias == expected_bias
            and oi_trend == expected_bias
            and wall_alert in {"RESISTANCE_BREAK_RISK", "SUPPORT_BREAK_RISK"}
        )
        return (
            direction in {"CE", "PE"}
            and
            sweep_quality == "STRONG"
            and int(spike.get("price_breadth") or 0) >= min_breadth
            and int(spike.get("volume_breadth") or 0) >= min_breadth
            and (structure.get("alignment") or "").upper() == "SUPPORTIVE"
            and bool(structure.get("five_min_ready"))
            and pressure_bias == expected_bias
            and flow_edge >= min_flow_edge
            and oi_trend == expected_bias
            and (build_up in supportive_build_ups or squeeze_ok)
            and wall_ok
            and (watch_payload.get("signal_grade") or "").upper() in {"A", "A+"}
            and (watch_payload.get("confidence") or "").upper() == "HIGH"
            and float(watch_payload.get("score") or 0) >= min_context_score
            and float(watch_payload.get("entry_score") or 0) >= min_entry_score
        )

    def _fire_fast_spike_action(self, watch_payload, latest_1m, latest_5m):
        signal = watch_payload.get("direction")
        if signal not in {"CE", "PE"}:
            return False

        price = float(latest_1m.get("close") or watch_payload.get("price") or 0.0)
        spike_context = watch_payload.get("spike_context") or {}
        leader_strike = spike_context.get("leader_strike")
        if leader_strike is not None:
            strike = int(leader_strike)
            strike_reason = "option_leader_reentry"
        else:
            strike, strike_reason = self.strike_selector.select_strike_with_reason(
                price=price,
                signal=signal,
                volume_signal="SPIKE",
                strategy_score=watch_payload.get("score"),
                pressure_metrics=None,
                candle_time=latest_1m["time"],
            )
        option_contract = self._get_option_contract_snapshot(strike, signal, before_ts=latest_1m["time"])
        option_contract = self._greek_enriched_option_contract(
            option_contract,
            signal,
            price,
            before_ts=latest_1m["time"],
        )
        premium_guard = self._evaluate_premium_quality_guard(
            signal=signal,
            selected_option_contract=option_contract,
            candle_time=latest_1m["time"],
        )
        if premium_guard and premium_guard.get("label") not in {"PREMIUM_OK"}:
            self._safe_save_entry_decision_1m(
                ts=latest_1m["time"],
                pending=self.pending_entry_watch or watch_payload,
                decision="INVALIDATED",
                latest_1m=latest_1m,
                option_contract=option_contract,
                strike=strike,
                reason=f"Fast spike action blocked: {premium_guard.get('label')} ({premium_guard.get('reason')})",
            )
            return False

        option_price = (option_contract or {}).get("ltp")
        if option_price is None:
            return False

        stop_loss_option_price = round(float(option_price) * (1 - float(watch_payload.get("option_stop_loss_pct") or 18) / 100.0), 2)
        first_target_option_price = round(float(option_price) * (1 + float(watch_payload.get("option_target_pct") or 28) / 100.0), 2)
        balanced_pro = {
            "quality": "A",
            "setup": watch_payload.get("setup") or "BREAKOUT_CONFIRM",
            "tradability": "ACTION",
            "time_regime": "FAST_1M_SPIKE",
            "trend_day_context": spike_context.get("trend_day_context"),
        }
        self.strategy.last_entry_plan = {
            "entry_above": watch_payload.get("trigger_price") if signal == "CE" else None,
            "entry_below": watch_payload.get("trigger_price") if signal == "PE" else None,
            "invalidate_price": watch_payload.get("invalidate_price"),
            "first_target_price": watch_payload.get("first_target_price"),
        }
        entry_notification = {
            "instrument": self.instrument,
            "signal": signal,
            "strike": strike,
            "confidence": watch_payload.get("confidence"),
            "confidence_summary": watch_payload.get("confidence_summary"),
            "signal_type": watch_payload.get("setup"),
            "signal_grade": watch_payload.get("signal_grade"),
            "price": round(float(option_price), 2),
            "underlying_price": round(price, 2),
            "trigger_price": watch_payload.get("trigger_price"),
            "invalidate_price": watch_payload.get("invalidate_price"),
            "first_target_price": watch_payload.get("first_target_price"),
            "stop_loss_option_price": stop_loss_option_price,
            "first_target_option_price": first_target_option_price,
            "option_stop_loss_pct": watch_payload.get("option_stop_loss_pct"),
            "option_target_pct": watch_payload.get("option_target_pct"),
            "entry_bid": option_contract.get("top_bid_price") if option_contract else None,
            "entry_ask": option_contract.get("top_ask_price") if option_contract else None,
            "entry_spread": option_contract.get("spread") if option_contract else None,
            "execution_model": "15m context | 5m setup | 1m fast action",
            "trade_type": self._classify_action_trade_type(
                setup_type=watch_payload.get("setup") or "BREAKOUT_CONFIRM",
                high_expectancy_profile={"quality_tag": "TQ_CLEAN", "signal_family": "IMPULSE_BREAKOUT"},
                premium_guard=premium_guard,
            ),
            "exit_if": self._action_exit_if_line(
                signal,
                trigger_price=watch_payload.get("trigger_price"),
                invalidate_price=watch_payload.get("invalidate_price"),
                premium_sl=stop_loss_option_price,
            ),
            "pressure_read": watch_payload.get("pressure_read"),
            "context": watch_payload.get("context"),
            "risk_note": "FAST 1m action: size chhota rakho, premium chase mat karo, aur reversal pe exit fast.",
            "greek_summary": format_greek_summary(option_contract),
            "decision_label": f"CONFIRMED_{signal}_ENTRY",
            "reason": f"FAST_1M_SPIKE_ACTION | {watch_payload.get('reason')}",
        }
        telegram_sent = self._send_action_notification(
            self.notifier.send_entry_trigger_notification,
            entry_notification,
            label=f"{self.instrument} fast 1m {signal} entry",
        )
        self._safe_save_entry_decision_1m(
            ts=latest_1m["time"],
            pending=self.pending_entry_watch or watch_payload,
            decision="TRIGGERED",
            latest_1m=latest_1m,
            evaluation={
                "time": latest_1m["time"],
                "price": price,
                "status": "TRIGGERED",
                "reason": "FAST_1M_SPIKE_ACTION",
                "micro_reason": watch_payload.get("reason"),
            },
            option_contract=option_contract,
            strike=strike,
            reason=f"FAST_1M_SPIKE_ACTION | {watch_payload.get('reason')}",
        )
        self._safe_save_signal_issued(
            ts=latest_1m["time"],
            signal=signal,
            price=option_price,
            strike=strike,
            reason=f"FAST_1M_SPIKE_ACTION | {watch_payload.get('reason')}",
            balanced_pro=balanced_pro,
            oi_mode="FAST_1M_SPIKE",
            telegram_sent=telegram_sent,
            monitor_started=True,
            entry_window_end=latest_1m["time"] + timedelta(minutes=Config.SIGNAL_VALIDITY_MINUTES),
            underlying_price=price,
            option_contract=option_contract,
            strike_reason=strike_reason,
            option_data_source=self.option_data_source,
        )
        self._clear_pending_entry_watch()
        self._clear_pending_spike_watch()
        self._start_trade_monitor(signal, latest_5m, price, balanced_pro, strike, entry_time=latest_1m["time"])
        self.signals_generated += 1
        return True

    def _maybe_prepare_option_spike_watch(self):
        recent_1m_candles = self.db_reader.fetch_recent_candles_1m(self.instrument, limit=6)
        if len(recent_1m_candles) < 3:
            return
        recent_5m = self.db_reader.fetch_recent_candles_5m(self.instrument, limit=9)

        latest_1m = recent_1m_candles[-1]
        minute_key = latest_1m["time"]
        pending = self.pending_spike_watch
        if pending and pending.get("minute") == minute_key:
            return

        watch_payload = self._build_option_spike_watch_payload(recent_1m_candles)
        if not watch_payload:
            return

        self.pending_spike_watch = {
            "minute": minute_key,
            "payload": watch_payload,
        }
        self.pending_entry_watch = {
            "instrument": self.instrument,
            "direction": watch_payload["direction"],
            "trigger_price": float(watch_payload["trigger_price"]) if watch_payload.get("trigger_price") is not None else None,
            "invalidate_price": watch_payload.get("invalidate_price"),
            "first_target_price": watch_payload.get("first_target_price"),
            "option_stop_loss_pct": watch_payload.get("option_stop_loss_pct"),
            "option_target_pct": watch_payload.get("option_target_pct"),
            "option_trail_pct": watch_payload.get("option_trail_pct"),
            "risk_note": watch_payload.get("risk_note"),
            "context": watch_payload.get("context"),
            "score": watch_payload["score"],
            "entry_score": watch_payload["entry_score"],
            "confidence": watch_payload["confidence"],
            "signal_type": watch_payload["setup"],
            "signal_grade": watch_payload["signal_grade"],
            "watch_bucket": watch_payload["watch_bucket"],
            "quality": "A",
            "time_regime": "INTRAMINUTE_SPIKE",
            "created_at": latest_1m["time"],
            "last_checked_minute": None,
            "reason": watch_payload["reason"],
            "fast_track_ready": True,
            "strong_watch_setup": True,
            "starter_entry_ready": (
                bool(getattr(Config, "ENABLE_STARTER_ENTRY", True))
                and (watch_payload.get("signal_grade") or "").upper() in {"A", "A+"}
                and (watch_payload.get("confidence") or "").upper() in {"MEDIUM", "HIGH"}
                and float(watch_payload.get("score") or 0) >= float(getattr(Config, "STARTER_ENTRY_MIN_CONTEXT_SCORE", 84.0) or 84.0)
                and float(watch_payload.get("entry_score") or 0) >= float(getattr(Config, "STARTER_ENTRY_MIN_ENTRY_SCORE", 78.0) or 78.0)
            ),
            "hybrid_mode": True,
            "cautions": list(watch_payload.get("cautions") or []),
            "blockers": [],
            "pressure_conflict_level": "NONE",
            "spike_origin": True,
            "spike_context": watch_payload.get("spike_context"),
        }
        if self._fast_spike_action_ready(watch_payload):
            if self._fire_fast_spike_action(watch_payload, latest_1m, recent_5m[-1] if recent_5m else latest_1m):
                return
        self._maybe_send_watch_alert(watch_payload)

    def _process_5m_candle(self, candle_5m):
        """Process 5-minute candle and generate signal"""
        price = candle_5m["close"]
        
        # Update intraday ranges
        self._update_intraday_ranges(candle_5m)
        
        # Update indicators
        vwap_value = self.vwap.update(candle_5m)
        atr_value = self.atr.update(candle_5m)
        buffer = self.atr.get_buffer()
        
        # Update volume analysis. For index option buying, option-band CE/PE
        # participation is more useful than index/futures candle volume.
        self.volume.update(candle_5m)
        option_volume_signal = self._derive_option_volume_signal(self.option_data)
        volume_signal = option_volume_signal or self.volume.get_volume_signal(candle_5m["volume"])
        
        # Update OI analysis
        if self.option_data:
            self.oi.update(price, self.option_data.get("ce_oi", 0), self.option_data.get("pe_oi", 0))

        oi_signal = self.oi.get_oi_signal()
        oi_bias = self.oi.get_bias()
        oi_ladder_data = self._build_oi_ladder_context(price)
        pressure_metrics = (
            self.pressure.analyze(
                self.option_data,
                underlying_price=price,
                oi_ladder_data=oi_ladder_data,
            )
            if self.option_data
            else None
        )
        fallback_context = None
        if not self.option_data or not self.option_data.get("band_snapshots"):
            fallback_context = self._build_oi_snapshot_fallback_context(price, candle_5m["time"])
            oi_bias = fallback_context["oi_bias"]
            pressure_metrics = fallback_context["pressure_metrics"]
            if not oi_ladder_data:
                oi_ladder_data = {
                    "trend": fallback_context["oi_trend"],
                    "build_up": fallback_context["build_up"],
                    "support": fallback_context["support"],
                    "resistance": fallback_context["resistance"],
                    "ce_delta_total": fallback_context["ce_delta_total"],
                    "pe_delta_total": fallback_context["pe_delta_total"],
                }
        orb_high, orb_low = self._resolve_orb_levels(candle_5m)

        if self._is_debug_enabled():
            print(f"[Signal Service] Analysis for {candle_5m['time']}:")
            print("Price:", price)
            print("VWAP:", vwap_value)
            print("ATR:", atr_value)
            print("Volume Signal:", volume_signal)
            print("OI Signal:", oi_signal)
            print("OI Bias:", oi_bias)
            print("ORB High:", orb_high)
            print("ORB Low:", orb_low)

        support = oi_ladder_data["support"] if oi_ladder_data else None
        resistance = oi_ladder_data["resistance"] if oi_ladder_data else None
        base_bias = self._classify_base_bias(price, vwap_value, oi_bias, oi_ladder_data)
        participation_metrics = self._build_option_participation_metrics(candle_time=candle_5m["time"])

        # Get ATM options volume for advanced confirmation
        atm_ce_volume = self.option_data.get("ce_volume") if self.option_data else None
        atm_pe_volume = self.option_data.get("pe_volume") if self.option_data else None
        recent_candles_5m = self.db_reader.fetch_recent_candles_5m(self.instrument, limit=24)
        if not recent_candles_5m or recent_candles_5m[-1]["time"] != candle_5m["time"]:
            recent_candles_5m = (recent_candles_5m or []) + [candle_5m]
        option_sweep_context = self._build_option_sweep_context(
            candle_time=candle_5m["time"],
            price=price,
            atr=atr_value,
            recent_candles_5m=recent_candles_5m,
        )
        self.option_sweep_context = option_sweep_context
        trend_15m = self._derive_15m_trend_from_5m(recent_candles_5m)
        feed_health = self._assess_raw_feed_health(candle_5m["time"])
        can_trade_live = True if Config.TEST_MODE else self.time_utils.can_trade()
        # Generate signal
        signal, reason = self.strategy.generate_signal(
            price=price,
            orb_high=orb_high,
            orb_low=orb_low,
            vwap=vwap_value,
            atr=atr_value,
            volume_signal=volume_signal,
            oi_bias=oi_bias,
            oi_trend=oi_ladder_data["trend"] if oi_ladder_data else None,
            build_up=oi_ladder_data["build_up"] if oi_ladder_data else None,
            support=support,
            resistance=resistance,
            buffer=buffer,
            pressure_metrics=pressure_metrics,
            candle_high=candle_5m["high"],
            candle_low=candle_5m["low"],
            candle_close=candle_5m["close"],
            candle_open=candle_5m["open"],
            candle_tick_count=candle_5m.get("tick_count"),
            candle_time=candle_5m["time"],
            candle_volume=candle_5m["volume"],
            expiry=self.option_data.get("expiry") if self.option_data else None,
            atm_ce_volume=atm_ce_volume,
            atm_pe_volume=atm_pe_volume,
            recent_candles_5m=recent_candles_5m,
            trend_15m=trend_15m,
            participation_metrics=participation_metrics,
            oi_ladder_data=oi_ladder_data,
            option_sweep_context=option_sweep_context,
            can_trade=can_trade_live,
        )
        feed_health = OptionSignalGuard.maybe_relax_reject_for_strong_setup(
            self,
            feed_health,
            signal=signal,
        )
        if feed_health["label"] == "RISKY":
            self.strategy.last_cautions = list(dict.fromkeys(list(self.strategy.last_cautions or []) + ["feed_quality_risky"]))
        elif feed_health["label"] == "REJECT":
            self.strategy.last_blockers = list(dict.fromkeys(list(self.strategy.last_blockers or []) + ["feed_quality_reject"]))

        if signal and feed_health["label"] == "REJECT":
            signal = None
            reason = f"Raw data health rejected ({feed_health['summary']})"

        institutional_confluence_reason = None
        if signal:
            institutional_ok, institutional_reason = self._institutional_confluence_gate(
                signal=signal,
                trend_15m=trend_15m,
                pressure_metrics=pressure_metrics,
                oi_ladder_data=oi_ladder_data,
                participation_metrics=participation_metrics,
                option_sweep_context=option_sweep_context,
            )
            if institutional_ok:
                institutional_confluence_reason = institutional_reason
                reason = f"{reason} | {institutional_reason}"
            else:
                signal = None
                reason = f"Institutional confluence gate blocked ({institutional_reason})"
                self.strategy.last_blockers = list(
                    dict.fromkeys(list(self.strategy.last_blockers or []) + ["institutional_confluence_reject"])
                )
        
        candidate_signal = signal
        candidate_reason = reason
        self.current_expectancy_profile = None
        self.pending_pullback_watch_plan = None
        self._current_candle_time = candle_5m["time"]
        pre_ml_balanced_pro = self._build_balanced_pro_summary(
            base_bias,
            candidate_signal,
            fallback_context,
            pressure_metrics,
            actionable_signal=bool(signal),
        )
        
        # ML Feature Extraction and Filtering (FREE - scikit-learn)
        ml_features = None
        ml_prob = None
        if signal and ML_ENABLED and self.ml_feature_extractor:
            try:
                # Extract features for ML
                ml_features = self.ml_feature_extractor.extract_features(
                    instrument=self.instrument,
                    signal_direction=signal,
                    price=price,
                    vwap=vwap_value,
                    atr=atr_value,
                    score=self.strategy.last_score,
                    confidence=self.strategy.last_confidence,
                    time_regime=pre_ml_balanced_pro.get('time_regime', 'UNKNOWN'),
                    oi_ladder_data=oi_ladder_data,
                    pressure_metrics=pressure_metrics,
                    trend_15m=trend_15m,
                    recent_candles_5m=recent_candles_5m,
                    strategy_context={
                        'signal_type': self.strategy.last_signal_type,
                        'signal_grade': self.strategy.last_signal_grade,
                        'entry_score': self.strategy.last_entry_score,
                        'context_score': self.strategy.last_context_score,
                        'hybrid_mode': pre_ml_balanced_pro.get('setup', '').startswith('HYBRID')
                    },
                    entry_plan=self.strategy.last_entry_plan,
                    participation_metrics=participation_metrics,
                    market_regime=self.strategy.last_regime,
                )
                
                # Apply ML filter
                should_take, ml_reason, ml_prob = self.ml_filter.should_take_signal(ml_features)
                ml_explanation = self.ml_filter.explain_decision(ml_features, probability=ml_prob)
                tags = ",".join(ml_explanation.get("profile_tags") or [])
                positives = ",".join(ml_explanation.get("positives") or [])
                negatives = ",".join(ml_explanation.get("negatives") or [])
                if tags:
                    ml_features["decision_profile_tags"] = tags
                if positives:
                    ml_features["decision_positives"] = positives
                if negatives:
                    ml_features["decision_negatives"] = negatives
                
                if not should_take:
                    if getattr(self.ml_filter, "model_loaded", False):
                        signal = None
                        reason = f"ML Filtered ({ml_reason})"
                        print(f"[Signal Service] {self.instrument}: {reason}")
                    else:
                        print(
                            f"[Signal Service] {self.instrument}: "
                            f"Rule advisory only ({ml_reason})"
                        )
                else:
                    approval_label = (
                        "ML Approved"
                        if getattr(self.ml_filter, "model_loaded", False)
                        else "Rule advisory passed"
                    )
                    print(f"[Signal Service] {self.instrument}: {approval_label} ({ml_reason})")
                    
            except Exception as e:
                print(f"[Signal Service] ML processing error: {e}")
                ml_features = None
        
        signal, reason = self._apply_signal_cooldowns(signal, reason)
        if signal and self.active_trade_monitor:
            active_signal = self.active_trade_monitor.get("signal")
            active_entry_time = self.active_trade_monitor.get("entry_time")
            signal = None
            reason = f"Active trade monitor already running ({active_signal} from {active_entry_time}); skipping duplicate late 5m action"
        balanced_pro = self._build_balanced_pro_summary(
            base_bias,
            candidate_signal,
            fallback_context,
            pressure_metrics,
            actionable_signal=bool(signal),
        )
        balanced_pro["pressure_summary"] = self._build_pressure_summary(
            pressure_metrics=pressure_metrics,
            participation_metrics=participation_metrics,
            direction=(signal if signal in {"CE", "PE"} else candidate_signal if candidate_signal in {"CE", "PE"} else None),
        )

        option_candidates = []
        preferred_strikes = {}
        if self.option_data:
            for direction in ("CE", "PE"):
                preferred_strikes[direction], _ = self.strike_selector.select_strike_with_reason(
                    price=price,
                    signal=direction,
                    volume_signal=volume_signal,
                    strategy_score=self.strategy.last_score,
                    pressure_metrics=pressure_metrics,
                    cautions=getattr(self.strategy, "last_cautions", None),
                    option_chain_data=self.option_data,
                    setup_type=(balanced_pro or {}).get("setup"),
                    time_regime=(balanced_pro or {}).get("time_regime"),
                    candle_time=candle_5m["time"],
                )
            option_candidates = self._build_option_candidates(
                underlying_price=price,
                preferred_strikes=preferred_strikes,
                balanced_pro=balanced_pro,
            )

        # Strike selection
        selected_strike = None
        strike_reason = None
        selected_option_contract = None
        no_trade_zone = None
        signal_risk_profile = None
        wall_guard = None
        premium_guard = None
        if signal and self.option_data:
            selected_strike = preferred_strikes.get(signal)
            direction_candidates = [item for item in option_candidates if item["candidate_direction"] == signal]
            if direction_candidates:
                direction_candidates.sort(
                    key=lambda item: (
                        item["candidate_score"] + (6 if item.get("strike") == selected_strike else 0),
                        -abs(int(item.get("distance_from_atm") or 0)),
                    ),
                    reverse=True,
                )
                selected_option_contract = direction_candidates[0]
                selected_strike = selected_option_contract.get("strike")
                strike_reason = (
                    f"Option-ranked candidate selected | score={selected_option_contract.get('candidate_score')} "
                    f"| edge={selected_option_contract.get('expected_edge')} | {selected_option_contract.get('reason')}"
                )
            if not strike_reason:
                selected_strike, strike_reason = self.strike_selector.select_strike_with_reason(
                    price=price,
                    signal=signal,
                    volume_signal=volume_signal,
                    strategy_score=self.strategy.last_score,
                    pressure_metrics=pressure_metrics,
                    cautions=getattr(self.strategy, "last_cautions", None),
                    option_chain_data=self.option_data,
                    setup_type=self.strategy.last_signal_type,
                    time_regime=(balanced_pro or {}).get("time_regime"),
                    candle_time=candle_5m["time"],
                )
                selected_option_contract = self._get_option_contract_snapshot(selected_strike, signal, before_ts=candle_5m.get("close_time") or candle_5m["time"])

            self._current_risk_option_contract = selected_option_contract
            self._current_risk_reference_contract = self._get_atm_reference_option_contract(
                signal=signal,
                before_ts=candle_5m.get("close_time") or candle_5m["time"],
            )
            signal_risk_profile = self._resolve_trade_risk_profile(
                setup_type=self.strategy.last_signal_type,
                quality=balanced_pro.get("quality"),
                confidence=self.strategy.last_confidence,
                cautions=getattr(self.strategy, "last_cautions", None),
            )
            self._current_risk_option_contract = None
            self._current_risk_reference_contract = None

            no_trade_zone = self._classify_no_trade_zone(
                balanced_pro=balanced_pro,
                signal=signal,
                selected_option_contract=selected_option_contract,
            )
            wall_guard = self._evaluate_oi_wall_guard(
                signal=signal,
                price=price,
                oi_ladder_data=oi_ladder_data,
                pressure_metrics=pressure_metrics,
            )
            premium_guard = self._evaluate_premium_quality_guard(
                signal=signal,
                selected_option_contract=selected_option_contract,
                candle_time=candle_5m.get("close_time") or candle_5m["time"],
            )
            if no_trade_zone:
                signal = None
                reason = f"No-trade zone filtered ({no_trade_zone['label']}: {no_trade_zone['reason']})"
            elif wall_guard:
                signal = None
                reason = f"OI wall filtered ({wall_guard['label']}: {wall_guard['reason']})"
            elif premium_guard and premium_guard.get("label") not in {"PREMIUM_OK"}:
                if premium_guard.get("label") in {"PREMIUM_CHASED", "PREMIUM_CHASED_3M"}:
                    pullback_plan = self._build_no_chase_pullback_plan(
                        signal,
                        selected_strike,
                        selected_option_contract,
                        premium_guard,
                    )
                    if pullback_plan:
                        self.pending_pullback_watch_plan = pullback_plan
                        candidate_signal = signal
                        candidate_reason = (
                            f"{reason} | no_chase_wait_pullback={premium_guard['label']} "
                            f"| {premium_guard['reason']}"
                        )
                        balanced_pro["tradability"] = "WATCH"
                        balanced_pro["decision_state"] = "WATCH"
                        balanced_pro["watch_bucket"] = "WAIT_PULLBACK"
                signal = None
                reason = f"Premium quality filtered ({premium_guard['label']}: {premium_guard['reason']})"

        microstructure_reason = None
        structure_suggestion = None
        if signal:
            strict_confirmation = (self.strategy.last_signal_type or "").upper() in {"REVERSAL", "BREAKOUT_CONFIRM", "TRAP_REVERSAL"}
            confirmed, micro_reason, _, alternative_strike = self._confirm_signal_microstructure(
                signal=signal,
                selected_strike=selected_strike,
                timestamp=candle_5m["time"],
                price=price,
                strict=strict_confirmation,
            )
            if not confirmed and alternative_strike and alternative_strike != selected_strike:
                selected_strike = alternative_strike
                selected_option_contract = self._get_option_contract_snapshot(
                    selected_strike,
                    signal,
                    before_ts=candle_5m.get("close_time") or candle_5m["time"],
                )
                confirmed, micro_reason, _, _ = self._confirm_signal_microstructure(
                    signal=signal,
                    selected_strike=selected_strike,
                    timestamp=candle_5m["time"],
                    price=price,
                    strict=strict_confirmation,
                )
            if not confirmed:
                signal = None
                reason = f"Microstructure filtered ({micro_reason})"
            else:
                microstructure_reason = micro_reason
                structure_suggestion = self._build_option_structure_suggestion(
                    signal=signal,
                    selected_strike=selected_strike,
                    selected_option_contract=selected_option_contract,
                    balanced_pro=balanced_pro,
                    risk_profile=signal_risk_profile,
                )
                expectancy_profile = self._assess_high_expectancy_profile(
                    signal=signal,
                    candle_time=candle_5m["time"],
                    balanced_pro=balanced_pro,
                    selected_option_contract=selected_option_contract,
                    premium_guard=premium_guard,
                    risk_profile=signal_risk_profile,
                    price=price,
                )
                self.current_expectancy_profile = expectancy_profile
                self.strategy.last_signal_family = expectancy_profile.get("signal_family") or getattr(self.strategy, "last_signal_family", "UNKNOWN")
                if not expectancy_profile.get("allow_trade"):
                    candidate_signal = signal
                    candidate_reason = (
                        f"{reason} | expectancy={expectancy_profile.get('quality_tag')} "
                        f"| phase={expectancy_profile.get('entry_phase')} "
                        f"| {';'.join(expectancy_profile.get('reasons') or ['not_high_expectancy'])}"
                    )
                    if expectancy_profile.get("watch_only"):
                        balanced_pro["tradability"] = "WATCH"
                        balanced_pro["decision_state"] = "WATCH"
                        if expectancy_profile.get("quality_tag") == "PA_STRONG_WAIT_PREMIUM":
                            balanced_pro["watch_bucket"] = "PA_STRONG_WAIT_PREMIUM"
                        else:
                            balanced_pro["watch_bucket"] = "WATCH_CONFIRMATION_PENDING"
                        signal = None
                        reason = (
                            f"High expectancy gate pending "
                            f"({expectancy_profile.get('quality_tag')} / {expectancy_profile.get('entry_phase')})"
                        )
                    else:
                        signal = None
                        reason = (
                            f"High expectancy gate blocked "
                            f"({expectancy_profile.get('quality_tag')} / {expectancy_profile.get('entry_phase')} | "
                            f"{'; '.join(expectancy_profile.get('reasons') or ['not_high_expectancy'])})"
                        )
                if signal:
                    high_prob_ok, high_prob_reason = self._high_probability_action_gate(
                        signal=signal,
                        expectancy_profile=expectancy_profile,
                        premium_guard=premium_guard,
                        feed_health=feed_health,
                        selected_option_contract=selected_option_contract,
                    )
                    if high_prob_ok:
                        reason = f"{reason} | high_probability={high_prob_reason}"
                    else:
                        candidate_signal = signal
                        candidate_reason = f"{reason} | high_probability_block={high_prob_reason}"
                        balanced_pro["tradability"] = "WATCH"
                        balanced_pro["decision_state"] = "WATCH"
                        balanced_pro["watch_bucket"] = "WATCH_CONFIRMATION_PENDING"
                        signal = None
                        reason = f"High probability gate blocked ({high_prob_reason})"
                if signal:
                    trigger_price = (
                        self.strategy.last_entry_plan.get("entry_above")
                        if signal == "CE"
                        else self.strategy.last_entry_plan.get("entry_below")
                    )
                    execution_ok, execution_reason, execution_1m = self._evaluate_1m_execution_gate(
                        signal=signal,
                        trigger_price=trigger_price,
                        signal_entry_time=(selected_option_contract or {}).get("ts") or candle_5m.get("close_time") or candle_5m["time"],
                    )
                    if not execution_ok:
                        candidate_signal = signal
                        candidate_reason = f"{reason} | 1m_execution_pending={execution_reason}"
                        balanced_pro["tradability"] = "WATCH"
                        balanced_pro["decision_state"] = "WATCH"
                        balanced_pro["watch_bucket"] = "WAIT_1M_EXECUTION"
                        self._safe_save_entry_decision_1m(
                            ts=(execution_1m or {}).get("time") or candle_5m["time"],
                            pending={
                                "direction": signal,
                                "score": self.strategy.last_score,
                                "signal_type": self.strategy.last_signal_type,
                                "signal_grade": self.strategy.last_signal_grade,
                                "confidence": self.strategy.last_confidence,
                                "trigger_price": trigger_price,
                                "invalidate_price": self.strategy.last_entry_plan.get("invalidate_price"),
                                "first_target_price": self.strategy.last_entry_plan.get("first_target_price"),
                            },
                            decision="WAIT",
                            latest_1m=execution_1m or {"time": candle_5m["time"]},
                            option_contract=selected_option_contract,
                            strike=selected_strike,
                            reason=f"5m setup held for 1m execution: {execution_reason}",
                        )
                        signal = None
                        reason = f"1m execution pending ({execution_reason})"

        if option_candidates:
            candidate_rows = []
            for item in option_candidates:
                candidate_rows.append(
                    (
                        candle_5m["time"],
                        self.instrument,
                        float(price) if price is not None else None,
                        item.get("underlying_bias"),
                        item.get("setup_type"),
                        item.get("candidate_direction"),
                        int(item.get("strike")) if item.get("strike") is not None else None,
                        int(item.get("atm_strike")) if item.get("atm_strike") is not None else None,
                        int(item.get("distance_from_atm")) if item.get("distance_from_atm") is not None else None,
                        float(item.get("ltp")) if item.get("ltp") is not None else None,
                        float(item.get("top_bid_price")) if item.get("top_bid_price") is not None else None,
                        float(item.get("top_ask_price")) if item.get("top_ask_price") is not None else None,
                        float(item.get("spread")) if item.get("spread") is not None else None,
                        float(item.get("spread_percent")) if item.get("spread_percent") is not None else None,
                        float(item.get("iv")) if item.get("iv") is not None else None,
                        float(item.get("delta")) if item.get("delta") is not None else None,
                        float(item.get("theta")) if item.get("theta") is not None else None,
                        int(item.get("oi")) if item.get("oi") is not None else None,
                        int(item.get("volume")) if item.get("volume") is not None else None,
                        float(item.get("candidate_score")) if item.get("candidate_score") is not None else None,
                        int(item.get("candidate_rank")) if item.get("candidate_rank") is not None else None,
                        float(item.get("expected_edge")) if item.get("expected_edge") is not None else None,
                        bool(signal and item.get("candidate_direction") == signal and item.get("strike") == selected_strike),
                        item.get("reason"),
                    )
                )
            self.db_writer.insert_option_signal_candidates_5m(candidate_rows)

        # Enrich reason
        enriched_reason = reason
        if self.strategy.last_confidence:
            enriched_reason = f"{reason} | confidence={self.strategy.last_confidence} | regime={self.strategy.last_regime}"
        enriched_reason += f" | signal_type={self.strategy.last_signal_type} | signal_grade={self.strategy.last_signal_grade}"
        enriched_reason += (
            f" | base_bias={balanced_pro['bias']} | setup={balanced_pro['setup']} "
            f"| quality={balanced_pro['quality']} | tradability={balanced_pro['tradability']} "
            f"| time_regime={balanced_pro['time_regime']} "
            f"| context_score={balanced_pro['context_score']} | entry_score={balanced_pro['entry_score']} "
            f"| decision_state={balanced_pro['decision_state']} | watch_bucket={balanced_pro['watch_bucket']} "
            f"| pressure_conflict_level={balanced_pro['pressure_conflict_level']}"
        )
        if balanced_pro.get("confidence_summary"):
            enriched_reason += f" | confidence_summary={balanced_pro['confidence_summary']}"
        if oi_ladder_data and oi_ladder_data.get("oi_summary"):
            enriched_reason += f" | oi_summary={oi_ladder_data['oi_summary']}"
        if balanced_pro.get("pressure_summary", {}).get("summary"):
            enriched_reason += f" | pressure_summary={balanced_pro['pressure_summary']['summary']}"
        if option_sweep_context and option_sweep_context.get("summary"):
            enriched_reason += f" | option_sweep={option_sweep_context['summary']}"
        if microstructure_reason:
            enriched_reason += f" | microstructure={microstructure_reason}"
        if self.option_data_source:
            enriched_reason += f" | option_data_source={self.option_data_source}"
        if self.option_data_ts:
            enriched_reason += f" | option_data_ts={self.option_data_ts}"
        if self.strategy.last_heikin_ashi:
            enriched_reason += (
                f" | ha_bias={self.strategy.last_heikin_ashi.get('bias')}"
                f" | ha_strength={self.strategy.last_heikin_ashi.get('strength')}"
            )
        if fallback_context and fallback_context["fallback_used"]:
            enriched_reason += " | oi_mode=OI_ONLY_FALLBACK"
        if feed_health:
            enriched_reason += f" | {feed_health['summary']}"
        if self.strategy.last_blockers:
            enriched_reason += f" | blockers={', '.join(self.strategy.last_blockers)}"
        if self.strategy.last_cautions:
            enriched_reason += f" | cautions={', '.join(self.strategy.last_cautions)}"
        if strike_reason:
            enriched_reason += f" | strike_reason={strike_reason}"
        if no_trade_zone:
            enriched_reason += f" | no_trade_zone={no_trade_zone['label']}"
        if wall_guard:
            enriched_reason += f" | wall_guard={wall_guard['label']}"
        if premium_guard:
            enriched_reason += f" | premium_guard={premium_guard['label']}"
            if self.current_expectancy_profile:
                enriched_reason += (
                    f" | expectancy={self.current_expectancy_profile.get('quality_tag')}"
                    f" | entry_phase={self.current_expectancy_profile.get('entry_phase')}"
                    f" | premium_confirmed={self.current_expectancy_profile.get('premium_confirmed')}"
                    f" | path_quality={self.current_expectancy_profile.get('path_quality')}"
                    f" | signal_family={self.current_expectancy_profile.get('signal_family')}"
                    f" | session_phase={self.current_expectancy_profile.get('session_map_phase')}"
                )

        oi_mode = "OI_ONLY_FALLBACK" if fallback_context and fallback_context["fallback_used"] else "FULL_OPTION_BAND"

        # Save decision
        self._safe_save_strategy_decision(
            ts=candle_5m["time"],
            price=price,
            signal=signal,
            reason=enriched_reason,
            volume_signal=volume_signal,
            oi_bias=oi_bias,
            oi_trend=oi_ladder_data["trend"] if oi_ladder_data else None,
            build_up=oi_ladder_data["build_up"] if oi_ladder_data else None,
            pressure_metrics=pressure_metrics,
            ce_delta_total=oi_ladder_data["ce_delta_total"] if oi_ladder_data else None,
            pe_delta_total=oi_ladder_data["pe_delta_total"] if oi_ladder_data else None,
            pcr=self.option_data.get("pcr") if self.option_data else (fallback_context.get("pcr") if fallback_context else None),
            orb_high=orb_high,
            orb_low=orb_low,
            vwap=vwap_value,
            atr=atr_value,
            strike=selected_strike,
            balanced_pro=balanced_pro,
            oi_mode=oi_mode,
        )

        # Log decision
        self.audit_logger.log_decision(
            instrument=self.instrument,
            price=price,
            signal=signal or "NO_TRADE",
            strike=selected_strike,
            score=self.strategy.last_score,
            confidence=self.strategy.last_confidence,
            regime=self.strategy.last_regime,
            manual_guidance="MANUAL_TRADING_MODE",
            signal_valid_till=candle_5m["close_time"] + timedelta(minutes=Config.SIGNAL_VALIDITY_MINUTES) if candle_5m.get("close_time") else None,
            blockers=", ".join(self.strategy.last_blockers),
            cautions=", ".join(self.strategy.last_cautions),
            score_factors=", ".join(self.strategy.last_score_components),
            reason=reason,
            strike_reason=strike_reason,
        )

        # Send notification if signal
        if signal:
            signal_entry_time = (
                (selected_option_contract or {}).get("ts")
                or candle_5m.get("close_time")
                or candle_5m["time"]
            )
            selected_option_contract = self._greek_enriched_option_contract(
                selected_option_contract,
                signal,
                price,
                before_ts=signal_entry_time,
            )
            option_signal_price = (selected_option_contract or {}).get("ltp") if selected_option_contract else price
            spot_trigger_price = self.strategy.last_entry_plan.get("entry_above") if signal == "CE" else self.strategy.last_entry_plan.get("entry_below")
            spot_invalidate_price = self.strategy.last_entry_plan.get("invalidate_price")
            spot_target_price = self.strategy.last_entry_plan.get("first_target_price")
            projected_premium_sl, projected_premium_t1 = self._project_premium_levels_for_spot_map(
                selected_option_contract,
                signal,
                spot_invalidate_price,
                spot_target_price,
                before_ts=signal_entry_time,
            )
            self._current_risk_option_contract = selected_option_contract
            self._current_risk_reference_contract = self._get_atm_reference_option_contract(
                signal=signal,
                before_ts=signal_entry_time,
            )
            risk_profile = self._resolve_trade_risk_profile(
                setup_type=self.strategy.last_signal_type,
                quality=balanced_pro.get("quality"),
                confidence=self.strategy.last_confidence,
                cautions=getattr(self.strategy, "last_cautions", None),
            )
            self._current_risk_option_contract = None
            self._current_risk_reference_contract = None
            print(f"\n[Signal Service] SIGNAL GENERATED: {signal}")
            print(f"Score: {self.strategy.last_score} | Confidence: {self.strategy.last_confidence}")
            print(f"Strike: {selected_strike} | Reason: {reason}")
            print(f"Price: {option_signal_price} | Spot: {price} | Time: {candle_5m['time']}")
            expectancy_profile = self.current_expectancy_profile or {}
            trade_type = self._classify_action_trade_type(
                setup_type=self.strategy.last_signal_type,
                high_expectancy_profile=expectancy_profile,
                premium_guard=premium_guard,
            )
            exit_if = self._action_exit_if_line(
                signal,
                trigger_price=spot_trigger_price,
                invalidate_price=spot_invalidate_price,
                premium_sl=projected_premium_sl,
            )
            trade_notification = {
                "instrument": self.instrument,
                "signal": signal,
                "strike": selected_strike,
                "confidence": self.strategy.last_confidence,
                "confidence_summary": self.strategy.last_confidence_summary,
                "signal_type": self.strategy.last_signal_type,
                "setup_bucket": risk_profile["setup_bucket"],
                "decision_label": self._entry_decision_label(signal),
                "action_text": f"{signal} confirmed hai. Entry sirf apne planned risk ke saath lo.",
                "journal_note": self._build_journal_note(
                    self._entry_decision_label(signal),
                    f"{signal} confirmed hai. Entry sirf apne planned risk ke saath lo.",
                    strike_reason,
                ),
                "signal_grade": self.strategy.last_signal_grade,
                "price": round(option_signal_price, 2) if option_signal_price is not None else None,
                "spot_price": round(price, 2) if price is not None else None,
                "trigger_price": spot_trigger_price,
                "invalidate_price": spot_invalidate_price,
                "first_target_price": spot_target_price,
                "projected_premium_sl": projected_premium_sl,
                "projected_premium_t1": projected_premium_t1,
                "entry_bid": selected_option_contract.get("top_bid_price") if selected_option_contract else None,
                "entry_ask": selected_option_contract.get("top_ask_price") if selected_option_contract else None,
                "entry_spread": selected_option_contract.get("spread") if selected_option_contract else None,
                "time_regime": balanced_pro["time_regime"],
                "option_stop_loss_pct": risk_profile["hard_premium_stop_pct"],
                "option_target_pct": risk_profile["target_pct"],
                "option_trail_pct": risk_profile["trail_from_peak_pct"],
                "time_stop_warn_minutes": risk_profile["time_stop_warn_minutes"],
                "time_stop_exit_minutes": risk_profile["time_stop_exit_minutes"],
                "risk_note": risk_profile["risk_note"],
                "structure_suggestion": structure_suggestion,
                "rr_ratio": round(
                    float(risk_profile["target_pct"]) /
                    max(float(risk_profile["hard_premium_stop_pct"]), 1.0),
                    2,
                ),
                "reason": enriched_reason,
                "pressure_read": (balanced_pro.get("pressure_summary") or {}).get("summary"),
                "oi_read": (oi_ladder_data or {}).get("oi_summary"),
                "trend_15m": trend_15m,
                "institutional_read": institutional_confluence_reason,
                "greek_summary": format_greek_summary(selected_option_contract),
                "quality_tag": expectancy_profile.get("quality_tag"),
                "entry_phase": expectancy_profile.get("entry_phase"),
                "premium_confirmed": expectancy_profile.get("premium_confirmed"),
                "path_quality": expectancy_profile.get("path_quality"),
                "likely_runner": expectancy_profile.get("likely_runner"),
                "signal_family": expectancy_profile.get("signal_family"),
                "pro_check": expectancy_profile.get("pro_check"),
                "session_map_phase": expectancy_profile.get("session_map_phase"),
                "trade_thesis": self._build_trade_thesis(signal, balanced_pro=balanced_pro, premium_guard=premium_guard),
                "trade_type": trade_type,
                "exit_if": exit_if,
            }
            if self._suppress_live_actions:
                self._log(
                    f"Backfill-only process for {self.instrument} {candle_5m['time']} | "
                    f"Signal {signal} detected but live dispatch suppressed"
                )
            else:
                telegram_sent = self._send_action_notification(
                    self.notifier.send_trade_notification,
                    trade_notification,
                    label=f"{self.instrument} {signal} trade signal",
                )

                signal_saved = self._safe_save_signal_issued(
                    ts=signal_entry_time,
                    signal=signal,
                    price=option_signal_price,
                    strike=selected_strike,
                    reason=enriched_reason,
                    balanced_pro=balanced_pro,
                    oi_mode=oi_mode,
                    telegram_sent=telegram_sent,
                    monitor_started=True,
                    entry_window_end=(
                        signal_entry_time + timedelta(minutes=Config.SIGNAL_VALIDITY_MINUTES)
                        if signal_entry_time else None
                    ),
                    underlying_price=price,
                    option_contract=selected_option_contract,
                    strike_reason=strike_reason,
                    option_data_source=self.option_data_source,
                )
                if not signal_saved:
                    self._log("Signal issued save failed after trade notification dispatch")

                # Save ML features for training (async, don't block)
                if ml_features:
                    self._safe_save_ml_features(ml_features, ml_prob)

                self._clear_pending_entry_watch()
                self._clear_pending_spike_watch()
                self._start_trade_monitor(
                    signal,
                    candle_5m,
                    price,
                    balanced_pro,
                    selected_strike,
                    entry_time=signal_entry_time,
                )
                self.signals_generated += 1
        else:
            watch_payload = self._build_manual_watch_payload(
                candle_5m=candle_5m,
                price=price,
                candidate_signal=candidate_signal,
                candidate_reason=candidate_reason,
                balanced_pro=balanced_pro,
                orb_high=orb_high,
                orb_low=orb_low,
                support=support,
                resistance=resistance,
            )
            if self._suppress_live_actions:
                self._log(
                    f"Backfill-only process for {self.instrument} {candle_5m['time']} | "
                    "Watch evaluation updated without live alert"
                )
            else:
                self._set_pending_entry_watch(watch_payload, balanced_pro, candle_5m)
                self._maybe_send_watch_alert(watch_payload)
            print(
                f"[Signal Service] No signal | Bias: {balanced_pro['bias']} | "
                f"Setup: {balanced_pro['setup']} | Quality: {balanced_pro['quality']} | "
                f"Time: {balanced_pro['time_regime']} | "
                f"Score: {self.strategy.last_score} | Reason: {reason}"
            )

        self.candles_processed += 1
        return signal

    def _get_data_health_status(self):
        """Check DB freshness before generating signals."""
        latest_5m = self.db_reader.fetch_latest_candle(self.instrument, timeframe="5m")
        if not latest_5m:
            return False, "No 5m candles available in DB"

        now = self.time_utils.now_ist()
        
        # Fix timezone - ensure candle time is in IST
        candle_time = self._effective_candle_close_time(latest_5m)
        if candle_time.tzinfo is None:
            # If naive, assume UTC and convert to IST
            from datetime import timezone
            import pytz
            candle_time = candle_time.replace(tzinfo=timezone.utc).astimezone(pytz.timezone('Asia/Kolkata'))
        elif str(candle_time.tzinfo) != 'Asia/Kolkata':
            # Convert to IST
            import pytz
            candle_time = candle_time.astimezone(pytz.timezone('Asia/Kolkata'))
        
        candle_age = max(0.0, (now - candle_time).total_seconds())
        if not Config.TEST_MODE and candle_age > 8 * 60:
            recent_candles = self.db_reader.fetch_recent_candles_5m(
                instrument=self.instrument,
                limit=24,
            )
            pending = self._collect_unprocessed_5m_candles(recent_candles, now)
            if pending:
                self._stale_backlog_replay = True
                return True, None
            self._stale_backlog_replay = False
            return False, f"Latest 5m candle is stale ({int(candle_age)}s old)"
        self._stale_backlog_replay = False

        latest_oi_snapshot = self.db_reader.fetch_latest_oi_snapshot(self.instrument)
        if latest_oi_snapshot:
            oi_time = latest_oi_snapshot["ts"]
            if oi_time.tzinfo is None:
                from datetime import timezone
                import pytz
                oi_time = oi_time.replace(tzinfo=timezone.utc).astimezone(pytz.timezone('Asia/Kolkata'))
            elif str(oi_time.tzinfo) != 'Asia/Kolkata':
                import pytz
                oi_time = oi_time.astimezone(pytz.timezone('Asia/Kolkata'))
            
            oi_age = max(0.0, (now - oi_time).total_seconds())
            if not Config.TEST_MODE and oi_age > max(Config.OI_FETCH_INTERVAL * 2, 420):
                return False, f"Latest OI snapshot is stale ({int(oi_age)}s old)"

        return True, None

    def _build_no_chase_pullback_plan(self, signal, selected_strike, selected_option_contract, premium_guard):
        if not bool(getattr(Config, "NO_CHASE_PULLBACK_WATCH_ENABLED", True)):
            return None
        if signal not in {"CE", "PE"} or not selected_option_contract or not premium_guard:
            return None
        current_ltp = selected_option_contract.get("ltp") or premium_guard.get("current_ltp")
        try:
            current_ltp = float(current_ltp)
        except Exception:
            return None
        if current_ltp <= 0:
            return None
        retrace_pct = float(getattr(Config, "PREMIUM_PULLBACK_ENTRY_RETRACE_PCT", 8.0) or 8.0)
        entry_max = round(current_ltp * (1.0 - retrace_pct / 100.0), 2)
        return {
            "direction": signal,
            "strike": selected_strike or selected_option_contract.get("strike"),
            "current_ltp": round(current_ltp, 2),
            "entry_max": entry_max,
            "retrace_pct": retrace_pct,
            "label": premium_guard.get("label"),
            "reason": premium_guard.get("reason"),
        }

    def _maybe_send_data_health_alert(self, reason):
        if Config.TEST_MODE or not bool(getattr(Config, "ENABLE_DATA_HEALTH_TELEGRAM_ALERTS", False)):
            return
        now = time_module.time()
        cooldown = max(60, int(getattr(Config, "DATA_HEALTH_ALERT_COOLDOWN_SECONDS", 900)))
        reason_changed = reason != self.last_data_health_alert_reason
        cooldown_elapsed = now - float(self.last_data_health_alert_time or 0) >= cooldown
        if not reason_changed and not cooldown_elapsed:
            return
        self.last_data_health_alert_time = now
        self.last_data_health_alert_reason = reason
        self._run_async_notification(
            self.notifier.send_data_health_alert,
            {
                "instrument": self.instrument,
                "reason": reason,
                "status": "DOWN",
            },
        )

    def _send_data_recovered_alert(self):
        if (
            Config.TEST_MODE
            or not bool(getattr(Config, "ENABLE_DATA_HEALTH_TELEGRAM_ALERTS", False))
            or not self.last_data_health_alert_reason
        ):
            return
        recovered_from = self.last_data_health_alert_reason
        self.last_data_health_alert_reason = None
        self.last_data_health_alert_time = 0
        self._run_async_notification(
            self.notifier.send_data_health_alert,
            {
                "instrument": self.instrument,
                "reason": f"Recovered from: {recovered_from}",
                "status": "RECOVERED",
            },
        )

    def run(self):
        """Main signal service loop"""
        self.running = True
        self.watchdog.start({"phase": "starting"})
        self._print_startup_status()
        
        self._log("Starting signal analysis...")
        
        try:
            loop_count = 0
            while self.running:
                loop_count += 1
                if loop_count % 10 == 0:  # Log every 10th iteration
                    self._log(f"DEBUG Main loop iteration {loop_count}")
                
                gap_event = self.runtime_gap_detector.check()
                if gap_event:
                    self._handle_runtime_gap(gap_event)
                
                data_ok, pause_reason = self._get_data_health_status()
                if not data_ok:
                    if self.last_data_pause_reason != pause_reason:
                        self._log(f"Pausing signal generation: {pause_reason}")
                        self.last_data_pause_reason = pause_reason
                    self.data_pause_active = True
                    self.watchdog.touch({"phase": "data_pause", "reason": pause_reason})
                    self._maybe_send_data_health_alert(pause_reason)
                    time_module.sleep(30)
                    continue

                if self.data_pause_active:
                    self._log("Data stream healthy again. Resuming signal generation.")
                    self._send_data_recovered_alert()
                    self.data_pause_active = False
                    self.last_data_pause_reason = None
                    self.watchdog.touch({"phase": "resumed"})

                # Get recent 5-minute candles from database so we can backfill missed closed candles
                self._log("DEBUG Fetching candles from DB...")
                recent_candles = self.db_reader.fetch_recent_candles_5m(
                    instrument=self.instrument,
                    limit=24
                )
                self._log(f"DEBUG Fetched {len(recent_candles)} candles")
                
                if not recent_candles:
                    self._log("No candles found in database, waiting...")
                    time_module.sleep(30)
                    continue

                current_time = self.time_utils.now_ist()
                latest_candle = recent_candles[-1]
                candles_to_process = self._collect_unprocessed_5m_candles(recent_candles, current_time)

                if not candles_to_process:
                    candle_time = latest_candle["time"]
                    effective_close_time = self._effective_candle_close_time(latest_candle)
                    current_cmp, effective_close_time = self._coerce_comparable_datetimes(current_time, effective_close_time)
                    time_diff = current_cmp - effective_close_time
                    self._log(
                        f"DEBUG Candle Check | Time Diff: {time_diff} | Candle TS: {candle_time} | "
                        f"Effective Close: {effective_close_time} | Last Processed: {self.last_processed_5m_ts} | "
                        "Will Process: False"
                    )

                if candles_to_process:
                    if len(candles_to_process) > 1:
                        self._log(
                            f"Replaying {len(candles_to_process)} missed 5m candles for {self.instrument} "
                            f"from {candles_to_process[0]['time']} to {candles_to_process[-1]['time']}"
                        )
                    self._refresh_option_data_if_due()
                    replaying_stale_backlog = bool(self._stale_backlog_replay)
                    for idx, candle_5m in enumerate(candles_to_process):
                        candle_time = candle_5m["time"]
                        self._suppress_live_actions = replaying_stale_backlog or idx < (len(candles_to_process) - 1)
                        self._log(
                            f"Processing 5m candle | Time: {candle_time} | Price: {candle_5m['close']} | "
                            f"Mode: {'BACKFILL' if self._suppress_live_actions else 'LIVE'}"
                        )
                        self._current_candle_time = candle_time
                        self._process_5m_candle(candle_5m)
                        self.last_processed_5m_ts = candle_time
                    self._suppress_live_actions = False
                    self._stale_backlog_replay = False
                    latest_candle = candles_to_process[-1]

                current_minute = current_time.replace(second=0, microsecond=0)
                if current_minute != self.last_monitor_check_minute:
                    if not self.active_trade_monitor:
                        self._maybe_prepare_option_spike_watch()
                    if self.pending_entry_watch:
                        self._maybe_fire_pending_entry_watch(latest_candle)
                    if self.active_trade_monitor:
                        self._maybe_send_trade_monitor_update(latest_candle)
                    self.last_monitor_check_minute = current_minute
                
                # Periodic checks
                current_time = time_module.time()
                
                # Heartbeat check
                if current_time - self.last_heartbeat >= self.heartbeat_interval:
                    self._print_heartbeat()
                    self.last_heartbeat = current_time
                
                # Market status check
                if current_time - self.last_market_status_check >= self.market_status_interval:
                    self._check_market_status()
                    self.last_market_status_check = current_time
                
                # Wait for next candle
                time_module.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            print(f"\n[{self.time_utils.now_ist().strftime('%H:%M:%S')}] [Signal Service] Shutdown requested by user")
        except Exception as e:
            self._log(f"Unexpected error: {e}")
            self._log(traceback.format_exc())
        finally:
            self.running = False
            self.watchdog.stop()
            self._log("Signal service stopped")

    def stop(self):
        """Stop signal service"""
        self.running = False
        self._log("Stop signal sent")

    def _print_heartbeat(self):
        """Print periodic heartbeat status"""
        current_time = self.time_utils.now_ist()
        self.watchdog.touch(
            {
                "phase": "heartbeat",
                "candles_processed": self.candles_processed,
                "signals_generated": self.signals_generated,
                "cooldown_remaining": self.signal_cooldown_remaining,
                "data_pause_active": self.data_pause_active,
            }
        )
        
        print(f"\n[Signal Service] Heartbeat | IST: {current_time.strftime('%H:%M:%S')} | "
              f"Status: {'✅ RUNNING' if self.running else '❌ STOPPED'} | "
              f"Candles Processed: {self.candles_processed} | "
              f"Signals Generated: {self.signals_generated} | "
              f"Last Signal: {self.time_utils.format_time(self.last_signal_time) if self.last_signal_time else 'None'} | "
              f"Cooldown: {self.signal_cooldown_remaining}")
    
    def _check_market_status(self):
        """Check and report market status"""
        current_time = self.time_utils.now_ist()
        current_time_only = current_time.time()
        
        # Market hours (9:15 AM - 3:30 PM IST)
        market_open = time(9, 15)
        market_close = time(15, 30)
        
        # Weekend check
        is_weekend = current_time.weekday() >= 5  # 5=Saturday, 6=Sunday
        
        # Determine market status
        if is_weekend:
            market_status = "WEEKEND"
            status_msg = "Market closed for weekend"
        elif current_time_only < market_open:
            market_status = "PRE_MARKET"
            status_msg = "Market not yet opened"
        elif current_time_only > market_close:
            market_status = "POST_MARKET"
            status_msg = "Market closed for today"
        else:
            market_status = "MARKET_OPEN"
            status_msg = "Market is open"
        
        # Report status change
        if market_status != self.last_market_status:
            print(f"\n[Signal Service] Market Status Update: {status_msg}")
            print(f"[Signal Service] Current Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
            
            if market_status == "WEEKEND":
                print(f"[Signal Service] Weekend Mode - No signals expected")
                print(f"[Signal Service] Next market open: Monday {market_open.strftime('%H:%M')} IST")
            elif market_status == "PRE_MARKET":
                print(f"[Signal Service] Pre-market - Waiting for market open at {market_open.strftime('%H:%M')} IST")
            elif market_status == "POST_MARKET":
                print(f"[Signal Service] Post-market - Market closed for today")
                print(f"[Signal Service] Next market open: Tomorrow {market_open.strftime('%H:%M')} IST")
            elif market_status == "MARKET_OPEN":
                print(f"[Signal Service] Market Open - Analyzing signals")
                print(f"[Signal Service] Market closes at {market_close.strftime('%H:%M')} IST")
            
            print(f"[Signal Service] Status: {'✅ RUNNING' if self.running else '❌ STOPPED'}")
            print(f"[Signal Service] Database: {'✅ CONNECTED' if self.db_writer.enabled else '❌ DISABLED'}")
            print(f"[Signal Service] Strategy: {type(self.strategy).__name__}")
            print(f"[Signal Service] Session Stats: {self.candles_processed} candles, {self.signals_generated} signals")
            print("[Signal Service] " + "="*50)
            
            self.last_market_status = market_status
    
    def get_status(self):
        """Get current signal service status"""
        return {
            "running": self.running,
            "instrument": self.instrument,
            "strategy": self.strategy.__class__.__name__,
            "last_signal_time": self.last_signal_time,
            "session_decisions": len(self.session_decisions),
            "signal_cooldown": self.signal_cooldown_remaining,
            "candles_processed": self.candles_processed,
            "signals_generated": self.signals_generated,
        }


def main():
    """Main entry point for signal service"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", default=Config.SYMBOL)
    args = parser.parse_args()
    
    # Handle comma-separated instruments
    instrument = args.instrument
    if ',' in instrument:
        instruments = [inst.strip() for inst in instrument.split(',')]
        # Filter out empty strings
        instruments = [inst for inst in instruments if inst]
        print(f"[Signal Service] Starting for instruments: {instruments}")
        # Start multiple signal services (one per instrument)
        services = []
        for inst in instruments:
            service = SignalService(instrument=inst)
            services.append(service)
        
        try:
            # Run all services concurrently
            import threading
            threads = []
            for service in services:
                thread = threading.Thread(target=service.run)
                thread.daemon = True
                thread.start()
                threads.append(thread)
            
            # Keep main thread alive
            while any(thread.is_alive() for thread in threads):
                time_module.sleep(1)
                
        except KeyboardInterrupt:
            print("\n[Signal Service] Shutting down...")
            for service in services:
                service.stop()
        except Exception as e:
            print(f"[Signal Service] Error: {e}")
    else:
        signal_service = SignalService(instrument=instrument)
    
    try:
        signal_service.run()
    except KeyboardInterrupt:
        print("\n[Signal Service] Shutting down...")
        signal_service.stop()
    except Exception as e:
        print(f"[Signal Service] Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
