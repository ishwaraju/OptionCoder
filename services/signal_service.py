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
from collections import deque
from datetime import timedelta, time, datetime

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils.time_utils import TimeUtils
from shared.utils.log_utils import log_with_timestamp
from config import Config, get_config_for_instrument
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
from shared.indicators.multi_timeframe_trend import calculate_trend_from_candles
from strategies.shared.breakout_strategy import BreakoutStrategy
from strategies.shared.strike_selector import StrikeSelector
from strategies.banknifty import BankNiftyActionableRules
from strategies.nifty import NiftyActionableRules
from strategies.sensex import SensexActionableRules
from shared.utils.logger import TradeLogger
from shared.utils.notifier import Notifier
from shared.utils.instrument_profile import get_instrument_profile
from shared.utils.runtime_gap_detector import RuntimeGapDetector
from shared.utils.service_watchdog import ServiceWatchdog
from shared.utils.option_data_cache import OptionDataCache

# ML Signal Enhancement (FREE - scikit-learn)
try:
    from shared.ml.feature_extractor import MLFeatureExtractor
    from shared.ml.signal_filter import MLSignalFilter
    ML_ENABLED = True
except ImportError as e:
    print(f"[Signal Service] ML modules not available: {e}")
    ML_ENABLED = False


class SignalService:
    def __init__(self, instrument=None):
        self.time_utils = TimeUtils()
        self.profile = get_instrument_profile(instrument)
        self.instrument = self.profile["instrument"]
        self.watchdog = ServiceWatchdog("signal_service", self.instrument)
        
        # Get instrument-specific config
        self.config = get_config_for_instrument(self.instrument)
        
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
        
        # Strategy
        self.strategy = BreakoutStrategy(instrument=self.instrument)
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

    def _log(self, message):
        """Log with HH:mm:ss IST timestamp prefix"""
        ts = self.time_utils.now_ist().strftime('%H:%M:%S')
        print(f"[{ts}] [Signal Service] {message}")

    def _handle_runtime_gap_recovery(self, reason_label):
        """Handle recovery after confirmed system sleep or a large runtime gap."""
        self._log(f"🔄 Recovering after {reason_label}...")
        
        # Clear last processed timestamp to allow processing new candles
        if self.last_processed_5m_ts:
            self._log(f"   Last processed candle: {self.last_processed_5m_ts}")
            self.last_processed_5m_ts = None
            self._log("   ✅ Cleared last_processed timestamp for fresh start")
        
        # Reset data pause state
        if self.data_pause_active:
            self.data_pause_active = False
            self.last_data_pause_reason = None
            self._log("   ✅ Reset data pause state")
        
        # Clear active trade monitor (trade may have expired)
        if self.active_trade_monitor:
            self._log("   ⚠️  Clearing active trade monitor (may have expired during sleep)")
            self.active_trade_monitor = None
        if self.pending_entry_watch:
            self._log("   ⚠️  Clearing pending entry watch (needs fresh 1m confirmation after sleep)")
            self.pending_entry_watch = None
        
        # Refresh indicator state from recent history
        try:
            self._restore_indicator_state()
            self._log("   ✅ Restored indicator state from history")
        except Exception as e:
            self._log(f"   ⚠️  Could not restore indicators: {e}")
        
        self._log("🔄 Recovery complete. Resuming normal operation...")

    def _handle_runtime_gap(self, gap_event):
        kind = gap_event["kind"]
        wall_gap = gap_event["wall_gap"]
        active_gap = gap_event["active_gap"]
        suspended_gap = gap_event["suspended_gap"]

        if kind == "system_sleep":
            self._log(
                "⚠️  SYSTEM SLEEP/WAKE DETECTED! "
                f"Wall gap: {wall_gap:.1f}s ({wall_gap/60:.1f} min) | "
                f"active runtime: {active_gap:.1f}s | suspended: {suspended_gap:.1f}s. "
                "Recovering..."
            )
            self._handle_runtime_gap_recovery("system sleep/wake")
            return

        self._log(
            "⚠️  PROCESSING/FEED GAP DETECTED! "
            f"Wall gap: {wall_gap:.1f}s ({wall_gap/60:.1f} min) | "
            f"active runtime: {active_gap:.1f}s | suspended: {suspended_gap:.1f}s. "
            "This is not a confirmed system sleep. Recovering..."
        )
        self._handle_runtime_gap_recovery("processing/feed gap")

    def _start_trade_monitor(self, signal, candle_5m, price, balanced_pro, selected_strike):
        """Start 1-minute manual trade monitor after a signal."""
        option_contract = self._get_option_contract_snapshot(selected_strike, signal, before_ts=candle_5m.get("close_time") or candle_5m["time"])
        option_entry_price = option_contract.get("ltp") if option_contract else None
        self.active_trade_monitor = {
            "signal": signal,
            "entry_time": candle_5m["time"],
            "entry_price": option_entry_price if option_entry_price is not None else price,
            "entry_underlying_price": price,
            "strike": selected_strike,
            "entry_bid": option_contract.get("top_bid_price") if option_contract else None,
            "entry_ask": option_contract.get("top_ask_price") if option_contract else None,
            "entry_spread": option_contract.get("spread") if option_contract else None,
            "entry_iv": option_contract.get("iv") if option_contract else None,
            "entry_delta": option_contract.get("delta") if option_contract else None,
            "max_favorable_option_ltp": option_entry_price,
            "max_adverse_option_ltp": option_entry_price,
            "quality": balanced_pro["quality"],
            "time_regime": balanced_pro["time_regime"],
            "last_notified_minute": None,
            "minutes_active": 0,
        }

    @staticmethod
    def _spread_percent(option_row):
        ltp = option_row.get("ltp") if option_row else None
        spread = option_row.get("spread") if option_row else None
        if not ltp or spread is None:
            return None
        try:
            return round((float(spread) / float(ltp)) * 100, 4)
        except Exception:
            return None

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

    def _score_option_candidate(self, row, direction, preferred_strike, underlying_price):
        ltp = float(row.get("ltp") or 0)
        spread = float(row.get("spread") or 0)
        spread_percent = self._spread_percent(row) or 999.0
        bid_qty = int(row.get("top_bid_quantity") or 0)
        ask_qty = int(row.get("top_ask_quantity") or 0)
        volume = int(row.get("volume") or 0)
        oi = int(row.get("oi") or 0)
        delta_abs = abs(float(row.get("delta") or 0))
        theta_abs = abs(float(row.get("theta") or 0))
        distance = abs(int(row.get("strike") or 0) - int(preferred_strike or row.get("strike") or 0))
        strike_gap = self.profile["strike_step"] or Config.STRIKE_STEP.get(self.instrument, 50)

        target_delta = 0.5 if self.strategy.last_score >= 75 else 0.62
        spread_score = max(0.0, 30.0 - min(spread_percent, 10.0) * 4.0)
        depth_score = min(15.0, min(bid_qty, ask_qty) / 20.0)
        volume_score = min(18.0, volume / 300.0)
        oi_score = min(10.0, oi / 20000.0)
        delta_score = max(0.0, 15.0 * (1.0 - min(abs(delta_abs - target_delta) / 0.45, 1.0)))
        proximity_score = max(0.0, 12.0 - (distance / max(strike_gap, 1)) * 4.0)

        target_price = (self.strategy.last_entry_plan or {}).get("first_target_price")
        target_move = abs(float(target_price) - float(underlying_price)) if target_price is not None and underlying_price is not None else float(strike_gap)
        expected_move = delta_abs * target_move
        theta_penalty = theta_abs * 0.25
        expected_edge = round(expected_move - spread - theta_penalty, 2)
        edge_score = max(0.0, min(20.0, expected_edge))

        atm_row = None
        atm = (self.option_data or {}).get("atm")
        if atm is not None:
            atm_row = self._get_option_contract_snapshot(atm, direction)
        iv_penalty = 0.0
        if atm_row and atm_row.get("iv") and row.get("iv"):
            atm_iv = float(atm_row["iv"])
            if atm_iv > 0:
                iv_markup = (float(row["iv"]) - atm_iv) / atm_iv
                if iv_markup > 0.12:
                    iv_penalty = min(8.0, iv_markup * 20.0)

        candidate_score = round(
            spread_score + depth_score + volume_score + oi_score + delta_score + proximity_score + edge_score - iv_penalty,
            2,
        )
        reason_parts = [
            f"spread={spread:.2f} ({spread_percent:.2f}%)",
            f"delta={delta_abs:.2f}",
            f"vol={volume}",
            f"oi={oi}",
            f"edge={expected_edge:.2f}",
        ]
        if iv_penalty > 0:
            reason_parts.append("iv_rich")

        return {
            **dict(row),
            "candidate_direction": direction,
            "candidate_score": candidate_score,
            "expected_edge": expected_edge,
            "spread_percent": spread_percent,
            "reason": " | ".join(reason_parts),
        }

    def _build_option_candidates(self, underlying_price, preferred_strikes=None, signal_direction=None, balanced_pro=None):
        if not self.option_data:
            return []

        preferred_strikes = preferred_strikes or {}
        band_rows = self.option_data.get("band_snapshots") or []
        candidates = []
        for direction in ("CE", "PE"):
            if signal_direction and direction != signal_direction:
                continue
            preferred_strike = preferred_strikes.get(direction)
            direction_rows = [
                row for row in band_rows
                if row.get("option_type") == direction and abs(int(row.get("distance_from_atm") or 99)) <= 3
            ]
            scored = [
                self._score_option_candidate(row, direction, preferred_strike, underlying_price)
                for row in direction_rows
            ]
            scored.sort(key=lambda item: (item["candidate_score"], -abs(int(item.get("distance_from_atm") or 0))), reverse=True)
            top_rows = scored[:3]
            for rank, item in enumerate(top_rows, start=1):
                candidates.append({
                    **item,
                    "candidate_rank": rank,
                    "underlying_bias": (balanced_pro or {}).get("bias"),
                    "setup_type": (balanced_pro or {}).get("setup"),
                })
        return candidates

    def _clear_pending_entry_watch(self):
        self.pending_entry_watch = None

    @staticmethod
    def _one_minute_trigger_volume_ok(recent_1m_candles):
        if not recent_1m_candles:
            return False

        latest = recent_1m_candles[-1]
        latest_volume = latest.get("volume") or 0
        if latest_volume <= 0:
            return False

        prior = recent_1m_candles[-4:-1]
        if not prior:
            return True

        prior_volumes = [candle.get("volume") or 0 for candle in prior]
        avg_prior_volume = sum(prior_volumes) / len(prior_volumes) if prior_volumes else 0
        previous_volume = prior_volumes[-1] if prior_volumes else 0
        minimum_needed = max(avg_prior_volume, previous_volume * 0.9)
        return latest_volume >= minimum_needed

    @staticmethod
    def _pending_watch_risk_reward_ok(pending):
        trigger_price = pending.get("trigger_price")
        invalidate_price = pending.get("invalidate_price")
        first_target = pending.get("first_target_price")
        if trigger_price is None or invalidate_price is None or first_target is None:
            return True

        risk = abs(float(trigger_price) - float(invalidate_price))
        reward = abs(float(first_target) - float(trigger_price))
        if risk <= 0:
            return False
        return reward >= (risk * 0.9)

    @staticmethod
    def _pending_watch_not_too_late(pending, latest_price):
        trigger_price = pending.get("trigger_price")
        first_target = pending.get("first_target_price")
        direction = pending.get("direction")
        if trigger_price is None or first_target is None or latest_price is None or direction not in {"CE", "PE"}:
            return True

        total_path = abs(float(first_target) - float(trigger_price))
        if total_path <= 0:
            return True

        covered = (
            float(latest_price) - float(trigger_price)
            if direction == "CE"
            else float(trigger_price) - float(latest_price)
        )
        return covered <= (total_path * 0.55)

    def _pending_watch_max_minutes(self, pending):
        if not pending:
            return 20

        if pending.get("hybrid_mode"):
            if self.instrument == "BANKNIFTY":
                return 18
            if self.instrument == "SENSEX":
                return 16
            return 30

        if self.instrument == "SENSEX":
            return 14
        if self.instrument == "BANKNIFTY":
            return 16
        return 20

    @staticmethod
    def _candle_close_strength(candle, direction):
        high = candle.get("high")
        low = candle.get("low")
        close = candle.get("close")
        if high is None or low is None or close is None:
            return 0.0

        range_size = float(high) - float(low)
        if range_size <= 0:
            return 0.0

        if direction == "CE":
            return (float(close) - float(low)) / range_size
        return (float(high) - float(close)) / range_size

    @staticmethod
    def _candle_body_ratio(candle):
        high = candle.get("high")
        low = candle.get("low")
        open_price = candle.get("open")
        close = candle.get("close")
        if None in {high, low, open_price, close}:
            return 0.0

        range_size = float(high) - float(low)
        if range_size <= 0:
            return 0.0
        return abs(float(close) - float(open_price)) / range_size

    def _pending_watch_quality_ok(self, pending, latest, previous):
        direction = pending.get("direction")
        if direction not in {"CE", "PE"}:
            return True, None

        close_strength = self._candle_close_strength(latest, direction)
        body_ratio = self._candle_body_ratio(latest)
        hybrid_mode = pending.get("hybrid_mode", False)
        fast_track_ready = pending.get("fast_track_ready", False)
        strong_watch_setup = pending.get("strong_watch_setup", False)
        minutes_since_watch = pending.get("minutes_since_watch", 0)
        trigger_price = pending.get("trigger_price")
        invalidate_price = pending.get("invalidate_price")

        min_close_strength = 0.52
        min_body_ratio = 0.18

        if self.instrument == "BANKNIFTY":
            min_close_strength = 0.6
            min_body_ratio = 0.24
        elif self.instrument == "SENSEX":
            min_close_strength = 0.58
            min_body_ratio = 0.22
        elif self.instrument == "NIFTY":
            min_close_strength = 0.55
            min_body_ratio = 0.2

        if hybrid_mode and strong_watch_setup:
            min_close_strength -= 0.04
            min_body_ratio -= 0.03
        if fast_track_ready:
            min_close_strength -= 0.03

        if close_strength < min_close_strength:
            return False, "1m trigger close weak"
        if body_ratio < min_body_ratio:
            return False, "1m trigger body weak"

        if self.instrument == "NIFTY" and invalidate_price is not None and trigger_price is not None:
            total_risk = abs(float(trigger_price) - float(invalidate_price))
            if total_risk > 0:
                current_buffer = (
                    float(latest["close"]) - float(invalidate_price)
                    if direction == "CE"
                    else float(invalidate_price) - float(latest["close"])
                )
                if current_buffer < (total_risk * 0.28):
                    return False, "1m trigger too close to invalidation"

        if self.instrument in {"BANKNIFTY", "SENSEX"} and minutes_since_watch >= 8:
            if close_strength < (min_close_strength + 0.06):
                return False, "late 1m trigger close not strong enough"

        prev_close = previous.get("close")
        prev_open = previous.get("open")
        if prev_close is not None and prev_open is not None:
            if direction == "CE" and float(prev_close) < float(prev_open) and close_strength < 0.62 and self.instrument == "NIFTY":
                return False, "1m trigger against fresh opposite candle"
            if direction == "PE" and float(prev_close) > float(prev_open) and close_strength < 0.62 and self.instrument == "NIFTY":
                return False, "1m trigger against fresh opposite candle"

        return True, None

    @staticmethod
    def _pending_watch_has_caution(pending, caution):
        cautions = pending.get("cautions") or []
        return caution in cautions

    def _pending_watch_conflicts_too_high(self, pending):
        setup = (pending.get("signal_type") or "NONE").upper()
        cautions = set(pending.get("cautions") or [])
        blockers = set(pending.get("blockers") or [])
        pressure_conflict_level = (pending.get("pressure_conflict_level") or "NONE").upper()
        time_regime = (pending.get("time_regime") or "").upper()

        if setup != "BREAKOUT_CONFIRM":
            return False, None

        opposite_pressure = "opposite_pressure" in cautions
        weak_participation = (
            "participation_weak" in cautions
            or "participation_delta_missing" in cautions
        )
        retest_wait = "late_confirmation_wait_retest" in cautions
        expiry_mode = "expiry_day_mode" in cautions

        if opposite_pressure and weak_participation and pressure_conflict_level in {"MILD", "MODERATE", "HIGH"}:
            return True, "breakout watch has opposite pressure with weak participation"

        if retest_wait and opposite_pressure:
            return True, "breakout watch is already in retest-wait mode"

        if self.instrument == "SENSEX" and time_regime == "LATE_DAY" and (
            opposite_pressure or pressure_conflict_level != "NONE"
        ):
            return True, "late-day SENSEX breakout watch too conflicted"

        if self.instrument in {"NIFTY", "BANKNIFTY"} and expiry_mode and opposite_pressure and weak_participation:
            return True, "expiry breakout watch has poor option confirmation"

        if "direction_present_but_filters_incomplete" in blockers and opposite_pressure and weak_participation:
            return True, "direction incomplete with opposite pressure and weak participation"

        return False, None

    def _set_pending_entry_watch(self, watch_payload, balanced_pro, candle_5m):
        if not watch_payload:
            self._clear_pending_entry_watch()
            return

        direction = watch_payload.get("direction")
        trigger_price = watch_payload.get("trigger_price")
        if direction not in {"CE", "PE"} or trigger_price is None:
            self._clear_pending_entry_watch()
            return

        score = watch_payload.get("score") or 0
        entry_score = watch_payload.get("entry_score") or 0
        watch_bucket = watch_payload.get("watch_bucket")
        signal_grade = watch_payload.get("signal_grade")
        confidence = watch_payload.get("confidence")
        setup = watch_payload.get("setup")
        hybrid_mode = Config.HYBRID_MANUAL_MODE
        fast_track_ready = (
            score >= 74
            and entry_score >= 68
            and watch_bucket == "WATCH_CONFIRMATION_PENDING"
            and confidence in {"MEDIUM", "HIGH"}
        )
        if hybrid_mode and setup in {"REVERSAL", "BREAKOUT_CONFIRM", "RETEST", "TRAP_REVERSAL"}:
            fast_track_ready = fast_track_ready or (
                score >= 70
                and entry_score >= 60
                and confidence in {"MEDIUM", "HIGH"}
            )
        strong_watch_setup = (
            score >= 76
            and entry_score >= 70
            and watch_bucket in {"WATCH_CONFIRMATION_PENDING", "WATCH_SETUP"}
        )
        if hybrid_mode:
            strong_watch_setup = strong_watch_setup or (
                score >= 70
                and entry_score >= 60
                and watch_bucket in {"WATCH_CONFIRMATION_PENDING", "WATCH_SETUP", "WATCH_CONTEXT"}
                and setup in {"BREAKOUT_CONFIRM", "RETEST", "REVERSAL", "TRAP_REVERSAL"}
            )

        if watch_bucket == "WATCH_CONTEXT" and not strong_watch_setup:
            self._clear_pending_entry_watch()
            return
        min_context_score = 64 if hybrid_mode else 68
        min_entry_score = 54 if hybrid_mode else 60
        if score < min_context_score and entry_score < min_entry_score:
            self._clear_pending_entry_watch()
            return

        temp_pending = {
            "instrument": self.instrument,
            "signal_type": watch_payload.get("setup"),
            "cautions": list(watch_payload.get("cautions") or []),
            "blockers": list(watch_payload.get("blockers") or []),
            "pressure_conflict_level": (
                getattr(self.strategy, "last_pressure_conflict_level", None)
                or (balanced_pro or {}).get("pressure_conflict_level")
            ),
            "time_regime": (balanced_pro or {}).get("time_regime"),
        }
        conflicts_too_high, _ = self._pending_watch_conflicts_too_high(temp_pending)
        if conflicts_too_high:
            self._clear_pending_entry_watch()
            return

        self.pending_entry_watch = {
            "instrument": self.instrument,
            "direction": direction,
            "trigger_price": float(trigger_price),
            "invalidate_price": watch_payload.get("invalidate_price"),
            "first_target_price": watch_payload.get("first_target_price"),
            "score": score,
            "entry_score": entry_score,
            "confidence": confidence,
            "signal_type": watch_payload.get("setup"),
            "signal_grade": signal_grade,
            "watch_bucket": watch_payload.get("watch_bucket"),
            "quality": (balanced_pro or {}).get("quality"),
            "time_regime": (balanced_pro or {}).get("time_regime"),
            "created_at": candle_5m["time"],
            "last_checked_minute": None,
            "reason": watch_payload.get("reason"),
            "fast_track_ready": fast_track_ready,
            "strong_watch_setup": strong_watch_setup,
            "hybrid_mode": hybrid_mode,
            "cautions": list(watch_payload.get("cautions") or []),
            "blockers": list(watch_payload.get("blockers") or []),
            "pressure_conflict_level": (
                getattr(self.strategy, "last_pressure_conflict_level", None)
                or (balanced_pro or {}).get("pressure_conflict_level")
            ),
        }

    def _evaluate_pending_entry_watch(self, recent_1m_candles):
        if not self.pending_entry_watch or len(recent_1m_candles) < 2:
            return None

        pending = self.pending_entry_watch
        latest = recent_1m_candles[-1]
        previous = recent_1m_candles[-2]
        created_at = pending["created_at"]
        if created_at is not None and latest["time"] is not None:
            if getattr(created_at, "tzinfo", None) is None and getattr(latest["time"], "tzinfo", None) is not None:
                created_at = created_at.replace(tzinfo=latest["time"].tzinfo)
            elif getattr(created_at, "tzinfo", None) is not None and getattr(latest["time"], "tzinfo", None) is None:
                latest = {**latest, "time": latest["time"].replace(tzinfo=created_at.tzinfo)}
                previous = {
                    **previous,
                    "time": previous["time"].replace(tzinfo=created_at.tzinfo)
                    if getattr(previous["time"], "tzinfo", None) is None else previous["time"],
                }
        minutes_since_watch = int((latest["time"] - created_at).total_seconds() // 60)
        if minutes_since_watch < 1:
            return None
        max_watch_minutes = self._pending_watch_max_minutes(pending)
        if minutes_since_watch > max_watch_minutes:
            return {"status": "EXPIRED", "reason": "1m confirmation window expired"}
        pending["minutes_since_watch"] = minutes_since_watch

        conflicts_too_high, conflict_reason = self._pending_watch_conflicts_too_high(pending)
        if conflicts_too_high:
            return {"status": "INVALIDATED", "reason": conflict_reason}

        trigger_price = pending["trigger_price"]
        invalidate_price = pending.get("invalidate_price")
        direction = pending["direction"]
        fast_track_ready = pending.get("fast_track_ready", False)
        hybrid_mode = pending.get("hybrid_mode", False)
        one_min_buffer = 2 if self.instrument == "NIFTY" else 5
        if fast_track_ready:
            one_min_buffer = 1 if self.instrument == "NIFTY" else 3
        elif hybrid_mode:
            one_min_buffer = 1 if self.instrument == "NIFTY" else 2
        if self.instrument == "BANKNIFTY" and minutes_since_watch >= 6:
            one_min_buffer = max(one_min_buffer, 4)
        if self.instrument == "SENSEX" and minutes_since_watch >= 6:
            one_min_buffer = max(one_min_buffer, 3)

        if not self._pending_watch_risk_reward_ok(pending):
            return {"status": "INVALIDATED", "reason": "Watch risk-reward not attractive for 1m trigger"}

        if direction == "CE":
            if invalidate_price is not None and latest["low"] <= invalidate_price:
                return {"status": "INVALIDATED", "reason": "Watch invalidated before 1m trigger"}
            trigger_hit = latest["close"] > trigger_price and latest["high"] >= trigger_price + one_min_buffer
            body_ok = latest["close"] >= latest["open"]
            follow_through_ok = latest["close"] > previous["high"] or (
                previous["close"] > trigger_price and latest["close"] >= previous["close"]
            )
            if hybrid_mode:
                trigger_hit = trigger_hit or (latest["high"] >= trigger_price and latest["close"] >= trigger_price)
                follow_through_ok = follow_through_ok or latest["close"] >= trigger_price
        else:
            if invalidate_price is not None and latest["high"] >= invalidate_price:
                return {"status": "INVALIDATED", "reason": "Watch invalidated before 1m trigger"}
            trigger_hit = latest["close"] < trigger_price and latest["low"] <= trigger_price - one_min_buffer
            body_ok = latest["close"] <= latest["open"]
            follow_through_ok = latest["close"] < previous["low"] or (
                previous["close"] < trigger_price and latest["close"] <= previous["close"]
            )
            if hybrid_mode:
                trigger_hit = trigger_hit or (latest["low"] <= trigger_price and latest["close"] <= trigger_price)
                follow_through_ok = follow_through_ok or latest["close"] <= trigger_price

        volume_ok = self._one_minute_trigger_volume_ok(recent_1m_candles)
        if hybrid_mode and pending.get("strong_watch_setup") and latest.get("volume", 0) > 0:
            volume_ok = True if latest["volume"] >= max(previous.get("volume", 0) * 0.8, 1) else volume_ok
        if fast_track_ready and trigger_hit and volume_ok:
            if direction == "CE":
                body_ok = body_ok or latest["high"] >= trigger_price + (one_min_buffer * 2)
                follow_through_ok = follow_through_ok or latest["close"] >= trigger_price
            else:
                body_ok = body_ok or latest["low"] <= trigger_price - (one_min_buffer * 2)
                follow_through_ok = follow_through_ok or latest["close"] <= trigger_price
        elif hybrid_mode and trigger_hit and volume_ok:
            body_ok = body_ok or abs((latest["close"] or 0) - (latest["open"] or 0)) > 0
        if trigger_hit and not self._pending_watch_not_too_late(pending, latest["close"]):
            return {"status": "INVALIDATED", "reason": "1m trigger arrived too late after move extension"}
        if trigger_hit:
            quality_ok, quality_reason = self._pending_watch_quality_ok(pending, latest, previous)
            if not quality_ok:
                return {"status": "INVALIDATED", "reason": quality_reason}
        if not trigger_hit or not body_ok or not follow_through_ok or not volume_ok:
            return None

        return {
            "status": "TRIGGERED",
            "price": latest["close"],
            "time": latest["time"],
            "reason": "1m trigger confirmed after 5m watch",
        }

    def _maybe_fire_pending_entry_watch(self, latest_5m_candle):
        if not self.pending_entry_watch:
            return

        recent_1m_candles = self.db_reader.fetch_recent_candles_1m(self.instrument, limit=6)
        if len(recent_1m_candles) < 2:
            return

        latest_1m = recent_1m_candles[-1]
        minute_key = latest_1m["time"]
        if self.pending_entry_watch.get("last_checked_minute") == minute_key:
            return
        self.pending_entry_watch["last_checked_minute"] = minute_key

        evaluation = self._evaluate_pending_entry_watch(recent_1m_candles)
        if not evaluation:
            return

        if evaluation["status"] in {"EXPIRED", "INVALIDATED"}:
            self._clear_pending_entry_watch()
            return

        pending = self.pending_entry_watch
        signal = pending["direction"]
        trigger_price = pending["trigger_price"]
        strike = None
        option_contract = None
        if self.option_data:
            strike, _ = self.strike_selector.select_strike_with_reason(
                price=evaluation["price"],
                signal=signal,
                volume_signal="NORMAL",
                strategy_score=pending["score"],
                pressure_metrics=None,
            )
            option_contract = self._get_option_contract_snapshot(strike, signal, before_ts=evaluation["time"])

        self.notifier.send_entry_trigger_notification(
            {
                "instrument": self.instrument,
                "signal": signal,
                "strike": strike,
                "confidence": pending.get("confidence"),
                "signal_type": pending.get("signal_type"),
                "signal_grade": pending.get("signal_grade"),
                "price": round(evaluation["price"], 2),
                "trigger_price": trigger_price,
            }
        )

        balanced_pro = {
            "quality": pending.get("quality"),
            "setup": pending.get("signal_type"),
            "tradability": "ACTION",
            "time_regime": pending.get("time_regime"),
        }
        self.strategy.last_entry_plan = {
            "entry_above": trigger_price if signal == "CE" else None,
            "entry_below": trigger_price if signal == "PE" else None,
            "invalidate_price": pending.get("invalidate_price"),
            "first_target_price": pending.get("first_target_price"),
        }
        self._safe_save_signal_issued(
            ts=evaluation["time"],
            signal=signal,
            price=(option_contract or {}).get("ltp") if option_contract else evaluation["price"],
            strike=strike,
            reason=(
                f"1m entry trigger after 5m watch | setup={pending.get('signal_type')} "
                f"| watch_bucket={pending.get('watch_bucket')} | base_reason={pending.get('reason')}"
            ),
            balanced_pro=balanced_pro,
            oi_mode="WATCH_TO_1M_TRIGGER",
            telegram_sent=Config.ENABLE_ALERTS,
            monitor_started=True,
            entry_window_end=evaluation["time"] + timedelta(minutes=Config.SIGNAL_VALIDITY_MINUTES),
            underlying_price=evaluation["price"],
            option_contract=option_contract,
            strike_reason="watch-trigger strike selection",
            option_data_source=self.option_data_source,
        )
        self._start_trade_monitor(signal, latest_5m_candle, evaluation["price"], balanced_pro, strike)
        self.signals_generated += 1
        self._clear_pending_entry_watch()

    def _evaluate_trade_monitor(self, recent_1m_candles, recent_5m_candles):
        """Generate momentum-focused option-buyer guidance using 1m and 5m structure."""
        if not self.active_trade_monitor or not recent_1m_candles:
            return None

        signal = self.active_trade_monitor["signal"]
        entry_price = self.active_trade_monitor["entry_price"]
        strike = self.active_trade_monitor.get("strike")
        latest_1m = recent_1m_candles[-1]
        prior_window = recent_1m_candles[-6:-1] if len(recent_1m_candles) >= 6 else recent_1m_candles[:-1]
        if not prior_window:
            prior_window = recent_1m_candles
        micro_high = max(candle["high"] for candle in prior_window)
        micro_low = min(candle["low"] for candle in prior_window)
        recent_closes = [candle["close"] for candle in recent_1m_candles[-3:]]
        last_two_closes = [candle["close"] for candle in recent_1m_candles[-2:]]
        last_close = latest_1m["close"]
        option_snapshot = self._get_option_contract_snapshot(strike, signal, before_ts=latest_1m["time"])
        option_price = option_snapshot.get("ltp") if option_snapshot and option_snapshot.get("ltp") is not None else last_close
        vwap_value = self.vwap.get_vwap()
        time_regime = self.strategy.last_time_regime
        pnl_points = (
            float(option_price) - float(entry_price)
            if option_price is not None and entry_price is not None
            else None
        )
        recent_5m_candles = recent_5m_candles or []
        active_5m = recent_5m_candles[-1] if recent_5m_candles else None
        prior_5m = recent_5m_candles[-2] if len(recent_5m_candles) >= 2 else active_5m

        if option_price is not None:
            max_fav = self.active_trade_monitor.get("max_favorable_option_ltp")
            max_adv = self.active_trade_monitor.get("max_adverse_option_ltp")
            self.active_trade_monitor["max_favorable_option_ltp"] = option_price if max_fav is None else max(max_fav, option_price)
            self.active_trade_monitor["max_adverse_option_ltp"] = option_price if max_adv is None else min(max_adv, option_price)

        if signal == "CE":
            structure_break = last_close < micro_low
            vwap_break = vwap_value is not None and last_close < vwap_value
            momentum_strong = last_close >= micro_high or (len(recent_closes) >= 3 and recent_closes[-1] > recent_closes[-2] > recent_closes[-3])
            momentum_fading = len(recent_closes) >= 3 and recent_closes[-1] < recent_closes[-2] and recent_closes[-2] <= recent_closes[-3]
            five_min_break = prior_5m is not None and last_close < prior_5m["low"]
            structure_text = f"1m intact | prev5m {'safe' if not five_min_break else 'under test'}"
        else:
            structure_break = last_close > micro_high
            vwap_break = vwap_value is not None and last_close > vwap_value
            momentum_strong = last_close <= micro_low or (len(recent_closes) >= 3 and recent_closes[-1] < recent_closes[-2] < recent_closes[-3])
            momentum_fading = len(recent_closes) >= 3 and recent_closes[-1] > recent_closes[-2] and recent_closes[-2] >= recent_closes[-3]
            five_min_break = prior_5m is not None and last_close > prior_5m["high"]
            structure_text = f"1m intact | prev5m {'safe' if not five_min_break else 'under test'}"

        minutes_active = max(1, int((latest_1m["time"] - self.active_trade_monitor["entry_time"]).total_seconds() // 60))
        self.active_trade_monitor["minutes_active"] = minutes_active

        guidance = "HOLD_WITH_TRAIL"
        reason = "Normal pullback is okay. Trend is acceptable while the recent 1-minute structure holds."

        if structure_break and vwap_break:
            guidance = "EXIT_BIAS"
            reason = "Recent 1-minute structure broke with VWAP loss. This is more than a normal pullback."
        elif five_min_break and minutes_active >= 3:
            guidance = "EXIT_BIAS"
            reason = "Previous 5-minute structure broke against your trade. Momentum may be losing control."
        elif minutes_active >= 5 and (pnl_points is not None and pnl_points <= 0):
            guidance = "TIME_DECAY_RISK"
            reason = "Move has not expanded yet. For an option buyer, slow trades can become time-decay trades."
        elif momentum_fading and (pnl_points is not None and pnl_points > 0):
            guidance = "BOOK_PARTIAL"
            reason = "Profit is available and momentum is slowing. Partial booking can reduce pressure."
        elif momentum_strong and (pnl_points is not None and pnl_points > 0):
            guidance = "HOLD_STRONG"
            reason = "Fresh momentum is expanding. Do not react to a normal 1-minute pullback."
        elif momentum_fading:
            guidance = "MOMENTUM_PAUSE"
            reason = "Momentum paused, but this still looks like a normal pullback unless structure breaks."
        elif len(last_two_closes) == 2 and ((signal == "CE" and last_two_closes[-1] <= last_two_closes[-2]) or (signal == "PE" and last_two_closes[-1] >= last_two_closes[-2])):
            guidance = "NORMAL_PULLBACK"
            reason = "A small opposite candle is normal. No real structure damage yet."

        if time_regime == "ENDGAME" and (pnl_points is not None and pnl_points > 0) and guidance in {"HOLD_STRONG", "HOLD_WITH_TRAIL"}:
            guidance = "BOOK_PARTIAL"
            reason = "Late-day move is in profit; partial booking is safer for an option buyer."

        return {
            "instrument": self.instrument,
            "signal": signal,
            "guidance": guidance,
            "reason": reason,
            "structure": structure_text,
            "price": last_close,
            "option_price": option_price,
            "entry_price": entry_price,
            "entry_underlying_price": self.active_trade_monitor.get("entry_underlying_price"),
            "option_bid": option_snapshot.get("top_bid_price") if option_snapshot else None,
            "option_ask": option_snapshot.get("top_ask_price") if option_snapshot else None,
            "option_spread": option_snapshot.get("spread") if option_snapshot else None,
            "strike": strike,
            "pnl_points": pnl_points,
            "max_favorable_ltp": self.active_trade_monitor.get("max_favorable_option_ltp"),
            "max_adverse_ltp": self.active_trade_monitor.get("max_adverse_option_ltp"),
            "quality": self.active_trade_monitor["quality"],
            "time_regime": time_regime,
            "heikin_ashi": (self.strategy.last_heikin_ashi or {}).get("bias"),
        }

    def _maybe_send_trade_monitor_update(self, latest_5m_candle):
        """Send Telegram/manual monitor update every minute after a live signal."""
        if not self.active_trade_monitor:
            return

        recent_1m_candles = self.db_reader.fetch_recent_candles_1m(self.instrument, limit=20)
        if not recent_1m_candles:
            return

        latest_1m = recent_1m_candles[-1]
        minute_key = latest_1m["time"]
        if self.active_trade_monitor["last_notified_minute"] == minute_key:
            return

        recent_5m_candles = self.db_writer.fetch_recent_candles_5m(self.instrument, limit=3)
        monitor_data = self._evaluate_trade_monitor(recent_1m_candles, recent_5m_candles)
        if not monitor_data:
            return

        self.active_trade_monitor["last_notified_minute"] = minute_key
        self._safe_save_trade_monitor_event(minute_key, monitor_data)
        self._safe_save_option_signal_outcome(minute_key, monitor_data)
        self.notifier.send_trade_monitor_update(monitor_data)

        if monitor_data["guidance"] == "EXIT_BIAS" or self.active_trade_monitor["minutes_active"] >= 20:
            self.active_trade_monitor = None

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

        signal_type = (self.strategy.last_signal_type or "NONE").upper()
        signal_grade = (self.strategy.last_signal_grade or "SKIP").upper()
        confidence = (self.strategy.last_confidence or "LOW").upper()
        regime = (self.strategy.last_regime or "UNKNOWN").upper()
        score = float(self.strategy.last_entry_score or self.strategy.last_score or 0)

        if signal_type == "CONTINUATION" and not Config.ALLOW_CONTINUATION_ENTRY:
            breakout_memory = getattr(self.strategy, "breakout_memory", None) or {}
            recent_breakout = bool(
                breakout_memory
                and candle_time is not None
                and breakout_memory.get("session_day") == candle_time.date()
                and int((candle_time - breakout_memory.get("time")).total_seconds() // 60) <= 45
            )
            if not (recent_breakout and signal_grade in {"A", "A+"} and confidence == "HIGH" and score >= 78):
                return False
        if self.instrument == "SENSEX":
            return SensexActionableRules.should_allow_signal(
                signal_type=signal_type,
                signal_grade=signal_grade,
                confidence=confidence,
                regime=regime,
                candle_time=candle_time,
                score=self.strategy.last_score,
                entry_score=self.strategy.last_entry_score,
                pressure_conflict_level=self.strategy.last_pressure_conflict_level,
            )
        if signal_type not in Config.OPTION_BUYER_ALERT_TYPES:
            return False
        if (
            self.instrument == "BANKNIFTY"
            and BankNiftyActionableRules.should_allow_b_grade_breakout(
                signal_type=signal_type,
                signal_grade=signal_grade,
                confidence=confidence,
                regime=regime,
                candle_time=candle_time,
                score=score,
                pressure_conflict_level=self.strategy.last_pressure_conflict_level,
            )
        ):
            return True
        if self.instrument == "NIFTY" and NiftyActionableRules.should_allow_signal(
            signal_type=signal_type,
            signal_grade=signal_grade,
            confidence=confidence,
            regime=regime,
            score=score,
            pressure_conflict_level=self.strategy.last_pressure_conflict_level,
        ):
            return True
        if signal_type == "BREAKOUT_CONFIRM" and signal_grade == "B" and confidence in {"MEDIUM", "HIGH"} and score >= 80:
            return True
        if signal_grade not in Config.OPTION_BUYER_ALERT_GRADES:
            return False
        if confidence not in {"MEDIUM", "HIGH"}:
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
            "context_score": self.strategy.last_context_score,
            "entry_score": self.strategy.last_entry_score,
            "decision_state": decision_state,
            "watch_bucket": self.strategy.last_watch_bucket,
            "pressure_conflict_level": self.strategy.last_pressure_conflict_level,
            "confidence_summary": self.strategy.last_confidence_summary,
            "pressure_summary": None,
        }

    @staticmethod
    def _safe_ratio(numerator, denominator):
        if denominator in (None, 0):
            return 0.0
        return float(numerator) / float(denominator)

    @staticmethod
    def _participation_row_weight(distance_from_atm):
        distance = abs(distance_from_atm if distance_from_atm is not None else 99)
        weights = {
            0: 1.8,
            1: 1.25,
            2: 0.75,
        }
        return weights.get(distance, 0.35)

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
            near_put = pressure_metrics.get("near_put_pressure_ratio")
            near_call = pressure_metrics.get("near_call_pressure_ratio")
            if near_put is not None and near_call is not None:
                bits.append(f"near PE {near_put} / CE {near_call}")

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
            "ce_top_ask_price": atm_ce.get("top_ask_price") if atm_ce else None,
            "pe_top_bid_price": atm_pe.get("top_bid_price") if atm_pe else None,
            "pe_top_ask_price": atm_pe.get("top_ask_price") if atm_pe else None,
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
            "ce_top_ask_price": atm_ce.get("top_ask_price") if atm_ce else None,
            "pe_top_bid_price": atm_pe.get("top_bid_price") if atm_pe else None,
            "pe_top_ask_price": atm_pe.get("top_ask_price") if atm_pe else None,
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
            if signal is None and reason and reason.startswith("Option-buyer filter blocked live alert"):
                actionable_block_reason = "option_buyer_filter"

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
            )
            self.db_writer.insert_trade_monitor_event_1m(row)
        except Exception as e:
            self._log(f"DB save error (trade monitor): {e}")

    def _safe_save_option_signal_outcome(self, ts, monitor_data):
        """Persist option-premium outcome snapshots for fired signals."""
        try:
            if not self.active_trade_monitor:
                return
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
                int(self.active_trade_monitor.get("minutes_active") or 0),
                monitor_data.get("guidance"),
                monitor_data.get("reason"),
            )
            self.db_writer.insert_option_signal_outcome_1m(row)
        except Exception as e:
            self._log(f"DB save error (option outcome): {e}")

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
        except Exception as e:
            self._log(f"DB save error (signal issued): {e}")

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

        context_bits = [
            f"Bias={balanced_pro.get('bias')}",
            f"Setup={setup}",
            f"Regime={self.strategy.last_regime}",
            f"Bucket={watch_bucket}",
        ]
        context = " | ".join(bit for bit in context_bits if bit and "None" not in bit)
        if candidate_signal in {"CE", "PE"}:
            action_hint = "Candidate mila hai. Entry tabhi lena jab candle close hold kare aur chart bhi support kare."
        elif setup == "RETEST":
            action_hint = "Retest hold kare tabhi entry socho. Break ho to skip karo."
        elif setup == "REVERSAL":
            action_hint = "Reversal hai. Next candle confirm kare tabhi entry lena."
        elif setup == "BREAKOUT_CONFIRM":
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

        return {
            "instrument": self.instrument,
            "direction": direction,
            "setup": setup,
            "signal_grade": self.strategy.last_signal_grade,
            "confidence": self.strategy.last_confidence,
            "confidence_summary": self.strategy.last_confidence_summary,
            "score": self.strategy.last_context_score or self.strategy.last_score,
            "entry_score": self.strategy.last_entry_score,
            "price": round(price, 2) if price is not None else None,
            "trigger_price": round(trigger_price, 2) if trigger_price is not None else None,
            "invalidate_price": round(invalidate_price, 2) if invalidate_price is not None else None,
            "first_target_price": self.strategy.last_entry_plan.get("first_target_price"),
            "blockers": blockers,
            "cautions": list(self.strategy.last_cautions or []),
            "watch_bucket": watch_bucket,
            "context": f"{context} | ContextScore={self.strategy.last_context_score} | EntryScore={self.strategy.last_entry_score}",
            "reason": candidate_reason,
            "action_hint": action_hint,
            "entry_if": entry_if,
            "avoid_if": avoid_if,
            "participation_read": participation_read,
            "pressure_read": pressure_summary.get("summary") if pressure_summary else None,
            "key": (
                self.instrument,
                direction,
                setup,
            ),
        }

    def _maybe_send_watch_alert(self, watch_payload):
        if not watch_payload or not Config.ENABLE_ALERTS:
            return

        now_ts = time_module.time()
        state = self.watch_alert_state.get(watch_payload["key"])
        if state and now_ts - state["time"] < 20 * 60:
            score_improved = (watch_payload.get("score") or 0) >= state.get("score", 0) + 5
            entry_score_improved = (watch_payload.get("entry_score") or 0) >= state.get("entry_score", 0) + 4
            blockers_reduced = len(watch_payload.get("blockers") or []) < state.get("blocker_count", 99)
            if not any([score_improved, entry_score_improved, blockers_reduced]):
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

    def _process_5m_candle(self, candle_5m):
        """Process 5-minute candle and generate signal"""
        price = candle_5m["close"]
        
        # Update intraday ranges
        self._update_intraday_ranges(candle_5m)
        
        # Update indicators
        vwap_value = self.vwap.update(candle_5m)
        atr_value = self.atr.update(candle_5m)
        buffer = self.atr.get_buffer()
        
        # Update volume analysis
        self.volume.update(candle_5m)
        volume_signal = self.volume.get_volume_signal(candle_5m["volume"])
        
        # Update OI analysis
        if self.option_data:
            self.oi.update(price, self.option_data.get("ce_oi", 0), self.option_data.get("pe_oi", 0))

        oi_signal = self.oi.get_oi_signal()
        oi_bias = self.oi.get_bias()
        pressure_metrics = self.pressure.analyze(self.option_data) if self.option_data else None
        oi_ladder_data = self._build_oi_ladder_context(price)
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
        trend_15m = self._derive_15m_trend_from_5m(recent_candles_5m)
        
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
            can_trade=True if Config.TEST_MODE else self.time_utils.can_trade(),
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
        )
        
        candidate_signal = signal
        candidate_reason = reason
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
                    entry_plan=self.strategy.last_entry_plan
                )
                
                # Apply ML filter
                should_take, ml_reason, ml_prob = self.ml_filter.should_take_signal(ml_features)
                
                if not should_take:
                    # ML filtered out the signal
                    signal = None
                    reason = f"ML Filtered ({ml_reason})"
                    print(f"[Signal Service] {self.instrument}: {reason}")
                else:
                    print(f"[Signal Service] {self.instrument}: ML Approved ({ml_reason})")
                    
            except Exception as e:
                print(f"[Signal Service] ML processing error: {e}")
                ml_features = None
        
        signal, reason = self._apply_signal_cooldowns(signal, reason)
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
                )
                selected_option_contract = self._get_option_contract_snapshot(selected_strike, signal, before_ts=candle_5m.get("close_time") or candle_5m["time"])

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
        if self.strategy.last_blockers:
            enriched_reason += f" | blockers={', '.join(self.strategy.last_blockers)}"
        if self.strategy.last_cautions:
            enriched_reason += f" | cautions={', '.join(self.strategy.last_cautions)}"
        if strike_reason:
            enriched_reason += f" | strike_reason={strike_reason}"

        oi_mode = "OI_ONLY_FALLBACK" if fallback_context and fallback_context["fallback_used"] else "FULL_OPTION_BAND"

        # Save decision
        self._safe_save_strategy_decision(
            ts=candle_5m["time"],
            price=price,
            signal=candidate_signal,
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
            signal=candidate_signal or "NO_TRADE",
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
            option_signal_price = (selected_option_contract or {}).get("ltp") if selected_option_contract else price
            print(f"\n[Signal Service] SIGNAL GENERATED: {signal}")
            print(f"Score: {self.strategy.last_score} | Confidence: {self.strategy.last_confidence}")
            print(f"Strike: {selected_strike} | Reason: {reason}")
            print(f"Price: {option_signal_price} | Spot: {price} | Time: {candle_5m['time']}")
            self.notifier.send_trade_notification(
                {
                    "instrument": self.instrument,
                    "signal": signal,
                    "strike": selected_strike,
                    "confidence": self.strategy.last_confidence,
                    "confidence_summary": self.strategy.last_confidence_summary,
                    "signal_type": self.strategy.last_signal_type,
                    "signal_grade": self.strategy.last_signal_grade,
                    "price": round(option_signal_price, 2) if option_signal_price is not None else None,
                    "spot_price": round(price, 2) if price is not None else None,
                    "trigger_price": self.strategy.last_entry_plan.get("entry_above") if signal == "CE" else self.strategy.last_entry_plan.get("entry_below"),
                    "invalidate_price": self.strategy.last_entry_plan.get("invalidate_price"),
                    "first_target_price": self.strategy.last_entry_plan.get("first_target_price"),
                    "time_regime": balanced_pro["time_regime"],
                    "reason": enriched_reason,
                    "pressure_read": (balanced_pro.get("pressure_summary") or {}).get("summary"),
                    "oi_read": (oi_ladder_data or {}).get("oi_summary"),
                }
            )
            self._safe_save_signal_issued(
                ts=candle_5m["time"],
                signal=signal,
                price=option_signal_price,
                strike=selected_strike,
                reason=enriched_reason,
                balanced_pro=balanced_pro,
                oi_mode=oi_mode,
                telegram_sent=Config.ENABLE_ALERTS,
                monitor_started=True,
                entry_window_end=(
                    candle_5m["close_time"] + timedelta(minutes=Config.SIGNAL_VALIDITY_MINUTES)
                    if candle_5m.get("close_time") else None
                ),
                underlying_price=price,
                option_contract=selected_option_contract,
                strike_reason=strike_reason,
                option_data_source=self.option_data_source,
            )
            
            # Save ML features for training (async, don't block)
            if ml_features:
                self._safe_save_ml_features(ml_features, ml_prob)
            
            self._clear_pending_entry_watch()
            self._start_trade_monitor(signal, candle_5m, price, balanced_pro, selected_strike)
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
            return False, f"Latest 5m candle is stale ({int(candle_age)}s old)"

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
                    time_module.sleep(30)
                    continue

                if self.data_pause_active:
                    self._log("Data stream healthy again. Resuming signal generation.")
                    self.data_pause_active = False
                    self.last_data_pause_reason = None
                    self.watchdog.touch({"phase": "resumed"})

                # Get latest 5-minute candle from database
                self._log("DEBUG Fetching candles from DB...")
                latest_candles = self.db_reader.fetch_recent_candles_5m(
                    instrument=self.instrument,
                    limit=1
                )
                self._log(f"DEBUG Fetched {len(latest_candles)} candles")
                
                if not latest_candles:
                    self._log("No candles found in database, waiting...")
                    time_module.sleep(30)
                    continue
                
                latest_candle = latest_candles[0]
                current_time = self.time_utils.now_ist()
                
                # Check if this is a newly closed 5m candle (use close time, not start time)
                candle_time = latest_candle["time"]
                effective_close_time = self._effective_candle_close_time(latest_candle)
                time_diff = current_time - effective_close_time
                is_new = time_diff < timedelta(minutes=6) and candle_time != self.last_processed_5m_ts
                
                # DEBUG logging
                if not is_new:
                    self._log(
                        f"DEBUG Candle Check | Time Diff: {time_diff} | Candle TS: {candle_time} | "
                        f"Effective Close: {effective_close_time} | Last Processed: {self.last_processed_5m_ts} | "
                        f"Will Process: {is_new}"
                    )
                
                if is_new:
                    # Process the candle
                    self._log(f"Processing new 5m candle | Time: {candle_time} | Price: {latest_candle['close']}")
                    self._current_candle_time = candle_time
                    self._refresh_option_data_if_due()
                    self._process_5m_candle(latest_candle)
                    self.last_processed_5m_ts = candle_time

                current_minute = current_time.replace(second=0, microsecond=0)
                if current_minute != self.last_monitor_check_minute:
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
