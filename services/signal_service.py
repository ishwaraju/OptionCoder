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
from strategies.sensex import SensexActionableRules
from shared.utils.logger import TradeLogger
from shared.utils.notifier import Notifier
from shared.utils.instrument_profile import get_instrument_profile
from shared.utils.service_watchdog import ServiceWatchdog
from shared.utils.option_data_cache import OptionDataCache


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
        self.strategy = BreakoutStrategy()
        self.strike_selector = StrikeSelector()
        
        # Logging and notifications
        self.audit_logger = TradeLogger()
        self.notifier = Notifier()
        
        # State tracking
        self.running = False
        self.last_option_fetch = 0
        self.last_signal_time = 0
        self.signal_cooldown_remaining = 0
        self.last_emitted_signal = None
        self.session_decisions = []
        self.pending_entry_watch = None
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
        
        # Sleep detection
        self.last_loop_time = time_module.time()
        self.sleep_threshold_seconds = 90  # Avoid false positives from the normal 60s loop sleep
        
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

    def _handle_wake_from_sleep(self):
        """Handle recovery after system sleep/lock detected."""
        self._log("🔄 Recovering from system sleep...")
        
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
        
        # Refresh indicator state from recent history
        try:
            self._restore_indicator_state()
            self._log("   ✅ Restored indicator state from history")
        except Exception as e:
            self._log(f"   ⚠️  Could not restore indicators: {e}")
        
        self._log("🔄 Recovery complete. Resuming normal operation...")

    def _start_trade_monitor(self, signal, candle_5m, price, balanced_pro, selected_strike):
        """Start 1-minute manual trade monitor after a signal."""
        self.active_trade_monitor = {
            "signal": signal,
            "entry_time": candle_5m["time"],
            "entry_price": price,
            "strike": selected_strike,
            "quality": balanced_pro["quality"],
            "time_regime": balanced_pro["time_regime"],
            "last_notified_minute": None,
            "minutes_active": 0,
        }

    def _evaluate_trade_monitor(self, recent_1m_candles, recent_5m_candles):
        """Generate momentum-focused option-buyer guidance using 1m and 5m structure."""
        if not self.active_trade_monitor or not recent_1m_candles:
            return None

        signal = self.active_trade_monitor["signal"]
        entry_price = self.active_trade_monitor["entry_price"]
        latest_1m = recent_1m_candles[-1]
        prior_window = recent_1m_candles[-6:-1] if len(recent_1m_candles) >= 6 else recent_1m_candles[:-1]
        if not prior_window:
            prior_window = recent_1m_candles
        micro_high = max(candle["high"] for candle in prior_window)
        micro_low = min(candle["low"] for candle in prior_window)
        recent_closes = [candle["close"] for candle in recent_1m_candles[-3:]]
        last_two_closes = [candle["close"] for candle in recent_1m_candles[-2:]]
        last_close = latest_1m["close"]
        vwap_value = self.vwap.get_vwap()
        time_regime = self.strategy.last_time_regime
        pnl_points = last_close - entry_price if signal == "CE" else entry_price - last_close
        recent_5m_candles = recent_5m_candles or []
        active_5m = recent_5m_candles[-1] if recent_5m_candles else None
        prior_5m = recent_5m_candles[-2] if len(recent_5m_candles) >= 2 else active_5m

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
        elif minutes_active >= 5 and pnl_points <= 0:
            guidance = "TIME_DECAY_RISK"
            reason = "Move has not expanded yet. For an option buyer, slow trades can become time-decay trades."
        elif momentum_fading and pnl_points > 0:
            guidance = "BOOK_PARTIAL"
            reason = "Profit is available and momentum is slowing. Partial booking can reduce pressure."
        elif momentum_strong and pnl_points > 0:
            guidance = "HOLD_STRONG"
            reason = "Fresh momentum is expanding. Do not react to a normal 1-minute pullback."
        elif momentum_fading:
            guidance = "MOMENTUM_PAUSE"
            reason = "Momentum paused, but this still looks like a normal pullback unless structure breaks."
        elif len(last_two_closes) == 2 and ((signal == "CE" and last_two_closes[-1] <= last_two_closes[-2]) or (signal == "PE" and last_two_closes[-1] >= last_two_closes[-2])):
            guidance = "NORMAL_PULLBACK"
            reason = "A small opposite candle is normal. No real structure damage yet."

        if time_regime == "ENDGAME" and pnl_points > 0 and guidance in {"HOLD_STRONG", "HOLD_WITH_TRAIL"}:
            guidance = "BOOK_PARTIAL"
            reason = "Late-day move is in profit; partial booking is safer for an option buyer."

        return {
            "instrument": self.instrument,
            "signal": signal,
            "guidance": guidance,
            "reason": reason,
            "structure": structure_text,
            "price": last_close,
            "entry_price": entry_price,
            "pnl_points": pnl_points,
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
        self.notifier.send_trade_monitor_update(monitor_data)

        if monitor_data["guidance"] == "EXIT_BIAS" or self.active_trade_monitor["minutes_active"] >= 20:
            self.active_trade_monitor = None

    def _classify_base_bias(self, price, vwap_value, oi_bias, oi_ladder_data):
        """Balanced Pro layer 1: derive directional bias."""
        if vwap_value is None:
            return "NEUTRAL"

        oi_trend = oi_ladder_data["trend"] if oi_ladder_data else None
        build_up = oi_ladder_data["build_up"] if oi_ladder_data else None

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
        score = float(self.strategy.last_score or 0)

        if signal_type == "CONTINUATION" and not Config.ALLOW_CONTINUATION_ENTRY:
            return False
        if self.instrument == "SENSEX":
            return SensexActionableRules.should_allow_signal(
                signal_type=signal_type,
                signal_grade=signal_grade,
                confidence=confidence,
                regime=regime,
                candle_time=candle_time,
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
            )
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
        tradability = "TRADE" if actionable_signal else ("WATCH" if (signal or bias != "NEUTRAL") else "NO_TRADE")
        return {
            "bias": bias,
            "setup": setup,
            "quality": quality,
            "tradability": tradability,
            "time_regime": self.strategy.last_time_regime,
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
        price_change = 0 if self.prev_price is None else price - self.prev_price

        self.prev_price = price
        self.prev_total_oi = sum(ce_oi_ladder.values()) + sum(pe_oi_ladder.values())

        oi_ladder_data = self.oi_ladder.analyze(
            ce_oi_ladder,
            pe_oi_ladder,
            price_change,
            atm=self.option_data.get("atm"),
        )

        if self._is_debug_enabled():
            print("\n[Signal Service] OI Ladder Analysis:")
            print("Support:", oi_ladder_data["support"])
            print("Resistance:", oi_ladder_data["resistance"])
            print("Trend:", oi_ladder_data["trend"])
            print("Build-up:", oi_ladder_data["build_up"])

        return oi_ladder_data

    def _resolve_orb_levels(self, candle_5m):
        """Get ORB levels"""
        if Config.TEST_MODE:
            return candle_5m["high"] + 10, candle_5m["low"] - 10

        self.orb.add_candle(candle_5m)
        if self.orb.is_orb_ready():
            return self.orb.get_orb_levels()

        orb_high, orb_low = self.orb.calculate_orb()
        if orb_high is None or orb_low is None:
            return self.orb.get_fallback_levels([])
        return orb_high, orb_low

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
                float(monitor_data.get("price")) if monitor_data.get("price") is not None else None,
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

    def _safe_save_signal_issued(self, ts, signal, price, strike, reason, balanced_pro, oi_mode, telegram_sent=True, monitor_started=True, entry_window_end=None):
        """Persist only actual fired actionable signals to DB."""
        try:
            row = (
                ts,
                self.instrument,
                signal,
                float(price) if price is not None else None,
                int(strike) if strike is not None else None,
                int(self.strategy.last_score),
                balanced_pro["quality"] if balanced_pro else None,
                balanced_pro["setup"] if balanced_pro else None,
                balanced_pro["tradability"] if balanced_pro else None,
                balanced_pro["time_regime"] if balanced_pro else None,
                oi_mode,
                reason,
                bool(telegram_sent),
                bool(monitor_started),
                entry_window_end,
            )
            self.db_writer.insert_signal_issued(row)
        except Exception as e:
            self._log(f"DB save error (signal issued): {e}")

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
        )
        
        candidate_signal = signal
        candidate_reason = reason
        self._current_candle_time = candle_5m["time"]
        signal, reason = self._apply_signal_cooldowns(signal, reason)
        balanced_pro = self._build_balanced_pro_summary(
            base_bias,
            candidate_signal,
            fallback_context,
            pressure_metrics,
            actionable_signal=bool(signal),
        )

        # Strike selection
        selected_strike = None
        strike_reason = None
        if signal and self.option_data:
            selected_strike, strike_reason = self.strike_selector.select_strike_with_reason(
                price=price,
                signal=signal,
                volume_signal=volume_signal,
                strategy_score=self.strategy.last_score,
                pressure_metrics=pressure_metrics,
            )

        # Enrich reason
        enriched_reason = reason
        if self.strategy.last_confidence:
            enriched_reason = f"{reason} | confidence={self.strategy.last_confidence} | regime={self.strategy.last_regime}"
        enriched_reason += f" | signal_type={self.strategy.last_signal_type} | signal_grade={self.strategy.last_signal_grade}"
        enriched_reason += (
            f" | base_bias={balanced_pro['bias']} | setup={balanced_pro['setup']} "
            f"| quality={balanced_pro['quality']} | tradability={balanced_pro['tradability']} "
            f"| time_regime={balanced_pro['time_regime']}"
        )
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
            print(f"\n[Signal Service] SIGNAL GENERATED: {signal}")
            print(f"Score: {self.strategy.last_score} | Confidence: {self.strategy.last_confidence}")
            print(f"Strike: {selected_strike} | Reason: {reason}")
            print(f"Price: {price} | Time: {candle_5m['time']}")
            self._safe_save_signal_issued(
                ts=candle_5m["time"],
                signal=signal,
                price=price,
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
            )
            self._start_trade_monitor(signal, candle_5m, price, balanced_pro, selected_strike)
            self.signals_generated += 1
        else:
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
                
                # Sleep detection - check if system was asleep/locked
                current_loop_time = time_module.time()
                loop_gap = current_loop_time - self.last_loop_time
                if loop_gap > self.sleep_threshold_seconds:
                    self._log(f"⚠️  SYSTEM SLEEP DETECTED! Gap: {loop_gap:.1f}s ({loop_gap/60:.1f} min). Recovering...")
                    self._handle_wake_from_sleep()
                self.last_loop_time = current_loop_time
                
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
                if self.active_trade_monitor and current_minute != self.last_monitor_check_minute:
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
