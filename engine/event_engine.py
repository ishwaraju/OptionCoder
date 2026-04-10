import time
from datetime import timedelta
from utils.time_utils import TimeUtils
from config import Config
from core.candle_manager import CandleManager
from core.vwap import VWAPCalculator
from core.orb import ORB
from core.atr import ATRCalculator
from core.volume_analyzer import VolumeAnalyzer
from core.oi_analyzer import OIAnalyzer
from core.option_chain import OptionChain
from core.oi_ladder import OILadder
from core.pressure_analyzer import PressureAnalyzer
from core.historical_backfill import HistoricalBackfill
from core.spread_filter import SpreadFilter
from core.oi_quote_confirmation import OIQuoteConfirmation
from core.connection_manager import ConnectionManager
from strategy.breakout_strategy import BreakoutStrategy
from strategy.strike_selector import StrikeSelector
from db.writer import DBWriter
from utils.logger import TradeLogger
from utils.notifier import Notifier
from utils.shutdown_manager import ShutdownManager


class EventEngine:
    def __init__(self, live_feed, watchdog=None):
        self.time_utils = TimeUtils()
        self.live_feed = live_feed
        self.watchdog = watchdog

        # Core components
        self.candle_manager = CandleManager()
        self.vwap = VWAPCalculator()
        self.orb = ORB()
        self.atr = ATRCalculator()
        self.volume = VolumeAnalyzer()
        self.oi = OIAnalyzer()
        self.option_chain = OptionChain()
        self.oi_ladder = OILadder()
        self.pressure = PressureAnalyzer()
        self.historical_backfill = HistoricalBackfill()
        self.connection_manager = ConnectionManager(
            live_feed=self.live_feed,
            candle_manager=self.candle_manager,
            historical_backfill=self.historical_backfill,
        )
        self.spread_filter = SpreadFilter()
        self.oi_quote_confirmation = OIQuoteConfirmation()
        self.strategy = BreakoutStrategy()
        self.strike_selector = StrikeSelector()

        # DB
        self.instrument = Config.SYMBOL
        self.db = DBWriter()
        self.audit_logger = TradeLogger()
        self.notifier = Notifier()
        
        # Shutdown Manager
        self.shutdown_manager = ShutdownManager()

        # Timers
        self.last_option_fetch = 0
        self.last_heartbeat = 0

        # Store previous values for OI build-up
        self.prev_price = None
        self.prev_total_oi = None

        self.option_data = None
        self.signal_cooldown_remaining = 0
        self.last_emitted_signal = None
        self.session_decisions = []
        self.pending_entry_watch = None
        self.day_high_5m = None
        self.day_low_5m = None
        self.opening_range_30_high = None
        self.opening_range_30_low = None

        self._restore_indicator_state()

    def _print_startup_status(self):
        live_snapshot = self.live_feed.get_live_data()
        diagnostics = live_snapshot.get("connection_diagnostics", {})
        print("Startup Summary:")
        print("Instrument:", self.instrument)
        print("Feed Connected:", live_snapshot.get("feed_connected"))
        print("Reconnect Attempts:", diagnostics.get("reconnect_attempts", 0))
        print("Connection Stability:", diagnostics.get("stability_score"))

    def _is_debug_enabled(self):
        return Config.DEBUG or Config.CONSOLE_MODE == "DETAILED"

    def _debug_print(self, *args, **kwargs):
        if self._is_debug_enabled():
            print(*args, **kwargs)

    def _print_heartbeat(self):
        live_snapshot = self.live_feed.get_live_data()
        diagnostics = live_snapshot.get("connection_diagnostics", {})
        print(
            "\nHeartbeat | IST:",
            self.time_utils.current_time(),
            "| feed_connected:",
            live_snapshot.get("feed_connected"),
            "| data_age:",
            live_snapshot.get("data_age_seconds"),
            "| reconnects:",
            diagnostics.get("reconnect_attempts", 0),
            "| stability:",
            diagnostics.get("stability_score"),
        )

    def _touch_watchdog(self, extra_status=None):
        if not self.watchdog:
            return

        live_snapshot = self.live_feed.get_live_data()
        status = {
            "feed_connected": live_snapshot.get("feed_connected"),
            "data_age_seconds": live_snapshot.get("data_age_seconds"),
            "price": live_snapshot.get("price"),
            "reconnect_attempts": live_snapshot.get("reconnect_attempts"),
        }
        if extra_status:
            status.update(extra_status)
        self.watchdog.touch(status)

    def _print_completed_1m_summary(self, candle_1m):
        print(
            "Completed 1m |",
            candle_1m["datetime"],
            "| O:",
            candle_1m["open"],
            "H:",
            candle_1m["high"],
            "L:",
            candle_1m["low"],
            "C:",
            candle_1m["close"],
            "| vol:",
            candle_1m["volume"],
            "| ticks:",
            candle_1m.get("tick_count", 0),
        )

    def _print_completed_5m_summary(self, candle_5m, volume_signal, vwap_value, atr_value):
        print(
            "\n5m Closed |",
            candle_5m["time"],
            "| O:",
            candle_5m["open"],
            "H:",
            candle_5m["high"],
            "L:",
            candle_5m["low"],
            "C:",
            candle_5m["close"],
            "| vol:",
            candle_5m["volume"],
            "| ticks:",
            candle_5m.get("tick_count", 0),
            "| vol_sig:",
            volume_signal,
            "| vwap:",
            vwap_value,
            "| atr:",
            atr_value,
        )

    def _print_recovery_summary(self, recovered_count):
        diagnostics = self.live_feed.get_live_data().get("connection_diagnostics", {})
        print(
            "Recovery Summary | backfilled:",
            recovered_count,
            "| reconnects:",
            diagnostics.get("reconnect_attempts", 0),
            "| stability:",
            diagnostics.get("stability_score"),
        )

    def _restore_indicator_state(self):
        if not self.db.enabled:
            return

        recent_candles = self.db.fetch_recent_candles_5m(
            instrument=self.instrument,
            limit=Config.STATE_RECOVERY_5M_BARS,
        )
        if not recent_candles:
            # Try historical backfill if DB has no data
            self._debug_print("No DB candles found, trying historical backfill for warmup")
            warmup_candles = self.historical_backfill.warmup_indicators_on_startup(
                num_bars=Config.STATE_RECOVERY_5M_BARS
            )
            if warmup_candles:
                for candle in warmup_candles:
                    self.candle_manager.ingest_completed_5m(candle)
                    self.vwap.update(candle)
                    self.atr.update(candle)
                    self.volume.update(candle)
                    self.orb.add_candle(candle)
                self._debug_print(f"Warmed up with {len(warmup_candles)} historical 5m bars")
            return

        for candle in recent_candles:
            self.candle_manager.ingest_completed_5m(candle)
            self.vwap.update(candle)
            self.atr.update(candle)
            self.volume.update(candle)
            self.orb.add_candle(candle)

        if not self.orb.is_orb_ready():
            self.orb.calculate_orb()

        self._debug_print(f"Recovered {len(recent_candles)} historical 5m candles for indicator warmup")

    # =========================
    # DB Save Helpers
    # =========================
    def _safe_save_1m_candle(self, candle_1m):
        try:
            row = (
                candle_1m["datetime"],     # ts
                self.instrument,           # instrument
                float(candle_1m["open"]),
                float(candle_1m["high"]),
                float(candle_1m["low"]),
                float(candle_1m["close"]),
                int(candle_1m["volume"]),
            )
            self.db.insert_candle_1m(row)
        except Exception as e:
            print("DB save error (1m candle):", e)

    def _safe_save_5m_candle(self, candle_5m):
        try:
            row = (
                candle_5m["time"],         # ts (5m slot time)
                self.instrument,           # instrument
                float(candle_5m["open"]),
                float(candle_5m["high"]),
                float(candle_5m["low"]),
                float(candle_5m["close"]),
                int(candle_5m["volume"]),
            )
            self.db.insert_candle_5m(row)
        except Exception as e:
            print("DB save error (5m candle):", e)

    def _safe_save_oi_snapshot(self, ts, price):
        """
        Save minute-level OI snapshot.
        """
        try:
            if not self.option_data:
                return

            row = (
                ts,
                self.instrument,
                float(price) if price is not None else None,
                int(self.option_data.get("ce_oi")) if self.option_data.get("ce_oi") is not None else None,
                int(self.option_data.get("pe_oi")) if self.option_data.get("pe_oi") is not None else None,
                int(self.option_data.get("ce_volume")) if self.option_data.get("ce_volume") is not None else None,
                int(self.option_data.get("pe_volume")) if self.option_data.get("pe_volume") is not None else None,
                int(self.option_data.get("ce_volume_band")) if self.option_data.get("ce_volume_band") is not None else None,
                int(self.option_data.get("pe_volume_band")) if self.option_data.get("pe_volume_band") is not None else None,
                float(self.option_data.get("pcr")) if self.option_data.get("pcr") is not None else None,
            )
            self.db.insert_oi_1m(row)
        except Exception as e:
            print("DB save error (OI snapshot):", e)

    def _safe_save_option_band_snapshots(self, ts):
        try:
            if not self.option_data:
                return

            band_snapshots = self.option_data.get("band_snapshots", [])
            if not band_snapshots:
                return

            rows = []
            for snapshot in band_snapshots:
                rows.append(
                    (
                        ts,
                        self.instrument,
                        int(snapshot["atm_strike"]),
                        int(snapshot["strike"]),
                        int(snapshot["distance_from_atm"]),
                        snapshot["option_type"],
                        int(snapshot["security_id"]) if snapshot.get("security_id") is not None else None,
                        int(snapshot["oi"]) if snapshot.get("oi") is not None else None,
                        int(snapshot["volume"]) if snapshot.get("volume") is not None else None,
                        float(snapshot["ltp"]) if snapshot.get("ltp") is not None else None,
                        float(snapshot["iv"]) if snapshot.get("iv") is not None else None,
                    )
                )

            self.db.insert_option_band_snapshots_1m(rows)
        except Exception as e:
            print("DB save error (option band snapshots):", e)

    def _safe_save_strategy_decision(
            self,
            ts,
            price,
            signal,
            reason,
            volume_signal,
            oi_bias,
            oi_trend,
            build_up,
            pressure_metrics,
            ce_delta_total,
            pe_delta_total,
            pcr,
            orb_high,
            orb_low,
            vwap,
            atr,
            strike,
    ):
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
            )
            self.db.insert_strategy_decision_5m(row)
        except Exception as e:
            print("DB save error (strategy decision):", e)

    def _arm_entry_watch(self, candle_5m, signal, selected_strike, orb_high, orb_low):
        if not Config.ENABLE_1M_TRIGGER or not signal:
            return
        if self.strategy.last_signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST"}:
            return

        setup_price = candle_5m["close"]
        base_level = orb_high if signal == "CE" else orb_low
        if base_level is None:
            base_level = setup_price

        if signal == "CE":
            trigger_price = max(setup_price, base_level)
        else:
            trigger_price = min(setup_price, base_level)

        self.pending_entry_watch = {
            "signal": signal,
            "signal_type": self.strategy.last_signal_type,
            "signal_grade": self.strategy.last_signal_grade,
            "confidence": self.strategy.last_confidence,
            "strike": selected_strike,
            "trigger_price": trigger_price,
            "setup_price": setup_price,
            "valid_until": candle_5m["close_time"] + timedelta(minutes=Config.ENTRY_TRIGGER_VALIDITY_MINUTES)
            if candle_5m.get("close_time") is not None else None,
        }

    def _process_1m_entry_trigger(self, candle_1m):
        watch = self.pending_entry_watch
        if not watch:
            return

        close_time = candle_1m.get("close_time")
        if watch.get("valid_until") is not None and close_time is not None and close_time > watch["valid_until"]:
            self.pending_entry_watch = None
            return

        body = abs(candle_1m["close"] - candle_1m["open"])
        bullish_body = candle_1m["close"] > candle_1m["open"] and body >= Config.ENTRY_TRIGGER_MIN_BODY
        bearish_body = candle_1m["close"] < candle_1m["open"] and body >= Config.ENTRY_TRIGGER_MIN_BODY
        tick_count_ok = candle_1m.get("tick_count", 0) >= 2

        price_triggered = False
        if (
                watch["signal"] == "CE"
                and bullish_body
                and tick_count_ok
                and candle_1m["high"] >= watch["trigger_price"]
                and candle_1m["close"] >= watch["trigger_price"]
        ):
            price_triggered = True

        if (
                watch["signal"] == "PE"
                and bearish_body
                and tick_count_ok
                and candle_1m["low"] <= watch["trigger_price"]
                and candle_1m["close"] <= watch["trigger_price"]
        ):
            price_triggered = True

        if not price_triggered:
            return

        # OI/Quote confirmation check
        oi_confirmed, oi_reason, oi_confidence = self.oi_quote_confirmation.should_trigger_1m_confirmation(
            signal=watch["signal"],
            current_time=candle_1m["datetime"]
        )
        
        # Spread filter check
        spread_confirmed = True
        spread_reason = "Spread OK"
        if self.option_data:
            should_filter, filter_reason, alternative_strike = self.spread_filter.should_filter_signal(
                option_chain_data=self.option_data,
                signal=watch["signal"],
                selected_strike=watch["strike"]
            )
            if should_filter:
                spread_confirmed = False
                spread_reason = filter_reason

        # Final trigger decision
        triggered = price_triggered and oi_confirmed and spread_confirmed
        
        if not triggered:
            self._debug_print(f"\n1m trigger blocked - Price: {price_triggered}, OI: {oi_confirmed}, Spread: {spread_confirmed}")
            self._debug_print(f"OI Reason: {oi_reason}")
            self._debug_print(f"Spread Reason: {spread_reason}")
            return

        print("\n=========== 1M ENTRY TRIGGER ===========")
        print("Time:", close_time or candle_1m["datetime"])
        print("Signal:", watch["signal"])
        print("Signal Type:", watch["signal_type"])
        print("Signal Grade:", watch["signal_grade"])
        print("Trigger Price:", watch["trigger_price"])
        print("1m Candle:", candle_1m)
        print(f"OI Confirmation: {oi_confirmed} (confidence: {oi_confidence:.2f}) - {oi_reason}")
        print(f"Spread Confirmation: {spread_confirmed} - {spread_reason}")
        
        # Get confirmation summary for logging
        confirmation_summary = self.oi_quote_confirmation.get_confirmation_summary()
        print("Confirmation Summary:", confirmation_summary)
        print("========================================\n")

        self.notifier.send_entry_trigger_notification(
            {
                "signal": watch["signal"],
                "strike": watch["strike"],
                "confidence": watch["confidence"],
                "signal_type": watch["signal_type"],
                "signal_grade": watch["signal_grade"],
                "price": candle_1m["close"],
                "trigger_price": watch["trigger_price"],
                "oi_confirmed": oi_confirmed,
                "oi_confidence": oi_confidence,
                "oi_reason": oi_reason,
                "spread_confirmed": spread_confirmed,
                "spread_reason": spread_reason,
                "confirmation_summary": confirmation_summary
            }
        )
        self.pending_entry_watch = None

    def _recover_missing_candles(self, connection_state):
        if not (connection_state["recovered"] and connection_state["missing_candles"]):
            return

        for candle in connection_state["missing_candles"]:
            self.candle_manager.add_historical_candle(candle)
            self._safe_save_1m_candle(candle)
            candle_5m = self.candle_manager.add_minute_candle(candle)
            if candle_5m:
                self._safe_save_5m_candle(candle_5m)
                self.vwap.update(candle_5m)
                self.atr.update(candle_5m)
                self.volume.update(candle_5m)
                self.orb.add_candle(candle_5m)

        self._print_recovery_summary(len(connection_state["missing_candles"]))
        self._touch_watchdog({"phase": "recovered", "backfilled": len(connection_state["missing_candles"])})

    def _refresh_option_data_if_due(self):
        if time.time() - self.last_option_fetch <= 5:
            return

        latest_option_data = self.option_chain.fetch_option_chain()
        self.last_option_fetch = time.time()
        if latest_option_data:
            self.option_data = latest_option_data

    def _save_completed_1m_artifacts(self, completed_1m):
        self._safe_save_1m_candle(completed_1m)
        self._safe_save_oi_snapshot(completed_1m["datetime"], completed_1m["close"])
        self._safe_save_option_band_snapshots(completed_1m["datetime"])

        if not self.option_data:
            return

        self.oi_quote_confirmation.add_oi_snapshot(
            timestamp=completed_1m["datetime"],
            ce_oi=self.option_data.get("ce_oi"),
            pe_oi=self.option_data.get("pe_oi"),
            ce_volume=self.option_data.get("ce_volume"),
            pe_volume=self.option_data.get("pe_volume"),
            price=completed_1m["close"]
        )
        self.oi_quote_confirmation.add_quote_snapshot(
            timestamp=completed_1m["datetime"],
            option_data=self.option_data
        )

    def _update_intraday_ranges(self, candle_5m):
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

    def _build_oi_ladder_context(self, price):
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
            print("\n===== OI LADDER =====")
            print("Support:", oi_ladder_data["support"])
            print("Resistance:", oi_ladder_data["resistance"])
            print("Trend:", oi_ladder_data["trend"])
            print("Build-up:", oi_ladder_data["build_up"])
            print("OI Shift:", oi_ladder_data["oi_shift"])
            print("=====================")

        return oi_ladder_data

    def _resolve_orb_levels(self, candle_5m):
        if Config.TEST_MODE:
            return candle_5m["high"] + 10, candle_5m["low"] - 10

        self.orb.add_candle(candle_5m)
        if self.orb.is_orb_ready():
            return self.orb.get_orb_levels()

        orb_high, orb_low = self.orb.calculate_orb()
        if orb_high is None or orb_low is None:
            return self.orb.get_fallback_levels(self.candle_manager.get_all_5min_candles())
        return orb_high, orb_low

    def _apply_signal_cooldowns(self, signal, reason):
        signal, reason = self.connection_manager.apply_reconnect_cooldown(
            signal=signal,
            reason=reason,
            score=self.strategy.last_score,
            signal_type=self.strategy.last_signal_type,
            confidence=self.strategy.last_confidence,
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

    def _process_completed_5m_candle(self, candle_5m, price):
        self._update_intraday_ranges(candle_5m)
        self._safe_save_5m_candle(candle_5m)

        vwap_value = self.vwap.update(candle_5m)
        atr_value = self.atr.update(candle_5m)
        buffer = self.atr.get_buffer()

        self.volume.update(candle_5m)
        volume_signal = self.volume.get_volume_signal(candle_5m["volume"])
        self._print_completed_5m_summary(candle_5m, volume_signal, vwap_value, atr_value)

        if self.option_data:
            self.oi.update(price, self.option_data.get("ce_oi", 0), self.option_data.get("pe_oi", 0))

        oi_signal = self.oi.get_oi_signal()
        oi_bias = self.oi.get_bias()
        pressure_metrics = self.pressure.analyze(self.option_data) if self.option_data else None
        oi_ladder_data = self._build_oi_ladder_context(price)
        orb_high, orb_low = self._resolve_orb_levels(candle_5m)

        if self._is_debug_enabled():
            print("Price:", price)
            print("VWAP:", vwap_value)
            print("ATR:", atr_value)
            print("Volume Signal:", volume_signal)
            print("OI Signal:", oi_signal)
            print("OI Bias:", oi_bias)
            print("ORB High:", orb_high)
            print("ORB Low:", orb_low)
            if pressure_metrics:
                print("Pressure Bias:", pressure_metrics["pressure_bias"])
                print("Near CE/PE Vol:", pressure_metrics["near_ce_volume"], "/", pressure_metrics["near_pe_volume"])
                print("Strongest CE/PE Strike:", pressure_metrics["strongest_ce_strike"], "/", pressure_metrics["strongest_pe_strike"])
            if self.option_data:
                print(
                    "Option Data Updated | PCR:",
                    self.option_data.get("pcr"),
                    "| CE IV:", self.option_data.get("ce_iv"),
                    "| PE IV:", self.option_data.get("pe_iv")
                )
                print("PCR:", self.option_data.get("pcr"))

        support = oi_ladder_data["support"] if oi_ladder_data else None
        resistance = oi_ladder_data["resistance"] if oi_ladder_data else None

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
        )
        signal, reason = self._apply_signal_cooldowns(signal, reason)

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
            selected_strike, strike_reason = self.strategy.expiry_rules.adjust_strike_choice(
                expiry_value=self.option_data.get("expiry"),
                signal=signal,
                strike=selected_strike,
                strike_reason=strike_reason,
                price=price,
                strategy_score=self.strategy.last_score,
                confidence=self.strategy.last_confidence,
            )

        score_factors_text = ", ".join(self.strategy.last_score_components)
        blockers_text = ", ".join(self.strategy.last_blockers)
        cautions_text = ", ".join(self.strategy.last_cautions)
        manual_guidance = self._get_manual_guidance(
            signal=signal,
            score=self.strategy.last_score,
            confidence=self.strategy.last_confidence,
            signal_type=self.strategy.last_signal_type,
            signal_grade=self.strategy.last_signal_grade,
            blockers=self.strategy.last_blockers,
            cautions=self.strategy.last_cautions,
        )
        signal_valid_till = (
            candle_5m["close_time"] + timedelta(minutes=Config.SIGNAL_VALIDITY_MINUTES)
            if signal and candle_5m.get("close_time") is not None
            else None
        )
        enriched_reason = reason
        if self.strategy.last_confidence:
            enriched_reason = f"{reason} | confidence={self.strategy.last_confidence} | regime={self.strategy.last_regime}"
        enriched_reason += f" | signal_type={self.strategy.last_signal_type} | signal_grade={self.strategy.last_signal_grade}"
        if blockers_text:
            enriched_reason += f" | blockers={blockers_text}"
        if cautions_text:
            enriched_reason += f" | cautions={cautions_text}"
        if strike_reason:
            enriched_reason += f" | strike_reason={strike_reason}"

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
            pcr=self.option_data.get("pcr") if self.option_data else None,
            orb_high=orb_high,
            orb_low=orb_low,
            vwap=vwap_value,
            atr=atr_value,
            strike=selected_strike,
        )

        self.audit_logger.log_decision(
            instrument=self.instrument,
            price=price,
            signal=signal or "NO_TRADE",
            strike=selected_strike,
            score=self.strategy.last_score,
            confidence=self.strategy.last_confidence,
            regime=self.strategy.last_regime,
            manual_guidance=manual_guidance,
            signal_valid_till=signal_valid_till,
            blockers=blockers_text,
            cautions=cautions_text,
            score_factors=score_factors_text,
            reason=reason,
            strike_reason=strike_reason,
        )
        self.session_decisions.append(
            {
                "time": candle_5m["time"],
                "signal": signal or "NO_TRADE",
                "score": self.strategy.last_score,
                "confidence": self.strategy.last_confidence,
                "regime": self.strategy.last_regime,
                "signal_type": self.strategy.last_signal_type,
                "signal_grade": self.strategy.last_signal_grade,
                "manual_guidance": manual_guidance,
                "blockers": blockers_text,
                "cautions": cautions_text,
            }
        )

        self.notifier.send_strategy_decision({
            "signal": signal or "NO_TRADE",
            "price": price,
            "score": self.strategy.last_score,
            "confidence": self.strategy.last_confidence,
            "regime": self.strategy.last_regime,
            "reason": reason,
            "manual_guidance": manual_guidance,
            "blockers": blockers_text,
            "cautions": cautions_text,
        })

        print(
            "\nDecision |",
            f"{signal or 'NO TRADE'} | score {self.strategy.last_score} | "
            f"conf {self.strategy.last_confidence} | regime {self.strategy.last_regime} | "
            f"type {self.strategy.last_signal_type} | grade {self.strategy.last_signal_grade} | "
            f"guidance {manual_guidance}"
        )
        if self._is_debug_enabled():
            print("Time:", self.time_utils.current_time())
            print("Price:", price)
            print("Expiry Day Mode:", "ON" if self.strategy.last_is_expiry_day else "OFF")
            print("Decision Reason:", reason)
            if signal_valid_till is not None:
                print("Signal Valid Till:", signal_valid_till)
            print("Day High / Low:", self.day_high_5m, "/", self.day_low_5m)
            print("Opening 30m High / Low:", self.opening_range_30_high, "/", self.opening_range_30_low)
            if self.strategy.last_blockers:
                print("Blockers:", ", ".join(self.strategy.last_blockers))
            if self.strategy.last_cautions:
                print("Caution:", ", ".join(self.strategy.last_cautions))

        if signal and self.option_data:
            print("Action:", f"TRADE NOW: {signal}")
            print("Recommended Strike:", selected_strike)
            print("Strike Reason:", strike_reason)
            print("Execution Note:", f"Take {signal} at strike {selected_strike}")
            if signal_valid_till is not None:
                print("Signal Ageing:", f"Use within {Config.SIGNAL_VALIDITY_MINUTES} minutes")
            self.notifier.send_trade_notification(
                {
                    "signal": signal,
                    "strike": selected_strike,
                    "confidence": self.strategy.last_confidence,
                    "signal_type": self.strategy.last_signal_type,
                    "signal_grade": self.strategy.last_signal_grade,
                }
            )
            self._arm_entry_watch(candle_5m, signal, selected_strike, orb_high, orb_low)

            if self._is_debug_enabled():
                print("\n================ SIGNAL ================")
                print("Time:", self.time_utils.current_time())
                print("Signal:", signal)
                print("Price:", price)
                print("Strike:", selected_strike)
                print("VWAP:", vwap_value)
                print("ORB High:", orb_high)
                print("ORB Low:", orb_low)
                print("PCR:", self.option_data.get("pcr"))
                print("OI Signal:", oi_signal)
                print("OI Trend:", oi_ladder_data["trend"] if oi_ladder_data else None)
                print("Build-up:", oi_ladder_data["build_up"] if oi_ladder_data else None)
                print("Pressure Bias:", pressure_metrics["pressure_bias"] if pressure_metrics else None)
                print("Strategy Score:", self.strategy.last_score)
                print("Signal Type:", self.strategy.last_signal_type)
                print("Signal Grade:", self.strategy.last_signal_grade)
                print("Confidence:", self.strategy.last_confidence)
                print("Regime:", self.strategy.last_regime)
                print("Score Factors:", ", ".join(self.strategy.last_score_components))
                if self.strategy.last_blockers:
                    print("Blockers:", ", ".join(self.strategy.last_blockers))
                if self.strategy.last_cautions:
                    print("Caution:", ", ".join(self.strategy.last_cautions))
                print("Volume Signal:", volume_signal)
                print("Strike Reason:", strike_reason)
                print("Reason:", reason)
                print("========================================\n")
        elif self._is_debug_enabled():
            print("Action: NO TRADE")
            if self.strategy.last_score_components:
                print("Context:", ", ".join(self.strategy.last_score_components))
            print("=======================================\n")

    def _handle_new_minute(self, candle_1m, price):
        self._debug_print("New 1m started:", candle_1m["datetime"], "| price:", candle_1m["open"])

        completed_1m = self.candle_manager.get_last_1min_candle()
        if not completed_1m or completed_1m.get("close_time") != candle_1m["datetime"]:
            return False

        if self._is_debug_enabled():
            self._print_completed_1m_summary(completed_1m)

        self._process_1m_entry_trigger(completed_1m)
        self._save_completed_1m_artifacts(completed_1m)

        candle_5m = self.candle_manager.add_minute_candle(completed_1m)
        if candle_5m:
            self._process_completed_5m_candle(candle_5m, price)

        return True

    def run(self):
        print("Event Engine Started...")
        print("Current IST Time:", self.time_utils.current_time())
        self._print_startup_status()
        self._touch_watchdog({"phase": "startup"})
        
        # Start shutdown manager
        self.shutdown_manager.start_shutdown_monitor()
        
        # Display shutdown info
        shutdown_status = self.shutdown_manager.get_shutdown_status()
        print(f"Bot will shutdown at: {shutdown_status['shutdown_time'].strftime('%H:%M:%S IST')}")
        if shutdown_status['auto_system_shutdown']:
            print(f"System will shutdown at: {shutdown_status['system_shutdown_time'].strftime('%H:%M:%S IST')}")

        while True:
            # =========================
            # Heartbeat every 30 sec
            # =========================
            if time.time() - self.last_heartbeat > 30:
                self._print_heartbeat()
                self._touch_watchdog({"phase": "heartbeat"})
                self.last_heartbeat = time.time()
                
                # Check shutdown status
                shutdown_status = self.shutdown_manager.get_shutdown_status()
                if shutdown_status['is_shutting_down']:
                    print("Shutdown in progress...")
                    break
                
                # Display time to shutdown
                if shutdown_status['time_to_shutdown']:
                    time_left = shutdown_status['time_to_shutdown']
                    hours, remainder = divmod(time_left.total_seconds(), 3600)
                    minutes, seconds = divmod(remainder, 60)
                    print(f"Time to shutdown: {int(hours)}h {int(minutes)}m {int(seconds)}s")

            # =========================
            # Market closed check
            # =========================
            if not self.time_utils.is_market_open() and not Config.TEST_MODE:
                print("Market Closed:", self.time_utils.current_time())
                if self.session_decisions:
                    self._write_session_summary()
                time.sleep(60)
                continue

            # =========================
            # Get live tick data
            # =========================
            live_data = self.live_feed.get_live_data()
            price = live_data.get("price")
            futures_volume = live_data.get("futures_volume", 0)
            futures_oi = live_data.get("oi", 0)
            data_age_seconds = live_data.get("data_age_seconds")
            feed_connected = live_data.get("feed_connected", True)
            last_tick_epoch = live_data.get("last_tick_epoch", 0)

            if price is None:
                self._touch_watchdog({"phase": "waiting_for_price"})
                time.sleep(1)
                continue

            last_any_tick_age_seconds = None
            if last_tick_epoch:
                last_any_tick_age_seconds = max(0.0, time.time() - last_tick_epoch)

            effective_data_age_seconds = data_age_seconds
            if last_any_tick_age_seconds is not None:
                if effective_data_age_seconds is None:
                    effective_data_age_seconds = last_any_tick_age_seconds
                else:
                    effective_data_age_seconds = min(effective_data_age_seconds, last_any_tick_age_seconds)

            connection_state = self.connection_manager.evaluate_feed_health(
                feed_connected=feed_connected,
                effective_data_age_seconds=effective_data_age_seconds,
            )
            if connection_state["skip_processing"]:
                self._touch_watchdog({"phase": "feed_stale"})
                time.sleep(1)
                continue

            self._recover_missing_candles(connection_state)

            self._refresh_option_data_if_due()

            # =========================
            # Tick → 1-min candle
            # =========================
            candle_1m, new_minute = self.candle_manager.add_tick(price, futures_volume)

            if new_minute:
                if not self._handle_new_minute(candle_1m, price):
                    time.sleep(1)
                    continue

            self._touch_watchdog({"phase": "loop_alive"})
            time.sleep(1)

    def _get_manual_guidance(self, signal, score, confidence, signal_type, signal_grade, blockers, cautions):
        if signal_type == "CONTINUATION" and not Config.ALLOW_CONTINUATION_ENTRY:
            return "WATCHLIST"
        if signal and signal_grade in ["A+", "A"] and "far_from_vwap" not in cautions and "opposite_pressure" not in cautions:
            return "ENTRY"
        if signal and signal_grade in ["B", "WATCH"]:
            return "ENTRY_SMALL"
        if signal and confidence in ["HIGH", "MEDIUM"] and ("far_from_vwap" in cautions or "near_resistance" in cautions or "near_support" in cautions):
            return "AVOID CHASE"
        if signal_type == "NONE" and score >= 60 and not blockers:
            return "WATCHLIST"
        if "opening_session" in cautions or "expiry_day_mode" in cautions:
            return "WAIT"
        if blockers:
            return "HIGH RISK"
        return "WATCHLIST"

    def _write_session_summary(self):
        total = len(self.session_decisions)
        if total == 0:
            return

        trade_signals = [d for d in self.session_decisions if d["signal"] in ["CE", "PE"]]
        no_trade = [d for d in self.session_decisions if d["signal"] == "NO_TRADE"]
        high_conf = [d for d in self.session_decisions if d["confidence"] == "HIGH"]
        expiry_skips = [d for d in self.session_decisions if "expiry" in d["blockers"]]

        summary_lines = [
            f"Session Summary - {self.time_utils.today_str()}",
            f"Instrument: {self.instrument}",
            f"Total decisions: {total}",
            f"Trade signals: {len(trade_signals)}",
            f"No-trade decisions: {len(no_trade)}",
            f"High-confidence decisions: {len(high_conf)}",
            f"Expiry filter skips: {len(expiry_skips)}",
        ]

        if self.session_decisions:
            summary_lines.append("Recent decisions:")
            for decision in self.session_decisions[-5:]:
                summary_lines.append(
                    f"{decision['time']} | {decision['signal']} | {decision['manual_guidance']} | "
                    f"score={decision['score']} | conf={decision['confidence']} | regime={decision['regime']} | "
                    f"type={decision['signal_type']} | grade={decision['signal_grade']}"
                )

        self.audit_logger.write_session_summary("\n".join(summary_lines) + "\n")
