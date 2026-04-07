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
from strategy.breakout_strategy import BreakoutStrategy
from strategy.strike_selector import StrikeSelector
from db.writer import DBWriter
from utils.logger import TradeLogger
from utils.notifier import Notifier


class EventEngine:
    def __init__(self, live_feed):
        self.time_utils = TimeUtils()
        self.live_feed = live_feed

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
        self.strategy = BreakoutStrategy()
        self.strike_selector = StrikeSelector()

        # DB
        self.instrument = Config.SYMBOL
        self.db = DBWriter()
        self.audit_logger = TradeLogger()
        self.notifier = Notifier()

        # Timers
        self.last_option_fetch = 0
        self.last_heartbeat = 0

        # Store previous values for OI build-up
        self.prev_price = None
        self.prev_total_oi = None

        self.option_data = None
        self.feed_stale = False
        self.last_stale_log = 0
        self.reconnect_cooldown_remaining = 0
        self.signal_cooldown_remaining = 0
        self.last_emitted_signal = None
        self.session_decisions = []
        self.day_high_5m = None
        self.day_low_5m = None
        self.opening_range_30_high = None
        self.opening_range_30_low = None

        self._restore_indicator_state()

    def _restore_indicator_state(self):
        if not self.db.enabled:
            return

        recent_candles = self.db.fetch_recent_candles_5m(
            instrument=self.instrument,
            limit=Config.STATE_RECOVERY_5M_BARS,
        )
        if not recent_candles:
            return

        for candle in recent_candles:
            self.candle_manager.five_min_candles.append(dict(candle))
            self.vwap.update(candle)
            self.atr.update(candle)
            self.volume.update(candle)
            self.orb.add_candle(candle)

        if not self.orb.is_orb_ready():
            self.orb.calculate_orb()

        print(f"Recovered {len(recent_candles)} historical 5m candles for indicator warmup")

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

    def run(self):
        print("Event Engine Started...")
        print("Current IST Time:", self.time_utils.current_time())

        while True:
            # =========================
            # Heartbeat every 30 sec
            # =========================
            if time.time() - self.last_heartbeat > 30:
                print("\nBot Running | Time:", self.time_utils.current_time())
                self.last_heartbeat = time.time()

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

            if price is None:
                time.sleep(1)
                continue

            if (
                    not Config.TEST_MODE
                    and (
                        not feed_connected
                        or data_age_seconds is None
                        or data_age_seconds > Config.LIVE_DATA_STALE_SECONDS
                    )
            ):
                if not self.feed_stale:
                    print("Live feed stale. Resetting open candles and waiting for fresh ticks...")
                    self.candle_manager.reset_incomplete_candles()
                    self.feed_stale = True
                    self.last_stale_log = time.time()
                elif time.time() - self.last_stale_log > 30:
                    print("Live feed still stale. Skipping candle generation...")
                    self.last_stale_log = time.time()
                time.sleep(1)
                continue

            if self.feed_stale:
                print("Live feed recovered. Resuming candle generation.")
                self.feed_stale = False
                self.reconnect_cooldown_remaining = Config.RECONNECT_COOLDOWN_BARS

            # Keep option chain fresh independently of 5m candle formation so
            # 1m OI snapshots can be saved with the latest CE/PE data.
            if time.time() - self.last_option_fetch > 5:
                latest_option_data = self.option_chain.fetch_option_chain()
                self.last_option_fetch = time.time()

                if latest_option_data:
                    self.option_data = latest_option_data

            # =========================
            # Tick → 1-min candle
            # =========================
            candle_1m, new_minute = self.candle_manager.add_tick(price, futures_volume)

            if new_minute:
                print("New 1-min Candle:", candle_1m)

                # Save 1m candle
                self._safe_save_1m_candle(candle_1m)

                # Save 1m OI snapshot (aligned with minute timestamp)
                self._safe_save_oi_snapshot(candle_1m["datetime"], price)
                self._safe_save_option_band_snapshots(candle_1m["datetime"])

                # =========================
                # 1-min → 5-min candle
                # =========================
                candle_5m = self.candle_manager.add_minute_candle(candle_1m)

                if candle_5m:
                    print("\n========== New 5-min Candle ==========")
                    print(candle_5m)

                    self.day_high_5m = candle_5m["high"] if self.day_high_5m is None else max(self.day_high_5m, candle_5m["high"])
                    self.day_low_5m = candle_5m["low"] if self.day_low_5m is None else min(self.day_low_5m, candle_5m["low"])
                    if candle_5m["time"].time() < self.time_utils._parse_clock("09:45"):
                        self.opening_range_30_high = (
                            candle_5m["high"] if self.opening_range_30_high is None
                            else max(self.opening_range_30_high, candle_5m["high"])
                        )
                        self.opening_range_30_low = (
                            candle_5m["low"] if self.opening_range_30_low is None
                            else min(self.opening_range_30_low, candle_5m["low"])
                        )

                    # Save 5m candle
                    self._safe_save_5m_candle(candle_5m)

                    # ===== Indicators =====
                    vwap_value = self.vwap.update(candle_5m)
                    atr_value = self.atr.update(candle_5m)
                    buffer = self.atr.get_buffer()

                    self.volume.update(candle_5m)
                    volume_signal = self.volume.get_volume_signal(candle_5m["volume"])

                    # ===== OI Analyzer =====
                    if self.option_data:
                        self.oi.update(
                            price,
                            self.option_data.get("ce_oi", 0),
                            self.option_data.get("pe_oi", 0)
                        )

                    oi_signal = self.oi.get_oi_signal()
                    oi_bias = self.oi.get_bias()
                    pressure_metrics = self.pressure.analyze(self.option_data) if self.option_data else None

                    # ===== OI Ladder =====
                    oi_ladder_data = None
                    if self.option_data:
                        ce_oi_ladder = self.option_data.get("ce_oi_ladder", {})
                        pe_oi_ladder = self.option_data.get("pe_oi_ladder", {})

                        # Price change
                        price_change = 0
                        if self.prev_price:
                            price_change = price - self.prev_price

                        self.prev_price = price
                        self.prev_total_oi = sum(ce_oi_ladder.values()) + sum(pe_oi_ladder.values())

                        oi_ladder_data = self.oi_ladder.analyze(
                            ce_oi_ladder,
                            pe_oi_ladder,
                            price_change,
                            atm=self.option_data.get("atm"),
                        )

                    if Config.CONSOLE_MODE == "DETAILED":
                        print("\n===== OI LADDER =====")
                        print("Support:", oi_ladder_data["support"])
                        print("Resistance:", oi_ladder_data["resistance"])
                        print("Trend:", oi_ladder_data["trend"])
                        print("Build-up:", oi_ladder_data["build_up"])
                        print("OI Shift:", oi_ladder_data["oi_shift"])
                        print("=====================")

                    # ===== ORB =====
                    if Config.TEST_MODE:
                        orb_high = candle_5m["high"] + 10
                        orb_low = candle_5m["low"] - 10
                    else:
                        self.orb.add_candle(candle_5m)

                        if not self.orb.is_orb_ready():
                            orb_high, orb_low = self.orb.calculate_orb()
                            if orb_high is None or orb_low is None:
                                orb_high, orb_low = self.orb.get_fallback_levels(
                                    self.candle_manager.get_all_5min_candles()
                                )
                        else:
                            orb_high, orb_low = self.orb.get_orb_levels()

                    # ===== Debug Prints =====
                    if Config.CONSOLE_MODE == "DETAILED":
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

                    # ===== Strategy =====
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
                        expiry=self.option_data.get("expiry") if self.option_data else None,
                    )

                    if self.reconnect_cooldown_remaining > 0:
                        signal = None
                        reason = (
                            f"Reconnect cooldown active ({self.reconnect_cooldown_remaining} bars left)"
                            f" | score={self.strategy.last_score}"
                        )
                        self.reconnect_cooldown_remaining -= 1

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
                            "manual_guidance": manual_guidance,
                            "blockers": blockers_text,
                            "cautions": cautions_text,
                        }
                    )

                    # ===== Decision Output =====
                    print("\n============= DECISION =============")
                    print("Time:", self.time_utils.current_time())
                    print("Price:", price)
                    print("Expiry Day Mode:", "ON" if self.strategy.last_is_expiry_day else "OFF")
                    print("Regime:", self.strategy.last_regime)
                    print("Strategy Score:", self.strategy.last_score)
                    print("Confidence:", self.strategy.last_confidence)
                    print("Decision Reason:", reason)
                    print("Manual Guidance:", manual_guidance)
                    if signal_valid_till is not None:
                        print("Signal Valid Till:", signal_valid_till)
                    print("Day High / Low:", self.day_high_5m, "/", self.day_low_5m)
                    print("Opening 30m High / Low:", self.opening_range_30_high, "/", self.opening_range_30_low)
                    if self.strategy.last_blockers:
                        print("Blockers:", ", ".join(self.strategy.last_blockers))
                    if self.strategy.last_cautions:
                        print("Caution:", ", ".join(self.strategy.last_cautions))
                    print(
                        "Decision Line:",
                        f"{signal or 'NO TRADE'} | score {self.strategy.last_score} | "
                        f"conf {self.strategy.last_confidence} | regime {self.strategy.last_regime}"
                    )

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
                            }
                        )

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
                    else:
                        print("Action: NO TRADE")
                        if self.strategy.last_score_components:
                            print("Context:", ", ".join(self.strategy.last_score_components))
                        print("=======================================\n")

            time.sleep(1)

    def _get_manual_guidance(self, signal, score, confidence, blockers, cautions):
        if signal and confidence == "HIGH" and "far_from_vwap" not in cautions and "opposite_pressure" not in cautions:
            return "ENTRY OK"
        if signal and confidence in ["HIGH", "MEDIUM"] and ("far_from_vwap" in cautions or "near_resistance" in cautions or "near_support" in cautions):
            return "AVOID CHASE"
        if signal and confidence == "MEDIUM":
            return "REDUCE SIZE"
        if "opening_session" in cautions or "expiry_day_mode" in cautions:
            return "WAIT"
        if blockers:
            return "HIGH RISK"
        return "WAIT"

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
                    f"score={decision['score']} | conf={decision['confidence']} | regime={decision['regime']}"
                )

        self.audit_logger.write_session_summary("\n".join(summary_lines) + "\n")
