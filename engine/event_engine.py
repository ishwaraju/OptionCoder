import time
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

        # Timers
        self.last_option_fetch = 0
        self.last_heartbeat = 0

        # Store previous values for OI build-up
        self.prev_price = None
        self.prev_total_oi = None

        self.option_data = None

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
                time.sleep(60)
                continue

            # =========================
            # Get live tick data
            # =========================
            live_data = self.live_feed.get_live_data()
            price = live_data.get("price")
            futures_volume = live_data.get("futures_volume", 0)
            futures_oi = live_data.get("oi", 0)

            if price is None:
                time.sleep(1)
                continue

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
                    signal, reason = self.strategy.generate_signal(
                        price=price,
                        orb_high=orb_high,
                        orb_low=orb_low,
                        vwap=vwap_value,
                        volume_signal=volume_signal,
                        oi_bias=oi_bias,
                        oi_trend=oi_ladder_data["trend"] if oi_ladder_data else None,
                        build_up=oi_ladder_data["build_up"] if oi_ladder_data else None,
                        can_trade=True if Config.TEST_MODE else self.time_utils.can_trade(),
                        buffer=buffer,
                        pressure_metrics=pressure_metrics,
                    )

                    selected_strike = None
                    if signal and self.option_data:
                        selected_strike = self.strike_selector.select_strike(
                            price=price,
                            signal=signal,
                            volume_signal=volume_signal,
                            strategy_score=self.strategy.last_score,
                            pressure_metrics=pressure_metrics,
                        )

                    self._safe_save_strategy_decision(
                        ts=candle_5m["time"],
                        price=price,
                        signal=signal,
                        reason=reason,
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

                    # ===== Signal Output =====
                    if signal and self.option_data:
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
                        print("Score Factors:", ", ".join(self.strategy.last_score_components))
                        print("Volume Signal:", volume_signal)
                        print("Reason:", reason)
                        print("========================================\n")

            time.sleep(1)
