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
from strategy.breakout_strategy import BreakoutStrategy
from strategy.strike_selector import StrikeSelector


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
        self.strategy = BreakoutStrategy()
        self.strike_selector = StrikeSelector()

        # Timers
        self.last_option_fetch = 0
        self.last_heartbeat = 0

        # Store previous values for OI build-up
        self.prev_price = None
        self.prev_total_oi = None

        self.option_data = None

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

            # =========================
            # Tick → 1-min candle
            # =========================
            candle_1m, new_minute = self.candle_manager.add_tick(price, futures_volume)

            if new_minute:
                print("New 1-min Candle:", candle_1m)

                # =========================
                # 1-min → 5-min candle
                # =========================
                candle_5m = self.candle_manager.add_minute_candle(candle_1m)

                if candle_5m:
                    print("\n========== New 5-min Candle ==========")
                    print(candle_5m)

                    # ===== Indicators =====
                    vwap_value = self.vwap.update(candle_5m)
                    atr_value = self.atr.update(candle_5m)
                    buffer = self.atr.get_buffer()

                    self.volume.update(candle_5m)
                    volume_signal = self.volume.get_volume_signal(candle_5m["volume"])

                    # ===== Option Chain =====
                    if time.time() - self.last_option_fetch > 5:
                        print("\nFetching Option Chain Data...")
                        self.option_data = self.option_chain.fetch_option_chain()
                        self.last_option_fetch = time.time()

                        if self.option_data:
                            print("Option Data Updated | PCR:",
                                  self.option_data["pcr"],
                                  "| CE IV:", self.option_data["ce_iv"],
                                  "| PE IV:", self.option_data["pe_iv"])
                        else:
                            print("Option Chain fetch failed")

                    # ===== OI Analyzer =====
                    if self.option_data:
                        self.oi.update(
                            price,
                            self.option_data["ce_oi"],
                            self.option_data["pe_oi"]
                        )

                    oi_signal = self.oi.get_oi_signal()
                    oi_bias = self.oi.get_bias()

                    # ===== OI Ladder =====
                    oi_ladder_data = None
                    if self.option_data:
                        ce_oi_ladder = self.option_data.get("ce_oi_ladder", {})
                        pe_oi_ladder = self.option_data.get("pe_oi_ladder", {})

                        # Price change
                        price_change = 0
                        if self.prev_price:
                            price_change = price - self.prev_price

                        # Total OI change
                        total_oi = sum(ce_oi_ladder.values()) + sum(pe_oi_ladder.values())
                        oi_change = 0
                        if self.prev_total_oi:
                            oi_change = total_oi - self.prev_total_oi

                        self.prev_price = price
                        self.prev_total_oi = total_oi

                        oi_ladder_data = self.oi_ladder.analyze(
                            ce_oi_ladder,
                            pe_oi_ladder,
                            price_change,
                            oi_change
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

                    if self.option_data:
                        print("PCR:", self.option_data["pcr"])

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
                        buffer=buffer
                    )

                    # ===== Signal Output =====
                    if signal and self.option_data:
                        strike = self.strike_selector.select_strike(
                            price,
                            signal,
                            volume_signal
                        )

                        print("\n================ SIGNAL ================")
                        print("Time:", self.time_utils.current_time())
                        print("Signal:", signal)
                        print("Price:", price)
                        print("Strike:", strike)
                        print("VWAP:", vwap_value)
                        print("ORB High:", orb_high)
                        print("ORB Low:", orb_low)
                        print("PCR:", self.option_data["pcr"])
                        print("OI Signal:", oi_signal)
                        print("OI Trend:", oi_ladder_data["trend"] if oi_ladder_data else None)
                        print("Build-up:", oi_ladder_data["build_up"] if oi_ladder_data else None)
                        print("Volume Signal:", volume_signal)
                        print("Reason:", reason)
                        print("========================================\n")

            time.sleep(1)