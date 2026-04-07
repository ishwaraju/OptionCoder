import websocket
import json
import threading
import time
import ssl

from config import Config
from core.binary_parser import BinaryParser
from core.live_data import LiveData
from utils.time_utils import TimeUtils


class LiveFeed:
    def __init__(self, instruments):
        self.time_utils = TimeUtils()
        self.parser = BinaryParser()
        self.live_data = LiveData()

        self.client_id = Config.DHAN_CLIENT_ID
        self.access_token = Config.DHAN_ACCESS_TOKEN

        self.ws_url = f"wss://api-feed.dhan.co?version=2&token={self.access_token}&clientId={self.client_id}&authType=2"

        self.instruments = instruments
        self.ws = None
        self.reconnect_delay = 5
        self.last_snapshot_time = 0
        self.is_connected = False
        self.last_tick_epoch = 0

        # Store OI ladder data
        self.ce_oi = {}
        self.pe_oi = {}
        self.ce_volume = {}
        self.pe_volume = {}

    # =========================
    # WebSocket Open
    # =========================
    def on_open(self, ws):
        print("WebSocket Connected:", self.time_utils.current_time())
        self.is_connected = True
        self.reconnect_delay = 5

        # INDEX → Ticker
        ticker_msg = {
            "RequestCode": 15,
            "InstrumentCount": 1,
            "InstrumentList": [
                {"ExchangeSegment": "IDX_I", "SecurityId": "13"}
            ]
        }

        # FUTURES + CE + PE → Full
        full_instruments = [
            inst for inst in self.instruments if inst["SecurityId"] != "13"
        ]

        full_msg = {
            "RequestCode": 21,
            "InstrumentCount": len(full_instruments),
            "InstrumentList": full_instruments
        }

        ws.send(json.dumps(ticker_msg))
        ws.send(json.dumps(full_msg))

        print("Subscribed INDEX (Ticker) + FUT/CE/PE (Full)")

    # =========================
    # WebSocket Message
    # =========================
    def on_message(self, ws, message):
        data = self.parser.parse_packet(message)

        if data is None:
            return

        security_id = data["security_id"]
        self.last_tick_epoch = time.time()

        # INDEX
        if security_id == 13:
            self.live_data.update_index_data(
                data.get("price"),
                data.get("open"),
                data.get("high"),
                data.get("low"),
                data.get("volume", 0)
            )

        # FUTURES
        elif security_id == Config.NIFTY_FUTURE_ID:
            self.live_data.update_futures_data(
                data.get("volume", 0),
                data.get("oi", 0)
            )

        # OPTIONS (Multiple Strikes)
        elif security_id in Config.STRIKE_MAP:
            strike_info = Config.STRIKE_MAP[security_id]
            strike = strike_info["strike"]
            opt_type = strike_info["type"]

            oi = data.get("oi", 0)
            volume = data.get("volume", 0)

            if opt_type == "CE":
                self.ce_oi[strike] = oi
                self.ce_volume[strike] = volume
            else:
                self.pe_oi[strike] = oi
                self.pe_volume[strike] = volume

        # Print snapshot every 30 sec
        if time.time() - self.last_snapshot_time > 30:
            snapshot = self.live_data.get_snapshot()

            print("\n===== LIVE MARKET =====")
            print("Price:", snapshot["price"],
                  "| Fut Vol:", snapshot["futures_volume"],
                  "| Fut OI:", snapshot["oi"])

            # OI Ladder Print
            if Config.CONSOLE_MODE == "DETAILED":
                print("\n----- OI LADDER -----")
                strikes = sorted(set(list(self.ce_oi.keys()) + list(self.pe_oi.keys())))

                for strike in strikes:
                    ce_oi = self.ce_oi.get(strike, 0)
                    pe_oi = self.pe_oi.get(strike, 0)
                    print(f"{strike} | CE OI: {ce_oi} | PE OI: {pe_oi}")

                print("---------------------\n")
            else:
                print("Tracked Strikes:", len(set(list(self.ce_oi.keys()) + list(self.pe_oi.keys()))))

            self.last_snapshot_time = time.time()

    # =========================
    # Error
    # =========================
    def on_error(self, ws, error):
        print("WebSocket Error:", error)

    # =========================
    # Close + Reconnect
    # =========================
    def on_close(self, ws, close_status_code, close_msg):
        self.is_connected = False
        print("WebSocket Closed. Reconnecting in", self.reconnect_delay, "sec")
        time.sleep(self.reconnect_delay)
        self.reconnect_delay = min(self.reconnect_delay * 2, 60)

        if self.time_utils.is_market_open():
            self.connect()
        elif Config.AUTO_SWITCH_TO_MOCK_AFTER_CLOSE or Config.TEST_MODE:
            print("Market Closed - Switching to MOCK feed")
            self.start_mock_feed()
        else:
            print("Market Closed - Live feed stopped")

    # =========================
    # Connect
    # =========================
    def connect(self):
        if Config.TEST_MODE:
            print("Running in MOCK Live Feed Mode")
            self.start_mock_feed()
            return

        if not self.time_utils.is_market_open():
            if Config.AUTO_SWITCH_TO_MOCK_AFTER_CLOSE:
                print("Market Closed - Using MOCK feed")
                self.start_mock_feed()
            else:
                print("Market Closed - Live feed not started")
            return

        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

        thread = threading.Thread(
            target=lambda: self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
        )
        thread.daemon = True
        thread.start()

    # =========================
    # MOCK FEED
    # =========================
    def start_mock_feed(self):
        import random

        print("Starting MOCK market data...")

        def run_mock():
            price = 20000

            while True:
                price += random.randint(-20, 20)

                self.live_data.update_index_data(
                    price,
                    price - 20,
                    price + 20,
                    price - 40,
                    random.randint(1000, 5000)
                )

                self.live_data.update_futures_data(
                    random.randint(1000, 5000),
                    random.randint(100000, 200000)
                )

                time.sleep(1)

        thread = threading.Thread(target=run_mock)
        thread.daemon = True
        thread.start()

    # =========================
    # Snapshot for Engine
    # =========================
    def get_live_data(self):
        snapshot = self.live_data.get_snapshot()

        snapshot["ce_oi_ladder"] = self.ce_oi
        snapshot["pe_oi_ladder"] = self.pe_oi
        snapshot["ce_volume_ladder"] = self.ce_volume
        snapshot["pe_volume_ladder"] = self.pe_volume
        snapshot["feed_connected"] = self.is_connected
        snapshot["last_tick_epoch"] = self.last_tick_epoch

        return snapshot
