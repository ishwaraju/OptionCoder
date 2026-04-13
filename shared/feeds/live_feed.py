import websocket
import json
import threading
import time
import ssl
from threading import Lock

from config import Config
from shared.feeds.binary_parser import BinaryParser
from shared.feeds.live_data import LiveData
from shared.utils.future_id_cache import FutureIdCache
from shared.utils.time_utils import TimeUtils


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
        
        # Enhanced reconnection strategy
        self.max_reconnect_delay = 30
        self.min_reconnect_delay = 2
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 50
        self.connection_stability_score = 100
        self.last_successful_connect_time = 0

        self.force_reconnect_enabled = True
        self.reconnect_lock = Lock()
        self.reconnect_scheduled = False
        self.manual_close_in_progress = False
        self.market_open_wait_scheduled = False

        # Connection diagnostics
        self.connection_diagnostics = {
            'total_connections': 0,
            'successful_connections': 0,
            'failed_connections': 0,
            'connection_drops': 0,
            'error_codes': {},
            'last_error_time': None,
            'avg_connection_duration': 0,
            'connection_start_times': []
        }
        self.last_ping_time = 0
        self.ping_interval = 30  # seconds
        self.keep_alive_enabled = True
        self.future_id_cache = FutureIdCache()
        self.future_ids = self.future_id_cache.load_all()
        self.index_security_map = {
            int(security_id): instrument
            for instrument, security_id in getattr(Config, "SECURITY_IDS", {}).items()
            if security_id is not None
        }
        self.future_security_map = {
            int(security_id): instrument
            for instrument, security_id in self.future_ids.items()
            if security_id is not None
        }

        # Store OI ladder data
        self.ce_oi = {}
        self.pe_oi = {}
        self.ce_volume = {}
        self.pe_volume = {}

    def _is_debug_enabled(self):
        return Config.DEBUG or Config.CONSOLE_MODE == "DETAILED"

    def _debug_print(self, *args, **kwargs):
        if self._is_debug_enabled():
            print(*args, **kwargs)

    # =========================
    # WebSocket Open
    # =========================
    def on_open(self, ws):
        connect_time = self.time_utils.now_ist()
        print("WebSocket Connected:", self.time_utils.current_time())
        self.is_connected = True
        self.market_open_wait_scheduled = False
        self.last_successful_connect_time = connect_time
        self.reconnect_attempts = 0
        self.reconnect_delay = self.min_reconnect_delay
        
        # Update diagnostics
        self.connection_diagnostics['total_connections'] += 1
        self.connection_diagnostics['successful_connections'] += 1
        self.connection_diagnostics['connection_start_times'].append(connect_time)
        
        # Reset connection stability score on successful connection
        self.connection_stability_score = min(100, self.connection_stability_score + 10)
        
        # Start keep-alive ping mechanism
        if self.keep_alive_enabled:
            self._start_keep_alive()

        # Calculate average connection duration
        self._update_connection_duration_stats()
        
        self._debug_print(f"Connection diagnostics: {self.connection_diagnostics['successful_connections']}/{self.connection_diagnostics['total_connections']} successful")
        self._debug_print(f"Connection stability score: {self.connection_stability_score}/100")
        self.reconnect_delay = 5

        # Subscribe instruments in different modes:
        # - Index: Ticker mode (15) for LTP
        # - Futures: Quote mode (4) for LTP + Volume (both NSE_FNO and BSE_FNO)
        if self.instruments:
            index_instruments = [i for i in self.instruments if i.get("ExchangeSegment") == "IDX_I"]
            nse_futures_instruments = [i for i in self.instruments if i.get("ExchangeSegment") == "NSE_FNO"]
            bse_futures_instruments = [i for i in self.instruments if i.get("ExchangeSegment") == "BSE_FNO"]
            
            # Subscribe Index in Ticker mode
            if index_instruments:
                ws.send(json.dumps({
                    "RequestCode": 15,
                    "InstrumentCount": len(index_instruments),
                    "InstrumentList": index_instruments,
                }))
                print(f"Subscribed {len(index_instruments)} INDEX instruments in TICKER mode")
            
            # Subscribe NSE Futures in Quote mode (for volume)
            if nse_futures_instruments:
                ws.send(json.dumps({
                    "RequestCode": 4,
                    "InstrumentCount": len(nse_futures_instruments),
                    "InstrumentList": nse_futures_instruments,
                }))
                print(f"Subscribed {len(nse_futures_instruments)} NSE FUTURES instruments in QUOTE mode (with volume)")
            
            # Subscribe BSE Futures in Quote mode (for volume) - for SENSEX
            if bse_futures_instruments:
                ws.send(json.dumps({
                    "RequestCode": 4,
                    "InstrumentCount": len(bse_futures_instruments),
                    "InstrumentList": bse_futures_instruments,
                }))
                print(f"Subscribed {len(bse_futures_instruments)} BSE FUTURES instruments in QUOTE mode (with volume)")

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
        if security_id in self.index_security_map:
            self.live_data.update_index_data(
                self.index_security_map[security_id],
                data.get("price"),
                data.get("open"),
                data.get("high"),
                data.get("low"),
                data.get("volume", 0)
            )

        # FUTURES
        elif security_id in self.future_security_map or security_id == getattr(Config, "NIFTY_FUTURE_ID", 0):
            instrument = self.future_security_map.get(security_id) or self.index_security_map.get(getattr(Config, "NIFTY_FUTURE_ID", 0), "NIFTY")
            self.live_data.update_futures_data(
                instrument,
                data.get("volume", 0),
                data.get("oi", 0)
            )

        # OPTIONS (Multiple Strikes)
        elif security_id in getattr(Config, "STRIKE_MAP", {}):
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

            self._debug_print("\n===== LIVE MARKET =====")
            self._debug_print("Price:", snapshot["price"],
                              "| Fut Vol:", snapshot["futures_volume"],
                              "| Fut OI:", snapshot["oi"])

            # OI Ladder Print
            if self._is_debug_enabled():
                self._debug_print("\n----- OI LADDER -----")
                strikes = sorted(set(list(self.ce_oi.keys()) + list(self.pe_oi.keys())))

                for strike in strikes:
                    ce_oi = self.ce_oi.get(strike, 0)
                    pe_oi = self.pe_oi.get(strike, 0)
                    self._debug_print(f"{strike} | CE OI: {ce_oi} | PE OI: {pe_oi}")

                self._debug_print("---------------------\n")
            else:
                self._debug_print("Tracked Strikes:", len(set(list(self.ce_oi.keys()) + list(self.pe_oi.keys()))))

            self.last_snapshot_time = time.time()

    # =========================
    # Error
    # =========================
    def on_error(self, ws, error):
        error_time = self.time_utils.now_ist()
        print("WebSocket Error:", error)
        
        # Enhanced error diagnostics
        error_str = str(error)
        self.connection_diagnostics['last_error_time'] = error_time
        self.connection_diagnostics['error_codes'][error_str] = self.connection_diagnostics['error_codes'].get(error_str, 0) + 1
        
        # Specific error handling for common issues
        if "Errno 54" in error_str:
            print("Connection reset by peer detected - possible network issue or server timeout")
            self.connection_stability_score = max(0, self.connection_stability_score - 8)
        elif "Errno 61" in error_str:
            print("Connection refused - server may be unavailable")
            self.connection_stability_score = max(0, self.connection_stability_score - 10)
        elif "timeout" in error_str.lower():
            print("Connection timeout - network latency issue")
            self.connection_stability_score = max(0, self.connection_stability_score - 6)
        else:
            self.connection_stability_score = max(0, self.connection_stability_score - 5)

        self.reconnect_attempts += 1
        self.connection_diagnostics['failed_connections'] += 1
        
        # Log detailed diagnostics
        print(f"Error diagnostics: {error_str} | Count: {self.connection_diagnostics['error_codes'][error_str]} | Stability: {self.connection_stability_score}/100")

    # =========================
    # Close + Reconnect
    # =========================
    def on_close(self, ws, close_status_code, close_msg):
        self.is_connected = False
        self.connection_stability_score = max(0, self.connection_stability_score - 3)
        self.connection_diagnostics['connection_drops'] += 1
        
        # Enhanced reconnection logic
        if self.reconnect_attempts < self.max_reconnect_attempts:
            self._schedule_reconnect("socket_closed")
        else:
            print(f"Max reconnection attempts ({self.max_reconnect_attempts}) reached. Switching to recovery mode.")
            self._enter_recovery_mode()

    def _schedule_reconnect(self, reason):
        with self.reconnect_lock:
            if self.reconnect_scheduled:
                return
            self.reconnect_scheduled = True

        def run():
            try:
                # Adaptive reconnection delay based on connection stability
                if self.connection_stability_score < 50:
                    self.reconnect_delay = min(self.reconnect_delay * 1.5, self.max_reconnect_delay)
                elif self.connection_stability_score > 80:
                    self.reconnect_delay = max(self.min_reconnect_delay, self.reconnect_delay * 0.8)
                else:
                    self.reconnect_delay = min(self.reconnect_delay * 1.2, self.max_reconnect_delay)
                
                print(f"WebSocket reconnect scheduled: {reason} | delay: {self.reconnect_delay:.1f}sec | stability: {self.connection_stability_score}/100")
                time.sleep(self.reconnect_delay)

                if self.time_utils.is_market_open():
                    self.connect()
                elif getattr(Config, "AUTO_SWITCH_TO_MOCK_AFTER_CLOSE", False) or Config.TEST_MODE:
                    print("Market Closed - Switching to MOCK feed")
                    self.start_mock_feed()
                else:
                    print("Market Closed - Live feed stopped")
            finally:
                with self.reconnect_lock:
                    self.reconnect_scheduled = False

        thread = threading.Thread(target=run)
        thread.daemon = True
        thread.start()

    def _schedule_market_open_connect(self):
        with self.reconnect_lock:
            if self.market_open_wait_scheduled:
                return
            self.market_open_wait_scheduled = True

        def wait_for_open():
            try:
                market_open_time = self.time_utils._parse_clock(Config.ORB_START)
                print(f"Market not open yet. Waiting for {Config.ORB_START} IST to auto-connect...")

                while True:
                    if Config.TEST_MODE:
                        self.start_mock_feed()
                        return

                    current_time = self.time_utils.current_time()
                    if current_time >= market_open_time:
                        print("Market open reached. Starting live feed automatically...")
                        self.connect()
                        return

                    time.sleep(15)
            finally:
                with self.reconnect_lock:
                    self.market_open_wait_scheduled = False

        thread = threading.Thread(target=wait_for_open, daemon=True)
        thread.start()

    def force_reconnect(self):
        """Force websocket reconnection when feed is stale"""
        if not self.force_reconnect_enabled:
            return
            
        print("Force reconnecting due to stale feed...")
        
        # Close existing connection if it exists
        if self.ws:
            try:
                self.manual_close_in_progress = True
                self.ws.close()
            except:
                pass
            finally:
                self.manual_close_in_progress = False
        
        # Reset connection state
        self.is_connected = False
        self.reconnect_delay = 5
        
        # Wait briefly before reconnecting
        time.sleep(2)
        
        if self.time_utils.is_market_open():
            self.connect()
        else:
            print("Market closed - not reconnecting")

    # =========================
    # Connect
    # =========================
    def connect(self):
        with self.reconnect_lock:
            self.reconnect_scheduled = False

        # Check TEST_MODE first - this should override all other logic
        if Config.TEST_MODE:
            print("Running in MOCK Live Feed Mode")
            self.start_mock_feed()
            return

        if not self.time_utils.is_market_open():
            if self.time_utils.current_time() < self.time_utils._parse_clock(Config.ORB_START):
                self._schedule_market_open_connect()
                return
            if getattr(Config, "AUTO_SWITCH_TO_MOCK_AFTER_CLOSE", False):
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
        self.is_connected = True
        self.last_tick_epoch = time.time()

        def run_mock():
            price = 20000

            while True:
                price += random.randint(-20, 20)
                self.last_tick_epoch = time.time()

                instrument_names = sorted(set(self.index_security_map.values()) | set(self.future_security_map.values()) or {"NIFTY"})
                for instrument in instrument_names:
                    instrument_price = price + random.randint(-10, 10)
                    self.live_data.update_index_data(
                        instrument,
                        instrument_price,
                        instrument_price - 20,
                        instrument_price + 20,
                        instrument_price - 40,
                        random.randint(1000, 5000)
                    )
                    self.live_data.update_futures_data(
                        instrument,
                        random.randint(1000, 5000),
                        random.randint(100000, 200000)
                    )

                time.sleep(1)

        thread = threading.Thread(target=run_mock)
        thread.daemon = True
        thread.start()

    def _enter_recovery_mode(self):
        """Enter recovery mode when connection is unstable"""
        print("Entering recovery mode - using buffered data and periodic reconnection attempts")
        self.reconnect_attempts = 0
        self.reconnect_delay = self.max_reconnect_delay
        
        def recovery_loop():
            while self.reconnect_attempts < self.max_reconnect_attempts and not self.is_connected:
                time.sleep(30)  # Wait 30 seconds between recovery attempts
                if self.time_utils.is_market_open():
                    print("Recovery mode - attempting reconnection...")
                    self.connect()
                    self.reconnect_attempts += 1
                else:
                    break
                    
            if not self.is_connected:
                print("Recovery mode failed - switching to mock feed")
                self.start_mock_feed()
        
        thread = threading.Thread(target=recovery_loop)
        thread.daemon = True
        thread.start()

    def _start_keep_alive(self):
        """Start keep-alive ping mechanism to maintain connection"""
        def ping_loop():
            while self.is_connected and self.keep_alive_enabled:
                try:
                    current_time = self.time_utils.now_ist()
                    if self.last_ping_time == 0 or (current_time - self.last_ping_time).total_seconds() >= self.ping_interval:
                        # Send ping (WebSocket ping frame)
                        if self.ws and hasattr(self.ws, 'ping'):
                            self.ws.ping()
                            self.last_ping_time = current_time
                            self._debug_print(f"Keep-alive ping sent at {current_time.strftime('%H:%M:%S')}")
                    
                    time.sleep(10)  # Check every 10 seconds
                    
                except Exception as e:
                    self._debug_print(f"Keep-alive ping error: {e}")
                    break
        
        thread = threading.Thread(target=ping_loop)
        thread.daemon = True
        thread.start()

    def _update_connection_duration_stats(self):
        """Update connection duration statistics"""
        if len(self.connection_diagnostics['connection_start_times']) > 1:
            # Calculate average connection duration
            durations = []
            current_time = self.time_utils.now_ist()
            
            for i, start_time in enumerate(self.connection_diagnostics['connection_start_times'][:-1]):
                # Estimate duration (this is approximate since we don't have exact end times)
                duration = (current_time - start_time).total_seconds() / (len(self.connection_diagnostics['connection_start_times']) - i)
                durations.append(duration)
            
            if durations:
                self.connection_diagnostics['avg_connection_duration'] = sum(durations) / len(durations)

    def get_connection_diagnostics(self):
        """Get detailed connection diagnostics"""
        return {
            'total_connections': self.connection_diagnostics['total_connections'],
            'successful_connections': self.connection_diagnostics['successful_connections'],
            'failed_connections': self.connection_diagnostics['failed_connections'],
            'connection_drops': self.connection_diagnostics['connection_drops'],
            'error_codes': self.connection_diagnostics['error_codes'],
            'last_error_time': self.connection_diagnostics['last_error_time'],
            'avg_connection_duration': self.connection_diagnostics['avg_connection_duration'],
            'stability_score': self.connection_stability_score,
            'reconnect_attempts': self.reconnect_attempts,
            'current_delay': self.reconnect_delay
        }

    def get_live_data(self, instrument=None):
        snapshot = self.live_data.get_snapshot(instrument)

        snapshot["ce_oi_ladder"] = self.ce_oi
        snapshot["pe_oi_ladder"] = self.pe_oi
        snapshot["ce_volume_ladder"] = self.ce_volume
        snapshot["pe_volume_ladder"] = self.pe_volume
        snapshot["feed_connected"] = self.is_connected
        snapshot["last_tick_epoch"] = self.last_tick_epoch
        snapshot["connection_stability"] = self.connection_stability_score
        snapshot["reconnect_attempts"] = self.reconnect_attempts
        snapshot["connection_diagnostics"] = self.get_connection_diagnostics()

        return snapshot
