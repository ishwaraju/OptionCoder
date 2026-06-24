import websocket
import json
import threading
import time
import ssl
import os
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
        
        # Enhanced reconnection strategy with rate limit protection
        self.max_reconnect_delay = 300  # 5 minutes max
        self.min_reconnect_delay = 10   # 10 seconds min
        self.reconnect_delay = 10
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 20
        self.connection_stability_score = 100
        self.last_successful_connect_time = 0
        self.rate_limited_until = None  # Circuit breaker for 429 errors
        self.current_connection_open_epoch = None
        self.successful_live_tick_seen = False
        self.rapid_disconnect_epochs = []
        self.min_stable_connection_seconds = int(os.getenv("DHAN_MIN_STABLE_WS_SECONDS", "20"))
        self.rapid_disconnect_window_seconds = int(os.getenv("DHAN_RAPID_DROP_WINDOW_SECONDS", "120"))
        self.rapid_disconnect_threshold = int(os.getenv("DHAN_RAPID_DROP_THRESHOLD", "3"))
        self.rapid_disconnect_cooldown_seconds = int(os.getenv("DHAN_RAPID_DROP_COOLDOWN_SECONDS", "300"))

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
        self.option_security_map = {}
        self.subscribed_option_ids = set()
        self.option_ticks = {}

    def _is_debug_enabled(self):
        return Config.DEBUG or Config.CONSOLE_MODE == "DETAILED"

    def _debug_print(self, *args, **kwargs):
        if self._is_debug_enabled():
            print(*args, **kwargs)

    def rate_limit_remaining_seconds(self):
        """Return active broker rate-limit cooldown, if any."""
        if not self.rate_limited_until:
            return 0
        return max(0, int(self.rate_limited_until - time.time()))

    def is_rate_limited(self):
        return self.rate_limit_remaining_seconds() > 0

    # =========================
    # WebSocket Open
    # =========================
    def on_open(self, ws):
        connect_time = self.time_utils.now_ist()
        print("WebSocket Connected:", self.time_utils.current_time())
        self.is_connected = True
        self.market_open_wait_scheduled = False
        self.last_successful_connect_time = connect_time
        self.current_connection_open_epoch = time.time()
        self.successful_live_tick_seen = False
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

            self._subscribe_option_quotes()

    def update_option_subscription_map(self, option_contracts):
        for contract in option_contracts or []:
            security_id = contract.get("security_id")
            if security_id is None:
                continue
            try:
                security_id = int(security_id)
            except (TypeError, ValueError):
                continue
            self.option_security_map[security_id] = {
                "instrument": (contract.get("instrument") or "").upper(),
                "strike": int(contract.get("strike")) if contract.get("strike") is not None else None,
                "atm_strike": int(contract.get("atm_strike")) if contract.get("atm_strike") is not None else None,
                "distance_from_atm": int(contract.get("distance_from_atm")) if contract.get("distance_from_atm") is not None else None,
                "option_type": (contract.get("option_type") or "").upper(),
                "exchange_segment": contract.get("exchange_segment") or "NSE_FNO",
            }

    def refresh_option_subscriptions(self, option_contracts):
        self.update_option_subscription_map(option_contracts)
        self._subscribe_option_quotes()

    def _subscribe_option_quotes(self):
        if not self.ws or not self.is_connected or not self.option_security_map:
            return

        pending_by_segment = {}
        for security_id, info in self.option_security_map.items():
            if security_id in self.subscribed_option_ids:
                continue
            segment = info.get("exchange_segment") or "NSE_FNO"
            pending_by_segment.setdefault(segment, []).append(
                {"ExchangeSegment": segment, "SecurityId": str(security_id)}
            )

        for segment, instruments in pending_by_segment.items():
            for start in range(0, len(instruments), 100):
                chunk = instruments[start:start + 100]
                try:
                    self.ws.send(json.dumps({
                        "RequestCode": 4,
                        "InstrumentCount": len(chunk),
                        "InstrumentList": chunk,
                    }))
                    self.subscribed_option_ids.update(int(item["SecurityId"]) for item in chunk)
                    print(f"Subscribed {len(chunk)} {segment} OPTIONS instruments in QUOTE mode")
                except Exception as exc:
                    print(f"Option quote subscription failed for {segment}: {exc}")

    # =========================
    # WebSocket Message
    # =========================
    def on_message(self, ws, message):
        data = self.parser.parse_packet(message)

        if data is None:
            return

        security_id = data["security_id"]
        self.last_tick_epoch = time.time()
        self.successful_live_tick_seen = True
        self.rate_limited_until = None
        self.rapid_disconnect_epochs = []

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
                data.get("price"),
                data.get("volume", 0),
                data.get("oi", 0),
                data.get("open"),
                data.get("high"),
                data.get("low"),
            )

        # OPTIONS (Multiple Strikes)
        elif security_id in self.option_security_map or security_id in getattr(Config, "STRIKE_MAP", {}):
            strike_info = self.option_security_map.get(security_id) or Config.STRIKE_MAP[security_id]
            strike = strike_info["strike"]
            opt_type = strike_info.get("option_type") or strike_info.get("type")
            instrument = strike_info.get("instrument")

            existing = self.option_ticks.get(security_id, {})
            merged = {
                **strike_info,
                **existing,
                "security_id": security_id,
                "strike": strike,
                "option_type": opt_type,
                "last_update_dt": self.time_utils.now_ist(),
            }
            for field in ("price", "volume", "oi", "open", "high", "low"):
                if data.get(field) is not None:
                    merged[field] = data.get(field)
            self.option_ticks[security_id] = merged

            oi = merged.get("oi", 0)
            volume = merged.get("volume", 0)

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
        if "429" in error_str or "Too many requests" in error_str or "blocked" in error_str.lower():
            print("🚨 RATE LIMITED (429) - Client ID blocked by Dhan!")
            print("🚨 Stopping reconnection attempts for 5 minutes...")
            self.connection_stability_score = 0
            # Set circuit breaker - don't reconnect for 5 minutes
            self.rate_limited_until = time.time() + 300
            self.reconnect_delay = 300  # 5 minutes
        elif "Errno 54" in error_str:
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
        now = time.time()
        connection_duration = None
        if self.current_connection_open_epoch is not None:
            connection_duration = max(0, now - self.current_connection_open_epoch)
        self.current_connection_open_epoch = None
        self.connection_stability_score = max(0, self.connection_stability_score - 3)
        self.connection_diagnostics['connection_drops'] += 1

        if (
            not self.successful_live_tick_seen
            and connection_duration is not None
            and connection_duration < self.min_stable_connection_seconds
            and self.time_utils.is_market_open()
        ):
            window_start = now - self.rapid_disconnect_window_seconds
            self.rapid_disconnect_epochs = [
                epoch for epoch in self.rapid_disconnect_epochs
                if epoch >= window_start
            ]
            self.rapid_disconnect_epochs.append(now)
            print(
                "WebSocket closed before any live tick "
                f"({connection_duration:.1f}s). Rapid drops: "
                f"{len(self.rapid_disconnect_epochs)}/{self.rapid_disconnect_threshold}"
            )
            if len(self.rapid_disconnect_epochs) >= self.rapid_disconnect_threshold:
                self.rate_limited_until = max(
                    self.rate_limited_until or 0,
                    now + self.rapid_disconnect_cooldown_seconds,
                )
                self.reconnect_delay = max(
                    self.reconnect_delay,
                    self.rapid_disconnect_cooldown_seconds,
                )
                print(
                    "🚫 Rapid WebSocket disconnect guard active. "
                    f"Cooling down for {self.rapid_disconnect_cooldown_seconds}s before reconnect."
                )
        self.successful_live_tick_seen = False
        
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

            # Check circuit breaker for rate limiting
            if self.rate_limited_until and time.time() < self.rate_limited_until:
                remaining = int(self.rate_limited_until - time.time())
                print(f"🚫 Reconnection blocked due to rate limiting. Retry in {remaining}s")
                return

            self.reconnect_scheduled = True

        def run():
            try:
                # Check circuit breaker again before waiting
                if self.rate_limited_until and time.time() < self.rate_limited_until:
                    remaining = int(self.rate_limited_until - time.time())
                    print(f"🚫 Rate limited - waiting {remaining}s before reconnect...")
                    time.sleep(remaining)

                # Adaptive reconnection delay based on connection stability
                if self.connection_stability_score < 50:
                    self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
                elif self.connection_stability_score > 80:
                    self.reconnect_delay = max(self.min_reconnect_delay, self.reconnect_delay * 0.9)
                else:
                    self.reconnect_delay = min(self.reconnect_delay * 1.5, self.max_reconnect_delay)

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
            return False

        remaining = self.rate_limit_remaining_seconds()
        if remaining > 0:
            print(f"Force reconnect skipped: Dhan rate-limit cooldown active. Retry in {remaining}s")
            return False

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
            return True
        else:
            print("Market closed - not reconnecting")
            return False

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

        remaining = self.rate_limit_remaining_seconds()
        if remaining > 0:
            print(f"Live feed connect skipped: Dhan rate-limit cooldown active. Retry in {remaining}s")
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
                        instrument_price + random.randint(-8, 8),
                        random.randint(1000, 5000),
                        random.randint(100000, 200000),
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

    def get_option_ticks(self, instrument=None):
        if instrument is None:
            return dict(self.option_ticks)
        instrument = instrument.upper()
        return {
            security_id: tick
            for security_id, tick in self.option_ticks.items()
            if (tick.get("instrument") or "").upper() == instrument
        }
