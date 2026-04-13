"""
Data Collection Service - Always Running
Responsible for:
- WebSocket connection management
- Tick data processing
- Candle generation (1m, 5m)
- Database storage
- No strategy logic
"""

import time as time_module
import sys
import os
import argparse
from datetime import timedelta, time

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils.time_utils import TimeUtils
from config import Config, get_config_for_instrument
from shared.indicators.candle_manager import CandleManager
from shared.db.writer import DBWriter
from shared.feeds.live_feed import LiveFeed
from shared.feeds.connection_manager import ConnectionManager
from shared.market.historical_backfill import HistoricalBackfill
from shared.utils.instrument_profile import get_instrument_profile
from shared.utils.service_watchdog import ServiceWatchdog
from shared.utils.log_utils import build_log_path
from shared.utils.volume_cache import VolumeCache


class DataCollector:
    def __init__(self, instruments=None, instrument=None, managed_instruments=None):
        self.time_utils = TimeUtils()
        self.managed_instruments = [item.upper() for item in (managed_instruments or [instrument or Config.SYMBOL])]
        self.profiles = {
            item: get_instrument_profile(item)
            for item in self.managed_instruments
        }
        self.instrument = self.managed_instruments[0]
        self.profile = self.profiles[self.instrument]
        self.instruments = instruments or []
        
        # Get instrument-specific config
        self.config = get_config_for_instrument(self.instrument)
        
        # Store PID for heartbeat
        self.pid = os.getpid()
        
        # Core data components
        self.candle_managers = {
            item: CandleManager()
            for item in self.managed_instruments
        }
        self.db = DBWriter()
        self.live_feed = LiveFeed(self.instruments)
        self.volume_cache = VolumeCache()
        
        # Connection management
        self.historical_backfill = HistoricalBackfill()
        self.connection_manager = ConnectionManager(
            live_feed=self.live_feed,
            candle_manager=self.candle_managers[self.instrument],
            historical_backfill=self.historical_backfill,
        )
        
        # State tracking
        self.last_tick_time = None
        self.last_heartbeat = 0
        self.running = False
        self.feed_stale_logged = False
        self.watchdogs = {
            item: ServiceWatchdog("data_collector", item)
            for item in self.managed_instruments
        }
        self.last_seen_cumulative_volume = {
            item: None
            for item in self.managed_instruments
        }
        self.last_seen_api_volume = {
            item: None
            for item in self.managed_instruments
        }
        self.last_processed_update_dt = {
            item: None
            for item in self.managed_instruments
        }
        
        # Market status tracking
        self.last_market_status_check = 0
        self.market_status_interval = 60  # Check every minute
        self.last_market_status = "UNKNOWN"
        
        # Data pause tracking
        self.data_pause_active = False
        self.last_data_pause_reason = None
        
        # Initialize with historical data
        self._restore_indicator_state()

    def _restore_indicator_state(self):
        """Restore indicator state from database on startup"""
        if not self.db.enabled:
            return

        restored_any = False
        for managed_instrument in self.managed_instruments:
            recent_candles = self.db.fetch_recent_candles_5m(
                instrument=managed_instrument,
                limit=self.config.STATE_RECOVERY_5M_BARS,
            )
            if not recent_candles:
                continue
            for candle in recent_candles:
                self.candle_managers[managed_instrument].ingest_completed_5m(candle)
            restored_any = True
            print(f"Restored {len(recent_candles)} historical 5m candles for {managed_instrument} warmup")

        if not restored_any:
            print("No DB candles found, trying historical backfill for warmup")
            warmup_candles = self.historical_backfill.warmup_indicators_on_startup(
                num_bars=self.config.STATE_RECOVERY_5M_BARS
            )
            if warmup_candles:
                for candle in warmup_candles:
                    self.candle_managers[self.instrument].ingest_completed_5m(candle)
                print(f"Warmed up with {len(warmup_candles)} historical 5m bars for {self.instrument}")

    def _print_startup_status(self):
        """Print startup status"""
        live_snapshot = self.live_feed.get_live_data(self.instrument)
        diagnostics = live_snapshot.get("connection_diagnostics", {})
        print("Data Collector Started:")
        print("Instrument:", self.instrument)
        print("Managed Instruments:", ", ".join(self.managed_instruments))
        print("Feed Connected:", live_snapshot.get("feed_connected"))
        print("Reconnect Attempts:", diagnostics.get("reconnect_attempts", 0))
        print("Connection Stability:", diagnostics.get("stability_score"))
        print("Total Instruments:", len(self.instruments))

    def _print_heartbeat(self):
        """Print heartbeat status"""
        live_snapshot = self.live_feed.get_live_data(self.instrument)
        diagnostics = live_snapshot.get("connection_diagnostics", {})
        for managed_instrument in self.managed_instruments:
            instrument_snapshot = self.live_feed.get_live_data(managed_instrument)
            self.watchdogs[managed_instrument].touch(
                {
                    "phase": "heartbeat",
                    "feed_connected": instrument_snapshot.get("feed_connected"),
                    "data_age_seconds": instrument_snapshot.get("data_age_seconds"),
                    "price": instrument_snapshot.get("price"),
                    "reconnect_attempts": diagnostics.get("reconnect_attempts", 0),
                    "stability_score": diagnostics.get("stability_score"),
                    "pid": self.pid,
                }
            )
        print(
            "\n[Data Collector] Heartbeat | IST:",
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
            print(f"\n[Data Collector] Market Status Update: {status_msg}")
            print(f"[Data Collector] Current Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
            
            if market_status == "WEEKEND":
                print(f"[Data Collector] Weekend Mode - No live data expected")
                print(f"[Data Collector] Next market open: Monday {market_open.strftime('%H:%M')} IST")
            elif market_status == "PRE_MARKET":
                print(f"[Data Collector] Pre-market - Waiting for market open at {market_open.strftime('%H:%M')} IST")
                # Set data pause for pre-market (will auto-resume when market opens)
                self.data_pause_active = True
                self.last_data_pause_reason = "Pre-market"
                for managed_instrument in self.managed_instruments:
                    self.watchdogs[managed_instrument].touch({"phase": "data_pause", "reason": "Pre-market", "pid": self.pid})
            elif market_status == "POST_MARKET":
                print(f"[Data Collector] Post-market - Market closed for today")
                print(f"[Data Collector] Next market open: Tomorrow {market_open.strftime('%H:%M')} IST")
                # Set data pause and show IDLE status
                self.data_pause_active = True
                self.last_data_pause_reason = "Market closed"
                for managed_instrument in self.managed_instruments:
                    self.watchdogs[managed_instrument].touch({"phase": "data_pause", "reason": "Market closed", "pid": self.pid})
            elif market_status == "MARKET_OPEN":
                print(f"[Data Collector] Market Open - Collecting live data")
                print(f"[Data Collector] Market closes at {market_close.strftime('%H:%M')} IST")
            
            # Get current live snapshot for status
            current_live_snapshot = self.live_feed.get_live_data(self.instrument)
            
            print(f"[Data Collector] Status: {'✅ RUNNING' if self.running else '❌ STOPPED'}")
            print(f"[Data Collector] Feed: {'✅ CONNECTED' if current_live_snapshot.get('feed_connected') else '❌ DISCONNECTED'}")
            print(f"[Data Collector] Database: {'✅ CONNECTED' if self.db.enabled else '❌ DISABLED'}")
            candle_summary = ", ".join(
                f"{inst}: {len(self.candle_managers[inst].get_all_1min_candles())}x1m/{len(self.candle_managers[inst].get_all_5min_candles())}x5m"
                for inst in self.managed_instruments
            )
            print(f"[Data Collector] Candles Today: {candle_summary}")
            print("[Data Collector] " + "="*50)
            
            self.last_market_status = market_status

    def _print_completed_1m_summary(self, instrument, candle_1m):
        """Print completed 1-minute candle summary"""
        print(
            "[Data Collector] Completed 1m |",
            instrument,
            "|",
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

    def _print_completed_5m_summary(self, instrument, candle_5m):
        """Print completed 5-minute candle summary"""
        print(
            "\n[Data Collector] 5m Closed |",
            instrument,
            "|",
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
        )

    def _safe_save_1m_candle(self, instrument, candle_1m):
        """Safely save 1-minute candle to database"""
        try:
            # Update volume from API cache if candle volume is 0
            candle_volume = int(candle_1m["volume"])
            if candle_volume == 0:
                api_volume = self.volume_cache.get(instrument)
                if api_volume:
                    candle_volume = int(api_volume)
                    # Update the candle object for display
                    candle_1m["volume"] = candle_volume

            row = (
                candle_1m["datetime"],     # ts
                instrument,                # instrument
                float(candle_1m["open"]),
                float(candle_1m["high"]),
                float(candle_1m["low"]),
                float(candle_1m["close"]),
                candle_volume,
            )
            self.db.insert_candle_1m(row)
        except Exception as e:
            print("[Data Collector] DB save error (1m candle):", e)

    def _safe_save_5m_candle(self, instrument, candle_5m):
        """Safely save 5-minute candle to database"""
        try:
            row = (
                candle_5m["time"],         # ts (5m slot time)
                instrument,                # instrument
                float(candle_5m["open"]),
                float(candle_5m["high"]),
                float(candle_5m["low"]),
                float(candle_5m["close"]),
                int(candle_5m["volume"]),
            )
            self.db.insert_candle_5m(row)
        except Exception as e:
            print("[Data Collector] DB save error (5m candle):", e)

    def _process_tick_data(self, instrument, tick_data):
        """Process incoming tick data and generate candles"""
        if not tick_data or 'price' not in tick_data:
            return

        price = tick_data['price']
        volume = self._extract_effective_tick_volume(instrument, tick_data)
        
        # Generate 1-minute candle
        candle_1m, is_new_1m = self.candle_managers[instrument].add_tick(price, volume)
        
        if is_new_1m and candle_1m:
            self._print_completed_1m_summary(instrument, candle_1m)
            self._safe_save_1m_candle(instrument, candle_1m)
            
            # Generate 5-minute candle
            candle_5m = self.candle_managers[instrument].add_minute_candle(candle_1m)
            if candle_5m:
                self._print_completed_5m_summary(instrument, candle_5m)
                self._safe_save_5m_candle(instrument, candle_5m)

    def _extract_effective_tick_volume(self, instrument, tick_data):
        """
        Convert feed volume into per-tick incremental volume for candle building.

        The live feed exposes cumulative traded volume, and for indices that field
        can be zero while the paired futures subscription carries the actual
        market volume. We prefer futures volume when available, then fall back
        to index volume, and store only the positive delta.

        Note: WebSocket doesn't provide volume (confirmed by testing).
        Volume will be updated from API cache when candle completes.
        """
        cumulative_volume = tick_data.get("futures_volume")
        if cumulative_volume in (None, 0):
            cumulative_volume = tick_data.get("volume", 0)

        # If no volume from WebSocket, return 0 - will be updated from API later
        if cumulative_volume in (None, 0):
            return 0

        try:
            cumulative_volume = int(cumulative_volume or 0)
        except (TypeError, ValueError):
            cumulative_volume = 0

        if cumulative_volume < 0:
            cumulative_volume = 0

        last_seen = self.last_seen_cumulative_volume[instrument]
        if last_seen is None:
            self.last_seen_cumulative_volume[instrument] = cumulative_volume
            return 0

        # Reset baseline if exchange/broker resets cumulative volume.
        if cumulative_volume < last_seen:
            self.last_seen_cumulative_volume[instrument] = cumulative_volume
            return 0

        delta = cumulative_volume - last_seen
        self.last_seen_cumulative_volume[instrument] = cumulative_volume
        return max(0, delta)

    def _handle_connection_events(self, live_snapshot):
        """Handle connection events and recovery"""
        connection_state = self.connection_manager.evaluate_feed_health(
            live_snapshot.get("feed_connected"),
            live_snapshot.get("data_age_seconds"),
        )
        
        if connection_state.get("recovered") and connection_state.get("missing_candles"):
            # Handle recovered missing candles
            for candle in connection_state["missing_candles"]:
                self.candle_managers[self.instrument].add_historical_candle(candle)
                self._safe_save_1m_candle(self.instrument, candle)
                candle_5m = self.candle_managers[self.instrument].add_minute_candle(candle)
                if candle_5m:
                    self._safe_save_5m_candle(self.instrument, candle_5m)
            
            print(f"[Data Collector] Recovery Summary | backfilled: {len(connection_state['missing_candles'])}")

        return connection_state

    def _cleanup_on_shutdown(self):
        """Save any incomplete candles on shutdown"""
        # Save current incomplete 1m candle
        for instrument in self.managed_instruments:
            current_1m = self.candle_managers[instrument].finalize_current_1m()
            if current_1m:
                self._safe_save_1m_candle(instrument, current_1m)
                print(f"[Data Collector] Saved incomplete 1m candle on shutdown for {instrument}")
            current_5m = self.candle_managers[instrument].finalize_current_5m()
            if current_5m:
                self._safe_save_5m_candle(instrument, current_5m)
                print(f"[Data Collector] Saved incomplete 5m candle on shutdown for {instrument}")

    def connect(self):
        """Connect to live data feed"""
        print("[Data Collector] Connecting to live feed...")
        self.live_feed.connect()
        
        # Wait for connection to establish (longer for mock mode)
        wait_time = 5 if Config.TEST_MODE else 3
        time_module.sleep(wait_time)
        
        # Verify connection
        live_snapshot = self.live_feed.get_live_data(self.instrument)
        if live_snapshot.get("feed_connected"):
            print("[Data Collector] Connected successfully")
            return True
        else:
            # Check if we're in TEST_MODE and mock feed is acceptable
            if Config.TEST_MODE:
                print("[Data Collector] Waiting for MOCK feed to initialize...")
                # Give mock feed more time to start
                time_module.sleep(2)
                live_snapshot = self.live_feed.get_live_data()
                if live_snapshot.get("feed_connected"):
                    print("[Data Collector] MOCK feed initialized successfully")
                    return True
                else:
                    print("[Data Collector] MOCK feed initialization failed")
                    return False
            else:
                # Check if market is closed (this is expected behavior)
                if not self.time_utils.is_market_open():
                    print("[Data Collector] Market closed - Live connection waiting for market open")
                    return True  # Don't fail, this is expected behavior
                else:
                    print("[Data Collector] Connection failed - Market is open but cannot connect")
                    return False

    def run_forever(self):
        """Main data collection loop - runs forever"""
        for managed_instrument in self.managed_instruments:
            self.watchdogs[managed_instrument].start({"phase": "starting", "pid": self.pid})
        
        # Check market status first
        self._check_market_status()
        
        if self.last_market_status in ["POST_MARKET", "WEEKEND", "PRE_MARKET"]:
            reason = "Market closed" if self.last_market_status != "PRE_MARKET" else "Pre-market"
            print(f"[Data Collector] {reason} - Entering IDLE mode")
            self.data_pause_active = True
            self.last_data_pause_reason = reason
            self.running = True
            self._print_startup_status()
        else:
            # Try to connect for market open hours
            if not self.connect():
                print("[Data Collector] Failed to start - connection error")
                return

        self.running = True
        self._print_startup_status()
        
        print("[Data Collector] Starting data collection loop...")
        
        try:
            while self.running:
                current_time = time_module.time()
                
                # Market status check
                if current_time - self.last_market_status_check >= self.market_status_interval:
                    self._check_market_status()
                    self.last_market_status_check = current_time
                    
                    # Check if we should resume from data pause (market opened)
                    if self.data_pause_active:
                        # Re-check market status
                        self._check_market_status()
                        if self.last_market_status == "MARKET_OPEN":
                            print(f"[Data Collector] Market opened - Resuming data collection")
                            self.data_pause_active = False
                            self.last_data_pause_reason = None
                            for managed_instrument in self.managed_instruments:
                                self.watchdogs[managed_instrument].touch({"phase": "resumed", "reason": "Market opened", "pid": self.pid})
                
                # Only proceed if not in data pause
                if self.data_pause_active:
                    # Touch watchdog to maintain IDLE status
                    for managed_instrument in self.managed_instruments:
                        self.watchdogs[managed_instrument].touch({"phase": "data_pause", "reason": self.last_data_pause_reason, "pid": self.pid})
                    time_module.sleep(1)
                    continue
                
                # Get live data
                live_snapshot = self.live_feed.get_live_data(self.instrument)
                connection_state = self._handle_connection_events(live_snapshot)
                
                # Process tick data if available
                if not connection_state.get("skip_processing"):
                    for managed_instrument in self.managed_instruments:
                        instrument_snapshot = self.live_feed.get_live_data(managed_instrument)
                        update_dt = instrument_snapshot.get("last_update_dt")
                        if instrument_snapshot.get("price") is None or update_dt is None:
                            continue
                        if self.last_processed_update_dt[managed_instrument] == update_dt:
                            continue
                        self.last_processed_update_dt[managed_instrument] = update_dt
                        self._process_tick_data(managed_instrument, instrument_snapshot)
                else:
                    for managed_instrument in self.managed_instruments:
                        instrument_snapshot = self.live_feed.get_live_data(managed_instrument)
                        self.watchdogs[managed_instrument].touch(
                            {
                                "phase": "waiting_for_fresh_feed",
                                "feed_connected": instrument_snapshot.get("feed_connected"),
                                "data_age_seconds": instrument_snapshot.get("data_age_seconds"),
                                "price": instrument_snapshot.get("price"),
                                "pid": self.pid,
                            }
                        )
                
                # Periodic heartbeat
                current_time = time_module.time()
                if current_time - self.last_heartbeat >= 30:  # Every 30 seconds
                    self._print_heartbeat()
                    self.last_heartbeat = current_time
                
                # Market status check
                if current_time - self.last_market_status_check >= self.market_status_interval:  # Every minute
                    self._check_market_status()
                    self.last_market_status_check = current_time
                
                # Small sleep to prevent CPU overload
                time_module.sleep(0.1)
                
        except KeyboardInterrupt:
            print("\n[Data Collector] Shutdown requested by user")
        except Exception as e:
            print(f"[Data Collector] Unexpected error: {e}")
        finally:
            self._cleanup_on_shutdown()
            self.running = False
            for managed_instrument in self.managed_instruments:
                self.watchdogs[managed_instrument].stop()
            print("[Data Collector] Data collection stopped")

    def stop(self):
        """Stop data collection"""
        self.running = False
        print("[Data Collector] Stop signal sent")

    def get_status(self):
        """Get current collector status"""
        live_snapshot = self.live_feed.get_live_data(self.instrument)
        return {
            "running": self.running,
            "instrument": self.instrument,
            "feed_connected": live_snapshot.get("feed_connected"),
            "data_age_seconds": live_snapshot.get("data_age_seconds"),
            "last_1m_candle": self.candle_managers[self.instrument].get_last_candle_time(),
            "total_1m_candles": len(self.candle_managers[self.instrument].get_all_1min_candles()),
            "total_5m_candles": len(self.candle_managers[self.instrument].get_all_5min_candles()),
        }


def main():
    """Main entry point for data collector service"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", default=Config.SYMBOL)
    parser.add_argument("--instruments", nargs="+")
    args = parser.parse_args()
    managed_instruments = [item.upper() for item in (args.instruments or [args.instrument])]
    subscriptions = []
    for managed_instrument in managed_instruments:
        profile = get_instrument_profile(managed_instrument)
        subscriptions.append({"ExchangeSegment": "IDX_I", "SecurityId": str(profile["security_id"])})
        if profile.get("future_id"):
            subscriptions.append({"ExchangeSegment": "NSE_FNO", "SecurityId": str(profile["future_id"])})
            print(f"[Data Collector] {managed_instrument}: Index {profile['security_id']} + Futures {profile['future_id']}")
        else:
            print(f"[Data Collector] {managed_instrument}: Index only {profile['security_id']} (no futures ID cached)")
    
    collector = DataCollector(
        instruments=subscriptions,
        instrument=managed_instruments[0],
        managed_instruments=managed_instruments,
    )
    
    try:
        collector.run_forever()
    except KeyboardInterrupt:
        print("\n[Data Collector] Shutting down...")
        collector.stop()
    except Exception as e:
        print(f"[Data Collector] Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
