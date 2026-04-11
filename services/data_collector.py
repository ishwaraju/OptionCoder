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
from config import Config
from shared.indicators.candle_manager import CandleManager
from shared.db.writer import DBWriter
from shared.feeds.live_feed import LiveFeed
from shared.feeds.connection_manager import ConnectionManager
from shared.market.historical_backfill import HistoricalBackfill
from shared.utils.instrument_profile import get_instrument_profile
from shared.utils.service_watchdog import ServiceWatchdog


class DataCollector:
    def __init__(self, instruments=None, instrument=None):
        self.time_utils = TimeUtils()
        self.profile = get_instrument_profile(instrument)
        self.instrument = self.profile["instrument"]
        self.instruments = instruments or []
        
        # Core data components
        self.candle_manager = CandleManager()
        self.db = DBWriter()
        self.live_feed = LiveFeed(self.instruments)
        
        # Connection management
        self.historical_backfill = HistoricalBackfill()
        self.connection_manager = ConnectionManager(
            live_feed=self.live_feed,
            candle_manager=self.candle_manager,
            historical_backfill=self.historical_backfill,
        )
        
        # State tracking
        self.last_tick_time = None
        self.last_heartbeat = 0
        self.running = False
        self.feed_stale_logged = False
        self.watchdog = ServiceWatchdog("data_collector", self.instrument)
        
        # Market status tracking
        self.last_market_status_check = 0
        self.market_status_interval = 60  # Check every minute
        self.last_market_status = "UNKNOWN"
        
        # Initialize with historical data
        self._restore_indicator_state()

    def _restore_indicator_state(self):
        """Restore indicator state from database on startup"""
        if not self.db.enabled:
            return

        recent_candles = self.db.fetch_recent_candles_5m(
            instrument=self.instrument,
            limit=Config.STATE_RECOVERY_5M_BARS,
        )
        if not recent_candles:
            # Try historical backfill if DB has no data
            print("No DB candles found, trying historical backfill for warmup")
            warmup_candles = self.historical_backfill.warmup_indicators_on_startup(
                num_bars=Config.STATE_RECOVERY_5M_BARS
            )
            if warmup_candles:
                for candle in warmup_candles:
                    self.candle_manager.ingest_completed_5m(candle)
                print(f"Warmed up with {len(warmup_candles)} historical 5m bars")
            return

        for candle in recent_candles:
            self.candle_manager.ingest_completed_5m(candle)

        print(f"Restored {len(recent_candles)} historical 5m candles for warmup")

    def _print_startup_status(self):
        """Print startup status"""
        live_snapshot = self.live_feed.get_live_data()
        diagnostics = live_snapshot.get("connection_diagnostics", {})
        print("Data Collector Started:")
        print("Instrument:", self.instrument)
        print("Feed Connected:", live_snapshot.get("feed_connected"))
        print("Reconnect Attempts:", diagnostics.get("reconnect_attempts", 0))
        print("Connection Stability:", diagnostics.get("stability_score"))
        print("Total Instruments:", len(self.instruments))

    def _print_heartbeat(self):
        """Print heartbeat status"""
        live_snapshot = self.live_feed.get_live_data()
        diagnostics = live_snapshot.get("connection_diagnostics", {})
        self.watchdog.touch(
            {
                "phase": "heartbeat",
                "feed_connected": live_snapshot.get("feed_connected"),
                "data_age_seconds": live_snapshot.get("data_age_seconds"),
                "price": live_snapshot.get("price"),
                "reconnect_attempts": diagnostics.get("reconnect_attempts", 0),
                "stability_score": diagnostics.get("stability_score"),
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
            elif market_status == "POST_MARKET":
                print(f"[Data Collector] Post-market - Market closed for today")
                print(f"[Data Collector] Next market open: Tomorrow {market_open.strftime('%H:%M')} IST")
            elif market_status == "MARKET_OPEN":
                print(f"[Data Collector] Market Open - Collecting live data")
                print(f"[Data Collector] Market closes at {market_close.strftime('%H:%M')} IST")
            
            # Get current live snapshot for status
            current_live_snapshot = self.live_feed.get_live_data()
            
            print(f"[Data Collector] Status: {'✅ RUNNING' if self.running else '❌ STOPPED'}")
            print(f"[Data Collector] Feed: {'✅ CONNECTED' if current_live_snapshot.get('feed_connected') else '❌ DISCONNECTED'}")
            print(f"[Data Collector] Database: {'✅ CONNECTED' if self.db.enabled else '❌ DISABLED'}")
            print(f"[Data Collector] Candles Today: {len(self.candle_manager.get_all_1min_candles())} 1m, {len(self.candle_manager.get_all_5min_candles())} 5m")
            print("[Data Collector] " + "="*50)
            
            self.last_market_status = market_status

    def _print_completed_1m_summary(self, candle_1m):
        """Print completed 1-minute candle summary"""
        print(
            "[Data Collector] Completed 1m |",
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

    def _print_completed_5m_summary(self, candle_5m):
        """Print completed 5-minute candle summary"""
        print(
            "\n[Data Collector] 5m Closed |",
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

    def _safe_save_1m_candle(self, candle_1m):
        """Safely save 1-minute candle to database"""
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
            print("[Data Collector] DB save error (1m candle):", e)

    def _safe_save_5m_candle(self, candle_5m):
        """Safely save 5-minute candle to database"""
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
            print("[Data Collector] DB save error (5m candle):", e)

    def _process_tick_data(self, tick_data):
        """Process incoming tick data and generate candles"""
        if not tick_data or 'price' not in tick_data:
            return

        price = tick_data['price']
        volume = tick_data.get('volume', 1)
        
        # Generate 1-minute candle
        candle_1m, is_new_1m = self.candle_manager.add_tick(price, volume)
        
        if is_new_1m and candle_1m:
            self._print_completed_1m_summary(candle_1m)
            self._safe_save_1m_candle(candle_1m)
            
            # Generate 5-minute candle
            candle_5m = self.candle_manager.add_minute_candle(candle_1m)
            if candle_5m:
                self._print_completed_5m_summary(candle_5m)
                self._safe_save_5m_candle(candle_5m)

    def _handle_connection_events(self, live_snapshot):
        """Handle connection events and recovery"""
        connection_state = self.connection_manager.evaluate_feed_health(
            live_snapshot.get("feed_connected"),
            live_snapshot.get("data_age_seconds"),
        )
        
        if connection_state.get("recovered") and connection_state.get("missing_candles"):
            # Handle recovered missing candles
            for candle in connection_state["missing_candles"]:
                self.candle_manager.add_historical_candle(candle)
                self._safe_save_1m_candle(candle)
                candle_5m = self.candle_manager.add_minute_candle(candle)
                if candle_5m:
                    self._safe_save_5m_candle(candle_5m)
            
            print(f"[Data Collector] Recovery Summary | backfilled: {len(connection_state['missing_candles'])}")

        return connection_state

    def _cleanup_on_shutdown(self):
        """Save any incomplete candles on shutdown"""
        # Save current incomplete 1m candle
        current_1m = self.candle_manager.finalize_current_1m()
        if current_1m:
            self._safe_save_1m_candle(current_1m)
            print("[Data Collector] Saved incomplete 1m candle on shutdown")
        
        # Save current incomplete 5m candle
        current_5m = self.candle_manager.finalize_current_5m()
        if current_5m:
            self._safe_save_5m_candle(current_5m)
            print("[Data Collector] Saved incomplete 5m candle on shutdown")

    def connect(self):
        """Connect to live data feed"""
        print("[Data Collector] Connecting to live feed...")
        self.live_feed.connect()
        
        # Wait for connection to establish
        time_module.sleep(3)
        
        # Verify connection
        live_snapshot = self.live_feed.get_live_data()
        if live_snapshot.get("feed_connected"):
            print("[Data Collector] Connected successfully")
            return True
        else:
            print("[Data Collector] Connection failed")
            return False

    def run_forever(self):
        """Main data collection loop - runs forever"""
        self.watchdog.start({"phase": "starting"})
        if not self.connect():
            print("[Data Collector] Failed to start - connection error")
            return

        self.running = True
        self._print_startup_status()
        
        print("[Data Collector] Starting data collection loop...")
        
        try:
            while self.running:
                # Get live data
                live_snapshot = self.live_feed.get_live_data()
                connection_state = self._handle_connection_events(live_snapshot)
                
                # Process tick data if available
                if not connection_state.get("skip_processing") and live_snapshot.get("price") is not None:
                    self._process_tick_data(live_snapshot)
                else:
                    self.watchdog.touch(
                        {
                            "phase": "waiting_for_fresh_feed",
                            "feed_connected": live_snapshot.get("feed_connected"),
                            "data_age_seconds": live_snapshot.get("data_age_seconds"),
                            "price": live_snapshot.get("price"),
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
            self.watchdog.stop()
            print("[Data Collector] Data collection stopped")

    def stop(self):
        """Stop data collection"""
        self.running = False
        print("[Data Collector] Stop signal sent")

    def get_status(self):
        """Get current collector status"""
        live_snapshot = self.live_feed.get_live_data()
        return {
            "running": self.running,
            "instrument": self.instrument,
            "feed_connected": live_snapshot.get("feed_connected"),
            "data_age_seconds": live_snapshot.get("data_age_seconds"),
            "last_1m_candle": self.candle_manager.get_last_candle_time(),
            "total_1m_candles": len(self.candle_manager.get_all_1min_candles()),
            "total_5m_candles": len(self.candle_manager.get_all_5min_candles()),
        }


def main():
    """Main entry point for data collector service"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", default=Config.SYMBOL)
    args = parser.parse_args()
    profile = get_instrument_profile(args.instrument)
    instruments = [
        {"ExchangeSegment": "IDX_I", "SecurityId": str(profile["security_id"])},
    ]
    collector = DataCollector(instruments=instruments, instrument=args.instrument)
    
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
