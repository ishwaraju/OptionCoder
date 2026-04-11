"""
Master Service - Hybrid Combined Operation
Runs all services together in a single process for convenience:
- Data Collector
- Signal Service  
- OI Collector
"""

import time as time_module
import sys
import os
import threading
from datetime import timedelta, time

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.data_collector import DataCollector
from services.signal_service import SignalService
from services.oi_collector import OICollector
from shared.utils.time_utils import TimeUtils
from config import Config


class MasterService:
    """Master service that manages all trading services"""
    
    def __init__(self):
        self.time_utils = TimeUtils()
        
        # Service instances
        self.data_collector = DataCollector()
        self.signal_service = SignalService()
        self.oi_collector = OICollector()
        
        # Thread management
        self.threads = {}
        self.running = False
        
        # Status tracking
        self.last_heartbeat = 0
        self.heartbeat_interval = 30  # Every 30 seconds
        self.last_market_status_check = 0
        self.market_status_interval = 60  # Every minute
        self.last_market_status = "UNKNOWN"
        
        print(f"[Master Service] Initialized")
        print(f"[Master Service] Services: Data Collector, Signal Service, OI Collector")
    
    def _print_startup_status(self):
        """Print startup status"""
        print(f"\n[Master Service] Started:")
        print(f"Data Collector: {'READY' if self.data_collector else 'FAILED'}")
        print(f"Signal Service: {'READY' if self.signal_service else 'FAILED'}")
        print(f"OI Collector: {'READY' if self.oi_collector else 'FAILED'}")
        print(f"Operation Mode: HYBRID (All services together)")
        print(f"Control: Ctrl+C to stop all services")
    
    def _run_data_collector(self):
        """Run data collector in separate thread"""
        try:
            print(f"[Master Service] Starting Data Collector...")
            self.data_collector.run_forever()
        except Exception as e:
            print(f"[Master Service] Data Collector error: {e}")
    
    def _run_signal_service(self):
        """Run signal service in separate thread"""
        try:
            print(f"[Master Service] Starting Signal Service...")
            self.signal_service.run_forever()
        except Exception as e:
            print(f"[Master Service] Signal Service error: {e}")
    
    def _run_oi_collector(self):
        """Run OI collector in separate thread"""
        try:
            print(f"[Master Service] Starting OI Collector...")
            self.oi_collector.run_forever()
        except Exception as e:
            print(f"[Master Service] OI Collector error: {e}")
    
    def _start_all_services(self):
        """Start all services in separate threads"""
        print(f"[Master Service] Starting all services...")
        
        # Create and start threads
        self.threads['data_collector'] = threading.Thread(
            target=self._run_data_collector,
            name="DataCollector",
            daemon=True
        )
        
        self.threads['signal_service'] = threading.Thread(
            target=self._run_signal_service,
            name="SignalService",
            daemon=True
        )
        
        self.threads['oi_collector'] = threading.Thread(
            target=self._run_oi_collector,
            name="OICollector",
            daemon=True
        )
        
        # Start all threads
        for name, thread in self.threads.items():
            thread.start()
            print(f"[Master Service] {name.replace('_', ' ').title()} started")
        
        # Give services time to start
        time_module.sleep(3)
        
        print(f"[Master Service] All services started")
    
    def _stop_all_services(self):
        """Stop all services"""
        print(f"[Master Service] Stopping all services...")
        
        # Send stop signals
        self.data_collector.stop()
        self.signal_service.stop()
        self.oi_collector.stop()
        
        # Wait for threads to finish (with timeout)
        for name, thread in self.threads.items():
            if thread.is_alive():
                print(f"[Master Service] Waiting for {name} to stop...")
                thread.join(timeout=5)
                if thread.is_alive():
                    print(f"[Master Service] {name} did not stop gracefully")
        
        print(f"[Master Service] All services stopped")
    
    def _get_combined_status(self):
        """Get combined status of all services"""
        data_status = self.data_collector.get_status() if self.data_collector else {}
        signal_status = self.signal_service.get_status() if self.signal_service else {}
        oi_status = self.oi_collector.get_status() if self.oi_collector else {}
        
        return {
            "master_running": self.running,
            "data_collector": data_status,
            "signal_service": signal_status,
            "oi_collector": oi_status,
            "total_threads": len([t for t in self.threads.values() if t.is_alive()])
        }
    
    def _print_heartbeat(self):
        """Print combined heartbeat status"""
        current_time = self.time_utils.now_ist()
        status = self._get_combined_status()
        
        print(f"\n[Master Service] Heartbeat | IST: {current_time.strftime('%H:%M:%S')} | "
              f"Status: {'RUNNING' if self.running else 'STOPPED'} | "
              f"Active Threads: {status['total_threads']}")
        
        # Individual service status
        if status['data_collector']:
            dc = status['data_collector']
            print(f"  Data Collector: {'RUNNING' if dc.get('running') else 'STOPPED'} | "
                  f"Feed: {dc.get('feed_connected', 'Unknown')}")
        
        if status['signal_service']:
            ss = status['signal_service']
            print(f"  Signal Service: {'RUNNING' if ss.get('running') else 'STOPPED'} | "
                  f"Candles: {ss.get('candles_processed', 0)} | "
                  f"Signals: {ss.get('signals_generated', 0)}")
        
        if status['oi_collector']:
            oi = status['oi_collector']
            print(f"  OI Collector: {'RUNNING' if oi.get('running') else 'STOPPED'} | "
                  f"OI Snapshots: {oi.get('oi_snapshots_collected', 0)} | "
                  f"Option Bands: {oi.get('option_bands_collected', 0)}")
    
    def _check_market_status(self):
        """Check and report market status"""
        current_time = self.time_utils.now_ist()
        current_time_only = current_time.time()
        status = self._get_combined_status()
        
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
            print(f"\n[Master Service] Market Status Update: {status_msg}")
            print(f"[Master Service] Current Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
            
            if market_status == "WEEKEND":
                print(f"[Master Service] Weekend Mode - Limited activity")
                print(f"[Master Service] Next market open: Monday {market_open.strftime('%H:%M')} IST")
            elif market_status == "PRE_MARKET":
                print(f"[Master Service] Pre-market - Warming up for {market_open.strftime('%H:%M')} IST")
            elif market_status == "POST_MARKET":
                print(f"[Master Service] Post-market - Winding down for today")
                print(f"[Master Service] Next market open: Tomorrow {market_open.strftime('%H:%M')} IST")
            elif market_status == "MARKET_OPEN":
                print(f"[Master Service] Market Open - Full operation active")
                print(f"[Master Service] Market closes at {market_close.strftime('%H:%M')} IST")
            
            print(f"[Master Service] Active Services: {status['total_threads']}")
            print(f"[Master Service] Operation Mode: HYBRID (All-in-One)")
            print("[Master Service] " + "="*50)
            
            self.last_market_status = market_status
    
    def run_forever(self):
        """Main master service loop"""
        self.running = True
        self._print_startup_status()
        
        print(f"[Master Service] Starting hybrid operation...")
        
        try:
            # Start all services
            self._start_all_services()
            
            # Main monitoring loop
            while self.running:
                current_time = time_module.time()
                
                # Periodic heartbeat
                if current_time - self.last_heartbeat >= self.heartbeat_interval:
                    self._print_heartbeat()
                    self.last_heartbeat = current_time
                
                # Market status check
                if current_time - self.last_market_status_check >= self.market_status_interval:
                    self._check_market_status()
                    self.last_market_status_check = current_time
                
                # Check if any service died
                active_threads = len([t for t in self.threads.values() if t.is_alive()])
                if active_threads < len(self.threads):
                    print(f"[Master Service] Warning: {len(self.threads) - active_threads} services may have stopped")
                
                # Small sleep
                time_module.sleep(1)
                
        except KeyboardInterrupt:
            print(f"\n[Master Service] Shutdown requested by user")
        except Exception as e:
            print(f"[Master Service] Unexpected error: {e}")
        finally:
            self._stop_all_services()
            self.running = False
            print(f"[Master Service] Master service stopped")
    
    def stop(self):
        """Stop master service"""
        self.running = False
        print(f"[Master Service] Stop signal sent")
    
    def get_status(self):
        """Get master service status"""
        return self._get_combined_status()


def main():
    """Main entry point for master service"""
    import sys
    
    print(f"[Master Service] Hybrid Trading System")
    print(f"[Master Service] Running: Data Collector + Signal Service + OI Collector")
    print(f"[Master Service] Use Ctrl+C to stop all services")
    print(f"[Master Service] For individual control, use separate services")
    print()
    
    # Create and start master service
    master_service = MasterService()
    
    try:
        master_service.run_forever()
    except KeyboardInterrupt:
        print(f"\n[Master Service] Shutdown requested")
        master_service.stop()


if __name__ == "__main__":
    main()
