"""
OI Collector Service - Separate Service
Responsible for:
- OI snapshot collection from Dhan API
- Option band data collection
- OI trend analysis
- Database storage of OI data
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
from shared.db.writer import DBWriter
from shared.db.reader import DBReader
from dhan_client import DhanClient
from shared.market.option_chain import OptionChain
from shared.market.oi_analyzer import OIAnalyzer
from shared.utils.instrument_profile import get_instrument_profile
from shared.utils.service_watchdog import ServiceWatchdog


class OICollector:
    """OI and option band data collection service"""
    
    def __init__(self, instrument=None):
        # Core components
        self.time_utils = TimeUtils()
        self.db_writer = DBWriter()
        self.db_reader = DBReader()
        self.dhan_client = DhanClient()
        self.option_chain = OptionChain()
        self.oi_analyzer = OIAnalyzer()
        
        # State tracking
        self.profile = get_instrument_profile(instrument)
        self.instrument = self.profile["instrument"]
        self.watchdog = ServiceWatchdog("oi_collector", self.instrument)
        self.running = False
        self.last_oi_collection = 0
        self.oi_collection_interval = 300  # Every 5 minutes (full data)
        self.last_option_band_collection = 0
        self.option_band_interval = 300  # Every 5 minutes
        
        # Hybrid OI change tracking
        self.last_change_tracking = 0
        self.change_tracking_interval = 60  # Every 1 minute (change tracking)
        self.last_ce_oi = 0
        self.last_pe_oi = 0
        self.last_ce_volume = 0
        self.last_pe_volume = 0
        self.oi_changes_detected = 0
        self.significant_change_threshold = 1000  # Minimum OI change to track
        
        # Status tracking
        self.last_heartbeat = 0
        self.heartbeat_interval = 30  # Every 30 seconds
        self.last_market_status_check = 0
        self.market_status_interval = 60  # Every minute
        self.last_market_status = "UNKNOWN"
        self.oi_snapshots_collected = 0
        self.option_bands_collected = 0
        
        # Instrument configuration
        self.security_id = self.profile["security_id"]
        self.exchange_segment = "NSE_FNO"
        self.instrument_type = "FUTIDX"
        
        print(f"[OI Collector] Initialized for {self.instrument}")
        print(f"[OI Collector] Security ID: {self.security_id}")
        print(f"[OI Collector] Hybrid Mode: Change tracking every {self.change_tracking_interval}s, Full OI every {self.oi_collection_interval}s")
        print(f"[OI Collector] Change threshold: {self.significant_change_threshold:,} OI units")

    def _is_collection_window_open(self):
        return Config.TEST_MODE or self.time_utils.is_market_open()

    def _build_oi_snapshot_row(
        self,
        timestamp,
        current_price,
        total_ce_oi,
        total_pe_oi,
        total_ce_volume,
        total_pe_volume,
        ce_volume_band,
        pe_volume_band,
        pcr,
        ce_oi_change=0,
        pe_oi_change=0,
        total_oi_change=0,
        oi_sentiment="SIDEWAYS",
        oi_bias_strength=0.0,
        total_volume=0,
        volume_change=0,
        volume_pcr=0.0,
        max_ce_oi_strike=0,
        max_pe_oi_strike=0,
        oi_concentration=0.0,
        oi_trend="SIDEWAYS",
        trend_strength=0.0,
        support_level=0.0,
        resistance_level=0.0,
        oi_range_width=0.0,
        previous_ts=None,
        data_age_seconds=0,
        data_quality="GOOD",
        max_ce_oi_amount=0,
        max_pe_oi_amount=0,
        oi_spread=0.0,
        liquidity_score=0.0,
    ):
        return (
            timestamp, self.instrument, current_price,
            total_ce_oi, total_pe_oi, total_ce_volume, total_pe_volume, ce_volume_band, pe_volume_band, pcr,
            ce_oi_change, pe_oi_change, total_oi_change,
            oi_sentiment, oi_bias_strength,
            total_volume, volume_change, volume_pcr,
            max_ce_oi_strike, max_pe_oi_strike, oi_concentration,
            oi_trend, trend_strength,
            support_level, resistance_level, oi_range_width,
            previous_ts, data_age_seconds, data_quality,
            max_ce_oi_amount, max_pe_oi_amount, oi_spread, liquidity_score
        )

    def _fetch_option_chain_payload(self):
        option_data = self.option_chain.fetch_option_chain()
        if not option_data:
            return None

        band_snapshots = option_data.get("band_snapshots") or []
        if not band_snapshots:
            print("[OI Collector] No option band snapshots available")
            return None

        return option_data
    
    def _print_startup_status(self):
        """Print startup status"""
        print(f"\n[OI Collector] Started:")
        print(f"Instrument: {self.instrument}")
        print(f"Database: {'ENABLED' if self.db_writer.enabled else 'DISABLED'}")
        print(f"OI Change Tracking: Every {self.change_tracking_interval//60} minute (significant changes only)")
        print(f"Full OI Collection: Every {self.oi_collection_interval//60} minutes")
        print(f"Option Band Collection: Every {self.option_band_interval//60} minutes")
        print(f"Dhan Client: {'CONNECTED' if self.dhan_client.connected else 'DISCONNECTED'}")
    
    def _collect_oi_snapshot(self):
        """Collect OI snapshot from Dhan API"""
        try:
            if not self._is_collection_window_open():
                return False

            current_time = self.time_utils.now_ist()

            option_data = self._fetch_option_chain_payload()
            if not option_data:
                print(f"[OI Collector] No option chain data available")
                return False

            band_snapshots = option_data["band_snapshots"]
            ce_rows = [row for row in band_snapshots if row["option_type"] == "CE"]
            pe_rows = [row for row in band_snapshots if row["option_type"] == "PE"]

            total_ce_oi = sum(row["oi"] for row in ce_rows)
            total_pe_oi = sum(row["oi"] for row in pe_rows)
            total_ce_volume = sum(row["volume"] for row in ce_rows)
            total_pe_volume = sum(row["volume"] for row in pe_rows)
            ce_volume_band = total_ce_volume
            pe_volume_band = total_pe_volume
            total_volume = total_ce_volume + total_pe_volume
            pcr = float(option_data.get("pcr") or 0.0)
            current_price = option_data.get("underlying_price") or self._get_current_price()
            max_ce_oi_strike = int(option_data.get("max_call_oi_strike") or 0)
            max_pe_oi_strike = int(option_data.get("max_put_oi_strike") or 0)
            max_ce_oi_amount = max((row["oi"] for row in ce_rows), default=0)
            max_pe_oi_amount = max((row["oi"] for row in pe_rows), default=0)

            oi_data = self._build_oi_snapshot_row(
                timestamp=current_time,
                current_price=current_price,
                total_ce_oi=total_ce_oi,
                total_pe_oi=total_pe_oi,
                total_ce_volume=total_ce_volume,
                total_pe_volume=total_pe_volume,
                ce_volume_band=ce_volume_band,
                pe_volume_band=pe_volume_band,
                pcr=pcr,
                total_volume=total_volume,
                volume_pcr=(total_pe_volume / total_ce_volume) if total_ce_volume > 0 else 0.0,
                max_ce_oi_strike=max_ce_oi_strike,
                max_pe_oi_strike=max_pe_oi_strike,
                previous_ts=current_time,
                max_ce_oi_amount=max_ce_oi_amount,
                max_pe_oi_amount=max_pe_oi_amount,
            )

            self.db_writer.insert_oi_1m(oi_data)
            self.oi_snapshots_collected += 1
            
            print(f"[OI Collector] OI Snapshot | Time: {current_time.strftime('%H:%M:%S')} | "
                  f"CE OI: {total_ce_oi:,} | PE OI: {total_pe_oi:,} | PCR: {pcr:.3f}")
            
            return True
            
        except Exception as e:
            print(f"[OI Collector] Error collecting OI snapshot: {e}")
            return False
    
    def _collect_option_bands(self):
        """Collect option band data"""
        try:
            if not self._is_collection_window_open():
                return False

            current_time = self.time_utils.now_ist()

            option_data = self._fetch_option_chain_payload()
            if not option_data:
                print(f"[OI Collector] No option chain data for bands")
                return False

            current_price = option_data.get("underlying_price") or self._get_current_price()
            atm_strike = int(option_data.get("atm") or (round(current_price / 50) * 50))
            option_band_rows = []

            for snapshot in option_data["band_snapshots"]:
                option_band_rows.append(
                    (
                        current_time,
                        self.instrument,
                        int(snapshot["atm_strike"]),
                        int(snapshot["strike"]),
                        int(abs(snapshot["strike"] - atm_strike)),
                        snapshot["option_type"],
                        snapshot.get("security_id"),
                        int(snapshot.get("oi", 0)),
                        int(snapshot.get("volume", 0)),
                        float(snapshot.get("ltp", 0)),
                        float(snapshot.get("iv", 0)),
                        float(snapshot.get("top_bid_price")) if snapshot.get("top_bid_price") is not None else None,
                        int(snapshot.get("top_bid_quantity")) if snapshot.get("top_bid_quantity") is not None else None,
                        float(snapshot.get("top_ask_price")) if snapshot.get("top_ask_price") is not None else None,
                        int(snapshot.get("top_ask_quantity")) if snapshot.get("top_ask_quantity") is not None else None,
                        float(snapshot.get("spread")) if snapshot.get("spread") is not None else None,
                        float(snapshot.get("average_price")) if snapshot.get("average_price") is not None else None,
                        int(snapshot.get("previous_oi")) if snapshot.get("previous_oi") is not None else None,
                        int(snapshot.get("previous_volume")) if snapshot.get("previous_volume") is not None else None,
                        float(snapshot.get("delta")) if snapshot.get("delta") is not None else None,
                        float(snapshot.get("theta")) if snapshot.get("theta") is not None else None,
                        float(snapshot.get("gamma")) if snapshot.get("gamma") is not None else None,
                        float(snapshot.get("vega")) if snapshot.get("vega") is not None else None,
                    )
                )

            # Store option bands
            if option_band_rows:
                self.db_writer.insert_option_band_snapshots_1m(option_band_rows)
                self.option_bands_collected += len(option_band_rows)
                
                print(f"[OI Collector] Option Bands | Time: {current_time.strftime('%H:%M:%S')} | "
                      f"ATM: {atm_strike} | Bands: {len(option_band_rows)}")
            
            return True
            
        except Exception as e:
            print(f"[OI Collector] Error collecting option bands: {e}")
            return False
    
    def _get_current_price(self):
        """Get current price from recent candles"""
        try:
            recent_candles = self.db_reader.fetch_recent_candles_5m(
                instrument=self.instrument,
                limit=1
            )
            
            if recent_candles:
                return recent_candles[0].get('close')
            
            return None
            
        except Exception as e:
            print(f"[OI Collector] Error getting current price: {e}")
            return None
    
    def _should_collect_oi(self):
        """Check if OI collection is due"""
        current_time = time_module.time()
        return current_time - self.last_oi_collection >= self.oi_collection_interval
    
    def _should_collect_option_bands(self):
        """Check if option band collection is due"""
        current_time = time_module.time()
        return current_time - self.last_option_band_collection >= self.option_band_interval
    
    def _should_track_oi_changes(self):
        """Check if OI change tracking is due"""
        current_time = time_module.time()
        return current_time - self.last_change_tracking >= self.change_tracking_interval
    
    def _track_oi_changes(self):
        """Track OI changes every minute (hybrid approach)"""
        try:
            if not self._is_collection_window_open():
                return False

            current_time = self.time_utils.now_ist()

            option_data = self._fetch_option_chain_payload()
            if not option_data:
                return False

            band_snapshots = option_data["band_snapshots"]
            total_ce_oi = sum(row["oi"] for row in band_snapshots if row["option_type"] == "CE")
            total_pe_oi = sum(row["oi"] for row in band_snapshots if row["option_type"] == "PE")
            total_ce_volume = sum(row["volume"] for row in band_snapshots if row["option_type"] == "CE")
            total_pe_volume = sum(row["volume"] for row in band_snapshots if row["option_type"] == "PE")
            
            # Calculate changes
            ce_oi_change = total_ce_oi - self.last_ce_oi
            pe_oi_change = total_pe_oi - self.last_pe_oi
            ce_volume_change = total_ce_volume - self.last_ce_volume
            pe_volume_change = total_pe_volume - self.last_pe_volume
            
            # Check for significant changes
            significant_change = (
                abs(ce_oi_change) >= self.significant_change_threshold or
                abs(pe_oi_change) >= self.significant_change_threshold or
                abs(ce_volume_change) >= self.significant_change_threshold or
                abs(pe_volume_change) >= self.significant_change_threshold
            )
            
            if significant_change and (self.last_ce_oi > 0 and self.last_pe_oi > 0):
                # Save OI change snapshot
                self._save_oi_change_snapshot(
                    current_time, ce_oi_change, pe_oi_change,
                    ce_volume_change, pe_volume_change,
                    total_ce_oi, total_pe_oi
                )
                
                self.oi_changes_detected += 1
                
                print(f"[OI Collector] Significant Change Detected | Time: {current_time.strftime('%H:%M:%S')} | "
                      f"CE OI Change: {ce_oi_change:+,} | PE OI Change: {pe_oi_change:+,} | "
                      f"Total Changes: {self.oi_changes_detected}")
            
            # Update last values for next comparison
            self.last_ce_oi = total_ce_oi
            self.last_pe_oi = total_pe_oi
            self.last_ce_volume = total_ce_volume
            self.last_pe_volume = total_pe_volume
            
            return True
            
        except Exception as e:
            print(f"[OI Collector] Error tracking OI changes: {e}")
            return False
    
    def _save_oi_change_snapshot(self, timestamp, ce_oi_change, pe_oi_change, ce_volume_change, pe_volume_change, total_ce_oi, total_pe_oi):
        """Save OI change snapshot to database"""
        try:
            current_price = self._get_current_price()
            pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
            
            # Determine sentiment based on changes
            if ce_oi_change > 0 and pe_oi_change < 0:
                sentiment = "BULLISH"
                strength = min(abs(ce_oi_change) / 10000, 1.0)
            elif pe_oi_change > 0 and ce_oi_change < 0:
                sentiment = "BEARISH"
                strength = min(abs(pe_oi_change) / 10000, 1.0)
            elif ce_oi_change > 0 and pe_oi_change > 0:
                sentiment = "ACCUMULATION"
                strength = min((ce_oi_change + pe_oi_change) / 20000, 1.0)
            elif ce_oi_change < 0 and pe_oi_change < 0:
                sentiment = "DISTRIBUTION"
                strength = min(abs(ce_oi_change + pe_oi_change) / 20000, 1.0)
            else:
                sentiment = "SIDEWAYS"
                strength = 0.0
            
            # Save change snapshot (using existing OI table with change fields)
            change_data = self._build_oi_snapshot_row(
                timestamp=timestamp,
                current_price=current_price,
                total_ce_oi=total_ce_oi,
                total_pe_oi=total_pe_oi,
                total_ce_volume=0,
                total_pe_volume=0,
                ce_volume_band=0,
                pe_volume_band=0,
                pcr=pcr,
                ce_oi_change=ce_oi_change,
                pe_oi_change=pe_oi_change,
                total_oi_change=ce_oi_change + pe_oi_change,
                oi_sentiment=sentiment,
                oi_bias_strength=strength,
                total_volume=ce_volume_change + pe_volume_change,
                volume_change=ce_volume_change + pe_volume_change,
                volume_pcr=0.0,
                oi_trend=sentiment,
                trend_strength=strength,
                previous_ts=timestamp,
                data_age_seconds=0,
                data_quality="GOOD",
            )

            self.db_writer.insert_oi_1m(change_data)
            
        except Exception as e:
            print(f"[OI Collector] Error saving OI change snapshot: {e}")
    
    def _print_heartbeat(self):
        """Print periodic heartbeat status"""
        current_time = self.time_utils.now_ist()
        self.watchdog.touch(
            {
                "phase": "heartbeat",
                "dhan_connected": self.dhan_client.connected,
                "oi_snapshots_collected": self.oi_snapshots_collected,
                "option_bands_collected": self.option_bands_collected,
            }
        )
        
        print(f"\n[OI Collector] Heartbeat | IST: {current_time.strftime('%H:%M:%S')} | "
              f"Status: {'RUNNING' if self.running else 'STOPPED'} | "
              f"OI Snapshots: {self.oi_snapshots_collected} | "
              f"Option Bands: {self.option_bands_collected} | "
              f"Dhan: {'CONNECTED' if self.dhan_client.connected else 'DISCONNECTED'}")
    
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
            print(f"\n[OI Collector] Market Status Update: {status_msg}")
            print(f"[OI Collector] Current Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
            
            if market_status == "WEEKEND":
                print(f"[OI Collector] Weekend Mode - No OI collection expected")
                print(f"[OI Collector] Next market open: Monday {market_open.strftime('%H:%M')} IST")
            elif market_status == "PRE_MARKET":
                print(f"[OI Collector] Pre-market - Waiting for market open at {market_open.strftime('%H:%M')} IST")
            elif market_status == "POST_MARKET":
                print(f"[OI Collector] Post-market - Market closed for today")
                print(f"[OI Collector] Next market open: Tomorrow {market_open.strftime('%H:%M')} IST")
            elif market_status == "MARKET_OPEN":
                print(f"[OI Collector] Market Open - Collecting OI data")
                print(f"[OI Collector] Market closes at {market_close.strftime('%H:%M')} IST")
            
            print(f"[OI Collector] Status: {'RUNNING' if self.running else 'STOPPED'}")
            print(f"[OI Collector] Database: {'CONNECTED' if self.db_writer.enabled else 'DISCONNECTED'}")
            print(f"[OI Collector] Dhan API: {'CONNECTED' if self.dhan_client.connected else 'DISCONNECTED'}")
            print(f"[OI Collector] Session Stats: {self.oi_snapshots_collected} OI snapshots, {self.option_bands_collected} option bands")
            print("[OI Collector] " + "="*50)
            
            self.last_market_status = market_status
    
    def run_forever(self):
        """Main OI collection loop"""
        self.running = True
        self.watchdog.start({"phase": "starting", "dhan_connected": self.dhan_client.connected})
        self._print_startup_status()
        
        print("[OI Collector] Starting OI data collection...")
        
        try:
            while self.running:
                current_time = time_module.time()
                
                # Track OI changes every minute (hybrid approach)
                if self._should_track_oi_changes():
                    self._track_oi_changes()
                    self.last_change_tracking = current_time
                    self.watchdog.touch({"phase": "tracking_oi_changes", "dhan_connected": self.dhan_client.connected})
                
                # Collect full OI snapshot if due (every 5 minutes)
                if self._should_collect_oi():
                    self._collect_oi_snapshot()
                    self.last_oi_collection = current_time
                    self.watchdog.touch({"phase": "collecting_oi_snapshot", "dhan_connected": self.dhan_client.connected})
                
                # Collect option bands if due
                if self._should_collect_option_bands():
                    self._collect_option_bands()
                    self.last_option_band_collection = current_time
                    self.watchdog.touch({"phase": "collecting_option_bands", "dhan_connected": self.dhan_client.connected})
                
                # Periodic heartbeat
                if current_time - self.last_heartbeat >= self.heartbeat_interval:
                    self._print_heartbeat()
                    self.last_heartbeat = current_time
                
                # Market status check
                if current_time - self.last_market_status_check >= self.market_status_interval:
                    self._check_market_status()
                    self.last_market_status_check = current_time
                
                # Small sleep to prevent CPU overload
                time_module.sleep(1)
                
        except KeyboardInterrupt:
            print("\n[OI Collector] Shutdown requested by user")
        except Exception as e:
            print(f"[OI Collector] Unexpected error: {e}")
        finally:
            self.running = False
            self.watchdog.stop()
            print("[OI Collector] OI collection stopped")
    
    def stop(self):
        """Stop OI collection"""
        self.running = False
        print("[OI Collector] Stop signal sent")
    
    def get_status(self):
        """Get current OI collector status"""
        return {
            "running": self.running,
            "instrument": self.instrument,
            "oi_snapshots_collected": self.oi_snapshots_collected,
            "option_bands_collected": self.option_bands_collected,
            "dhan_connected": self.dhan_client.connected,
            "db_enabled": self.db_writer.enabled,
        }


def main():
    """Main entry point for OI collector service"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", default=Config.SYMBOL)
    args = parser.parse_args()
    oi_collector = OICollector(instrument=args.instrument)
    
    try:
        oi_collector.run_forever()
    except KeyboardInterrupt:
        print("\n[OI Collector] Shutdown requested")
        oi_collector.stop()


if __name__ == "__main__":
    main()
