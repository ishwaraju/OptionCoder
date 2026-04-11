"""
Recovery Service - Handles Data Recovery and Gap Filling
Responsible for:
- Detecting data gaps
- Fetching missing historical data
- Filling candle gaps
- Indicator state recovery
- Data integrity checks
"""

import time
import sys
import os
import argparse
from datetime import timedelta, datetime

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils.time_utils import TimeUtils
from config import Config
from shared.db.reader import DBReader
from shared.db.writer import DBWriter
from shared.indicators.candle_manager import CandleManager
from shared.market.historical_backfill import HistoricalBackfill
from dhan_client import DhanClient
from shared.utils.instrument_profile import get_instrument_profile


class RecoveryService:
    def __init__(self, instrument=None):
        self.time_utils = TimeUtils()
        self.db_reader = DBReader()
        self.db_writer = DBWriter()
        self.candle_manager = CandleManager()
        self.historical_backfill = HistoricalBackfill()
        
        # Dhan client for historical data
        self.dhan_client = DhanClient()
        
        self.profile = get_instrument_profile(instrument)
        self.instrument = self.profile["instrument"]
        self.security_id = self.profile["security_id"]

    def _print_status(self, message):
        """Print status message with timestamp"""
        print(f"[Recovery Service] {self.time_utils.current_time()} | {message}")

    def _detect_data_gaps(self, start_time, end_time, timeframe="5m"):
        """Detect gaps in candle data"""
        self._print_status(f"Detecting data gaps from {start_time} to {end_time}")
        
        # Fetch existing candles
        existing_candles = self.db_reader.fetch_candles_in_range(
            instrument=self.instrument,
            start_time=start_time,
            end_time=end_time,
            timeframe=timeframe
        )
        
        if not existing_candles:
            self._print_status("No existing candles found - full recovery needed")
            return [(start_time, end_time)]
        
        # Generate expected time slots
        expected_slots = []
        current_time = start_time
        slot_delta = timedelta(minutes=5) if timeframe == "5m" else timedelta(minutes=1)
        
        while current_time <= end_time:
            expected_slots.append(current_time)
            current_time += slot_delta
        
        # Find existing time slots
        existing_times = {candle["time"] for candle in existing_candles}
        
        # Find gaps
        gaps = []
        gap_start = None
        
        for slot in expected_slots:
            if slot not in existing_times:
                if gap_start is None:
                    gap_start = slot
            else:
                if gap_start is not None:
                    gaps.append((gap_start, slot - slot_delta))
                    gap_start = None
        
        # Handle trailing gap
        if gap_start is not None:
            gaps.append((gap_start, end_time))
        
        self._print_status(f"Found {len(gaps)} data gaps")
        for i, (gap_start, gap_end) in enumerate(gaps):
            self._print_status(f"Gap {i+1}: {gap_start} to {gap_end}")
        
        return gaps

    def _fetch_historical_candles(self, start_time, end_time, timeframe="5m"):
        """Fetch historical candles from Dhan API"""
        self._print_status(f"Fetching historical candles from {start_time} to {end_time}")
        
        try:
            # Check if it's weekend (Saturday/Sunday)
            if start_time.weekday() >= 5:  # 5=Saturday, 6=Sunday
                self._print_status("Weekend detected - no market data available")
                return []
            
            # Convert to required format
            start_str = start_time.strftime("%Y-%m-%d")
            end_str = end_time.strftime("%Y-%m-%d")
            
            # Use proper DhanClient method
            if timeframe == "5m":
                # Use 5-minute interval
                data = self.dhan_client.get_intraday_data(
                    security_id=self.security_id,
                    exchange_segment="NSE_FNO",
                    instrument_type="FUTIDX",
                    from_date=start_str,
                    to_date=end_str,
                    interval=5
                )
            else:
                # Use 1-minute interval
                data = self.dhan_client.get_intraday_data(
                    security_id=self.security_id,
                    exchange_segment="NSE_FNO", 
                    instrument_type="FUTIDX",
                    from_date=start_str,
                    to_date=end_str,
                    interval=1
                )
            
            if not data:
                self._print_status("No historical data returned from API")
                return []
            
            # Convert to candle format
            candles = []
            for item in data:
                candle = {
                    "time": datetime.fromisoformat(item["timestamp"]).astimezone(self.time_utils.ist),
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                    "volume": int(item.get("volume", 0)),
                    "close_time": None
                }
                candles.append(candle)
            
            self._print_status(f"Fetched {len(candles)} historical candles")
            return candles
            
        except Exception as e:
            self._print_status(f"Error fetching historical data: {e}")
            return []

    def _fill_candle_gaps(self, gaps, timeframe="5m"):
        """Fill candle gaps with historical data"""
        total_filled = 0
        
        for gap_start, gap_end in gaps:
            self._print_status(f"Processing gap: {gap_start} to {gap_end}")
            
            # Fetch historical data for this gap
            candles = self._fetch_historical_candles(gap_start, gap_end, timeframe)
            
            if not candles:
                self._print_status(f"No data available for gap {gap_start} to {gap_end}")
                continue
            
            # Save candles to database
            for candle in candles:
                try:
                    if timeframe == "5m":
                        row = (
                            candle["time"],
                            self.instrument,
                            candle["open"],
                            candle["high"],
                            candle["low"],
                            candle["close"],
                            candle["volume"]
                        )
                        self.db_writer.insert_candle_5m(row)
                    else:
                        row = (
                            candle["time"],
                            self.instrument,
                            candle["open"],
                            candle["high"],
                            candle["low"],
                            candle["close"],
                            candle["volume"]
                        )
                        self.db_writer.insert_candle_1m(row)
                    
                    total_filled += 1
                    
                except Exception as e:
                    self._print_status(f"Error saving candle {candle['time']}: {e}")
        
        self._print_status(f"Filled {total_filled} candles in {len(gaps)} gaps")
        return total_filled

    def _recover_indicator_state(self, end_time):
        """Recover indicator state with historical data"""
        self._print_status("Recovering indicator state")
        
        # Get recent candles for indicator warmup
        start_time = end_time - timedelta(minutes=Config.STATE_RECOVERY_5M_BARS * 5)
        recent_candles = self._fetch_historical_candles(start_time, end_time, "5m")
        
        if not recent_candles:
            self._print_status("No recent candles for indicator recovery")
            return False
        
        # Add candles to indicator managers
        for candle in recent_candles:
            self.candle_manager.ingest_completed_5m(candle)
        
        self._print_status(f"Recovered indicator state with {len(recent_candles)} candles")
        return True

    def _verify_data_integrity(self, start_time, end_time, timeframe="5m"):
        """Verify data integrity after recovery"""
        self._print_status("Verifying data integrity")
        
        # Count expected vs actual candles
        expected_count = int((end_time - start_time).total_seconds() / (300 if timeframe == "5m" else 60))
        
        actual_candles = self.db_reader.fetch_candles_in_range(
            instrument=self.instrument,
            start_time=start_time,
            end_time=end_time,
            timeframe=timeframe
        )
        
        actual_count = len(actual_candles)
        
        integrity_score = (actual_count / expected_count) * 100 if expected_count > 0 else 0
        
        self._print_status(f"Data integrity: {actual_count}/{expected_count} candles ({integrity_score:.1f}%)")
        
        return integrity_score >= 95.0  # Consider 95%+ as good integrity

    def recover_today_data(self):
        """Recover today's missing data"""
        self._print_status("Starting today's data recovery")
        
        # Get today's date range
        now = self.time_utils.now_ist()
        
        # Check if it's weekend
        if now.weekday() >= 5:  # 5=Saturday, 6=Sunday
            self._print_status("Weekend detected - no market data available for today")
            return False
        
        today_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        today_end = now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        # If market is still open, use current time
        if now.time() < today_end.time():
            today_end = now
        
        # If before market start, try yesterday's data
        if now.time() < today_start.time():
            self._print_status("Before market hours - no data available for today yet")
            return False
        
        # Detect and fill 5-minute gaps
        gaps_5m = self._detect_data_gaps(today_start, today_end, "5m")
        if gaps_5m:
            filled_5m = self._fill_candle_gaps(gaps_5m, "5m")
            self._print_status(f"Filled {filled_5m} 5-minute candles")
        
        # Detect and fill 1-minute gaps
        gaps_1m = self._detect_data_gaps(today_start, today_end, "1m")
        if gaps_1m:
            filled_1m = self._fill_candle_gaps(gaps_1m, "1m")
            self._print_status(f"Filled {filled_1m} 1-minute candles")
        
        # Verify integrity
        integrity_5m = self._verify_data_integrity(today_start, today_end, "5m")
        integrity_1m = self._verify_data_integrity(today_start, today_end, "1m")
        
        success = integrity_5m and integrity_1m
        self._print_status(f"Recovery completed. Success: {success}")
        
        return success

    def recover_range_data(self, start_date, end_date):
        """Recover data for a specific date range"""
        self._print_status(f"Recovering data from {start_date} to {end_date}")
        
        start_time = datetime.combine(start_date, datetime.min.time()).replace(
            hour=9, minute=15, tzinfo=self.time_utils.ist
        )
        end_time = datetime.combine(end_date, datetime.min.time()).replace(
            hour=15, minute=30, tzinfo=self.time_utils.ist
        )
        
        # Detect and fill gaps
        gaps_5m = self._detect_data_gaps(start_time, end_time, "5m")
        gaps_1m = self._detect_data_gaps(start_time, end_time, "1m")
        
        if gaps_5m:
            self._fill_candle_gaps(gaps_5m, "5m")
        
        if gaps_1m:
            self._fill_candle_gaps(gaps_1m, "1m")
        
        # Verify integrity
        integrity_5m = self._verify_data_integrity(start_time, end_time, "5m")
        integrity_1m = self._verify_data_integrity(start_time, end_time, "1m")
        
        return integrity_5m and integrity_1m

    def quick_recovery_check(self):
        """Quick recovery check for recent data"""
        self._print_status("Performing quick recovery check")
        
        # Check last 30 minutes
        now = self.time_utils.now_ist()
        check_start = now - timedelta(minutes=30)
        
        gaps_5m = self._detect_data_gaps(check_start, now, "5m")
        
        if gaps_5m:
            self._print_status("Found recent gaps, filling...")
            self._fill_candle_gaps(gaps_5m, "5m")
            return True
        else:
            self._print_status("No recent gaps found")
            return False

    def get_recovery_status(self):
        """Get current recovery status"""
        now = self.time_utils.now_ist()
        today_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        
        # If current time is before market start, use yesterday's data or show 0 expected
        if now < today_start:
            # Before market hours - show yesterday's data or 0
            yesterday_start = today_start - timedelta(days=1)
            yesterday_end = today_start - timedelta(seconds=1)
            
            candles_5m = self.db_reader.fetch_candles_in_range(
                instrument=self.instrument,
                start_time=yesterday_start,
                end_time=yesterday_end,
                timeframe="5m"
            )
            
            candles_1m = self.db_reader.fetch_candles_in_range(
                instrument=self.instrument,
                start_time=yesterday_start,
                end_time=yesterday_end,
                timeframe="1m"
            )
            
            # Use full day expected counts for yesterday
            expected_5m = int((today_start - yesterday_start).total_seconds() / 300)  # Full trading day
            expected_1m = int((today_start - yesterday_start).total_seconds() / 60)   # Full trading day
            
            status_note = " (Before Market Hours - Showing Yesterday)"
        else:
            # During market hours - use today's data
            candles_5m = self.db_reader.fetch_candles_in_range(
                instrument=self.instrument,
                start_time=today_start,
                end_time=now,
                timeframe="5m"
            )
            
            candles_1m = self.db_reader.fetch_candles_in_range(
                instrument=self.instrument,
                start_time=today_start,
                end_time=now,
                timeframe="1m"
            )
            
            expected_5m = int((now - today_start).total_seconds() / 300)
            expected_1m = int((now - today_start).total_seconds() / 60)
            
            status_note = " (Market Hours)"
        
        return {
            "instrument": self.instrument,
            "current_time": now,
            "candles_5m": len(candles_5m),
            "expected_5m": expected_5m,
            "candles_1m": len(candles_1m),
            "expected_1m": expected_1m,
            "completeness_5m": (len(candles_5m) / expected_5m * 100) if expected_5m > 0 else 0,
            "completeness_1m": (len(candles_1m) / expected_1m * 100) if expected_1m > 0 else 0,
        }


def main():
    """Main entry point for recovery service"""
    from datetime import date
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", default=Config.SYMBOL)
    parser.add_argument("command", nargs="?")
    parser.add_argument("start_date", nargs="?")
    parser.add_argument("end_date", nargs="?")
    args = parser.parse_args()

    recovery = RecoveryService(instrument=args.instrument)
    
    if args.command:
        command = args.command
        
        if command == "today":
            print("Starting today's data recovery...")
            success = recovery.recover_today_data()
            print(f"Recovery {'completed successfully' if success else 'failed'}")
            
        elif command == "check":
            print("Performing quick recovery check...")
            gaps_found = recovery.quick_recovery_check()
            print(f"Check completed. Gaps found: {gaps_found}")
            
        elif command == "status":
            status = recovery.get_recovery_status()
            print("Recovery Status:")
            for key, value in status.items():
                print(f"  {key}: {value}")
                
        elif command == "range" and args.start_date and args.end_date:
            start_date = date.fromisoformat(args.start_date)
            end_date = date.fromisoformat(args.end_date)
            print(f"Recovering data from {start_date} to {end_date}...")
            success = recovery.recover_range_data(start_date, end_date)
            print(f"Range recovery {'completed successfully' if success else 'failed'}")
            
        else:
            print("Usage:")
            print("  python recovery_service.py today          - Recover today's missing data")
            print("  python recovery_service.py check          - Quick recovery check")
            print("  python recovery_service.py status         - Show recovery status")
            print("  python recovery_service.py range YYYY-MM-DD YYYY-MM-DD - Recover date range")
    else:
        print("Usage:")
        print("  python recovery_service.py today          - Recover today's missing data")
        print("  python recovery_service.py check          - Quick recovery check")
        print("  python recovery_service.py status         - Show recovery status")
        print("  python recovery_service.py range YYYY-MM-DD YYYY-MM-DD - Recover date range")


if __name__ == "__main__":
    main()
