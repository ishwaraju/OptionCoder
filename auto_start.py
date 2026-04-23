#!/usr/bin/env python3
"""
Auto Start Script for OptionCoder Services
Automatically starts all services at 9:14 AM on weekdays
"""

import time
import schedule
import subprocess
import os
import sys
from datetime import datetime
from shared.utils.time_utils import TimeUtils
from shared.utils.log_utils import log_with_timestamp
import pytz

class AutoScheduler:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.instruments = ["NIFTY", "BANKNIFTY", "SENSEX"]

    def _log(self, message):
        """Log with HH:mm:ss IST timestamp prefix"""
        ist_now = self.time_utils.now_ist()
        ts = ist_now.strftime('%H:%M:%S')
        print(f"[{ts}] {message}")

    @staticmethod
    def _find_running_pids(pattern):
        try:
            result = subprocess.run(
                ["pgrep", "-f", pattern],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                return []
            return [int(line.strip()) for line in result.stdout.splitlines() if line.strip().isdigit()]
        except Exception:
            return []

    def _verify_scalp_services_started(self, retries=4, delay_seconds=1.5):
        pending = set(self.instruments)
        started = {}
        for _ in range(retries):
            resolved = []
            for instrument in pending:
                pids = self._find_running_pids(f"services/scalp_signal_service.py --instrument {instrument}")
                if pids:
                    started[instrument] = pids
                    resolved.append(instrument)
            for instrument in resolved:
                pending.discard(instrument)
            if not pending:
                break
            time.sleep(delay_seconds)
        return started, sorted(pending)
        
    def start_collectors(self):
        """Start data collectors (non-blocking)"""
        try:
            self._log("🚀 Starting Data Collectors")
            cmd = ["python3", "tools/run_collectors.py", "start", "--instruments"] + self.instruments
            # Use Popen (non-blocking) instead of run (blocking)
            subprocess.Popen(cmd, cwd=os.getcwd(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self._log("✅ Collectors starter initiated (running in background)")
        except Exception as e:
            self._log(f"❌ Error starting collectors: {e}")
    
    def start_signals(self):
        """Start signal services (non-blocking)"""
        try:
            self._log("🚀 Starting Signal Services")
            cmd = ["python3", "tools/run_signals.py", "start", "--instruments"] + self.instruments
            # Use Popen (non-blocking) instead of run (blocking)
            subprocess.Popen(cmd, cwd=os.getcwd(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self._log("✅ Signal services initiated (running in background)")
        except Exception as e:
            self._log(f"❌ Error starting signals: {e}")
    
    def start_scalp_signals(self):
        """Start scalp signal services (non-blocking)"""
        try:
            self._log("🚀 Starting Scalp Signal Services (1m)")
            from shared.utils.log_utils import build_log_path
            log_file = build_log_path("run_scalp")
            cmd = ["python3", "-u", "tools/run_scalp.py", "start", "--instruments"] + self.instruments
            log_fh = open(log_file, "a")
            subprocess.Popen(
                cmd,
                cwd=os.getcwd(),
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                bufsize=1,
            )
            time.sleep(2)
            started, missing = self._verify_scalp_services_started()
            if missing:
                self._log(
                    f"⚠️ Scalp start incomplete. Running: {', '.join(f'{k}={v[0]}' for k, v in started.items()) or 'none'} | "
                    f"Missing: {', '.join(missing)} | log={log_file}"
                )
            else:
                self._log(
                    f"✅ Scalp services started: {', '.join(f'{k}={v[0]}' for k, v in started.items())} | log={log_file}"
                )
        except Exception as e:
            self._log(f"❌ Error starting scalp services: {e}")

    def stop_scalp_signals(self):
        """Stop scalp signal services"""
        try:
            self._log("🛑 Stopping Scalp Signal Services...")
            cmd = ["python3", "tools/run_scalp.py", "stop"]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd())
            stdout = (result.stdout or "").strip()
            stderr = (result.stderr or "").strip()
            if stdout:
                self._log(f"✅ Scalp stop output: {stdout}")
            elif stderr:
                self._log(f"⚠️ Scalp stop stderr: {stderr}")
            else:
                self._log("✅ Scalp services stop command completed")
        except Exception as e:
            self._log(f"❌ Error stopping scalp services: {e}")
    
    def start_telegram_bot(self):
        """Start telegram bot (non-blocking)"""
        try:
            self._log("🚀 Starting Telegram Bot")
            # Use unbuffered output (-u) and proper log file
            from shared.utils.log_utils import build_log_path
            log_file = build_log_path("telegram_bot_service")
            cmd = ["python3", "-u", "services/telegram_bot_service.py"]
            # Open log file for appending
            with open(log_file, 'a') as f:
                f.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Telegram Bot Started\n")
                f.flush()
            # Start with output redirected to log file
            log_fh = open(log_file, 'a')
            subprocess.Popen(
                cmd, 
                cwd=os.getcwd(),
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                bufsize=1
            )
            self._log(f"✅ Telegram Bot started (logging to: {log_file})")
        except Exception as e:
            self._log(f"❌ Error starting telegram bot: {e}")
    
    def stop_all_services(self):
        """Stop all services at market close"""
        ts = self.time_utils.now_ist().strftime('%H:%M:%S')
        self._log(f"🌅 Auto Stopping All Services")
        self._log("=" * 50)
        
        try:
            # Stop collectors
            self._log("🛑 Stopping Data Collectors...")
            cmd = ["python3", "tools/run_collectors.py", "stop"]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd())
            self._log(f"✅ Collectors stopped: {result.stdout}")
        except Exception as e:
            self._log(f"❌ Error stopping collectors: {e}")
        
        try:
            # Stop signals
            self._log("🛑 Stopping Signal Services...")
            cmd = ["python3", "tools/run_signals.py", "stop"]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd())
            self._log(f"✅ Signals stopped: {result.stdout}")
        except Exception as e:
            self._log(f"❌ Error stopping signals: {e}")

        self.stop_scalp_signals()

        try:
            # Stop telegram bot
            self._log("🛑 Stopping Telegram Bot...")
            cmd = ["pkill", "-f", "telegram_bot_service.py"]
            subprocess.run(cmd, capture_output=True, text=True)
            self._log("✅ Telegram Bot stopped")
        except Exception as e:
            self._log(f"❌ Error stopping telegram bot: {e}")
        
        self._log("=" * 50)
        self._log("🎉 All services stopped successfully!")

    def _preflight_checks(self):
        """Validate before starting services"""
        checks = []
        # Check DB connectivity
        try:
            from shared.db.pool import DBPool
            if not DBPool.initialize():
                checks.append("❌ Database not accessible")
            else:
                checks.append("✅ Database connected")
        except Exception as e:
            checks.append(f"❌ DB check failed: {e}")

        # Check future IDs cached
        try:
            from shared.utils.future_id_cache import FutureIdCache
            cache = FutureIdCache()
            future_ids = cache.load_all()
            for inst in self.instruments:
                if inst not in future_ids or not future_ids.get(inst):
                    checks.append(f"❌ {inst} future ID not cached")
                else:
                    checks.append(f"✅ {inst} future ID: {future_ids[inst]}")
        except Exception as e:
            checks.append(f"❌ Future ID check failed: {e}")

        self._log("🔍 Pre-flight Checks:")
        for check in checks:
            self._log(f"   {check}")
        self._log("")

        # Return False if any critical check failed
        return not any("❌" in c for c in checks)

    def start_all_services(self):
        """Start all services in correct order"""
        if not self.time_utils.is_weekday():
            self._log("📅 Weekend detected - Skipping auto start")
            return

        # Run pre-flight checks
        if not self._preflight_checks():
            self._log("🛑 Pre-flight checks failed - Not starting services")
            return

        self._log("🌅 Auto Starting All Services")
        self._log("=" * 50)

        # Start in order
        self.start_collectors()
        time.sleep(5)  # Wait for collectors to initialize

        self.start_signals()
        self.start_scalp_signals()  # Also start scalping
        time.sleep(4 * 60)  # Wait 4 minutes until 9:18 AM IST

        self.start_telegram_bot()

        self._log("=" * 50)
        self._log("🎉 All services started successfully!")
    
    def _get_ist_now(self):
        """Get current time in IST (India timezone)"""
        ist = pytz.timezone('Asia/Kolkata')
        return datetime.now(ist)
    
    def _check_timezone(self):
        """Check if system timezone is set to IST"""
        import time
        system_tz_offset = time.timezone if not time.daylight else time.altzone
        ist_offset = -19800  # IST is UTC+5:30 = -19800 seconds
        
        if system_tz_offset != ist_offset:
            self._log("⚠️  WARNING: System timezone is NOT set to IST (India Time)!")
            self._log(f"   Current offset: {system_tz_offset} seconds")
            self._log(f"   IST offset should be: {ist_offset} seconds (-05:30)")
            self._log("   Fix: System Preferences → Date & Time → Time Zone → Kolkata")
            self._log("   OR: Run in terminal: sudo systemsetup -settimezone Asia/Kolkata")
            self._log("")
            return False
        return True
    
    def run_scheduler(self):
        """Run the scheduler"""
        ist_now = self._get_ist_now()
        
        self._log("🤖 Auto Scheduler Started")
        self._check_timezone()
        self._log(f"⏰ Current IST Time: {ist_now.strftime('%Y-%m-%d %H:%M:%S')}")
        self._log("⏰ Services will auto-start at 9:14 AM IST on weekdays")
        self._log("🛑 Services will auto-stop at 3:40 PM IST on weekdays")
        self._log("📝 Schedule: Monday-Friday, 9:14 AM IST - 3:40 PM IST")
        self._log("🛑 Press Ctrl+C to stop scheduler")
        
        # Schedule daily at 9:14 AM IST (start)
        schedule.every().monday.at("09:14").do(self.start_all_services)
        schedule.every().tuesday.at("09:14").do(self.start_all_services)
        schedule.every().wednesday.at("09:14").do(self.start_all_services)
        schedule.every().thursday.at("09:14").do(self.start_all_services)
        schedule.every().friday.at("09:14").do(self.start_all_services)
        
        # Schedule daily at 3:40 PM IST (stop)
        schedule.every().monday.at("15:40").do(self.stop_all_services)
        schedule.every().tuesday.at("15:40").do(self.stop_all_services)
        schedule.every().wednesday.at("15:40").do(self.stop_all_services)
        schedule.every().thursday.at("15:40").do(self.stop_all_services)
        schedule.every().friday.at("15:40").do(self.stop_all_services)
        
        # Also start immediately if it's past 9:14 AM IST on weekday
        if (ist_now.weekday() < 5 and ist_now.hour >= 9 and (ist_now.hour > 9 or ist_now.minute >= 14)):
            if ist_now.hour < 15 or (ist_now.hour == 15 and ist_now.minute < 40):
                self._log("🚀 Current IST time is past 9:14 AM and before 3:40 PM - Starting services now!")
                self.start_all_services()
        
        # Track last run dates to prevent duplicate runs
        last_start_date = None
        last_stop_date = None
        
        # Run scheduler loop with IST timezone check
        while True:
            ist_now = self._get_ist_now()
            current_date = ist_now.date()
            
            # Check for 9:14 AM IST start (Monday-Friday)
            if (ist_now.weekday() < 5 and 
                ist_now.hour == 9 and ist_now.minute == 14 and
                current_date != last_start_date):
                self._log("🚀 Trigger: 9:14 AM IST - Starting services...")
                self.start_all_services()
                last_start_date = current_date
            
            # Check for 3:40 PM IST stop (Monday-Friday)
            if (ist_now.weekday() < 5 and 
                ist_now.hour == 15 and ist_now.minute == 40 and
                current_date != last_stop_date):
                self._log("🛑 Trigger: 3:40 PM IST - Stopping services...")
                self.stop_all_services()
                last_stop_date = current_date
            
            time.sleep(30)  # Check every 30 seconds

def main():
    # Setup logging to file (logs/YYYYMMDD/auto_start.log)
    import logging
    from pathlib import Path

    time_utils = TimeUtils()
    today_str = time_utils.now_ist().strftime('%Y%m%d')
    log_dir = Path(__file__).parent / "logs" / today_str
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "auto_start.log"

    # Configure logging to file + console
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Wrap print to also log to file
    original_print = print
    def print_to_file_and_console(*args, **kwargs):
        message = ' '.join(str(arg) for arg in args)
        original_print(*args, **kwargs)
        logging.info(message)

    # Replace global print for this module
    import builtins
    builtins.print = print_to_file_and_console

    scheduler = AutoScheduler()
    try:
        scheduler.run_scheduler()
    except KeyboardInterrupt:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🛑 Auto Scheduler stopped by user")

if __name__ == "__main__":
    main()
