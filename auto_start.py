#!/usr/bin/env python3
"""
Auto Start Script for OptionCoder Services
Automatically starts all services at 9:14 AM on weekdays
"""

import time
import schedule
import subprocess
import os
from datetime import datetime
from shared.utils.time_utils import TimeUtils
import pytz

class AutoScheduler:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.instruments = ["NIFTY", "BANKNIFTY", "SENSEX"]
        
    def start_collectors(self):
        """Start data collectors"""
        try:
            print(f"🚀 Starting Data Collectors at {datetime.now().strftime('%H:%M:%S')}")
            cmd = ["python3", "tools/run_collectors.py", "start", "--instruments"] + self.instruments
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd())
            print(f"✅ Collectors started: {result.stdout}")
        except Exception as e:
            print(f"❌ Error starting collectors: {e}")
    
    def start_signals(self):
        """Start signal services"""
        try:
            print(f"🚀 Starting Signal Services at {datetime.now().strftime('%H:%M:%S')}")
            cmd = ["python3", "tools/run_signals.py", "start", "--instruments"] + self.instruments
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd())
            print(f"✅ Signals started: {result.stdout}")
        except Exception as e:
            print(f"❌ Error starting signals: {e}")
    
    def start_telegram_bot(self):
        """Start telegram bot"""
        try:
            print(f"🚀 Starting Telegram Bot at {datetime.now().strftime('%H:%M:%S')}")
            cmd = ["python3", "services/telegram_bot_service.py"]
            # Run telegram bot in background
            subprocess.Popen(cmd, cwd=os.getcwd())
            print(f"✅ Telegram Bot started in background")
        except Exception as e:
            print(f"❌ Error starting telegram bot: {e}")
    
    def stop_all_services(self):
        """Stop all services at market close"""
        print(f"\n🌅 Auto Stopping All Services - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)
        
        try:
            # Stop collectors
            print("🛑 Stopping Data Collectors...")
            cmd = ["python3", "tools/run_collectors.py", "stop"]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd())
            print(f"✅ Collectors stopped: {result.stdout}")
        except Exception as e:
            print(f"❌ Error stopping collectors: {e}")
        
        try:
            # Stop signals
            print("🛑 Stopping Signal Services...")
            cmd = ["python3", "tools/run_signals.py", "stop"]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd())
            print(f"✅ Signals stopped: {result.stdout}")
        except Exception as e:
            print(f"❌ Error stopping signals: {e}")
        
        try:
            # Stop telegram bot
            print("🛑 Stopping Telegram Bot...")
            cmd = ["pkill", "-f", "telegram_bot_service.py"]
            subprocess.run(cmd, capture_output=True, text=True)
            print(f"✅ Telegram Bot stopped")
        except Exception as e:
            print(f"❌ Error stopping telegram bot: {e}")
        
        print("=" * 50)
        print(f"🎉 All services stopped successfully!")

    def start_all_services(self):
        """Start all services in correct order"""
        if not self.time_utils.is_weekday():
            print(f"📅 Weekend detected - Skipping auto start")
            return
            
        print(f"\n🌅 Auto Starting All Services - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)
        
        # Start in order
        self.start_collectors()
        time.sleep(5)  # Wait for collectors to initialize
        
        self.start_signals()
        time.sleep(5)  # Wait for signals to initialize
        
        self.start_telegram_bot()
        
        print("=" * 50)
        print(f"🎉 All services started successfully!")
    
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
            print("⚠️  WARNING: System timezone is NOT set to IST (India Time)!")
            print(f"   Current offset: {system_tz_offset} seconds")
            print(f"   IST offset should be: {ist_offset} seconds (-05:30)")
            print("   Fix: System Preferences → Date & Time → Time Zone → Kolkata")
            print("   OR: Run in terminal: sudo systemsetup -settimezone Asia/Kolkata")
            print("")
            return False
        return True
    
    def run_scheduler(self):
        """Run the scheduler"""
        ist_now = self._get_ist_now()
        
        print("🤖 Auto Scheduler Started")
        self._check_timezone()
        print(f"⏰ Current IST Time: {ist_now.strftime('%Y-%m-%d %H:%M:%S')}")
        print("⏰ Services will auto-start at 9:14 AM IST on weekdays")
        print("🛑 Services will auto-stop at 3:40 PM IST on weekdays")
        print("📝 Schedule: Monday-Friday, 9:14 AM IST - 3:40 PM IST")
        print("🛑 Press Ctrl+C to stop scheduler")
        
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
                print(f"🚀 Current IST time is past 9:14 AM and before 3:40 PM - Starting services now!")
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
                print(f"🚀 Trigger: 9:14 AM IST - Starting services...")
                self.start_all_services()
                last_start_date = current_date
            
            # Check for 3:40 PM IST stop (Monday-Friday)
            if (ist_now.weekday() < 5 and 
                ist_now.hour == 15 and ist_now.minute == 40 and
                current_date != last_stop_date):
                print(f"🛑 Trigger: 3:40 PM IST - Stopping services...")
                self.stop_all_services()
                last_stop_date = current_date
            
            time.sleep(30)  # Check every 30 seconds

def main():
    scheduler = AutoScheduler()
    try:
        scheduler.run_scheduler()
    except KeyboardInterrupt:
        print("\n🛑 Auto Scheduler stopped by user")

if __name__ == "__main__":
    main()
