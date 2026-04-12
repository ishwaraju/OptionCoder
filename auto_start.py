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
    
    def run_scheduler(self):
        """Run the scheduler"""
        print("🤖 Auto Scheduler Started")
        print("⏰ Services will auto-start at 9:14 AM on weekdays")
        print("🛑 Services will auto-stop at 3:40 PM on weekdays")
        print("📝 Schedule: Monday-Friday, 9:14 AM IST - 3:40 PM IST")
        print("🛑 Press Ctrl+C to stop scheduler")
        
        # Schedule daily at 9:14 AM (start)
        schedule.every().monday.at("09:14").do(self.start_all_services)
        schedule.every().tuesday.at("09:14").do(self.start_all_services)
        schedule.every().wednesday.at("09:14").do(self.start_all_services)
        schedule.every().thursday.at("09:14").do(self.start_all_services)
        schedule.every().friday.at("09:14").do(self.start_all_services)
        
        # Schedule daily at 3:40 PM (stop)
        schedule.every().monday.at("15:40").do(self.stop_all_services)
        schedule.every().tuesday.at("15:40").do(self.stop_all_services)
        schedule.every().wednesday.at("15:40").do(self.stop_all_services)
        schedule.every().thursday.at("15:40").do(self.stop_all_services)
        schedule.every().friday.at("15:40").do(self.stop_all_services)
        
        # Also start immediately if it's past 9:14 AM on weekday
        now = datetime.now()
        if (now.weekday() < 5 and now.hour >= 9 and now.minute >= 14):
            print(f"🚀 Current time is past 9:14 AM - Starting services now!")
            self.start_all_services()
        
        # Run scheduler loop
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

def main():
    scheduler = AutoScheduler()
    try:
        scheduler.run_scheduler()
    except KeyboardInterrupt:
        print("\n🛑 Auto Scheduler stopped by user")

if __name__ == "__main__":
    main()
