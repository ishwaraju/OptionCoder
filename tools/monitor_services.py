#!/usr/bin/env python3
"""
Service Monitor - Check if all services are running
Add to cron: */5 * * * * cd /Users/ishwar/Documents/OptionCoder && python3 tools/monitor_services.py
"""

import subprocess
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils.notifier import Notifier
from config import Config

def check_services():
    """Check all services and alert if any are down"""
    instruments = ["NIFTY", "BANKNIFTY", "SENSEX"]
    issues = []
    
    # Check data collectors
    result = subprocess.run(["python3", "tools/run_collectors.py", "status"], 
                          capture_output=True, text=True, cwd=os.getcwd())
    if "not running" in result.stdout.lower() or result.returncode != 0:
        issues.append("Data collectors not running")
    
    # Check signal services  
    result = subprocess.run(["python3", "tools/run_signals.py", "status"],
                          capture_output=True, text=True, cwd=os.getcwd())
    if "not running" in result.stdout.lower() or result.returncode != 0:
        issues.append("Signal services not running")
    
    # Check volume cache files
    for inst in instruments:
        cache_file = f".volume_cache/{inst.lower()}_volume.json"
        if not os.path.exists(cache_file):
            issues.append(f"{inst} volume cache missing")
    
    # Alert if issues found
    if issues and Config.ENABLE_ALERTS:
        notifier = Notifier()
        message = "🚨 SERVICE ALERT:\n" + "\n".join(f"• {i}" for i in issues)
        notifier.send_alert(message)
        print(message)
        return 1
    
    print("✅ All services healthy")
    return 0

if __name__ == "__main__":
    sys.exit(check_services())
