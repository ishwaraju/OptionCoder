#!/usr/bin/env python3
"""
Scalp Signal Service Management Tool

This script allows you to:
- Start scalp signal service
- Stop scalp signal service
- Check scalp service status

Usage:
    python3 tools/run_scalp.py start --instruments NIFTY BANKNIFTY SENSEX
    python3 tools/run_scalp.py stop
    python3 tools/run_scalp.py status
"""

import argparse
import subprocess
import sys
import os
import time
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INSTRUMENTS = ["NIFTY", "BANKNIFTY", "SENSEX"]


def start_scalp_services(instruments):
    """Start scalp signal services for given instruments"""
    print(f"🚀 Starting Scalp Services for: {', '.join(instruments)}")
    processes = []
    
    for instrument in instruments:
        log_file = REPO_ROOT / "logs" / time.strftime("%Y%m%d") / f"scalp_{instrument.lower()}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        cmd = [
            sys.executable, "-u",
            str(REPO_ROOT / "services" / "scalp_signal_service.py"),
            "--instrument", instrument
        ]
        
        with open(log_file, "a") as f:
            f.write(f"\n{'='*50}\n")
            f.write(f"Starting Scalp Service - {time.strftime('%H:%M:%S')}\n")
            f.write(f"{'='*50}\n\n")
        
        process = subprocess.Popen(
            cmd,
            stdout=open(log_file, "a"),
            stderr=subprocess.STDOUT,
            cwd=str(REPO_ROOT)
        )
        
        processes.append({"instrument": instrument, "process": process, "pid": process.pid})
        print(f"  ✅ {instrument}: pid={process.pid}")
        time.sleep(0.5)  # Small delay between starts
    
    print(f"\n🎯 All {len(processes)} scalp services started!")
    print(f"   Logs: logs/{time.strftime('%Y%m%d')}/scalp_*.log")
    return processes


def stop_scalp_services():
    """Stop all scalp signal services"""
    print("🛑 Stopping Scalp Services...")
    
    # Find and kill scalp_signal_service processes
    try:
        result = subprocess.run(
            ["pkill", "-f", "scalp_signal_service.py"],
            capture_output=True,
            text=True
        )
        time.sleep(1)
        print("  ✅ Scalp services stopped")
    except Exception as e:
        print(f"  ⚠️ Error stopping: {e}")


def check_scalp_status():
    """Check status of scalp services"""
    print("📊 Scalp Service Status")
    print("=" * 40)
    
    try:
        result = subprocess.run(
            ["pgrep", "-f", "scalp_signal_service.py"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0 and result.stdout.strip():
            pids = result.stdout.strip().split('\n')
            print(f"✅ Running: {len(pids)} service(s)")
            for pid in pids:
                print(f"   pid: {pid.strip()}")
        else:
            print("❌ No scalp services running")
            
    except Exception as e:
        print(f"⚠️ Error checking status: {e}")


def main():
    parser = argparse.ArgumentParser(description="Scalp Signal Service Manager")
    parser.add_argument(
        "command",
        choices=["start", "stop", "status"],
        help="Command to execute"
    )
    parser.add_argument(
        "--instruments",
        nargs="+",
        default=DEFAULT_INSTRUMENTS,
        help=f"Instruments to trade (default: {' '.join(DEFAULT_INSTRUMENTS)})"
    )
    
    args = parser.parse_args()
    
    if args.command == "start":
        start_scalp_services(args.instruments)
    elif args.command == "stop":
        stop_scalp_services()
    elif args.command == "status":
        check_scalp_status()


if __name__ == "__main__":
    main()
