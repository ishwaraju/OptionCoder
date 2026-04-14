#!/bin/bash

# Auto Start Script for OptionCoder
# This script starts the auto-scheduler in background

cd /Users/ishwar/Documents/OptionCoder

# Create date-based log directory (logs/YYYYMMDD/)
LOG_DATE=$(date +%Y%m%d)
LOG_DIR="logs/$LOG_DATE"
mkdir -p "$LOG_DIR"

# Kill any existing auto_start.py process
pkill -f "python3 auto_start.py"

# Start auto scheduler in background with unbuffered output
# Log file: logs/YYYYMMDD/auto_start.log
nohup python3 -u auto_start.py > "$LOG_DIR/auto_start.log" 2>&1 &

echo "✅ OptionCoder Auto Scheduler Started!"
echo "📝 Logs: $LOG_DIR/auto_start.log"
echo "🕘 Services will auto-start at 9:14 AM on weekdays"
