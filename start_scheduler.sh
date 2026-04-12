#!/bin/bash

# Auto Start Script for OptionCoder
# This script starts the auto-scheduler in background

cd /Users/ishwar/Documents/OptionCoder

# Kill any existing auto_start.py process
pkill -f "python3 auto_start.py"

# Start auto scheduler in background
nohup python3 auto_start.py > auto_start.log 2>&1 &

echo "✅ OptionCoder Auto Scheduler Started!"
echo "📝 Logs: auto_start.log"
echo "🕘 Services will auto-start at 9:14 AM on weekdays"
