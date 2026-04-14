#!/bin/bash
# Check OptionCoder AutoStart Status

echo "=== OptionCoder AutoStart Status ==="
echo ""

# 1. Check plist file exists
echo "1. Plist File:"
if [ -f ~/Library/LaunchAgents/com.optioncoder.autostart.plist ]; then
    echo "   ✅ Found: ~/Library/LaunchAgents/com.optioncoder.autostart.plist"
    ls -la ~/Library/LaunchAgents/com.optioncoder.autostart.plist
else
    echo "   ❌ Not found"
fi
echo ""

# 2. Check if loaded in launchctl
echo "2. Launch Agent Status:"
if launchctl list | grep -q com.optioncoder; then
    echo "   ✅ Loaded in launchctl"
    launchctl list | grep com.optioncoder
else
    echo "   ❌ Not loaded in launchctl"
fi
echo ""

# 3. Check if auto_start.py is running
echo "3. Process Status:"
PID=$(pgrep -f "auto_start.py")
if [ -n "$PID" ]; then
    echo "   ✅ Running (PID: $PID)"
    ps -p $PID -o pid,etime,%cpu,%mem,command
else
    echo "   ❌ Not running"
fi
echo ""

# 4. Check log file (in logs/YYYYMMDD/ folder)
echo "4. Recent Log Entries:"
TODAY=$(date +%Y%m%d)
LOG_FILE="/Users/ishwar/Documents/OptionCoder/logs/$TODAY/auto_start.log"
if [ -f "$LOG_FILE" ]; then
    echo "   📄 Log file exists: logs/$TODAY/auto_start.log"
    echo "   Last 5 lines:"
    tail -5 "$LOG_FILE" | sed 's/^/   /'
else
    # Try to find any recent auto_start.log
    RECENT_LOG=$(find /Users/ishwar/Documents/OptionCoder/logs -name "auto_start.log" -type f -mtime -1 2>/dev/null | head -1)
    if [ -n "$RECENT_LOG" ]; then
        echo "   📄 Recent log file: $RECENT_LOG"
        echo "   Last 5 lines:"
        tail -5 "$RECENT_LOG" | sed 's/^/   /'
    else
        echo "   ❌ Log file not found (expected: logs/$TODAY/auto_start.log)"
    fi
fi
echo ""

# 5. Check if services are running
echo "5. Service Processes:"
echo "   Data Collectors:"
pgrep -f "data_collector.py" | wc -l | xargs -I {} echo "      {} running"
echo "   OI Collectors:"
pgrep -f "oi_collector.py" | wc -l | xargs -I {} echo "      {} running"
echo "   Signal Services:"
pgrep -f "signal_service.py" | wc -l | xargs -I {} echo "      {} running"
echo ""

echo "=== End of Status ==="
