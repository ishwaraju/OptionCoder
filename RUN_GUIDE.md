# How to Run the Refactored Trading Bot

## Quick Start Guide

### Prerequisites
```bash
# Make sure you're in the right directory
cd /Users/ishwar/Documents/OptionCoder

# Activate virtual environment
source .venv/bin/activate
```

## 🎯 Operation Modes

### Option 1: Separate Services (Clean Architecture)
**Best for: Full control, debugging, clean separation**

### Option 2: Hybrid Master Service (Convenient)
**Best for: Simple operation, single terminal**

---

## OPTION 1: SEPARATE SERVICES (CLEAN ARCHITECTURE)

### Step 1: Start Data Collection (Always Running)

#### Terminal 1: Start Data Collector
```bash
python3 services/data_collector.py
```

**Expected Output:**
```
[Data Collector] Connecting to live feed...
[Data Collector] Connected successfully
[Data Collector] Starting data collection loop...
[Data Collector] Data Collector Started:
Instrument: NIFTY
Feed Connected: True
Total Instruments: 1
```

### What Data Collector Does:
- Connects to WebSocket feed
- Collects tick data 24/7
- Generates 1-minute and 5-minute candles
- Saves to database
- **Never stops** (unless you stop it)

### Step 2: Start Signal Service (Manual Trading)

#### Terminal 2: Start Signal Service
```bash
python3 services/signal_service.py
```

**Expected Output:**
```
[Signal Service] Started:
Instrument: NIFTY
Strategy: BreakoutStrategy
DB Enabled: True
Notifications: ENABLED
[Signal Service] Starting signal analysis...
```

### What Signal Service Does:
- Reads data from database
- Runs strategy analysis
- Generates buy/sell signals
- Sends notifications
- **Can start/stop anytime**

### Step 3: Start OI Collector (Optional - Enhanced Signals)

#### Terminal 3: Start OI Collector
```bash
python3 services/oi_collector.py
```

**Expected Output:**
```
[OI Collector] Initialized for NIFTY
[OI Collector] Security ID: 13
[OI Collector] Collection intervals: OI=300s, Bands=300s
[OI Collector] Starting OI data collection...
```

### What OI Collector Does:
- Collects OI snapshots from Dhan API
- Gathers option band data
- Calculates PCR and OI trends
- Stores OI data for signal enhancement
- **Improves signal quality significantly**

## Step 4: Monitor and Trade Manually

### Signal Output Examples:
```
[Signal Service] SIGNAL GENERATED: CE
Score: 72 | Confidence: HIGH
Strike: 19800 | Reason: ORB breakout with volume
Price: 19850 | Time: 10:30 AM

[Signal Service] No signal | Score: 45 | Reason: Score below threshold
```

### Manual Trading Actions:
1. **Watch for signals** in Terminal 2
2. **Analyze signal details** (score, confidence, strike)
3. **Execute trade manually** on your trading platform
4. **Stop/start signals** anytime with Ctrl+C

## Step 4: Recovery (If Needed)

### Check Recovery Status
```bash
python3 services/recovery_service.py status
```

### Recover Missing Data
```bash
python3 services/recovery_service.py today
```

### Quick Recovery Check
```bash
python3 services/recovery_service.py check
```

## Full Day Trading Workflow

### Morning Setup (9:00 AM)
```bash
# Terminal 1
source .venv/bin/activate
python3 services/data_collector.py

# Terminal 2  
source .venv/bin/activate
python3 services/signal_service.py
```

### During Trading Hours (9:15 AM - 3:30 PM)
- **Data Collector**: Runs continuously
- **Signal Service**: Generates signals every 5 minutes
- **You**: Trade manually based on signals

### Stop Signals (Anytime)
```bash
# In Terminal 2 (Signal Service)
Ctrl+C

# Restart anytime
python3 services/signal_service.py
```

### End of Day (3:30 PM)
```bash
# Stop both services
Ctrl+C in Terminal 2
Ctrl+C in Terminal 1
```

---

## 🎯 ALTERNATIVE: HYBRID MASTER SERVICE

### Single Terminal: Start All Services Together

#### Terminal 1: Start Master Service
```bash
python3 services/master_service.py
```

**Expected Output:**
```
[Master Service] Hybrid Trading System
[Master Service] Running: Data Collector + Signal Service + OI Collector
[Master Service] Use Ctrl+C to stop all services
[Master Service] Starting all services...
[Master Service] DataCollector started
[Master Service] SignalService started
[Master Service] OICollector started
[Master Service] All services started
```

### What Master Service Does:
- Starts all 3 services automatically
- Manages all services in one process
- Provides combined status monitoring
- **Single terminal operation**
- **Use Ctrl+C to stop all services**

### Benefits of Hybrid Mode:
- ✅ Only 1 terminal needed
- ✅ Simple start/stop operation
- ✅ Combined status monitoring
- ✅ Automatic service management

### Limitations of Hybrid Mode:
- ❌ Cannot stop/start signal service independently
- ❌ All services run together
- ❌ Harder to debug individual services

### Which Mode to Choose?

**Choose SEPARATE SERVICES if:**
- You want full control over signal service
- You need to debug individual services
- You prefer clean architecture
- You want to stop/start signals independently

**Choose HYBRID MODE if:**
- You want simple operation
- You don't need independent control
- You prefer single terminal
- You want convenience over control

---

## Troubleshooting

### Data Collector Issues
```bash
# Check connection
python3 services/data_collector.py

# If connection fails, check .env file
cat .env | grep DHAN
```

### Signal Service Issues
```bash
# Check database connection
python3 services/signal_service.py

# If no signals, check threshold
grep MIN_SCORE_THRESHOLD .env
```

### Recovery Issues
```bash
# Check data gaps
python3 services/recovery_service.py status

# Fill gaps
python3 services/recovery_service.py today
```

## Environment Variables Check

### Verify .env Configuration
```bash
# Check essential settings
echo "SYMBOL: $(grep SYMBOL .env)"
echo "DB_ENABLED: $(grep DB_ENABLED .env)"
echo "MIN_SCORE_THRESHOLD: $(grep MIN_SCORE_THRESHOLD .env)"
echo "TELEGRAM_ENABLED: $(grep TELEGRAM_ENABLED .env)"
```

## Advanced Usage

### Run with Custom Instruments
```bash
# Edit services/data_collector.py
# Change instruments list in main() function
instruments = [
    {"ExchangeSegment": "IDX_I", "SecurityId": "13"},  # NIFTY
    {"ExchangeSegment": "IDX_I", "SecurityId": "25"},  # BANKNIFTY
]
```

### Run in Background
```bash
# Start data collector in background
nohup python3 services/data_collector.py > data_collector.log 2>&1 &

# Start signal service in background  
nohup python3 services/signal_service.py > signal_service.log 2>&1 &

# Check logs
tail -f data_collector.log
tail -f signal_service.log
```

### Monitor with System Commands
```bash
# Check if services are running
ps aux | grep "services/"

# Check system resources
top -p $(pgrep -f "services/")

# Check logs
tail -f data_collector.log signal_service.log
```

## Success Indicators

### Data Collector Working
```
[Data Collector] Heartbeat | IST: 10:30:00 | feed_connected: True
[Data Collector] Completed 1m | 2026-04-10 10:30:00 | O:19850 H:19860 L:19840 C:19855
[Data Collector] 5m Closed | 2026-04-10 10:30 | O:19800 H:19860 L:19790 C:19855
```

### Signal Service Working
```
[Signal Service] SIGNAL GENERATED: CE
Score: 72 | Confidence: HIGH
Strike: 19800 | Reason: ORB breakout with volume
Price: 19850 | Time: 10:30 AM
```

### Database Working
```bash
# Check candle data
psql -h localhost -U postgres -d optioncoder -c "SELECT COUNT(*) FROM candles_1m;"

# Check signals
psql -h localhost -U postgres -d optioncoder -c "SELECT * FROM strategy_decisions_5m ORDER BY ts DESC LIMIT 5;"
```

## Quick Commands Reference

```bash
# Start services
python3 services/data_collector.py &
python3 services/signal_service.py &

# Check status
python3 services/recovery_service.py status

# Stop services
pkill -f "services/"

# View logs
tail -f data_collector.log signal_service.log

# Test imports
python3 -c "from services.data_collector import DataCollector; print('OK')"
```

## Next Steps After Setup

1. **Monitor signals** for first trading day
2. **Adjust parameters** in .env if needed
3. **Test manual trading** with paper trading
4. **Add BANKNIFTY** when ready for multi-symbol
5. **Create custom strategies** in strategies/ folder

## Support

If you encounter issues:
1. Check .env configuration
2. Verify database connection
3. Review service logs
4. Run recovery service if data gaps

## Summary

**Your manual trading system is ready!**
- Data collection: 24/7 automatic
- Signal generation: Start/stop anytime
- Manual trading: Based on quality signals
- No candle gaps: Recovery service available

**Happy Trading!**
