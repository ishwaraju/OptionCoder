#!/usr/bin/env python3
"""
System Health Check - Verify all components are working
Run this to check database tables and recent data
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from shared.db.reader import DBReader
from shared.utils.time_utils import TimeUtils

def check_system_health():
    """Check system health and data completeness"""
    
    print("=" * 70)
    print("OPTION CODER - SYSTEM HEALTH CHECK")
    print("=" * 70)
    print(f"Check Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    db = DBReader()
    time_utils = TimeUtils()
    
    if not db.enabled:
        print("❌ Database is DISABLED - Cannot check data")
        return
    
    print("✅ Database connection is active")
    print()
    
    instruments = ['NIFTY', 'BANKNIFTY', 'SENSEX']
    
    # Check today's date
    today = time_utils.now_ist().date()
    market_open = datetime.combine(today, datetime.min.time().replace(hour=9, minute=15))
    market_close = datetime.combine(today, datetime.min.time().replace(hour=15, minute=30))
    
    print(f"Market Date: {today}")
    print(f"Market Hours: 09:15 - 15:30 IST")
    print()
    
    for instrument in instruments:
        print(f"\n{'='*70}")
        print(f"INSTRUMENT: {instrument}")
        print(f"{'='*70}")
        
        # Check 1m candles
        print("\n📊 1-Minute Candles:")
        candles_1m = db.fetch_recent_candles_1m(instrument, limit=5)
        if candles_1m:
            print(f"   Recent {len(candles_1m)} candles:")
            for c in candles_1m[-5:]:
                print(f"     {c['time'].strftime('%H:%M')} | O:{c['open']:.2f} H:{c['high']:.2f} L:{c['low']:.2f} C:{c['close']:.2f} V:{c['volume']:,}")
        else:
            print("   ❌ No 1m candles found")
        
        # Check 5m candles
        print("\n📊 5-Minute Candles:")
        candles_5m = db.fetch_recent_candles_5m(instrument, limit=5)
        if candles_5m:
            print(f"   Recent {len(candles_5m)} candles:")
            for c in candles_5m[-5:]:
                print(f"     {c['time'].strftime('%H:%M')} | O:{c['open']:.2f} H:{c['high']:.2f} L:{c['low']:.2f} C:{c['close']:.2f} V:{c['volume']:,}")
        else:
            print("   ❌ No 5m candles found")
        
        # Check OI snapshots
        print("\n📈 OI Snapshots:")
        oi = db.fetch_latest_oi_snapshot(instrument)
        if oi:
            print(f"   Time: {oi['ts'].strftime('%H:%M:%S') if oi['ts'] else 'N/A'}")
            print(f"   Price: {oi['underlying_price']:.2f}" if oi['underlying_price'] else "   Price: N/A")
            print(f"   CE OI: {oi['ce_oi']:,} | PE OI: {oi['pe_oi']:,}")
            print(f"   PCR: {oi['pcr']:.3f}")
            print(f"   Sentiment: {oi['oi_sentiment']} | Trend: {oi['oi_trend']}")
            print(f"   Data Quality: {oi.get('data_quality', 'N/A')}")
        else:
            print("   ❌ No OI snapshot found")
        
        # Check strategy decisions
        print("\n🎯 Strategy Decisions:")
        decisions = db.fetch_strategy_decisions(instrument, limit=3)
        if decisions:
            for d in decisions[:3]:
                print(f"   {d['time'].strftime('%H:%M')} | Signal: {d['signal']} | Score: {d['score']} | Quality: {d['signal_quality']}")
        else:
            print("   ❌ No strategy decisions found")
        
        # Count candles for today
        print("\n📊 Today's Candle Count:")
        start_of_day = datetime.combine(today, datetime.min.time())
        end_of_day = start_of_day + timedelta(days=1)
        
        count_1m = db.get_candle_count(instrument, start_of_day, end_of_day, "1m")
        count_5m = db.get_candle_count(instrument, start_of_day, end_of_day, "5m")
        
        expected_1m = 375  # ~6.25 hours * 60 minutes
        expected_5m = 75   # ~6.25 hours * 12 candles per hour
        
        print(f"   1m candles: {count_1m}/{expected_1m} ({count_1m/max(expected_1m,1)*100:.1f}%)")
        print(f"   5m candles: {count_5m}/{expected_5m} ({count_5m/max(expected_5m,1)*100:.1f}%)")
        
        # Overall status
        print(f"\n📋 {instrument} Status:")
        if candles_1m and candles_5m and oi:
            print("   ✅ Data collection is working")
        elif candles_1m or candles_5m or oi:
            print("   ⚠️ Partial data - some components may have issues")
        else:
            print("   ❌ No data found - service not running or failed")
    
    # Check signals
    print(f"\n{'='*70}")
    print("SIGNALS ISSUED TODAY")
    print(f"{'='*70}")
    for instrument in instruments:
        signals = db._execute("""
            SELECT ts, signal, price, strike, strategy_score, signal_quality, reason
            FROM signals_issued
            WHERE instrument = %s
              AND DATE(ts AT TIME ZONE 'Asia/Kolkata') = CURRENT_DATE
            ORDER BY ts DESC
            LIMIT 3
        """, (instrument,))
        
        if signals:
            print(f"\n{instrument}:")
            for s in signals[:3]:
                print(f"   {s[0].strftime('%H:%M:%S')} | {s[1]} | Strike: {s[3]} | Score: {s[4]} | {s[5]}")
        else:
            print(f"\n{instrument}: No signals today")
    
    print(f"\n{'='*70}")
    print("HEALTH CHECK COMPLETE")
    print(f"{'='*70}")

if __name__ == "__main__":
    check_system_health()
