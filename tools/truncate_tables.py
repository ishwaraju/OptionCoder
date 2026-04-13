#!/usr/bin/env python3
"""
Truncate all database tables except instrument_profiles for fresh start
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
import psycopg2

def truncate_tables():
    """Truncate all tables except instrument_profiles"""
    
    tables_to_truncate = [
        'candles_1m',
        'candles_5m', 
        'oi_snapshots_1m',
        'option_band_snapshots_1m',
        'strategy_decisions_5m',
        'signals_issued',
        'trade_monitor_events_1m'
    ]
    
    try:
        conn = psycopg2.connect(Config.get_db_dsn())
        cursor = conn.cursor()
        
        print("=" * 60)
        print("TRUNCATING ALL DATA TABLES (except instrument_profiles)")
        print("=" * 60)
        
        # Check instrument_profiles count before
        cursor.execute("SELECT COUNT(*) FROM instrument_profiles")
        profile_count = cursor.fetchone()[0]
        print(f"\n✓ instrument_profiles: {profile_count} records (KEPT)")
        
        # Truncate each table
        for table in tables_to_truncate:
            cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
            print(f"✓ {table}: TRUNCATED")
        
        conn.commit()
        
        # Verify
        print("\n" + "=" * 60)
        print("VERIFICATION")
        print("=" * 60)
        
        for table in tables_to_truncate:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count} rows")
        
        # Check instrument_profiles still has data
        cursor.execute("SELECT instrument, security_id FROM instrument_profiles WHERE enabled = TRUE")
        profiles = cursor.fetchall()
        print(f"\ninstrument_profiles (KEPT):")
        for inst, sec_id in profiles:
            print(f"  - {inst}: security_id={sec_id}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ ALL TABLES TRUNCATED SUCCESSFULLY")
        print("=" * 60)
        print("\nFresh start ready! You can now run your bot.")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Safety confirmation
    print("\n⚠️  WARNING: This will DELETE ALL DATA except instrument_profiles!")
    print("   Tables to be cleared: candles, OI snapshots, signals, trades\n")
    
    response = input("Are you sure? Type 'yes' to continue: ")
    
    if response.lower() == 'yes':
        truncate_tables()
    else:
        print("\nCancelled.")
        sys.exit(0)
