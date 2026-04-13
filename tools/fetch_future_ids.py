#!/usr/bin/env python3
"""
Fetch and cache future security IDs for all instruments (NSE + BSE).
Run this to populate data/future_ids.json with NIFTY, BANKNIFTY, SENSEX futures IDs.

- NIFTY, BANKNIFTY: NSE indices (fetched from NSE_FNO)
- SENSEX: BSE index (fetched from BSE_FNO)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from shared.utils.future_id_cache import FutureIdCache
import requests


# Segment mapping for each instrument
INSTRUMENT_SEGMENTS = {
    "NIFTY": "NSE_FNO",
    "BANKNIFTY": "NSE_FNO",
    "SENSEX": "BSE_FNO"
}


def fetch_from_instrument_list(segment: str, instrument_name: str):
    """Fetch future ID from Dhan instrument CSV endpoint"""
    url = f'https://api.dhan.co/v2/instrument/{segment}'
    headers = {
        'Content-Type': 'application/json',
        'access-token': Config.DHAN_ACCESS_TOKEN,
        'client-id': Config.DHAN_CLIENT_ID
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"  ❌ API Error {response.status_code} for {segment}")
            return None

        lines = response.text.strip().split('\n')
        if len(lines) < 2:
            return None

        # Parse CSV - columns: 0=EXCH_ID, 1=SEGMENT, 2=SECURITY_ID, 4=INSTRUMENT,
        # 6=UNDERLYING_SYMBOL, 7=SYMBOL_NAME, 8=DISPLAY_NAME, 9=INSTRUMENT_TYPE
        for line in lines[1:]:
            values = line.split(',')
            if len(values) < 10:
                continue

            symbol_name = values[7]
            instrument_type = values[9]

            # Match instrument name and future type
            if instrument_name.upper() in symbol_name.upper():
                if 'FUT' in instrument_type.upper() or 'FUTIDX' in instrument_type.upper():
                    return int(values[2])  # SECURITY_ID

        return None

    except Exception as e:
        print(f"  ❌ Error fetching from {segment}: {e}")
        return None


def fetch_future_id(instrument: str):
    """Fetch future security ID for any instrument (NSE or BSE)"""
    segment = INSTRUMENT_SEGMENTS.get(instrument, "NSE_FNO")
    return fetch_from_instrument_list(segment, instrument)


def main():
    """Fetch and cache future IDs for all instruments"""
    print("=" * 60)
    print("🔍 Fetching Future Security IDs (NSE + BSE)")
    print("=" * 60)

    cache = FutureIdCache()
    instruments = ["NIFTY", "BANKNIFTY", "SENSEX"]

    # Load existing cached IDs
    existing = cache.load_all()
    print(f"\n📋 Existing cached IDs: {existing if existing else 'None'}")

    print("\n🌐 Fetching from Dhan Instrument API...")
    for instrument in instruments:
        segment = INSTRUMENT_SEGMENTS[instrument]
        print(f"\n  [{instrument}] from {segment}...")
        future_id = fetch_future_id(instrument)
        if future_id:
            cache.set(instrument, future_id)
            print(f"  ✅ {instrument}: {future_id}")
        else:
            print(f"  ❌ {instrument}: Not found")

    # Show final cached values
    final = cache.load_all()
    print(f"\n📊 Final cached IDs:")
    for instrument, future_id in sorted(final.items()):
        print(f"  {instrument}: {future_id}")

    print(f"\n💾 Saved to: {cache.path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
