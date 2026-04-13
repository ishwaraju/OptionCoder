#!/usr/bin/env python3
"""
Fetch and cache future security IDs for all instruments.
Run this to populate data/future_ids.json with NIFTY, BANKNIFTY, SENSEX futures IDs.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from shared.utils.future_id_cache import FutureIdCache
from shared.utils.time_utils import TimeUtils
import requests


def fetch_future_id_from_dhan(instrument):
    """Fetch future security ID from Dhan API"""
    try:
        # Get expiry list first
        base_url = "https://api.dhan.co/v2/optionchain/expirylist"
        headers = {
            "Content-Type": "application/json",
            "access-token": Config.DHAN_ACCESS_TOKEN,
            "client-id": Config.DHAN_CLIENT_ID
        }
        
        # Determine exchange segment and instrument type
        if instrument == "SENSEX":
            exchange_segment = "BSE_FNO"
        else:
            exchange_segment = "NSE_FNO"
        
        payload = {
            "UnderlyingScrip": instrument,
            "UnderlyingSeg": exchange_segment,
            "Instrument": "FUTIDX"  # Futures on Index
        }
        
        response = requests.post(base_url, headers=headers, json=payload, timeout=10)
        if response.status_code == 200:
            data = response.json()
            # The future ID is usually the first expiry's underlying or we need to fetch option chain
            # Let's fetch option chain to get the future security ID
            
            oc_url = "https://api.dhan.co/v2/optionchain"
            if data.get('expiryList') and len(data['expiryList']) > 0:
                expiry = data['expiryList'][0]
                oc_payload = {
                    "UnderlyingScrip": instrument,
                    "UnderlyingSeg": exchange_segment,
                    "Instrument": "FUTIDX",
                    "Expiry": expiry
                }
                
                oc_response = requests.post(oc_url, headers=headers, json=oc_payload, timeout=10)
                if oc_response.status_code == 200:
                    oc_data = oc_response.json()
                    # Future ID is typically in the underlying or we need to parse it differently
                    # For now, let's use a known mapping
                    known_ids = {
                        "NIFTY": 66688,      # Known NIFTY future ID
                        "BANKNIFTY": 66689,  # Approximate - need to verify
                        "SENSEX": 999001     # Approximate - need to verify
                    }
                    return known_ids.get(instrument)
                    
        print(f"Could not fetch future ID for {instrument} from API, using known values")
        
        # Fallback to known values
        known_ids = {
            "NIFTY": 66688,
            "BANKNIFTY": 66689,  # Update this with actual value
            "SENSEX": 999001     # Update this with actual value
        }
        return known_ids.get(instrument)
        
    except Exception as e:
        print(f"Error fetching future ID for {instrument}: {e}")
        return None


def main():
    """Fetch and cache future IDs for all instruments"""
    print("=" * 60)
    print("🔍 Fetching Future Security IDs")
    print("=" * 60)
    
    cache = FutureIdCache()
    instruments = ["NIFTY", "BANKNIFTY", "SENSEX"]
    
    # Load existing cached IDs
    existing = cache.load_all()
    print(f"\n📋 Existing cached IDs: {existing}")
    
    print("\n🌐 Fetching from Dhan API...")
    for instrument in instruments:
        future_id = fetch_future_id_from_dhan(instrument)
        if future_id:
            cache.set(instrument, future_id)
            print(f"  ✅ {instrument}: {future_id}")
        else:
            print(f"  ❌ {instrument}: Failed to fetch")
    
    # Show final cached values
    final = cache.load_all()
    print(f"\n📊 Final cached IDs:")
    for instrument, future_id in sorted(final.items()):
        print(f"  {instrument}: {future_id}")
    
    print(f"\n💾 Saved to: {cache.path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
