#!/usr/bin/env python3
"""
Test SENSEX futures volume API with BSE_FNO segment
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from dhan_client import DhanClient

def test_sensex_volume_api():
    """Test SENSEX futures volume API with BSE_FNO"""
    
    print("=" * 60)
    print("Testing SENSEX Futures Volume API")
    print("=" * 60)
    
    # Initialize Dhan client
    dhan_client = DhanClient()
    
    # Test with BSE_FNO segment
    security_id = 1105863  # SENSEX futures ID (BSXFUT)
    exchange_segment = "BSE_FNO"
    
    print(f"\nTesting with:")
    print(f"  Security ID: {security_id}")
    print(f"  Exchange Segment: {exchange_segment}")
    
    try:
        securities = {exchange_segment: [security_id]}
        result = dhan_client.dhan.quote_data(securities)
        
        print(f"\nAPI Response Status: {result.get('status')}")
        print(f"Full Response: {result}")
        
        if result.get("status") == "success":
            data = result.get("data", {}).get("data", {})
            fno_data = data.get(exchange_segment, {})
            instrument_data = fno_data.get(str(security_id), {})
            
            print(f"\n✅ API Call Successful!")
            print(f"  Instrument Data: {instrument_data}")
            print(f"  Volume: {instrument_data.get('volume', 'N/A')}")
            print(f"  LTP: {instrument_data.get('ltp', 'N/A')}")
        else:
            print(f"\n❌ API Call Failed")
            print(f"  Error: {result.get('message', 'Unknown error')}")
            print(f"  Code: {result.get('code', 'N/A')}")
    
    except Exception as e:
        print(f"\n❌ Exception: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    test_sensex_volume_api()
