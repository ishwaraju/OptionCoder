#!/usr/bin/env python3
"""
Check correct SENSEX futures security ID from Dhan API
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import csv
from config import Config

def check_sensex_future_id():
    """Fetch BSE FNO instrument list to find SENSEX futures"""
    
    # Try BSE_FNO segment (SENSEX is BSE index)
    url = 'https://api.dhan.co/v2/instrument/BSE_FNO'
    headers = {
        'Content-Type': 'application/json',
        'access-token': Config.DHAN_ACCESS_TOKEN,
        'client-id': Config.DHAN_CLIENT_ID
    }
    
    print(f"Fetching from: {url}")
    response = requests.get(url, headers=headers, timeout=10)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        # Parse CSV response
        lines = response.text.strip().split('\n')
        print(f"Total lines: {len(lines)}")
        
        # Parse header
        if len(lines) > 0:
            header = lines[0].split(',')
            print(f"Columns: {len(header)}")
            print(f"Header: {header[:10]}")  # Show first 10 columns
        
        # Search for SENSEX futures
        sensex_futures = []
        for line in lines[1:]:  # Skip header
            values = line.split(',')
            if len(values) < 10:
                continue
            
            # Column mapping based on header
            # 0: EXCH_ID, 1: SEGMENT, 2: SECURITY_ID, 3: ISIN, 4: INSTRUMENT, 
            # 5: UNDERLYING_SECURITY_ID, 6: UNDERLYING_SYMBOL, 7: SYMBOL_NAME, 8: DISPLAY_NAME, 9: INSTRUMENT_TYPE
            security_id = values[2]
            instrument = values[4]
            underlying_symbol = values[6]
            symbol_name = values[7]
            display_name = values[8]
            instrument_type = values[9]
            
            # Check all fields for SENSEX
            line_text = line.upper()
            if 'SENSEX' in line_text:
                if 'FUT' in instrument_type.upper() or 'FUTIDX' in instrument_type.upper():
                    sensex_futures.append({
                        'security_id': security_id,
                        'symbol_name': symbol_name,
                        'display_name': display_name,
                        'instrument_type': instrument_type,
                        'instrument': instrument,
                        'underlying_symbol': underlying_symbol
                    })
        
        if sensex_futures:
            print(f"\n✅ Found {len(sensex_futures)} SENSEX Futures:")
            for item in sensex_futures:
                print(f"   Symbol: {item['symbol_name']}")
                print(f"   Security ID: {item['security_id']}")
                print(f"   Instrument Type: {item['instrument_type']}")
                print(f"   Instrument: {item['instrument']}")
                print()
        else:
            print("\n❌ No SENSEX futures found in BSE_FNO")
            print("\nTrying NSE_FNO for comparison...")
            
            # Try NSE_FNO as well
            url_nse = 'https://api.dhan.co/v2/instrument/NSE_FNO'
            response_nse = requests.get(url_nse, headers=headers, timeout=10)
            print(f"NSE_FNO Status: {response_nse.status_code}")
            
            if response_nse.status_code == 200:
                lines_nse = response_nse.text.strip().split('\n')
                print(f"Total NSE_FNO lines: {len(lines_nse)}")
                
                for line in lines_nse[1:]:
                    values = line.split(',')
                    if len(values) < 10:
                        continue
                    
                    symbol_name = values[7]
                    instrument_type = values[9]
                    security_id = values[2]
                    
                    if 'SENSEX' in symbol_name.upper() and 'FUT' in instrument_type.upper():
                        print(f"\n✅ Found in NSE_FNO:")
                        print(f"   Symbol: {symbol_name}")
                        print(f"   Security ID: {security_id}")
                        print(f"   Instrument Type: {instrument_type}")
    else:
        print(f"API Error: {response.text}")

if __name__ == "__main__":
    check_sensex_future_id()
