#!/usr/bin/env python3
"""
OI Historical Backfill Tool
Load OI data for a specific date into oi_snapshots_1m table
Similar to candle reload but for OI data
"""

from argparse import ArgumentParser
from datetime import datetime, timedelta
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config
from dhan_client import DhanClient
from shared.db.pool import DBPool
from shared.db.writer import DBWriter
from shared.utils.time_utils import TimeUtils


def floor_to_1m(ts):
    """Floor timestamp to 1 minute"""
    return ts.replace(second=0, microsecond=0)


def floor_to_5m(ts):
    """Floor timestamp to 5 minutes"""
    return ts.replace(minute=(ts.minute // 5) * 5, second=0, microsecond=0)


def aggregate_5m_oi(oi_data):
    """Aggregate 1-minute OI data to 5-minute intervals"""
    buckets = {}
    
    for data in oi_data:
        bucket_ts = floor_to_5m(data['ts'])
        if bucket_ts not in buckets:
            buckets[bucket_ts] = {
                'ts': bucket_ts,
                'ce_oi': [],
                'pe_oi': [],
                'ce_volume': [],
                'pe_volume': [],
                'underlying_price': []
            }
        
        bucket = buckets[bucket_ts]
        bucket['ce_oi'].append(data['ce_oi'])
        bucket['pe_oi'].append(data['pe_oi'])
        bucket['ce_volume'].append(data['ce_volume'])
        bucket['pe_volume'].append(data['pe_volume'])
        bucket['underlying_price'].append(data['underlying_price'])
    
    # Calculate 5-minute aggregates
    aggregated = []
    for bucket_ts in sorted(buckets):
        bucket = buckets[bucket_ts]
        
        # Use last values for OI (OI doesn't aggregate like price)
        ce_oi = bucket['ce_oi'][-1] if bucket['ce_oi'] else 0
        pe_oi = bucket['pe_oi'][-1] if bucket['pe_oi'] else 0
        
        # Sum volumes
        ce_volume = sum(bucket['ce_volume'])
        pe_volume = sum(bucket['pe_volume'])
        
        # Use last price
        underlying_price = bucket['underlying_price'][-1] if bucket['underlying_price'] else 0
        
        # Calculate PCR
        pcr = pe_oi / ce_oi if ce_oi > 0 else 0
        
        aggregated.append({
            'ts': bucket_ts,
            'instrument': data['instrument'],
            'underlying_price': underlying_price,
            'ce_oi': ce_oi,
            'pe_oi': pe_oi,
            'ce_volume': ce_volume,
            'pe_volume': pe_volume,
            'ce_volume_band': ce_volume,  # Will be calculated later
            'pe_volume_band': pe_volume,  # Will be calculated later
            'pcr': pcr,
            # Enhanced fields
            'ce_oi_change': 0,
            'pe_oi_change': 0,
            'total_oi_change': 0,
            'oi_sentiment': 'NEUTRAL',
            'oi_bias_strength': 0.0,
            'total_volume': ce_volume + pe_volume,
            'volume_change': 0,
            'volume_pcr': pe_volume / ce_volume if ce_volume > 0 else 0,
            'max_ce_oi_strike': 0,
            'max_pe_oi_strike': 0,
            'oi_concentration': 0.0,
            'oi_trend': 'SIDEWAYS',
            'trend_strength': 0.0,
            'support_level': 0.0,
            'resistance_level': 0.0,
            'oi_range_width': 0.0,
            'previous_ts': None,
            'data_age_seconds': 0,
            'data_quality': 'GOOD',
            'max_ce_oi_amount': ce_oi,
            'max_pe_oi_amount': pe_oi,
            'oi_spread': abs(ce_oi - pe_oi),
            'liquidity_score': 0.0
        })
    
    return aggregated


def get_option_chain_oi(dhan_client, exchange, segment, expiry, current_time):
    """Get OI data from option chain"""
    try:
        option_chain = dhan_client.option_chain(exchange, segment, expiry)
        
        if not option_chain:
            return None, None, None, None, None, None
        
        # Calculate total OI and volume
        total_ce_oi = 0
        total_pe_oi = 0
        total_ce_volume = 0
        total_pe_volume = 0
        max_ce_oi_strike = 0
        max_pe_oi_strike = 0
        max_ce_oi_amount = 0
        max_pe_oi_amount = 0
        
        for strike_data in option_chain:
            ce_data = strike_data.get('CE', {})
            pe_data = strike_data.get('PE', {})
            
            ce_oi = ce_data.get('openInterest', 0)
            pe_oi = pe_data.get('openInterest', 0)
            ce_volume = ce_data.get('volume', 0)
            pe_volume = pe_data.get('volume', 0)
            
            total_ce_oi += ce_oi
            total_pe_oi += pe_oi
            total_ce_volume += ce_volume
            total_pe_volume += pe_volume
            
            # Track max OI strikes
            if ce_oi > max_ce_oi_amount:
                max_ce_oi_amount = ce_oi
                max_ce_oi_strike = strike_data.get('strike', 0)
            
            if pe_oi > max_pe_oi_amount:
                max_pe_oi_amount = pe_oi
                max_pe_oi_strike = strike_data.get('strike', 0)
        
        return total_ce_oi, total_pe_oi, total_ce_volume, total_pe_volume, max_ce_oi_strike, max_pe_oi_strike
    
    except Exception as e:
        print(f"Error getting option chain: {e}")
        return None, None, None, None, None, None


def get_historical_price(dhan_client, security_id, exchange_segment, instrument_type, target_date):
    """Get historical price data for the day"""
    try:
        # Get 1-minute historical data
        data = dhan_client.get_intraday_data(
            security_id=security_id,
            exchange_segment=exchange_segment,
            instrument_type=instrument_type,
            from_date=target_date,
            to_date=target_date,
            interval=1,
            oi=False
        )
        
        if not data:
            return []
        
        print(f"Raw data type: {type(data)}")
        
        # Check if API returned error
        if isinstance(data, dict) and data.get('status') == 'failure':
            print(f"API Error: {data.get('remarks', {}).get('error_message', 'Unknown error')}")
            print("Falling back to mock data generation...")
            return generate_mock_price_data(target_date)
        
        print(f"Raw data sample: {data[:3] if len(data) > 3 else data}")
        
        # Convert to list of dicts with timestamps
        price_data = []
        base_time = datetime.strptime(target_date, '%Y-%m-%d')
        
        # Handle different data formats
        if isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    # If item is a dict, extract price
                    price = item.get('close') or item.get('price') or item.get('ltp') or 0
                else:
                    # If item is a number, use it directly
                    try:
                        price = float(item)
                    except (ValueError, TypeError):
                        price = 0
                
                timestamp = base_time + timedelta(minutes=i * 5)  # Assume 5-minute intervals
                price_data.append({
                    'ts': timestamp,
                    'price': price
                })
        else:
            print(f"Unexpected data format: {type(data)}")
            return generate_mock_price_data(target_date)
        
        print(f"Processed {len(price_data)} price points")
        return price_data
    
    except Exception as e:
        print(f"Error getting historical price: {e}")
        print("Falling back to mock data generation...")
        return generate_mock_price_data(target_date)


def generate_mock_price_data(target_date):
    """Generate mock price data for testing when API fails"""
    print(f"Generating mock price data for {target_date}")
    
    price_data = []
    base_time = datetime.strptime(target_date, '%Y-%m-%d')
    
    # Generate mock price data for trading hours (9:15 AM to 3:30 PM)
    start_minute = 9 * 60 + 15  # 9:15 AM = 555 minutes
    end_minute = 15 * 60 + 30  # 3:30 PM = 930 minutes
    
    # Start with a base price
    base_price = 19800.0
    current_price = base_price
    
    for minute in range(start_minute, end_minute + 1, 5):  # 5-minute intervals
        # Generate realistic price movement
        import random
        price_change = random.uniform(-50, 50)  # Random change between -50 and +50
        current_price += price_change
        
        # Keep price within reasonable range
        current_price = max(19500, min(20100, current_price))
        
        timestamp = base_time + timedelta(minutes=minute)
        price_data.append({
            'ts': timestamp,
            'price': current_price
        })
    
    print(f"Generated {len(price_data)} mock price points")
    return price_data


def load_oi_day(date, instrument, security_id, exchange_segment, replace_day=False):
    """Load OI data for a specific day"""
    
    print(f"Loading OI data for {instrument} on {date}")

    if not (Config.USE_MOCK_DATA or Config.TEST_MODE):
        print("reload_oi_day is disabled in live mode.")
        print("Reason: this tool currently generates simulated OI data, not real historical option-chain snapshots.")
        print("Enable USE_MOCK_DATA=True or TEST_MODE=True if you intentionally want mock OI backfill.")
        return False
    
    # Initialize components
    dhan_client = DhanClient()
    db_writer = DBWriter()
    time_utils = TimeUtils()
    
    if not dhan_client.connected:
        print("Failed to connect to Dhan API")
        return False
    
    if not db_writer.enabled:
        print("Database not enabled")
        return False
    
    # Delete existing data if requested
    if replace_day:
        print(f"Deleting existing OI data for {date}")
        try:
            with DBPool.connection() as conn:
                if conn is None:
                    raise RuntimeError("DB pool connection unavailable")
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM oi_snapshots_1m WHERE DATE(ts) = %s AND instrument = %s", (date, instrument))
                    deleted = cur.rowcount
                    print(f"Deleted {deleted} existing OI records")
        except Exception as e:
            print(f"Error deleting existing data: {e}")
    
    # Get historical price data for the day
    print(f"Getting historical price data for {date}")
    price_data = get_historical_price(dhan_client, security_id, exchange_segment, 'INDEX', date)
    
    if not price_data:
        print("No price data found")
        return False
    
    print(f"Found {len(price_data)} price points")
    
    # Collect OI data for each minute
    oi_data = []
    base_date = datetime.strptime(date, '%Y-%m-%d')
    
    for i, price_point in enumerate(price_data):
        current_time = price_point['ts']
        underlying_price = price_point['price']
        
        # Get OI data (mock for now - would need real historical OI API)
        # For now, we'll simulate OI data based on price movement
        ce_oi = 1000000 + int(underlying_price * 50) + (i * 1000)  # Simulated
        pe_oi = 800000 + int((20000 - underlying_price) * 40) + (i * 800)  # Simulated
        ce_volume = 50000 + i * 100  # Simulated
        pe_volume = 40000 + i * 80  # Simulated
        
        # Calculate metrics (scaled to fit numeric(8,4) constraints)
        pcr = round(min(pe_oi / ce_oi if ce_oi > 0 else 0, 9999.9999), 4)
        total_volume = ce_volume + pe_volume
        volume_pcr = round(min(pe_volume / ce_volume if ce_volume > 0 else 0, 9999.9999), 4)
        oi_spread = round(min(abs(ce_oi - pe_oi) / 100000, 9999.9999), 4)  # Scale down to fit
        
        # Determine sentiment based on price movement
        if i > 0:
            price_change = underlying_price - price_data[i-1]['price']
            if price_change > 0:
                sentiment = "BULLISH"
                strength = round(min(abs(price_change) / 1000, 0.9999), 4)
            elif price_change < 0:
                sentiment = "BEARISH"
                strength = round(min(abs(price_change) / 1000, 0.9999), 4)
            else:
                sentiment = "SIDEWAYS"
                strength = 0.0
        else:
            sentiment = "NEUTRAL"
            strength = 0.0
        
        oi_record = {
            'ts': current_time,
            'instrument': instrument,
            'underlying_price': underlying_price,
            'ce_oi': ce_oi,
            'pe_oi': pe_oi,
            'ce_volume': ce_volume,
            'pe_volume': pe_volume,
            'ce_volume_band': ce_volume,
            'pe_volume_band': pe_volume,
            'pcr': pcr,
            # Enhanced fields
            'ce_oi_change': 0,
            'pe_oi_change': 0,
            'total_oi_change': 0,
            'oi_sentiment': sentiment,
            'oi_bias_strength': strength,
            'total_volume': total_volume,
            'volume_change': 0,
            'volume_pcr': volume_pcr,
            'max_ce_oi_strike': int(underlying_price / 50) * 50 + 100,
            'max_pe_oi_strike': int(underlying_price / 50) * 50 - 100,
            'oi_concentration': round(min(0.7, 0.9999), 4),
            'oi_trend': 'ACCUMULATION' if i > len(price_data)//2 else 'SIDEWAYS',
            'trend_strength': round(min(strength, 0.9999), 4),
            'support_level': underlying_price - 50,
            'resistance_level': underlying_price + 50,
            'oi_range_width': round(min(100, 9999.9999), 4),
            'previous_ts': price_data[i-1]['ts'] if i > 0 else None,
            'data_age_seconds': 0,
            'data_quality': 'GOOD',
            'max_ce_oi_amount': ce_oi,
            'max_pe_oi_amount': pe_oi,
            'oi_spread': oi_spread,
            'liquidity_score': round(min(total_volume / 1000000, 0.9999), 4)
        }
        
        oi_data.append(oi_record)
        
        # Progress indicator
        if (i + 1) % 100 == 0:
            print(f"Processed {i + 1}/{len(price_data)} OI records")
    
    print(f"Generated {len(oi_data)} OI records")
    
    # Insert 1-minute OI data
    print("Inserting 1-minute OI data...")
    inserted_1m = 0
    
    for record in oi_data:
        try:
            row = (
                record['ts'], record['instrument'], record['underlying_price'],
                record['ce_oi'], record['pe_oi'], record['ce_volume'], record['pe_volume'],
                record['ce_volume_band'], record['pe_volume_band'], record['pcr'],
                record['ce_oi_change'], record['pe_oi_change'], record['total_oi_change'],
                record['oi_sentiment'], record['oi_bias_strength'],
                record['total_volume'], record['volume_change'], record['volume_pcr'],
                record['max_ce_oi_strike'], record['max_pe_oi_strike'], record['oi_concentration'],
                record['oi_trend'], record['trend_strength'],
                record['support_level'], record['resistance_level'], record['oi_range_width'],
                record['previous_ts'], record['data_age_seconds'], record['data_quality'],
                record['max_ce_oi_amount'], record['max_pe_oi_amount'], record['oi_spread'], record['liquidity_score']
            )
            
            db_writer.insert_oi_1m(row)
            inserted_1m += 1
            
        except Exception as e:
            print(f"Error inserting 1m OI record: {e}")
    
    print(f"Inserted {inserted_1m} 1-minute OI records")
    
    # Create 5-minute aggregates
    print("Creating 5-minute OI aggregates...")
    oi_5m = aggregate_5m_oi(oi_data)
    print(f"Created {len(oi_5m)} 5-minute OI aggregates")
    
    # Insert 5-minute OI data
    print("Inserting 5-minute OI data...")
    inserted_5m = 0
    
    for record in oi_5m:
        try:
            row = (
                record['ts'], record['instrument'], record['underlying_price'],
                record['ce_oi'], record['pe_oi'], record['ce_volume'], record['pe_volume'],
                record['ce_volume_band'], record['pe_volume_band'], record['pcr'],
                record['ce_oi_change'], record['pe_oi_change'], record['total_oi_change'],
                record['oi_sentiment'], record['oi_bias_strength'],
                record['total_volume'], record['volume_change'], record['volume_pcr'],
                record['max_ce_oi_strike'], record['max_pe_oi_strike'], record['oi_concentration'],
                record['oi_trend'], record['trend_strength'],
                record['support_level'], record['resistance_level'], record['oi_range_width'],
                record['previous_ts'], record['data_age_seconds'], record['data_quality'],
                record['max_ce_oi_amount'], record['max_pe_oi_amount'], record['oi_spread'], record['liquidity_score']
            )
            
            db_writer.insert_oi_1m(row)
            inserted_5m += 1
            
        except Exception as e:
            print(f"Error inserting 5m OI record: {e}")
    
    print(f"Inserted {inserted_5m} 5-minute OI records")
    
    print(f"Successfully loaded OI data for {date}")
    print(f"Total OI records: {inserted_1m + inserted_5m}")
    
    return True


def main():
    parser = ArgumentParser(description="Load OI data for a specific date into oi_snapshots_1m table")
    parser.add_argument("--date", required=True, help="Trading date in YYYY-MM-DD")
    parser.add_argument("--symbol", default=Config.SYMBOL, help="Symbol (default: NIFTY)")
    parser.add_argument("--security-id", type=int, default=None, help="Security ID (auto-detected if not provided)")
    parser.add_argument("--exchange-segment", default="IDX_I", help="Exchange segment")
    parser.add_argument("--instrument-type", default="INDEX", help="Instrument type")
    parser.add_argument("--replace-day", action="store_true", help="Delete existing OI data for the day before insert")
    
    args = parser.parse_args()
    
    # Auto-detect security ID if not provided
    if args.security_id is None:
        if args.symbol == "NIFTY":
            args.security_id = getattr(Config, 'NIFTY_SECURITY_ID', 13)
        elif args.symbol == "BANKNIFTY":
            args.security_id = getattr(Config, 'BANKNIFTY_SECURITY_ID', 23)
        else:
            print(f"Unknown symbol: {args.symbol}")
            return 1
    
    print(f"Loading OI data for {args.symbol} ({args.security_id}) on {args.date}")
    
    success = load_oi_day(
        date=args.date,
        instrument=args.symbol,
        security_id=args.security_id,
        exchange_segment=args.exchange_segment,
        replace_day=args.replace_day
    )
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
