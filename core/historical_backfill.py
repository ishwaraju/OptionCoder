from dhan_client import DhanClient
from datetime import datetime, timedelta, timezone
from utils.time_utils import TimeUtils
from config import Config


class HistoricalBackfill:
    """
    Enhanced wrapper around Dhan historical APIs for candle recovery
    and indicator warmup using official intraday data.
    """

    def __init__(self):
        self.client = DhanClient()
        self.time_utils = TimeUtils()

    def _parse_api_timestamp(self, value):
        """Parse Dhan intraday timestamps returned as either ISO strings or Unix epoch seconds."""
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc).astimezone(self.time_utils.ist)

        if isinstance(value, str):
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    parsed = datetime.strptime(value, fmt)
                    return self.time_utils.ist.localize(parsed)
                except ValueError:
                    continue

        raise ValueError(f"Unsupported timestamp format: {value!r}")

    def fetch_intraday_candles(self, security_id, exchange_segment, instrument_type, from_date, to_date, interval=1, oi=False):
        response = self.client.get_intraday_data(
            security_id=security_id,
            exchange_segment=exchange_segment,
            instrument_type=instrument_type,
            from_date=from_date,
            to_date=to_date,
            interval=interval,
            oi=oi,
        )
        if not response:
            return []

        if isinstance(response, dict):
            if response.get("status") == "success" and isinstance(response.get("data"), list):
                return response["data"]
            if response.get("status") == "success" and isinstance(response.get("data"), dict):
                response = response["data"]
            if all(key in response for key in ["open", "high", "low", "close", "volume", "timestamp"]):
                rows = []
                timestamps = response.get("timestamp", [])
                for idx, ts in enumerate(timestamps):
                    rows.append(
                        {
                            "timestamp": ts,
                            "open": response["open"][idx],
                            "high": response["high"][idx],
                            "low": response["low"][idx],
                            "close": response["close"][idx],
                            "volume": response["volume"][idx],
                            "oi": response.get("oi", [None] * len(timestamps))[idx] if response.get("oi") else None,
                        }
                    )
                return rows

        return []

    def get_missing_candles_for_reconnect(self, last_candle_time, current_time=None):
        """
        Get missing candles since last known candle for reconnection recovery.
        
        Args:
            last_candle_time: datetime of last known candle
            current_time: Current time (defaults to now)
            
        Returns:
            list: Missing candle data or None if no significant gap
        """
        if current_time is None:
            current_time = self.time_utils.now_ist()
            
        if not last_candle_time:
            return None
            
        # Calculate time gap
        time_gap = current_time - last_candle_time
        
        # Only backfill if gap is significant (> 2 minutes)
        if time_gap.total_seconds() < 120:
            return None
            
        # Get security info from config
        security_id = Config.SECURITY_IDS.get(Config.SYMBOL)
        if not security_id:
            print(f"No security ID found for symbol {Config.SYMBOL}")
            return None
            
        # Calculate from_date (start from 2 minutes before last candle)
        from_date = last_candle_time - timedelta(minutes=2)
        to_date = current_time + timedelta(minutes=1)  # Small buffer
        
        # Format dates for API
        from_date_str = from_date.strftime("%Y-%m-%d")
        to_date_str = to_date.strftime("%Y-%m-%d")
        
        print(f"Backfilling candles from {from_date_str} to {to_date_str} (gap: {time_gap.total_seconds():.0f}s)")
        
        try:
            candles = self.fetch_intraday_candles(
                security_id=security_id,
                exchange_segment="IDX_I",
                instrument_type="INDEX",
                from_date=from_date_str,
                to_date=to_date_str,
                interval=1,
                oi=False
            )
            
            if not candles:
                print("No historical candles found for backfill")
                return None
                
            # Filter candles after last known candle
            filtered_candles = []
            for candle in candles:
                candle_time = self._parse_api_timestamp(candle["timestamp"])
                if candle_time > last_candle_time:
                    filtered_candles.append({
                        "datetime": candle_time,
                        "open": float(candle["open"]),
                        "high": float(candle["high"]),
                        "low": float(candle["low"]),
                        "close": float(candle["close"]),
                        "volume": int(candle["volume"]),
                        "tick_count": 1,  # Estimated
                    })
                    
            print(f"Backfilled {len(filtered_candles)} missing candles")
            return filtered_candles
            
        except Exception as e:
            print(f"Error during historical backfill: {e}")
            return None

    def warmup_indicators_on_startup(self, num_bars=50):
        """
        Fetch historical candles for indicator warmup on startup.
        
        Args:
            num_bars: Number of 5-minute bars to fetch
            
        Returns:
            list: Historical candles for warmup
        """
        security_id = Config.SECURITY_IDS.get(Config.SYMBOL)
        if not security_id:
            print(f"No security ID found for symbol {Config.SYMBOL}")
            return []
            
        current_time = self.time_utils.now_ist()
        
        # Calculate start time (num_bars * 5 minutes ago)
        start_time = current_time - timedelta(minutes=num_bars * 5)
        
        # Format dates
        from_date_str = start_time.strftime("%Y-%m-%d")
        to_date_str = current_time.strftime("%Y-%m-%d")
        
        print(f"Warming up indicators with {num_bars} 5-minute bars from {from_date_str}")
        
        try:
            candles = self.fetch_intraday_candles(
                security_id=security_id,
                exchange_segment="IDX_I",
                instrument_type="INDEX",
                from_date=from_date_str,
                to_date=to_date_str,
                interval=5,  # 5-minute candles
                oi=False
            )
            
            if not candles:
                print("No historical candles found for warmup")
                return []
                
            # Convert to standard format
            warmup_candles = []
            for candle in candles:
                candle_time = self._parse_api_timestamp(candle["timestamp"])
                warmup_candles.append({
                    "time": candle_time,
                    "open": float(candle["open"]),
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": float(candle["close"]),
                    "volume": int(candle["volume"]),
                })
                
            print(f"Warmed up indicators with {len(warmup_candles)} historical bars")
            return warmup_candles
            
        except Exception as e:
            print(f"Error during indicator warmup: {e}")
            return []

    def get_today_market_snapshot(self):
        """
        Get today's market data snapshot for context.
        
        Returns:
            dict: Market snapshot or None
        """
        security_id = Config.SECURITY_IDS.get(Config.SYMBOL)
        if not security_id:
            return None
            
        current_time = self.time_utils.now_ist()
        today_str = current_time.strftime("%Y-%m-%d")
        
        try:
            # Get today's daily data
            daily_data = self.client.get_historical_daily_data(
                security_id=security_id,
                exchange_segment="IDX_I",
                instrument_type="INDEX",
                from_date=today_str,
                to_date=today_str,
                oi=True
            )
            
            if daily_data and isinstance(daily_data, dict):
                return {
                    "date": today_str,
                    "open": float(daily_data.get("open", 0)),
                    "high": float(daily_data.get("high", 0)),
                    "low": float(daily_data.get("low", 0)),
                    "close": float(daily_data.get("close", 0)),
                    "volume": int(daily_data.get("volume", 0)),
                    "oi": int(daily_data.get("oi", 0)),
                }
                
        except Exception as e:
            print(f"Error fetching market snapshot: {e}")
            
        return None
