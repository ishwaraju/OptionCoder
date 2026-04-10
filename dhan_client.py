"""
Dhan API Client for Trading Application
Handles authentication and API interactions
"""

from dhanhq import dhanhq
from config import Config
import logging
import time


class DhanClient:
    """Dhan API wrapper class"""

    def __init__(self):
        """Initialize Dhan client with credentials"""
        self.client_id = Config.DHAN_CLIENT_ID
        self.access_token = Config.DHAN_ACCESS_TOKEN
        self.dhan = None
        self.connected = False

        if self.client_id and self.access_token:
            self.connect()

    def connect(self):
        """Connect to Dhan API"""
        try:
            self.dhan = dhanhq(self.client_id, self.access_token)
            
            # Try to get profile, but handle if method doesn't exist
            try:
                profile = self.dhan.get_positions()
            except AttributeError as e:
                if "get_profile" in str(e):
                    # get_profile method doesn't exist, use alternative connection check
                    logging.warning("get_profile method not available, using alternative connection check")
                    profile = {"status": "connected"}  # Mock profile for connection check
                else:
                    raise e
            except Exception as e:
                logging.error(f"Error getting profile: {e}")
                profile = None

            if profile:
                self.connected = True
                logging.info("Connected to Dhan API")
                return True
            else:
                logging.error("Failed to connect to Dhan API")
                return False

        except Exception as e:
            logging.error(f"Connection error: {e}")
            return False

    def reconnect(self):
        """Reconnect if disconnected"""
        if not self.connected:
            logging.info("Reconnecting to Dhan API...")
            time.sleep(2)
            self.connect()

    def is_connected(self):
        """Check connection"""
        return self.connected

    def get_market_quote(self, instrument_id, quote_mode='quote'):
        """Get market quote"""
        try:
            if not self.connected:
                self.reconnect()

            return self.dhan.get_market_quote(instrument_id, quote_mode)

        except Exception as e:
            logging.error(f"Market quote error: {e}")
            self.connected = False
            return None

    def get_market_quote_batch(self, instruments, quote_mode='quote'):
        """
        Fetch market quote snapshot for multiple instruments.
        quote_mode can be ticker / ohlc / quote depending on client support.
        """
        try:
            if not self.connected:
                self.reconnect()
            return self.dhan.get_market_quote(instruments, quote_mode)
        except Exception as e:
            logging.error(f"Market quote batch error: {e}")
            self.connected = False
            return None

    def get_intraday_data(self, security_id, exchange_segment, instrument_type, from_date=None, to_date=None, interval=1, oi=False):
        """Fetch historical intraday OHLC data using DhanHQ v2 API."""
        try:
            if not self.connected:
                self.reconnect()
            
            # DhanHQ v2 API requires from_date and to_date with time format
            from utils.time_utils import TimeUtils
            time_utils = TimeUtils()
            
            if not from_date:
                # Default to today's date with time
                from_date = f"{time_utils.today_str()} 09:15:00"
            if not to_date:
                # Default to today's date with time
                to_date = f"{time_utils.today_str()} 15:30:00"
            
            # Ensure from_date and to_date have time format for v2 API
            if len(from_date) == 10:  # Only date, no time
                from_date = f"{from_date} 09:15:00"
            if len(to_date) == 10:  # Only date, no time
                to_date = f"{to_date} 15:30:00"
            
            # DhanHQ v2 API supports all parameters except oi for intraday
            return self.dhan.intraday_minute_data(
                security_id=security_id,
                exchange_segment=exchange_segment,
                instrument_type=instrument_type,
                from_date=from_date,
                to_date=to_date,
                interval=interval
            )
            
        except Exception as e:
            logging.error(f"Intraday data error: {e}")
            self.connected = False
            return None

    def get_historical_daily_data(self, security_id, exchange_segment, instrument_type, from_date, to_date, expiry_code=0, oi=False):
        """Fetch daily OHLC data using DhanHQ v2 API."""
        try:
            if not self.connected:
                self.reconnect()
            
            # DhanHQ v2 API supports all parameters
            return self.dhan.historical_daily_data(
                security_id=security_id,
                exchange_segment=exchange_segment,
                instrument_type=instrument_type,
                from_date=from_date,
                to_date=to_date,
                expiry_code=expiry_code,
                oi=oi
            )
        except Exception as e:
            logging.error(f"Historical daily data error: {e}")
            self.connected = False
            return None

    def get_option_chain(self, exchange, segment, expiry):
        """Get option chain"""
        try:
            if not self.connected:
                self.reconnect()

            return self.dhan.get_option_chain(exchange, segment, expiry)

        except Exception as e:
            logging.error(f"Option chain error: {e}")
            self.connected = False
            return None

    def place_order(self, order_data):
        """Place order (Safe mode with paper trade check)"""
        try:
            if Config.PAPER_TRADE:
                logging.info(f"[PAPER TRADE] Order: {order_data}")
                return {"status": "paper_trade", "order": order_data}

            if not self.connected:
                self.reconnect()

            logging.info(f"Placing REAL order: {order_data}")
            return self.dhan.place_order(order_data)

        except Exception as e:
            logging.error(f"Order error: {e}")
            self.connected = False
            return None

    def get_positions(self):
        """Get positions"""
        try:
            if not self.connected:
                self.reconnect()

            return self.dhan.get_positions()

        except Exception as e:
            logging.error(f"Positions error: {e}")
            self.connected = False
            return None

    def get_order_book(self):
        """Get order book"""
        try:
            if not self.connected:
                self.reconnect()

            return self.dhan.get_order_book()

        except Exception as e:
            logging.error(f"Order book error: {e}")
            self.connected = False
            return None

    def cancel_order(self, order_id):
        """Cancel order"""
        try:
            if not self.connected:
                self.reconnect()

            return self.dhan.cancel_order(order_id)

        except Exception as e:
            logging.error(f"Cancel order error: {e}")
            self.connected = False
            return None

    def test_connection(self):
        """Test API connection"""
        if self.connected:
            print("Dhan API Connected")
        else:
            print("Dhan API NOT Connected")
