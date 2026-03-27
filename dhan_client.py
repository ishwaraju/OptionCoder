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
            profile = self.dhan.get_profile()

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