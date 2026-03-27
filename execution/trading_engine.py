"""
Trading Engine Module
Handles order placement, position management, and risk management
"""

import logging
from datetime import datetime
from dhan_client import DhanClient
from config import Config
from strategy.option_trader import OptionTrader

class TradingEngine:
    """Main trading engine for option buying"""
    
    def __init__(self):
        """Initialize trading engine"""
        self.client = DhanClient()
        self.option_trader = OptionTrader()
        self.positions = {}
        self.orders = {}
        self.trailing_sl = {}
        
    def buy_option(self, symbol, strike_price, option_type, quantity=None, 
                   exchange='NSE', segment='IDX_I', expiry=None, order_type='MARKET'):
        """
        Buy an option
        
        Args:
            symbol: Underlying symbol (e.g., 'NIFTY', 'BANKNIFTY')
            strike_price: Strike price
            option_type: 'CE' for Call, 'PE' for Put
            quantity: Number of lots (default from config)
            exchange: Exchange name
            segment: Segment name
            expiry: Expiry date
            order_type: 'MARKET' or 'LIMIT'
        
        Returns:
            Order response or None
        """
        if not self.client.is_connected():
            logging.error("Dhan client not connected")
            return None
        
        try:
            # Set default quantity
            if quantity is None:
                quantity = Config.DEFAULT_QUANTITY
            
            # Get expiry if not provided
            if expiry is None:
                expiry = self.option_trader.get_current_week_expiry()
            
            # Construct instrument ID (format may vary based on Dhan API)
            instrument_id = f"{symbol}_{expiry}_{strike_price}_{option_type}"
            
            # Prepare order data
            order_data = {
                'exchange': exchange,
                'segment': segment,
                'security_id': instrument_id,
                'transaction_type': 'BUY',
                'order_type': order_type,
                'quantity': quantity,
                'price': 0 if order_type == 'MARKET' else None,
                'validity': 'DAY',
                'disclosed_quantity': 0,
                'trigger_price': None,
                'product_type': 'INTRADAY'  # Can be 'DELIVERY' for positional
            }
            
            # Place order
            response = self.client.place_order(order_data)
            
            if response and 'data' in response:
                order_id = response['data'].get('orderId')
                if order_id:
                    self.orders[order_id] = {
                        'timestamp': datetime.now(),
                        'symbol': symbol,
                        'strike_price': strike_price,
                        'option_type': option_type,
                        'quantity': quantity,
                        'order_type': order_type,
                        'status': 'PLACED'
                    }
                    logging.info(f"Order placed successfully: {order_id}")
                    return response
            
            logging.error(f"Failed to place order: {response}")
            return None
            
        except Exception as e:
            logging.error(f"Error buying option: {e}")
            return None
    
    def sell_option(self, symbol, strike_price, option_type, quantity=None,
                   exchange='NSE', segment='IDX_I', expiry=None, order_type='MARKET'):
        """
        Sell an option (for closing positions)
        
        Args:
            Same as buy_option but transaction_type is 'SELL'
        
        Returns:
            Order response or None
        """
        if not self.client.is_connected():
            logging.error("Dhan client not connected")
            return None
        
        try:
            # Set default quantity
            if quantity is None:
                quantity = Config.DEFAULT_QUANTITY
            
            # Get expiry if not provided
            if expiry is None:
                expiry = self.option_trader.get_current_week_expiry()
            
            # Construct instrument ID
            instrument_id = f"{symbol}_{expiry}_{strike_price}_{option_type}"
            
            # Prepare order data
            order_data = {
                'exchange': exchange,
                'segment': segment,
                'security_id': instrument_id,
                'transaction_type': 'SELL',
                'order_type': order_type,
                'quantity': quantity,
                'price': 0 if order_type == 'MARKET' else None,
                'validity': 'DAY',
                'disclosed_quantity': 0,
                'trigger_price': None,
                'product_type': 'INTRADAY'
            }
            
            # Place order
            response = self.client.place_order(order_data)
            
            if response and 'data' in response:
                order_id = response['data'].get('orderId')
                if order_id:
                    self.orders[order_id] = {
                        'timestamp': datetime.now(),
                        'symbol': symbol,
                        'strike_price': strike_price,
                        'option_type': option_type,
                        'quantity': quantity,
                        'order_type': order_type,
                        'status': 'PLACED'
                    }
                    logging.info(f"Sell order placed successfully: {order_id}")
                    return response
            
            logging.error(f"Failed to place sell order: {response}")
            return None
            
        except Exception as e:
            logging.error(f"Error selling option: {e}")
            return None
    
    def get_current_positions(self):
        """Get current open positions"""
        try:
            positions = self.client.get_positions()
            if positions and 'data' in positions:
                # Filter for option positions
                option_positions = []
                for pos in positions['data']:
                    if pos.get('segment') in ['IDX_I', 'IDX_II']:
                        option_positions.append(pos)
                return option_positions
            return []
        except Exception as e:
            logging.error(f"Error getting positions: {e}")
            return []
    
    def calculate_pnl(self, position):
        """Calculate P&L for a position"""
        try:
            entry_price = position.get('averagePrice', 0)
            current_price = position.get('lastPrice', 0)
            quantity = position.get('quantity', 0)
            
            if position.get('transactionType') == 'BUY':
                pnl = (current_price - entry_price) * quantity
            else:
                pnl = (entry_price - current_price) * quantity
            
            return pnl
        except Exception as e:
            logging.error(f"Error calculating P&L: {e}")
            return 0
    
    def place_stop_loss(self, position, stop_loss_percent=None):
        """Place stop loss order for a position"""
        try:
            if stop_loss_percent is None:
                stop_loss_percent = Config.STOP_LOSS_PERCENT
            
            current_price = position.get('lastPrice', 0)
            quantity = position.get('quantity', 0)
            
            # Calculate stop loss price
            if position.get('transactionType') == 'BUY':
                stop_loss_price = current_price * (1 - stop_loss_percent / 100)
                # Place sell stop loss
                return self.sell_option(
                    position.get('symbol'),
                    position.get('strikePrice'),
                    position.get('optionType'),
                    quantity,
                    order_type='STOP_LOSS'
                )
            
            return None
            
        except Exception as e:
            logging.error(f"Error placing stop loss: {e}")
            return None
    
    def get_account_balance(self):
        """Get account balance and margin details"""
        try:
            # This would depend on Dhan API's specific endpoint
            # For now, returning a placeholder
            return {
                'balance': 0,
                'margin_used': 0,
                'margin_available': 0
            }
        except Exception as e:
            logging.error(f"Error getting account balance: {e}")
            return None
    
    def risk_check(self, order_value):
        """Check if order passes risk management rules"""
        try:
            account_balance = self.get_account_balance()
            if not account_balance:
                return False
            
            max_risk_amount = account_balance['balance'] * (Config.MAX_RISK_PERCENT / 100)
            
            if order_value > max_risk_amount:
                logging.warning(f"Order value {order_value} exceeds max risk {max_risk_amount}")
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"Error in risk check: {e}")
            return False
    
    def calculate_sl_target(self, entry_price):
        """
        Calculate stop loss, target, and trailing trigger prices
        
        Args:
            entry_price: Entry price of the option
        
        Returns:
            tuple: (stop_loss, target, trail_trigger)
        """
        sl = entry_price * 0.75
        target = entry_price * 1.50
        trail_trigger = entry_price * 1.20
        return sl, target, trail_trigger
    
    def calculate_position_size(self, balance, entry_price, sl_price, lot_size):
        """
        Calculate position size based on risk management
        
        Args:
            balance: Account balance
            entry_price: Entry price of the option
            sl_price: Stop loss price
            lot_size: Lot size for the symbol
        
        Returns:
            int: Number of lots to trade
        """
        risk_amount = balance * 0.02
        risk_per_lot = (entry_price - sl_price) * lot_size
        lots = int(risk_amount / risk_per_lot)
        return max(lots, 1)
    
    def get_lot_size(self, symbol):
        """
        Get lot size for different indices
        
        Args:
            symbol: Index symbol (NIFTY, BANKNIFTY, FINNIFTY, etc.)
        
        Returns:
            int: Lot size for the symbol
        """
        if symbol == "NIFTY":
            return 65
        elif symbol == "BANKNIFTY":
            return 30
        elif symbol == "FINNIFTY":
            return 60
        elif symbol == "MIDCPNIFTY":
            return 120
        elif symbol == "SENSEX":
            return 20
        else:
            return 50
    
    def get_total_pnl(self):
        """
        Calculate total P&L from all current positions
        
        Returns:
            float: Total P&L across all positions
        """
        positions = self.get_current_positions()
        total_pnl = 0

        for pos in positions:
            pnl = self.calculate_pnl(pos)
            total_pnl += pnl

        return total_pnl
    
    def manage_positions(self):
        """
        Manage open positions with stop loss, target, and trailing logic
        
        Monitors all current positions and exits based on SL/Target conditions
        """
        positions = self.get_current_positions()

        for pos in positions:
            entry_price = pos.get('averagePrice')
            current_price = pos.get('lastPrice')
            quantity = pos.get('quantity')
            symbol = pos.get('symbol')
            strike = pos.get('strikePrice')
            option_type = pos.get('optionType')

            sl, target, trail_trigger = self.calculate_sl_target(entry_price)

            position_id = f"{symbol}_{strike}_{option_type}"

            # Stop Loss
            if current_price <= sl:
                print("SL Hit. Exiting position.")
                self.sell_option(symbol, strike, option_type, quantity)
                return

            # Target
            if current_price >= target:
                print("Target Hit. Exiting position.")
                self.sell_option(symbol, strike, option_type, quantity)
                return

            # Trailing SL activate
            if current_price >= trail_trigger:
                if position_id not in self.trailing_sl:
                    self.trailing_sl[position_id] = current_price * 0.85

                # Update trailing SL if price goes higher
                new_trail = current_price * 0.85
                if new_trail > self.trailing_sl[position_id]:
                    self.trailing_sl[position_id] = new_trail

                print(f"Trailing SL: {self.trailing_sl[position_id]}")

                # If price falls to trailing SL → exit
                if current_price <= self.trailing_sl[position_id]:
                    print("Trailing SL Hit. Exiting position.")
                    self.sell_option(symbol, strike, option_type, quantity)
                    return
