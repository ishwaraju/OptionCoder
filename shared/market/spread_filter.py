"""
Spread Filter Module
Filters options based on bid-ask spread and liquidity
"""

from config import Config


class SpreadFilter:
    """
    Filters options based on spread and liquidity criteria
    """
    
    def __init__(self):
        # Configurable thresholds
        self.max_spread_percent = float(getattr(Config, 'MAX_SPREAD_PERCENT', '5.0'))
        self.min_bid_quantity = int(getattr(Config, 'MIN_BID_QUANTITY', '100'))
        self.min_ask_quantity = int(getattr(Config, 'MIN_ASK_QUANTITY', '100'))
        self.min_spread_rupees = float(getattr(Config, 'MIN_SPREAD_RUPEES', '0.5'))
        
    def is_option_liquid(self, option_data):
        """
        Check if an option meets liquidity criteria
        
        Args:
            option_data: Dictionary containing option data with bid/ask info
            
        Returns:
            tuple: (is_liquid, reason)
        """
        if not option_data:
            return False, "No option data"
            
        bid_price = option_data.get('top_bid_price', 0)
        ask_price = option_data.get('top_ask_price', 0)
        bid_quantity = option_data.get('top_bid_quantity', 0)
        ask_quantity = option_data.get('top_ask_quantity', 0)
        ltp = option_data.get('ltp', 0)
        
        # Check if bid/ask exist
        if not bid_price or not ask_price:
            return False, "Missing bid/ask prices"
            
        # Calculate spread
        spread = float(ask_price) - float(bid_price)
        
        # Minimum spread in rupees
        if spread < self.min_spread_rupees:
            return False, f"Spread too low: {spread:.2f} < {self.min_spread_rupees}"
            
        # Maximum spread as percentage of option price
        if ltp > 0:
            spread_percent = (spread / ltp) * 100
            if spread_percent > self.max_spread_percent:
                return False, f"Spread too wide: {spread_percent:.1f}% > {self.max_spread_percent}%"
        
        # Minimum bid/ask quantities
        if bid_quantity < self.min_bid_quantity:
            return False, f"Low bid quantity: {bid_quantity} < {self.min_bid_quantity}"
            
        if ask_quantity < self.min_ask_quantity:
            return False, f"Low ask quantity: {ask_quantity} < {self.min_ask_quantity}"
            
        return True, "Liquid"
    
    def filter_strike_options(self, option_chain_data, strike):
        """
        Filter CE and PE options for a specific strike
        
        Args:
            option_chain_data: Full option chain data
            strike: Strike price to filter
            
        Returns:
            dict: Filtered results for CE and PE
        """
        if not option_chain_data or not option_chain_data.get('band_snapshots'):
            return {'ce': {'liquid': False, 'reason': 'No data'}, 
                   'pe': {'liquid': False, 'reason': 'No data'}}
        
        result = {'ce': {'liquid': False, 'reason': 'Not found'}, 
                 'pe': {'liquid': False, 'reason': 'Not found'}}
        
        for snapshot in option_chain_data['band_snapshots']:
            if snapshot['strike'] == strike:
                option_type = snapshot['option_type'].lower()
                is_liquid, reason = self.is_option_liquid(snapshot)
                result[option_type] = {
                    'liquid': is_liquid,
                    'reason': reason,
                    'spread': snapshot.get('spread'),
                    'bid_price': snapshot.get('top_bid_price'),
                    'ask_price': snapshot.get('top_ask_price'),
                    'bid_quantity': snapshot.get('top_bid_quantity'),
                    'ask_quantity': snapshot.get('top_ask_quantity'),
                    'ltp': snapshot.get('ltp')
                }
        
        return result
    
    def get_best_liquid_strike(self, option_chain_data, signal_type, target_strike=None):
        """
        Find the best liquid strike near target or ATM
        
        Args:
            option_chain_data: Full option chain data
            signal_type: 'CE' or 'PE'
            target_strike: Preferred strike (optional)
            
        Returns:
            dict: Best strike info or None if no liquid options
        """
        if not option_chain_data or not option_chain_data.get('band_snapshots'):
            return None
            
        liquid_options = []
        
        for snapshot in option_chain_data['band_snapshots']:
            if snapshot['option_type'] == signal_type:
                is_liquid, reason = self.is_option_liquid(snapshot)
                if is_liquid:
                    liquid_options.append({
                        'strike': snapshot['strike'],
                        'distance_from_atm': snapshot.get('distance_from_atm', 0),
                        'spread': snapshot.get('spread', 0),
                        'ltp': snapshot.get('ltp', 0),
                        'bid_quantity': snapshot.get('top_bid_quantity', 0),
                        'ask_quantity': snapshot.get('top_ask_quantity', 0),
                        'spread_percent': (snapshot.get('spread', 0) / snapshot.get('ltp', 1)) * 100 if snapshot.get('ltp', 0) > 0 else 999
                    })
        
        if not liquid_options:
            return None
            
        # Sort by spread percentage (tightest spread first), then by distance from ATM
        liquid_options.sort(key=lambda x: (x['spread_percent'], abs(x['distance_from_atm'])))
        
        # If target strike is provided and liquid, prefer it
        if target_strike:
            for option in liquid_options:
                if option['strike'] == target_strike:
                    return option
        
        # Return the best liquid option
        return liquid_options[0]
    
    def should_filter_signal(self, option_chain_data, signal, selected_strike):
        """
        Determine if a signal should be filtered based on spread liquidity
        
        Args:
            option_chain_data: Option chain data
            signal: 'CE' or 'PE'
            selected_strike: Selected strike price
            
        Returns:
            tuple: (should_filter, reason, alternative_strike)
        """
        if not signal or not selected_strike:
            return True, "No signal or strike", None
            
        # Check liquidity of selected strike
        filtered = self.filter_strike_options(option_chain_data, selected_strike)
        option_data = filtered.get(signal.lower())
        
        if not option_data['liquid']:
            # Try to find alternative liquid strike
            alternative = self.get_best_liquid_strike(option_chain_data, signal)
            if alternative:
                return True, f"Selected strike illiquid: {option_data['reason']}", alternative['strike']
            else:
                return True, f"No liquid {signal} options available", None
        
        return False, "Liquid", None
