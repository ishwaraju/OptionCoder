"""
Option Trading Module
Handles option chain analysis and trading operations

Note:
This module is kept as an auxiliary/legacy helper. The active live decision
path is driven by EventEngine + BreakoutStrategy.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dhan_client import DhanClient
from config import Config
import logging

class OptionTrader:
    """Option trading class with analysis and execution capabilities"""
    
    def __init__(self):
        """Initialize option trader"""
        self.client = DhanClient()
        self.exchanges = ['NSE', 'BSE', 'MCX']
        self.segments = ['IDX_I', 'IDX_II', 'COM']
    
    def get_option_chain_data(self, exchange='NSE', segment='IDX_I', expiry_date=None):
        """
        Get option chain data for analysis
        
        Args:
            exchange: Exchange name (default: NSE)
            segment: Segment name (default: IDX_I for Index Options)
            expiry_date: Expiry date (default: current week expiry)
        
        Returns:
            DataFrame with option chain data
        """
        if not self.client.is_connected():
            logging.error("Dhan client not connected")
            return None
        
        try:
            # If no expiry provided, get current week expiry
            if not expiry_date:
                expiry_date = self.get_current_week_expiry()
            
            # Fetch option chain
            option_chain = self.client.get_option_chain(exchange, segment, expiry_date)
            
            if option_chain and 'data' in option_chain:
                df = pd.DataFrame(option_chain['data'])
                return self.process_option_chain(df)
            
            return None
            
        except Exception as e:
            logging.error(f"Error getting option chain: {e}")
            return None
    
    def get_current_week_expiry(self):
        """Get current week expiry date"""
        today = datetime.now()
        # Find Thursday (weekly expiry for Nifty)
        days_until_thursday = (3 - today.weekday()) % 7
        if days_until_thursday == 0:
            days_until_thursday = 7
        
        expiry = today + timedelta(days=days_until_thursday)
        return expiry.strftime('%Y-%m-%d')
    
    def process_option_chain(self, df):
        """Process and clean option chain data"""
        if df.empty:
            return df
        
        # Convert numeric columns
        numeric_columns = ['strike_price', 'last_price', 'volume', 'open_interest', 
                          'implied_volatility', 'delta', 'theta', 'gamma', 'vega']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Calculate additional metrics
        if 'volume' in df.columns and 'open_interest' in df.columns:
            df['volume_oi_ratio'] = df['volume'] / df['open_interest'].replace(0, 1)
        
        return df
    
    def analyze_option_chain(self, df, underlying_price=None):
        """
        Analyze option chain for trading opportunities
        
        Args:
            df: Option chain DataFrame
            underlying_price: Current underlying price
        
        Returns:
            Dictionary with analysis results
        """
        if df.empty:
            return None
        
        analysis = {
            'total_call_oi': 0,
            'total_put_oi': 0,
            'total_call_volume': 0,
            'total_put_volume': 0,
            'max_oi_call_strike': None,
            'max_oi_put_strike': None,
            'max_volume_call_strike': None,
            'max_volume_put_strike': None,
            'atm_strike': None
        }
        
        try:
            # Separate calls and puts
            calls = df[df['option_type'] == 'CE'] if 'option_type' in df.columns else df
            puts = df[df['option_type'] == 'PE'] if 'option_type' in df.columns else df
            
            # Calculate totals
            if not calls.empty:
                analysis['total_call_oi'] = calls['open_interest'].sum() if 'open_interest' in calls.columns else 0
                analysis['total_call_volume'] = calls['volume'].sum() if 'volume' in calls.columns else 0
                
                # Find max OI and volume strikes
                if 'open_interest' in calls.columns:
                    max_oi_call = calls.loc[calls['open_interest'].idxmax()]
                    analysis['max_oi_call_strike'] = max_oi_call['strike_price']
                
                if 'volume' in calls.columns:
                    max_vol_call = calls.loc[calls['volume'].idxmax()]
                    analysis['max_volume_call_strike'] = max_vol_call['strike_price']
            
            if not puts.empty:
                analysis['total_put_oi'] = puts['open_interest'].sum() if 'open_interest' in puts.columns else 0
                analysis['total_put_volume'] = puts['volume'].sum() if 'volume' in puts.columns else 0
                
                # Find max OI and volume strikes
                if 'open_interest' in puts.columns:
                    max_oi_put = puts.loc[puts['open_interest'].idxmax()]
                    analysis['max_oi_put_strike'] = max_oi_put['strike_price']
                
                if 'volume' in puts.columns:
                    max_vol_put = puts.loc[puts['volume'].idxmax()]
                    analysis['max_volume_put_strike'] = max_vol_put['strike_price']
            
            # Find ATM strike if underlying price provided
            if underlying_price and 'strike_price' in df.columns:
                df['distance'] = abs(df['strike_price'] - underlying_price)
                atm = df.loc[df['distance'].idxmin()]
                analysis['atm_strike'] = atm['strike_price']
            
            return analysis
            
        except Exception as e:
            logging.error(f"Error analyzing option chain: {e}")
            return analysis
    
    def find_best_options(self, df, option_type='CE', strategy='oi_based', top_n=5):
        """
        Find best options based on different strategies
        
        Args:
            df: Option chain DataFrame
            option_type: 'CE' for calls, 'PE' for puts
            strategy: 'oi_based', 'volume_based', 'price_based'
            top_n: Number of top options to return
        
        Returns:
            DataFrame with top options
        """
        if df.empty:
            return pd.DataFrame()
        
        try:
            # Filter by option type
            if 'option_type' in df.columns:
                filtered_df = df[df['option_type'] == option_type].copy()
            else:
                filtered_df = df.copy()
            
            if filtered_df.empty:
                return pd.DataFrame()
            
            # Sort based on strategy
            if strategy == 'oi_based' and 'open_interest' in filtered_df.columns:
                filtered_df = filtered_df.sort_values('open_interest', ascending=False)
            elif strategy == 'volume_based' and 'volume' in filtered_df.columns:
                filtered_df = filtered_df.sort_values('volume', ascending=False)
            elif strategy == 'price_based' and 'last_price' in filtered_df.columns:
                filtered_df = filtered_df.sort_values('last_price', ascending=True)
            
            return filtered_df.head(top_n)
            
        except Exception as e:
            logging.error(f"Error finding best options: {e}")
            return pd.DataFrame()
    
    def generate_signal(self, price, vwap, orb_high, orb_low, call_oi_change, put_oi_change):
        """
        Generate trading signal based on price action and OI analysis
        
        Args:
            price: Current price
            vwap: Volume weighted average price
            orb_high: Opening range breakout high
            orb_low: Opening range breakout low
            call_oi_change: Change in call open interest
            put_oi_change: Change in put open interest
        
        Returns:
            'CE' for call signal, 'PE' for put signal, or None for no signal
        """
        # CE Signal
        if price > orb_high and price > vwap:
            if put_oi_change > 0 and call_oi_change < 0:
                return "CE"
        
        # PE Signal
        if price < orb_low and price < vwap:
            if call_oi_change > 0 and put_oi_change < 0:
                return "PE"
        
        return None
    
    def select_itm_strike(self, nifty_price, option_type):
        """
        Select ITM (In-The-Money) strike price
        
        Args:
            nifty_price: Current Nifty price
            option_type: 'CE' for Call, 'PE' for Put
        
        Returns:
            int: Selected strike price
        """
        strike = round(nifty_price / 50) * 50
        
        if option_type == "CE":
            return strike - 100
        else:
            return strike + 100
