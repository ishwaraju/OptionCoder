"""
OI/Quote Snapshot Confirmation Module
Confirms OI and quote data consistency before signal triggers
"""

from config import Config
import time
from datetime import timedelta, datetime
import pytz


class OIQuoteConfirmation:
    """
    Validates OI and quote data consistency for signal confirmation
    """
    
    def __init__(self):
        # Confirmation thresholds
        self.min_oi_change_percent = float(getattr(Config, 'MIN_OI_CHANGE_PERCENT', '10.0'))
        self.min_volume_threshold = int(getattr(Config, 'MIN_VOLUME_THRESHOLD', '1000'))
        self.max_quote_age_seconds = int(getattr(Config, 'MAX_QUOTE_AGE_SECONDS', '30'))
        self.oi_confirmation_window = int(getattr(Config, 'OI_CONFIRMATION_WINDOW', '3'))  # minutes
        
        # IST timezone for consistent time handling
        self.ist = pytz.timezone('Asia/Kolkata')
        
        # Track OI changes over time
        self.oi_history = []
        self.quote_snapshots = []
        self.last_confirmation_time = 0
        
    def add_oi_snapshot(self, timestamp, ce_oi, pe_oi, ce_volume, pe_volume, price):
        """
        Add OI snapshot for tracking changes
        
        Args:
            timestamp: Snapshot timestamp
            ce_oi: Call option OI
            pe_oi: Put option OI  
            ce_volume: Call option volume
            pe_volume: Put option volume
            price: Underlying price
        """
        # Ensure timestamp is timezone-aware (IST)
        if timestamp and hasattr(timestamp, 'tzinfo') and timestamp.tzinfo is None:
            timestamp = self.ist.localize(timestamp)
        elif timestamp is None:
            timestamp = datetime.now(self.ist)
        
        snapshot = {
            'timestamp': timestamp,
            'ce_oi': ce_oi or 0,
            'pe_oi': pe_oi or 0,
            'ce_volume': ce_volume or 0,
            'pe_volume': pe_volume or 0,
            'total_oi': (ce_oi or 0) + (pe_oi or 0),
            'total_volume': (ce_volume or 0) + (pe_volume or 0),
            'price': price or 0
        }
        
        self.oi_history.append(snapshot)
        
        # Keep only recent history (last 30 minutes)
        cutoff_time = timestamp - timedelta(minutes=30)
        self.oi_history = [s for s in self.oi_history if s['timestamp'] > cutoff_time]
        
    def add_quote_snapshot(self, timestamp, option_data):
        """
        Add quote snapshot for market depth validation
        
        Args:
            timestamp: Snapshot timestamp
            option_data: Option chain data with bid/ask
        """
        if not option_data:
            return
            
        # Ensure timestamp is timezone-aware (IST)
        if timestamp and hasattr(timestamp, 'tzinfo') and timestamp.tzinfo is None:
            timestamp = self.ist.localize(timestamp)
        elif timestamp is None:
            timestamp = datetime.now(self.ist)
        
        snapshot = {
            'timestamp': timestamp,
            'ce_spread': option_data.get('ce_spread', 0),
            'pe_spread': option_data.get('pe_spread', 0),
            'ce_bid_qty': option_data.get('ce_top_bid_price', 0),
            'pe_bid_qty': option_data.get('pe_top_bid_price', 0),
            'ce_ask_qty': option_data.get('ce_top_ask_price', 0),
            'pe_ask_qty': option_data.get('pe_top_ask_price', 0),
            'atm_strike': option_data.get('atm', 0),
            'underlying_price': option_data.get('underlying_price', 0)
        }
        
        self.quote_snapshots.append(snapshot)
        
        # Keep only recent quotes (last 5 minutes)
        cutoff_time = timestamp - timedelta(minutes=5)
        self.quote_snapshots = [s for s in self.quote_snapshots if s['timestamp'] > cutoff_time]
        
    def get_oi_momentum(self, window_minutes=None):
        """
        Calculate OI momentum over specified window
        
        Args:
            window_minutes: Analysis window in minutes
            
        Returns:
            dict: OI momentum metrics
        """
        if window_minutes is None:
            window_minutes = self.oi_confirmation_window
            
        if len(self.oi_history) < 2:
            return {'momentum': 0, 'ce_change': 0, 'pe_change': 0, 'valid': False}
            
        # Get snapshots within window
        current_time = time.time() if isinstance(time.time(), float) else time.time()
        cutoff_time = current_time - (window_minutes * 60)
        
        recent_snapshots = [s for s in self.oi_history if s['timestamp'].timestamp() > cutoff_time]
        
        if len(recent_snapshots) < 2:
            return {'momentum': 0, 'ce_change': 0, 'pe_change': 0, 'valid': False}
            
        # Calculate changes
        oldest = recent_snapshots[0]
        newest = recent_snapshots[-1]
        
        ce_change = newest['ce_oi'] - oldest['ce_oi']
        pe_change = newest['pe_oi'] - oldest['pe_oi']
        total_change = ce_change + pe_change
        
        # Calculate percentage changes
        ce_change_pct = (ce_change / oldest['ce_oi'] * 100) if oldest['ce_oi'] > 0 else 0
        pe_change_pct = (pe_change / oldest['pe_oi'] * 100) if oldest['pe_oi'] > 0 else 0
        total_change_pct = (total_change / oldest['total_oi'] * 100) if oldest['total_oi'] > 0 else 0
        
        return {
            'momentum': total_change_pct,
            'ce_change': ce_change_pct,
            'pe_change': pe_change_pct,
            'ce_abs': ce_change,
            'pe_abs': pe_change,
            'total_abs': total_change,
            'valid': True,
            'volume_avg': sum(s['total_volume'] for s in recent_snapshots) / len(recent_snapshots)
        }
        
    def check_liquidity_confirmation(self, signal_type='BOTH'):
        """
        Check if current market liquidity meets confirmation criteria
        
        Args:
            signal_type: 'CE', 'PE', or 'BOTH'
            
        Returns:
            tuple: (confirmed, reason, metrics)
        """
        if not self.quote_snapshots:
            return False, "No quote data available", {}
            
        latest_quote = self.quote_snapshots[-1]
        current_time = time.time() if isinstance(time.time(), float) else time.time()
        
        # Check quote age
        quote_age = current_time - latest_quote['timestamp'].timestamp()
        if quote_age > self.max_quote_age_seconds:
            return False, f"Quote data too old: {quote_age:.0f}s", {'quote_age': quote_age}
            
        metrics = {
            'quote_age': quote_age,
            'ce_spread': latest_quote['ce_spread'],
            'pe_spread': latest_quote['pe_spread'],
            'ce_liquidity': latest_quote['ce_bid_qty'] > 0 and latest_quote['ce_ask_qty'] > 0,
            'pe_liquidity': latest_quote['pe_bid_qty'] > 0 and latest_quote['pe_ask_qty'] > 0
        }
        
        # Check spread limits
        max_spread = float(getattr(Config, 'MAX_SPREAD_PERCENT', '5.0'))
        if signal_type in ['CE', 'BOTH'] and latest_quote['ce_spread'] > max_spread:
            return False, f"CE spread too wide: {latest_quote['ce_spread']:.1f}%", metrics
            
        if signal_type in ['PE', 'BOTH'] and latest_quote['pe_spread'] > max_spread:
            return False, f"PE spread too wide: {latest_quote['pe_spread']:.1f}%", metrics
            
        # Check bid/ask presence
        if signal_type in ['CE', 'BOTH'] and not metrics['ce_liquidity']:
            return False, "CE lacks bid/ask liquidity", metrics
            
        if signal_type in ['PE', 'BOTH'] and not metrics['pe_liquidity']:
            return False, "PE lacks bid/ask liquidity", metrics
            
        return True, "Liquidity confirmed", metrics
        
    def confirm_signal_with_oi(self, signal, confidence_threshold=0.7):
        """
        Confirm signal based on OI momentum and liquidity
        
        Args:
            signal: 'CE' or 'PE' signal
            confidence_threshold: Minimum confidence required
            
        Returns:
            tuple: (confirmed, reason, confidence, metrics)
        """
        # Get OI momentum
        oi_momentum = self.get_oi_momentum()
        if not oi_momentum['valid']:
            return False, "Insufficient OI data", 0.0, oi_momentum
            
        # Get liquidity confirmation
        liquidity_confirmed, liquidity_reason, liquidity_metrics = self.check_liquidity_confirmation(signal)
        
        # Calculate OI confirmation score
        oi_score = 0.0
        if signal == 'CE':
            oi_score = min(1.0, abs(oi_momentum['ce_change']) / self.min_oi_change_percent)
        elif signal == 'PE':
            oi_score = min(1.0, abs(oi_momentum['pe_change']) / self.min_oi_change_percent)
            
        # Volume confirmation
        volume_score = min(1.0, oi_momentum['volume_avg'] / self.min_volume_threshold)
        
        # Combined confidence
        liquidity_score = 1.0 if liquidity_confirmed else 0.0
        combined_confidence = (oi_score * 0.4) + (volume_score * 0.3) + (liquidity_score * 0.3)
        
        metrics = {
            **oi_momentum,
            **liquidity_metrics,
            'oi_score': oi_score,
            'volume_score': volume_score,
            'liquidity_score': liquidity_score,
            'combined_confidence': combined_confidence
        }
        
        if combined_confidence >= confidence_threshold:
            return True, f"Signal confirmed (confidence: {combined_confidence:.2f})", combined_confidence, metrics
        else:
            return False, f"Insufficient confirmation (confidence: {combined_confidence:.2f})", combined_confidence, metrics
            
    def should_trigger_1m_confirmation(self, signal, current_time):
        """
        Check if 1m trigger should be confirmed based on OI/quote data
        
        Args:
            signal: 'CE' or 'PE' signal
            current_time: Current timestamp
            
        Returns:
            tuple: (should_trigger, reason, confidence)
        """
        # Rate limit confirmations
        if current_time.timestamp() - self.last_confirmation_time < 60:
            return False, "Confirmation rate limited", 0.0
            
        # Confirm signal
        confirmed, reason, confidence, metrics = self.confirm_signal_with_oi(signal)
        
        if confirmed:
            self.last_confirmation_time = current_time.timestamp()
            
        return confirmed, reason, confidence
        
    def get_confirmation_summary(self):
        """
        Get current confirmation state summary
        
        Returns:
            dict: Summary of confirmation metrics
        """
        oi_momentum = self.get_oi_momentum()
        liquidity_confirmed, liquidity_reason, liquidity_metrics = self.check_liquidity_confirmation()
        
        return {
            'oi_momentum': oi_momentum,
            'liquidity': {
                'confirmed': liquidity_confirmed,
                'reason': liquidity_reason,
                'metrics': liquidity_metrics
            },
            'data_quality': {
                'oi_snapshots': len(self.oi_history),
                'quote_snapshots': len(self.quote_snapshots),
                'last_oi_time': self.oi_history[-1]['timestamp'] if self.oi_history else None,
                'last_quote_time': self.quote_snapshots[-1]['timestamp'] if self.quote_snapshots else None
            }
        }
