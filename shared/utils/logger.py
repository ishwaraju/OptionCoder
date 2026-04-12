"""
Logger Module
Saves signals and trades to CSV
"""

import csv
import os
from datetime import datetime
import pytz


class TradeLogger:
    def __init__(self):
        self.ist = pytz.timezone('Asia/Kolkata')
        # self.signal_file = "data/signals.csv"  # Removed - using database as primary storage
        self.trade_file = "data/trades.csv"
        # self.decision_file = "data/decision_audit.csv"  # Removed - using database as primary storage
        # self.summary_file = f"data/session_summary_{self._now_ist().strftime('%Y%m%d')}.txt"  # Removed - using database as primary storage

        # Create files if not exist
        # self.create_file_if_not_exists(self.signal_file, [...])  # Removed - using database as primary storage

        self.create_file_if_not_exists(self.trade_file, [
            "time",
            "signal",
            "strike",
            "entry",
            "sl",
            "target",
            "exit",
            "pnl"
        ])

        # self.create_file_if_not_exists(self.decision_file, [...])  # Removed - using database as primary storage

    def create_file_if_not_exists(self, file, headers):
        """Create CSV file with headers"""
        os.makedirs(os.path.dirname(file), exist_ok=True)

        if not os.path.exists(file):
            with open(file, mode="w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(headers)

    def _now_ist(self):
        return datetime.now(self.ist)

    def log_signal(
            self,
            signal,
            strike,
            price,
            vwap,
            orb_high,
            orb_low,
            buffer,
            atr,
            candle_range,
            volume_signal,
            oi_bias,
            oi_strength,
            reason
    ):
        """Save signal to database (primary storage) - CSV method removed"""
        # Note: This method is kept for compatibility but actual storage happens via DBWriter
        # in signal_service.py _safe_save_strategy_decision method
        pass

    def log_trade(self, signal, strike, entry, sl, target, exit_price, pnl):
        """Save trade to trades.csv"""
        with open(self.trade_file, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                self._now_ist(),
                signal,
                strike,
                entry,
                sl,
                target,
                exit_price,
                pnl
            ])

    def log_decision(
            self,
            instrument,
            price,
            signal,
            strike,
            score,
            confidence,
            regime,
            manual_guidance,
            signal_valid_till,
            blockers,
            cautions,
            score_factors,
            reason,
            strike_reason,
    ):
        """Save decision to database (primary storage) - CSV method removed"""
        # Note: This method is kept for compatibility but actual storage happens via DBWriter
        # in signal_service.py _safe_save_strategy_decision method
        pass

    def write_session_summary(self, summary_text):
        """Write session summary to database (primary storage) - CSV method removed"""
        # Note: This method is kept for compatibility but actual storage happens via database queries
        # Session summaries can be generated from strategy_decisions_5m table
        pass
