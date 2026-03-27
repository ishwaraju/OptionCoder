"""
Logger Module
Saves signals and trades to CSV
"""

import csv
import os
from datetime import datetime


class TradeLogger:
    def __init__(self):
        self.signal_file = "data/signals.csv"
        self.trade_file = "data/trades.csv"

        # Create files if not exist
        self.create_file_if_not_exists(self.signal_file, [
            "time",
            "signal",
            "strike",
            "price",
            "vwap",
            "orb_high",
            "orb_low",
            "buffer",
            "atr",
            "candle_range",
            "volume_signal",
            "oi_bias",
            "oi_strength",
            "reason"
        ])

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

    def create_file_if_not_exists(self, file, headers):
        """Create CSV file with headers"""
        os.makedirs(os.path.dirname(file), exist_ok=True)

        if not os.path.exists(file):
            with open(file, mode="w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(headers)

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
        """Save signal to signals.csv"""
        with open(self.signal_file, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now(),
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
            ])

    def log_trade(self, signal, strike, entry, sl, target, exit_price, pnl):
        """Save trade to trades.csv"""
        with open(self.trade_file, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now(),
                signal,
                strike,
                entry,
                sl,
                target,
                exit_price,
                pnl
            ])