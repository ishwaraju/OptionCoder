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
        self.decision_file = "data/decision_audit.csv"
        self.summary_file = f"data/session_summary_{datetime.now().strftime('%Y%m%d')}.txt"

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

        self.create_file_if_not_exists(self.decision_file, [
            "time",
            "instrument",
            "price",
            "signal",
            "strike",
            "score",
            "confidence",
            "regime",
            "manual_guidance",
            "signal_valid_till",
            "blockers",
            "cautions",
            "score_factors",
            "reason",
            "strike_reason",
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
        with open(self.decision_file, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now(),
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
            ])

    def write_session_summary(self, summary_text):
        os.makedirs(os.path.dirname(self.summary_file), exist_ok=True)
        with open(self.summary_file, mode="w") as f:
            f.write(summary_text)
