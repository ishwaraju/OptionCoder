from datetime import time


class BankNiftyActionableRules:
    """Instrument-specific live alert rules for BANKNIFTY option buyers."""

    B_GRADE_START_TIME = time(11, 0)

    @classmethod
    def should_allow_b_grade_breakout(cls, signal_type, signal_grade, confidence, regime, candle_time):
        if signal_grade != "B":
            return False
        if signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM"}:
            return False
        if confidence not in {"MEDIUM", "HIGH"}:
            return False
        if regime != "TRENDING":
            return False
        if candle_time is None:
            return False
        return candle_time.time() >= cls.B_GRADE_START_TIME
