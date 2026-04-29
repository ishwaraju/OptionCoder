from datetime import time


class BankNiftyActionableRules:
    """Instrument-specific live alert rules for BANKNIFTY option buyers."""

    B_GRADE_START_TIME = time(10, 45)

    @classmethod
    def should_allow_b_grade_breakout(
        cls,
        signal_type,
        signal_grade,
        confidence,
        regime,
        candle_time,
        score=0,
        pressure_conflict_level="NONE",
    ):
        pressure_conflict_level = (pressure_conflict_level or "NONE").upper()
        if signal_grade != "B":
            return False
        if signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "CONTINUATION"}:
            return False
        if confidence not in {"MEDIUM", "HIGH"}:
            return False
        if regime not in {"TRENDING", "EXPANDING"}:
            return False
        if candle_time is None:
            return False
        if candle_time.time() < cls.B_GRADE_START_TIME:
            return False
        if pressure_conflict_level not in {"NONE", "MILD"}:
            return False
        return float(score or 0) >= 80
