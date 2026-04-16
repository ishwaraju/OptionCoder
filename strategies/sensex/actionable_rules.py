class SensexActionableRules:
    """Instrument-specific live alert rules for SENSEX option buyers."""

    @classmethod
    def should_allow_signal(cls, signal_type, signal_grade, confidence, regime, candle_time):
        signal_type = (signal_type or "NONE").upper()
        signal_grade = (signal_grade or "SKIP").upper()
        confidence = (confidence or "LOW").upper()
        regime = (regime or "UNKNOWN").upper()

        if signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM", "OPENING_DRIVE", "RETEST"}:
            return False
        if signal_grade not in {"A", "A+"}:
            return False
        if confidence not in {"MEDIUM", "HIGH"}:
            return False
        if regime not in {"TRENDING", "EXPANDING", "EXPIRY_DAY"}:
            return False
        return True
