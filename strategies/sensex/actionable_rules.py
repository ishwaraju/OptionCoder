class SensexActionableRules:
    """Instrument-specific live alert rules for SENSEX option buyers."""

    @classmethod
    def should_allow_signal(
        cls,
        signal_type,
        signal_grade,
        confidence,
        regime,
        candle_time,
        score=0,
        entry_score=0,
        pressure_conflict_level="NONE",
    ):
        signal_type = (signal_type or "NONE").upper()
        signal_grade = (signal_grade or "SKIP").upper()
        confidence = (confidence or "LOW").upper()
        regime = (regime or "UNKNOWN").upper()
        pressure_conflict_level = (pressure_conflict_level or "NONE").upper()

        minute_of_day = None
        if candle_time is not None:
            minute_of_day = candle_time.hour * 60 + candle_time.minute
        early_session = minute_of_day is not None and minute_of_day <= (10 * 60 + 45)
        late_session = minute_of_day is not None and (13 * 60) <= minute_of_day <= (14 * 60 + 30)

        if signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM", "OPENING_DRIVE", "RETEST", "CONTINUATION"}:
            return False
        if confidence not in {"MEDIUM", "HIGH"}:
            return False
        if regime not in {"TRENDING", "EXPANDING", "EXPIRY_DAY", "OPENING_EXPANSION"}:
            return False

        if signal_grade in {"A", "A+"}:
            return True

        if early_session and signal_grade == "B" and signal_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "CONTINUATION", "RETEST"}:
            return True

        if (
            late_session
            and signal_grade == "B"
            and signal_type in {"BREAKOUT_CONFIRM", "RETEST", "CONTINUATION"}
            and regime in {"TRENDING", "EXPANDING"}
            and pressure_conflict_level in {"NONE", "MILD"}
            and float(score or 0) >= 78
            and float(entry_score or 0) >= 88
        ):
            return True

        return False
