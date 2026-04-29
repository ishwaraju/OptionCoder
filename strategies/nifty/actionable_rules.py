class NiftyActionableRules:
    """Instrument-specific live alert rules for NIFTY option buyers."""

    @classmethod
    def should_allow_signal(
        cls,
        signal_type,
        signal_grade,
        confidence,
        regime,
        score=0,
        pressure_conflict_level="NONE",
    ):
        signal_type = (signal_type or "NONE").upper()
        signal_grade = (signal_grade or "SKIP").upper()
        confidence = (confidence or "LOW").upper()
        regime = (regime or "UNKNOWN").upper()
        pressure_conflict_level = (pressure_conflict_level or "NONE").upper()

        if signal_grade != "B":
            return False
        if signal_type not in {"BREAKOUT_CONFIRM", "RETEST", "CONTINUATION"}:
            return False
        if confidence not in {"MEDIUM", "HIGH"}:
            return False
        if regime not in {"TRENDING", "EXPANDING", "OPENING_EXPANSION"}:
            return False
        if pressure_conflict_level not in {"NONE", "MILD"}:
            return False
        return float(score or 0) >= 84
