from datetime import time


class InstrumentActionableRules:
    """Shared instrument-aware option-buyer actionability rules."""

    BANKNIFTY_B_GRADE_START_TIME = time(10, 45)

    @classmethod
    def should_allow_signal(
        cls,
        instrument,
        signal_type,
        signal_grade,
        confidence,
        regime,
        candle_time=None,
        score=0,
        entry_score=0,
        pressure_conflict_level="NONE",
    ):
        instrument = (instrument or "NIFTY").upper()
        signal_type = (signal_type or "NONE").upper()
        signal_grade = (signal_grade or "SKIP").upper()
        confidence = (confidence or "LOW").upper()
        regime = (regime or "UNKNOWN").upper()
        pressure_conflict_level = (pressure_conflict_level or "NONE").upper()

        if instrument == "NIFTY":
            return cls._allow_nifty(
                signal_type=signal_type,
                signal_grade=signal_grade,
                confidence=confidence,
                regime=regime,
                score=score,
                pressure_conflict_level=pressure_conflict_level,
            )
        if instrument == "BANKNIFTY":
            return cls._allow_banknifty(
                signal_type=signal_type,
                signal_grade=signal_grade,
                confidence=confidence,
                regime=regime,
                candle_time=candle_time,
                score=score,
                pressure_conflict_level=pressure_conflict_level,
            )
        if instrument == "SENSEX":
            return cls._allow_sensex(
                signal_type=signal_type,
                signal_grade=signal_grade,
                confidence=confidence,
                regime=regime,
                candle_time=candle_time,
                score=score,
                entry_score=entry_score,
                pressure_conflict_level=pressure_conflict_level,
            )
        return False

    @classmethod
    def _allow_nifty(cls, signal_type, signal_grade, confidence, regime, score, pressure_conflict_level):
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

    @classmethod
    def _allow_banknifty(
        cls,
        signal_type,
        signal_grade,
        confidence,
        regime,
        candle_time,
        score,
        pressure_conflict_level,
    ):
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
        if candle_time.time() < cls.BANKNIFTY_B_GRADE_START_TIME:
            return False
        if pressure_conflict_level not in {"NONE", "MILD"}:
            return False
        return float(score or 0) >= 80

    @classmethod
    def _allow_sensex(
        cls,
        signal_type,
        signal_grade,
        confidence,
        regime,
        candle_time,
        score,
        entry_score,
        pressure_conflict_level,
    ):
        minute_of_day = None
        if candle_time is not None:
            minute_of_day = candle_time.hour * 60 + candle_time.minute
        late_session = minute_of_day is not None and (13 * 60) <= minute_of_day <= (14 * 60 + 30)
        ultra_late_session = minute_of_day is not None and minute_of_day >= (14 * 60 + 25)

        if signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM", "OPENING_DRIVE", "RETEST", "CONTINUATION"}:
            return False
        if confidence not in {"MEDIUM", "HIGH"}:
            return False
        if regime not in {"TRENDING", "EXPANDING", "EXPIRY_DAY", "OPENING_EXPANSION"}:
            return False
        if minute_of_day is not None and minute_of_day >= (14 * 60 + 35):
            return False

        if ultra_late_session:
            return (
                signal_grade == "A+"
                and confidence == "HIGH"
                and pressure_conflict_level == "NONE"
                and float(score or 0) >= 88
                and float(entry_score or 0) >= 90
                and signal_type in {"BREAKOUT_CONFIRM", "RETEST"}
            )

        if signal_grade in {"A", "A+"}:
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
