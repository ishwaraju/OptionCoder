class InstrumentActionableRules:
    """Shared instrument-aware option-buyer actionability rules."""

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
                candle_time=candle_time,
                score=score,
                entry_score=entry_score,
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
    def _allow_nifty(cls, signal_type, signal_grade, confidence, regime, candle_time, score, entry_score, pressure_conflict_level):
        score = float(score or 0)
        entry_score = float(entry_score or 0)
        effective_score = max(score, entry_score)
        late_reversal_window = (
            candle_time is not None and (candle_time.hour, candle_time.minute) >= (13, 30)
        )
        if confidence not in {"MEDIUM", "HIGH"}:
            return False
        if pressure_conflict_level not in {"NONE", "MILD"}:
            return False
        if regime in {"UNKNOWN", "NO_DATA", "NO_TRADE_WINDOW"}:
            return False
        late_day_breakdown_ok = (
            signal_type == "BREAKOUT_CONFIRM"
            and regime in {"LATE_DAY_BREAKDOWN", "LATE_DAY_BREAKOUT"}
            and effective_score >= 70
            and signal_grade in {"WATCH", "B", "A", "A+"}
        )
        if late_day_breakdown_ok:
            return True

        if signal_type in {"REVERSAL", "TRAP_REVERSAL"}:
            return (
                signal_grade in {"A", "A+"}
                and confidence == "HIGH"
                and pressure_conflict_level == "NONE"
                and score >= 88
                and entry_score >= 84
                and not late_reversal_window
            )

        if signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "CONTINUATION"}:
            return False

        choppy_regime = regime in {"RANGING", "CHOPPY", "EXPIRY_DAY"}
        breakout_floor = 80 if choppy_regime else 76
        confirm_floor = 84 if choppy_regime else 80

        if signal_grade in {"A", "A+"}:
            return effective_score >= breakout_floor
        if signal_grade != "B":
            return False
        return effective_score >= confirm_floor

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
        score = float(score or 0)
        effective_score = score
        late_day_breakout_ok = (
            signal_type == "BREAKOUT_CONFIRM"
            and regime in {"LATE_DAY_BREAKDOWN", "LATE_DAY_BREAKOUT"}
            and signal_grade in {"WATCH", "B", "A", "A+"}
            and pressure_conflict_level in {"NONE", "MILD"}
            and effective_score >= 66
        )
        if late_day_breakout_ok:
            return True
        if confidence not in {"MEDIUM", "HIGH"}:
            return False
        if regime not in {"TRENDING", "EXPANDING", "CHOPPY", "LATE_DAY_BREAKDOWN", "LATE_DAY_BREAKOUT", "EXPIRY_DAY"}:
            return False
        if pressure_conflict_level not in {"NONE", "MILD"}:
            return False
        if signal_grade in {"A", "A+"}:
            if signal_type in {"REVERSAL", "TRAP_REVERSAL"}:
                return effective_score >= 88 and confidence == "HIGH"
            return signal_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "CONTINUATION"} and effective_score >= 78
        if signal_grade != "B":
            return False
        if signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "CONTINUATION"}:
            return False
        return effective_score >= 80

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
        minute_of_day = candle_time.hour * 60 + candle_time.minute if candle_time is not None else None
        late_session = minute_of_day is not None and minute_of_day >= (13 * 60)
        effective_score = max(float(score or 0), float(entry_score or 0))
        late_day_breakout_ok = (
            signal_type == "BREAKOUT_CONFIRM"
            and regime in {"LATE_DAY_BREAKDOWN", "LATE_DAY_BREAKOUT"}
            and signal_grade in {"WATCH", "B", "A", "A+"}
            and pressure_conflict_level in {"NONE", "MILD"}
            and effective_score >= 66
        )
        if late_day_breakout_ok:
            return True

        if signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM", "OPENING_DRIVE", "RETEST", "CONTINUATION", "REVERSAL", "TRAP_REVERSAL"}:
            return False
        if confidence not in {"MEDIUM", "HIGH"}:
            return False
        if regime not in {"TRENDING", "EXPANDING", "EXPIRY_DAY", "OPENING_EXPANSION", "LATE_DAY_BREAKDOWN", "LATE_DAY_BREAKOUT", "CHOPPY"}:
            return False

        if signal_grade in {"A", "A+"}:
            if signal_type in {"REVERSAL", "TRAP_REVERSAL"}:
                return (
                    confidence == "HIGH"
                    and pressure_conflict_level in {"NONE", "MILD"}
                    and effective_score >= (90 if late_session else 88)
                    and float(entry_score or 0) >= (90 if late_session else 86)
                )
            return effective_score >= (80 if late_session else 76)

        if (
            signal_grade == "B"
            and signal_type in {"BREAKOUT_CONFIRM", "RETEST", "CONTINUATION"}
            and regime in {"TRENDING", "EXPANDING", "LATE_DAY_BREAKDOWN", "LATE_DAY_BREAKOUT"}
            and pressure_conflict_level in {"NONE", "MILD"}
            and effective_score >= (78 if late_session else 70)
            and float(entry_score or 0) >= (80 if late_session else 72)
        ):
            return True

        return False
