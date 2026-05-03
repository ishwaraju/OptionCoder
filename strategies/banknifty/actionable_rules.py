from strategies.shared.actionable_rules import InstrumentActionableRules


class BankNiftyActionableRules:
    """Instrument-specific live alert rules for BANKNIFTY option buyers."""

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
        return InstrumentActionableRules.should_allow_signal(
            instrument="BANKNIFTY",
            signal_type=signal_type,
            signal_grade=signal_grade,
            confidence=confidence,
            regime=regime,
            candle_time=candle_time,
            score=score,
            pressure_conflict_level=pressure_conflict_level,
        )
