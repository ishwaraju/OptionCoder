from strategies.shared.actionable_rules import InstrumentActionableRules


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
        return InstrumentActionableRules.should_allow_signal(
            instrument="SENSEX",
            signal_type=signal_type,
            signal_grade=signal_grade,
            confidence=confidence,
            regime=regime,
            candle_time=candle_time,
            score=score,
            entry_score=entry_score,
            pressure_conflict_level=pressure_conflict_level,
        )
