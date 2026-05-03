from strategies.shared.actionable_rules import InstrumentActionableRules


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
        return InstrumentActionableRules.should_allow_signal(
            instrument="NIFTY",
            signal_type=signal_type,
            signal_grade=signal_grade,
            confidence=confidence,
            regime=regime,
            score=score,
            pressure_conflict_level=pressure_conflict_level,
        )
