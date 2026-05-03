from datetime import datetime

from strategies.shared.actionable_rules import InstrumentActionableRules
from strategies.shared.time_regime_thresholds import build_time_regime_thresholds


def test_unified_actionable_rules_match_nifty_b_grade_policy():
    allowed = InstrumentActionableRules.should_allow_signal(
        instrument="NIFTY",
        signal_type="BREAKOUT_CONFIRM",
        signal_grade="B",
        confidence="HIGH",
        regime="TRENDING",
        score=84,
        pressure_conflict_level="NONE",
    )

    assert allowed is True


def test_unified_actionable_rules_match_banknifty_time_gate():
    blocked = InstrumentActionableRules.should_allow_signal(
        instrument="BANKNIFTY",
        signal_type="BREAKOUT",
        signal_grade="B",
        confidence="MEDIUM",
        regime="TRENDING",
        candle_time=datetime(2026, 4, 16, 10, 25),
        score=85,
        pressure_conflict_level="NONE",
    )

    assert blocked is False


def test_unified_actionable_rules_match_sensex_ultra_late_guard():
    blocked = InstrumentActionableRules.should_allow_signal(
        instrument="SENSEX",
        signal_type="CONTINUATION",
        signal_grade="A+",
        confidence="HIGH",
        regime="TRENDING",
        candle_time=datetime(2026, 4, 16, 14, 28),
        score=90,
        entry_score=95,
        pressure_conflict_level="NONE",
    )

    assert blocked is False


def test_threshold_builder_preserves_sensex_midday_focus_behavior():
    thresholds = build_time_regime_thresholds(
        instrument="SENSEX",
        time_regime="MIDDAY",
        fallback_mode=False,
        market_regime="TRENDING",
    )

    assert thresholds["breakout_min_score"] == 58
    assert thresholds["confirm_min_score"] == 63
    assert thresholds["allow_fallback_continuation"] is False
