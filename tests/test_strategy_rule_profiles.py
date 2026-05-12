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


def test_unified_actionable_rules_allow_nifty_a_grade_breakout():
    allowed = InstrumentActionableRules.should_allow_signal(
        instrument="NIFTY",
        signal_type="BREAKOUT",
        signal_grade="A",
        confidence="HIGH",
        regime="TRENDING",
        score=80,
        pressure_conflict_level="NONE",
    )

    assert allowed is True


def test_unified_actionable_rules_allow_nifty_late_day_breakdown_watch():
    allowed = InstrumentActionableRules.should_allow_signal(
        instrument="NIFTY",
        signal_type="BREAKOUT_CONFIRM",
        signal_grade="WATCH",
        confidence="MEDIUM",
        regime="LATE_DAY_BREAKDOWN",
        score=70,
        entry_score=40,
        pressure_conflict_level="MILD",
    )

    assert allowed is True


def test_unified_actionable_rules_block_nifty_late_day_breakdown_below_score_floor():
    blocked = InstrumentActionableRules.should_allow_signal(
        instrument="NIFTY",
        signal_type="BREAKOUT_CONFIRM",
        signal_grade="WATCH",
        confidence="MEDIUM",
        regime="LATE_DAY_BREAKDOWN",
        score=69,
        pressure_conflict_level="MILD",
    )

    assert blocked is False


def test_unified_actionable_rules_allow_nifty_clean_breakout_in_choppy_regime():
    allowed = InstrumentActionableRules.should_allow_signal(
        instrument="NIFTY",
        signal_type="BREAKOUT",
        signal_grade="A",
        confidence="HIGH",
        regime="CHOPPY",
        score=83,
        pressure_conflict_level="NONE",
    )

    assert allowed is True


def test_unified_actionable_rules_keep_nifty_reversal_blocked_in_shared_rules():
    blocked = InstrumentActionableRules.should_allow_signal(
        instrument="NIFTY",
        signal_type="REVERSAL",
        signal_grade="A",
        confidence="HIGH",
        regime="CHOPPY",
        score=90,
        entry_score=86,
        pressure_conflict_level="NONE",
    )

    assert blocked is False


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


def test_unified_actionable_rules_allow_sensex_clean_reversal():
    allowed = InstrumentActionableRules.should_allow_signal(
        instrument="SENSEX",
        signal_type="REVERSAL",
        signal_grade="A",
        confidence="HIGH",
        regime="EXPANDING",
        candle_time=datetime(2026, 4, 16, 11, 20),
        score=92,
        entry_score=94,
        pressure_conflict_level="NONE",
    )

    assert allowed is True


def test_threshold_builder_preserves_sensex_midday_focus_behavior():
    thresholds = build_time_regime_thresholds(
        instrument="SENSEX",
        time_regime="MIDDAY",
        fallback_mode=False,
        market_regime="CHOPPY",
    )

    assert thresholds["breakout_min_score"] == 63
    assert thresholds["confirm_min_score"] == 66
    assert thresholds["allow_fallback_continuation"] is False


def test_threshold_builder_relaxes_nifty_midday_thresholds_when_trending():
    trending = build_time_regime_thresholds(
        instrument="NIFTY",
        time_regime="MIDDAY",
        fallback_mode=False,
        market_regime="TRENDING",
    )
    choppy = build_time_regime_thresholds(
        instrument="NIFTY",
        time_regime="MIDDAY",
        fallback_mode=False,
        market_regime="CHOPPY",
    )

    assert trending["breakout_min_score"] < choppy["breakout_min_score"]
    assert trending["continuation_min_score"] < choppy["continuation_min_score"]
