from shared.ml.signal_filter import MLSignalFilter


def test_rule_based_fallback_blocks_weak_signal_without_model():
    ml_filter = MLSignalFilter(model_path="models/does_not_exist.pkl", threshold=0.55)

    approved, reason, probability = ml_filter.should_take_signal(
        {
            "score": 54,
            "entry_score": 54,
            "adx": 16,
            "volume_ratio": 0.95,
            "spread_pct": 7.2,
            "risk_reward_ratio": 1.0,
            "time_regime": "LUNCH",
            "confidence": "LOW",
            "signal_grade": "B",
            "signal_type": "BREAKOUT",
            "pressure_conflict_level": "MODERATE",
            "participation_quality": "WEAK",
            "breadth_ratio": 0.9,
            "trend_aligned": False,
        }
    )

    assert approved is False
    assert probability < 0.55
    assert "Rule blocked" in reason


def test_rule_based_fallback_allows_high_quality_signal_without_model():
    ml_filter = MLSignalFilter(model_path="models/does_not_exist.pkl", threshold=0.55)

    approved, reason, probability = ml_filter.should_take_signal(
        {
            "score": 82,
            "entry_score": 85,
            "adx": 31,
            "volume_ratio": 1.9,
            "spread_pct": 1.4,
            "risk_reward_ratio": 2.1,
            "time_regime": "MID_MORNING",
            "confidence": "HIGH",
            "signal_grade": "A",
            "signal_type": "BREAKOUT_CONFIRM",
            "pressure_conflict_level": "NONE",
            "participation_quality": "STRONG",
            "breadth_ratio": 1.45,
            "trend_aligned": True,
        }
    )

    assert approved is True
    assert probability >= 0.55
    assert "Rule approved" in reason


def test_rule_based_fallback_tightens_midday_ranging_thresholds():
    ml_filter = MLSignalFilter(model_path="models/does_not_exist.pkl", threshold=0.55)

    approved, reason, probability = ml_filter.should_take_signal(
        {
            "instrument": "NIFTY",
            "score": 63,
            "entry_score": 64,
            "adx": 19,
            "volume_ratio": 1.12,
            "spread_pct": 2.8,
            "risk_reward_ratio": 1.22,
            "time_regime": "MIDDAY",
            "market_regime": "RANGING",
            "confidence": "MEDIUM",
            "signal_grade": "A",
            "signal_type": "BREAKOUT",
            "pressure_conflict_level": "NONE",
            "participation_quality": "NORMAL",
            "breadth_ratio": 1.1,
            "trend_aligned": True,
        }
    )

    assert approved is False
    assert "thr=" in reason
    assert probability < 0.70


def test_rule_based_fallback_relaxes_opening_trending_thresholds():
    ml_filter = MLSignalFilter(model_path="models/does_not_exist.pkl", threshold=0.55)

    approved, reason, probability = ml_filter.should_take_signal(
        {
            "instrument": "NIFTY",
            "score": 61,
            "entry_score": 62,
            "adx": 18,
            "volume_ratio": 1.08,
            "spread_pct": 1.5,
            "risk_reward_ratio": 1.35,
            "time_regime": "OPENING",
            "market_regime": "TRENDING",
            "confidence": "HIGH",
            "signal_grade": "A",
            "signal_type": "BREAKOUT_CONFIRM",
            "pressure_conflict_level": "NONE",
            "participation_quality": "STRONG",
            "breadth_ratio": 1.35,
            "trend_aligned": True,
        }
    )

    assert approved is True
    assert "opening_relaxed" in reason or "trend_support" in reason
    assert probability >= 0.55


def test_midday_confirmation_gets_narrow_volume_relief():
    ml_filter = MLSignalFilter(model_path="models/does_not_exist.pkl", threshold=0.55)

    profile = ml_filter._build_adaptive_profile(
        {
            "instrument": "NIFTY",
            "time_regime": "MIDDAY",
            "market_regime": "TRENDING",
            "signal_type": "BREAKOUT_CONFIRM",
            "participation_quality": "NORMAL",
            "confidence": "MEDIUM",
            "spread_pct": 2.0,
        }
    )

    assert profile["volume_floor"] == 1.1
    assert "midday_confirmation_volume_relief" in profile["label"]


def test_midday_confirmation_keeps_strict_volume_floor_when_participation_is_weak():
    ml_filter = MLSignalFilter(model_path="models/does_not_exist.pkl", threshold=0.55)

    profile = ml_filter._build_adaptive_profile(
        {
            "instrument": "NIFTY",
            "time_regime": "MIDDAY",
            "market_regime": "TRENDING",
            "signal_type": "BREAKOUT_CONFIRM",
            "participation_quality": "WEAK",
            "confidence": "MEDIUM",
            "spread_pct": 2.0,
        }
    )

    assert profile["volume_floor"] == 1.20
    assert "midday_confirmation_volume_relief" not in profile["label"]
