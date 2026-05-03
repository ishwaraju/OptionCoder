from strategies.shared.decision_rules import calculate_entry_score, derive_decision_state, grade_signal


def test_grade_signal_returns_a_plus_for_clean_high_confidence_score():
    assert grade_signal(85, "HIGH", [], [], "BREAKOUT") == "A+"


def test_calculate_entry_score_applies_penalties_and_bonuses():
    score = calculate_entry_score(
        score=78,
        breakout_body_ok=True,
        breakout_structure_ok=False,
        candle_liquidity_ok=True,
        volume_signal="STRONG",
        cautions=["far_from_vwap"],
        blockers=["volume_weak"],
        adx_trade_ok=True,
        mtf_trade_ok=False,
        pressure_conflict_level="MILD",
    )

    assert score == 64


def test_derive_decision_state_returns_watch_for_incomplete_high_score_context():
    decision = derive_decision_state(
        signal_type="NONE",
        signal=None,
        score=68,
        entry_score=55,
        confidence="MEDIUM",
        blockers=["weak_breakout_body"],
        cautions=[],
    )

    assert decision == "WATCH"
