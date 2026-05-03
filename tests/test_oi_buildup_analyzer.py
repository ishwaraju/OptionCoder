from shared.indicators.oi_buildup_analyzer import OIBuildupAnalyzer, quick_oi_check


def test_quick_oi_check_uses_chronological_order_for_bullish_confirmation():
    confirmed, score, reason = quick_oi_check(
        current_oi=110000,
        previous_oi=100000,
        current_price=101.5,
        previous_price=100.0,
        signal="CE",
    )

    assert confirmed is True
    assert score >= 50
    assert "Fresh long buildup" in reason


def test_flat_oi_participation_is_rejected():
    analyzer = OIBuildupAnalyzer(for_option_buyer=True)
    analyzer.update(100000, 100.0)
    analyzer.update(100200, 100.02)

    confirmed, score, reason = analyzer.confirm_signal("CE")

    assert confirmed is False
    assert score < 50
    assert "flat" in reason.lower()
