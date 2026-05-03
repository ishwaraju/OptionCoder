from strategies.shared.confirmation_retest_evaluator import confirmation_ready, retest_ready
from strategies.shared.continuation_evaluator import fallback_continuation_ready, high_score_continuation_ready


def test_confirmation_ready_accepts_basic_bullish_confirmation():
    assert confirmation_ready(
        direction="CE",
        active_direction="CE",
        price_vwap_aligned=True,
        close_beyond_level=True,
        not_extended=True,
        level_extension_ok=True,
        volume_signal="NORMAL",
        oi_bias_ok=True,
        oi_trend_ok=True,
        build_up_ok=True,
        no_opposite_pressure=True,
        candle_liquidity_ok=True,
        continuation_regime_ok=True,
        score=65,
        confirm_min_score=62,
    ) is True


def test_retest_ready_blocks_opening_session():
    assert retest_ready(
        direction="PE",
        active_direction="PE",
        price_vwap_aligned=True,
        retest_touch_ok=True,
        close_holds_level=True,
        volume_signal="STRONG",
        oi_bias_ok=True,
        oi_trend_ok=True,
        build_up_ok=True,
        no_opposite_pressure=True,
        candle_liquidity_ok=True,
        score=75,
        retest_min_score=60,
        opening_session=True,
        retest_regime_ok=True,
    ) is False


def test_high_score_continuation_ready_accepts_clean_setup():
    assert high_score_continuation_ready(
        price_vwap_aligned=True,
        volume_ok=True,
        score=80,
        high_continuation_min_score=75,
        oi_bias_ok=True,
        oi_trend_ok=True,
        build_up_ok=True,
        orb_extension_ok=True,
        candle_liquidity_ok=True,
        breakout_body_ok=True,
        breakout_structure_ok=True,
        ha_ok=True,
        far_from_vwap_ok=True,
        opposite_pressure_ok=True,
        continuation_regime_ok=True,
        adx_trade_ok=True,
        mtf_trade_ok=True,
        pressure_conflict_ok=True,
        focused_mode_ok=True,
    ) is True


def test_fallback_continuation_ready_requires_non_opening_session():
    assert fallback_continuation_ready(
        price_vwap_aligned=True,
        volume_ok=True,
        score=70,
        continuation_min_score=64,
        oi_bias_ok=True,
        oi_trend_ok=True,
        build_up_ok=True,
        candle_liquidity_ok=True,
        breakout_body_ok=True,
        breakout_structure_ok=True,
        ha_ok=True,
        opposite_pressure_ok=True,
        far_from_vwap_ok=True,
        opening_session=True,
        continuation_regime_ok=True,
        adx_ok=True,
        mtf_ok=True,
        pressure_conflict_ok=True,
        focused_mode_ok=True,
    ) is False
