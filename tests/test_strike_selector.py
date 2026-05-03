from strategies.shared.strike_selector import StrikeSelector


def test_expiry_noise_prefers_deeper_itm():
    selector = StrikeSelector("NIFTY")
    strike, reason = selector.select_strike_with_reason(
        price=22485,
        signal="CE",
        volume_signal="NORMAL",
        strategy_score=74,
        pressure_metrics={"pressure_bias": "BULLISH", "near_put_pressure_ratio": 1.1},
        cautions=["expiry_day_mode", "expiry_fast_decay", "participation_spread_wide"],
        setup_type="BREAKOUT",
        time_regime="MIDDAY",
    )

    assert strike == 22400
    assert "expiry premium is noisy" in reason


def test_reversal_prefers_itm_for_cleaner_premium():
    selector = StrikeSelector("BANKNIFTY")
    strike, reason = selector.select_strike_with_reason(
        price=51235,
        signal="PE",
        volume_signal="STRONG",
        strategy_score=82,
        pressure_metrics={"pressure_bias": "BEARISH", "near_call_pressure_ratio": 1.3},
        setup_type="REVERSAL",
        time_regime="MID_MORNING",
    )

    assert strike == 51300
    assert "reversal setups" in reason
