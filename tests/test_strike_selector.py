from strategies.shared.strike_selector import StrikeSelector
from datetime import datetime


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


def test_option_chain_quality_can_choose_better_delta_strike():
    selector = StrikeSelector("NIFTY")
    strike, reason = selector.select_strike_with_reason(
        price=22485,
        signal="CE",
        volume_signal="STRONG",
        strategy_score=86,
        pressure_metrics={"pressure_bias": "BULLISH", "near_put_pressure_ratio": 1.3, "strongest_pe_strike": 22500},
        option_chain_data={
            "atm": 22500,
            "band_snapshots": [
                {
                    "strike": 22500,
                    "distance_from_atm": 0,
                    "option_type": "CE",
                    "ltp": 98.0,
                    "spread": 3.8,
                    "volume": 180000,
                    "oi": 140000,
                    "delta": 0.39,
                    "theta": -16.0,
                    "iv": 22.0,
                    "top_bid_quantity": 20,
                    "top_ask_quantity": 18,
                },
                {
                    "strike": 22450,
                    "distance_from_atm": -1,
                    "option_type": "CE",
                    "ltp": 126.0,
                    "spread": 1.2,
                    "volume": 210000,
                    "oi": 175000,
                    "delta": 0.52,
                    "theta": -8.5,
                    "iv": 21.0,
                    "top_bid_quantity": 80,
                    "top_ask_quantity": 78,
                },
            ],
        },
        setup_type="BREAKOUT_CONFIRM",
        time_regime="MID_MORNING",
    )

    assert strike == 22450
    assert "buyer-quality pick" in reason


def test_candle_time_changes_late_session_strike_preference():
    selector = StrikeSelector("NIFTY")

    early_strike, _ = selector.select_strike_with_reason(
        price=22485,
        signal="CE",
        volume_signal="STRONG",
        strategy_score=78,
        pressure_metrics={"pressure_bias": "BULLISH", "near_put_pressure_ratio": 1.2},
        setup_type="BREAKOUT_CONFIRM",
        time_regime="MID_MORNING",
        candle_time=datetime(2026, 4, 16, 10, 20),
    )
    late_strike, _ = selector.select_strike_with_reason(
        price=22485,
        signal="CE",
        volume_signal="STRONG",
        strategy_score=78,
        pressure_metrics={"pressure_bias": "BULLISH", "near_put_pressure_ratio": 1.2},
        setup_type="BREAKOUT_CONFIRM",
        time_regime="MID_MORNING",
        candle_time=datetime(2026, 4, 16, 14, 20),
    )

    assert early_strike == 22500
    assert late_strike == 22400
