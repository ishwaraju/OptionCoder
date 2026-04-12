from datetime import datetime

import pytz

from config import Config
from strategies.shared.breakout_strategy import BreakoutStrategy


def _ist_dt(hour, minute):
    return pytz.timezone("Asia/Kolkata").localize(datetime(2026, 4, 8, hour, minute))


def test_invalid_flat_zero_volume_candle_is_rejected():
    strategy = BreakoutStrategy()
    strategy.time_utils.current_time = lambda: _ist_dt(10, 0).time()

    signal, reason = strategy.generate_signal(
        price=23950,
        orb_high=23940,
        orb_low=23910,
        vwap=23920,
        volume_signal="STRONG",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        can_trade=True,
        buffer=10,
        pressure_metrics={"pressure_bias": "BULLISH", "atm_pe_concentration": 0.3, "atm_ce_concentration": 0.1},
        atr=20,
        candle_high=23950,
        candle_low=23950,
        candle_close=23950,
        candle_open=23950,
        candle_tick_count=5,
        candle_time=_ist_dt(10, 0),
        candle_volume=0,
    )

    assert signal is None
    assert "Invalid candle data" in reason
    assert "invalid_candle_data" in strategy.last_blockers


def test_opening_drive_can_fire_during_opening_session():
    original_test_mode = Config.TEST_MODE
    Config.TEST_MODE = False
    try:
        strategy = BreakoutStrategy()
        strategy.time_utils.current_time = lambda: _ist_dt(9, 35).time()

        signal, reason = strategy.generate_signal(
            price=23990,
            orb_high=23960,
            orb_low=23920,
            vwap=23955,
            volume_signal="STRONG",
            oi_bias="BULLISH",
            oi_trend="BULLISH",
            build_up="LONG_BUILDUP",
            can_trade=True,
            buffer=10,
            pressure_metrics={"pressure_bias": "BULLISH", "atm_pe_concentration": 0.3, "atm_ce_concentration": 0.1},
            atr=25,
            candle_high=23995,
            candle_low=23962,
            candle_close=23990,
            candle_open=23964,
            candle_tick_count=6,
            candle_time=_ist_dt(9, 35),
            candle_volume=1200000,
        )

        assert signal == "CE"
        assert "Opening drive breakout up" in reason
        assert strategy.last_signal_type == "OPENING_DRIVE"
    finally:
        Config.TEST_MODE = original_test_mode


def test_aggressive_mode_allows_clean_mid_score_continuation():
    original_test_mode = Config.TEST_MODE
    original_aggressive_mode = Config.AGGRESSIVE_MODE
    Config.TEST_MODE = False
    Config.AGGRESSIVE_MODE = True
    try:
        strategy = BreakoutStrategy()
        strategy.time_utils.current_time = lambda: _ist_dt(10, 15).time()

        signal, reason = strategy.generate_signal(
            price=23994,
            orb_high=23990,
            orb_low=23920,
            vwap=23980,
            volume_signal="NORMAL",
            oi_bias="NEUTRAL",
            oi_trend="BULLISH",
            build_up="LONG_BUILDUP",
            can_trade=True,
            buffer=10,
            pressure_metrics={"pressure_bias": "BULLISH", "atm_pe_concentration": 0.1, "atm_ce_concentration": 0.1},
            atr=10,
            candle_high=24002,
            candle_low=23987,
            candle_close=23994,
            candle_open=23986,
            candle_tick_count=5,
            candle_time=_ist_dt(10, 15),
            candle_volume=900000,
        )

        assert signal == "CE"
        assert "Aggressive bullish continuation" in reason
        assert strategy.last_signal_type == "AGGRESSIVE_CONTINUATION"
    finally:
        Config.TEST_MODE = original_test_mode
        Config.AGGRESSIVE_MODE = original_aggressive_mode


def test_heikin_ashi_alignment_allows_bullish_breakout():
    strategy = BreakoutStrategy()
    strategy.time_utils.current_time = lambda: _ist_dt(10, 5).time()

    signal, reason = strategy.generate_signal(
        price=23990,
        orb_high=23960,
        orb_low=23920,
        vwap=23955,
        volume_signal="STRONG",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        can_trade=True,
        buffer=10,
        pressure_metrics={"pressure_bias": "BULLISH", "atm_pe_concentration": 0.3, "atm_ce_concentration": 0.1},
        atr=25,
        candle_high=23995,
        candle_low=23962,
        candle_close=23990,
        candle_open=23964,
        candle_tick_count=6,
        candle_time=_ist_dt(10, 5),
        candle_volume=1200000,
    )

    assert signal == "CE"
    assert strategy.last_heikin_ashi["bias"] == "BULLISH"
    assert "Breakout" in reason or "breakout" in reason


def test_heikin_ashi_opposite_blocks_bullish_breakout():
    strategy = BreakoutStrategy()
    strategy.time_utils.current_time = lambda: _ist_dt(10, 10).time()

    strategy.generate_signal(
        price=23970,
        orb_high=23960,
        orb_low=23920,
        vwap=23945,
        volume_signal="NORMAL",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        can_trade=True,
        buffer=10,
        pressure_metrics={"pressure_bias": "BULLISH", "atm_pe_concentration": 0.2, "atm_ce_concentration": 0.1},
        atr=20,
        candle_high=23976,
        candle_low=23950,
        candle_close=23970,
        candle_open=23954,
        candle_tick_count=5,
        candle_time=_ist_dt(10, 5),
        candle_volume=800000,
    )

    signal, reason = strategy.generate_signal(
        price=23990,
        orb_high=23960,
        orb_low=23920,
        vwap=23955,
        volume_signal="STRONG",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        can_trade=True,
        buffer=10,
        pressure_metrics={"pressure_bias": "BULLISH", "atm_pe_concentration": 0.3, "atm_ce_concentration": 0.1},
        atr=25,
        candle_high=24010,
        candle_low=23890,
        candle_close=23900,
        candle_open=24005,
        candle_tick_count=6,
        candle_time=_ist_dt(10, 10),
        candle_volume=1200000,
    )

    assert signal is None
    assert strategy.last_heikin_ashi["bias"] == "BEARISH"
