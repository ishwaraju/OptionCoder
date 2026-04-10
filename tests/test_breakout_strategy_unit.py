from strategy.breakout_strategy import BreakoutStrategy
from datetime import datetime


def test_allows_high_conviction_bullish_continuation_even_if_far_from_vwap():
    strategy = BreakoutStrategy()

    signal, reason = strategy.generate_signal(
        price=23900,
        orb_high=23820,
        orb_low=23740,
        vwap=23800,
        atr=40,
        volume_signal="STRONG",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=23750,
        resistance=23980,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BULLISH",
            "atm_ce_concentration": 0.10,
            "atm_pe_concentration": 0.30,
        },
        candle_high=23905,
        candle_low=23882,
        candle_close=23895,
        candle_open=23884,
    )

    assert signal == "CE"
    assert "continuation" in reason.lower()


def test_blocks_high_score_continuation_when_pressure_is_opposite():
    strategy = BreakoutStrategy()

    signal, _ = strategy.generate_signal(
        price=23900,
        orb_high=23820,
        orb_low=23740,
        vwap=23800,
        atr=40,
        volume_signal="STRONG",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=23750,
        resistance=23980,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BEARISH",
            "atm_ce_concentration": 0.30,
            "atm_pe_concentration": 0.10,
        },
        candle_high=23905,
        candle_low=23882,
        candle_close=23895,
        candle_open=23884,
    )

    assert signal is None


def test_allows_medium_score_continuation_follow_through_for_bullish_setup():
    strategy = BreakoutStrategy()

    signal, reason = strategy.generate_signal(
        price=23875,
        orb_high=23895,
        orb_low=23820,
        vwap=23840,
        atr=20,
        volume_signal="NORMAL",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=23820,
        resistance=23920,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BULLISH",
            "atm_ce_concentration": 0.10,
            "atm_pe_concentration": 0.24,
        },
        candle_high=23878,
        candle_low=23860,
        candle_close=23875,
        candle_open=23866,
        candle_tick_count=5,
    )

    assert signal == "CE"
    assert "Continuation follow-through" in reason


def test_no_valid_setup_is_split_into_specific_blockers():
    strategy = BreakoutStrategy()

    signal, _ = strategy.generate_signal(
        price=23845,
        orb_high=23830,
        orb_low=23810,
        vwap=23850,
        atr=15,
        volume_signal="WEAK",
        oi_bias="BEARISH",
        oi_trend="BEARISH",
        build_up="NO_CLEAR_SIGNAL",
        support=23820,
        resistance=23880,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BEARISH",
            "atm_ce_concentration": 0.25,
            "atm_pe_concentration": 0.10,
        },
        candle_high=23846,
        candle_low=23844,
        candle_close=23845,
        candle_open=23845,
        candle_tick_count=1,
    )

    assert signal is None
    assert "no_valid_setup" in strategy.last_blockers
    assert "volume_weak" in strategy.last_blockers
    assert "low_tick_density" in strategy.last_blockers


def test_retest_entry_triggers_after_breakout_watch_is_created():
    strategy = BreakoutStrategy()

    signal_1, _ = strategy.generate_signal(
        price=23910,
        orb_high=23900,
        orb_low=23850,
        vwap=23870,
        atr=15,
        volume_signal="NORMAL",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=23860,
        resistance=23940,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BULLISH",
            "atm_ce_concentration": 0.10,
            "atm_pe_concentration": 0.22,
        },
        candle_high=23914,
        candle_low=23905,
        candle_close=23910,
        candle_open=23906,
        candle_tick_count=5,
        candle_time=datetime(2026, 4, 10, 10, 0),
    )

    assert signal_1 == "CE"

    strategy._set_retest_setup("CE", 23900, datetime(2026, 4, 10, 10, 5), 68)

    signal_2, reason_2 = strategy.generate_signal(
        price=23906,
        orb_high=23920,
        orb_low=23850,
        vwap=23880,
        atr=14,
        volume_signal="NORMAL",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=23870,
        resistance=23935,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BULLISH",
            "atm_ce_concentration": 0.10,
            "atm_pe_concentration": 0.21,
        },
        candle_high=23908,
        candle_low=23901,
        candle_close=23905,
        candle_open=23902,
        candle_tick_count=4,
        candle_time=datetime(2026, 4, 10, 10, 10),
    )

    assert signal_2 == "CE"
    assert "retest" in reason_2.lower()


def test_choppy_regime_blocks_continuation_without_strong_volume():
    strategy = BreakoutStrategy()

    signal, _ = strategy.generate_signal(
        price=23855,
        orb_high=23840,
        orb_low=23810,
        vwap=23845,
        atr=25,
        volume_signal="NORMAL",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=23820,
        resistance=23880,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BULLISH",
            "atm_ce_concentration": 0.11,
            "atm_pe_concentration": 0.20,
        },
        candle_high=23858,
        candle_low=23852,
        candle_close=23855,
        candle_open=23853,
        candle_tick_count=5,
        candle_time=datetime(2026, 4, 10, 11, 0),
    )

    assert signal is None
    assert strategy.last_signal_type == "NONE"


def test_breakout_confirmation_triggers_after_weak_breakout_watch():
    strategy = BreakoutStrategy()

    signal_1, _ = strategy.generate_signal(
        price=23908,
        orb_high=23900,
        orb_low=23850,
        vwap=23880,
        atr=15,
        volume_signal="NORMAL",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=23860,
        resistance=23940,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BULLISH",
            "atm_ce_concentration": 0.10,
            "atm_pe_concentration": 0.22,
        },
        candle_high=23910,
        candle_low=23905,
        candle_close=23908,
        candle_open=23907,
        candle_tick_count=5,
        candle_time=datetime(2026, 4, 10, 10, 0),
    )

    assert signal_1 is None

    signal_2, reason_2 = strategy.generate_signal(
        price=23918,
        orb_high=23900,
        orb_low=23850,
        vwap=23888,
        atr=15,
        volume_signal="NORMAL",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=23870,
        resistance=23945,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BULLISH",
            "atm_ce_concentration": 0.10,
            "atm_pe_concentration": 0.22,
        },
        candle_high=23920,
        candle_low=23906,
        candle_close=23918,
        candle_open=23909,
        candle_tick_count=5,
        candle_time=datetime(2026, 4, 10, 10, 5),
    )

    assert signal_2 == "CE"
    assert "confirmation" in reason_2.lower()
