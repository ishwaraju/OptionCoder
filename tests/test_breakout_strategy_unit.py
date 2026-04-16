from strategies.shared.breakout_strategy import BreakoutStrategy
from datetime import datetime
from config import Config
from strategies.shared.expiry_day_rules import ExpiryDayRules


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
    assert "breakout" in reason.lower() or "continuation" in reason.lower()


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
        candle_time=datetime(2026, 4, 10, 10, 20),
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
    assert (
        "no_valid_setup" in strategy.last_blockers
        or "direction_present_but_filters_incomplete" in strategy.last_blockers
    )
    assert "volume_weak" in strategy.last_blockers or "low_tick_density" in strategy.last_blockers
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

    assert signal_1 is None
    assert strategy.confirmation_setup is not None or strategy.retest_setup is not None

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
    assert "retest" in reason_2.lower() or "confirmation" in reason_2.lower()


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


def test_high_conviction_opening_breakdown_allowed_even_if_far_from_vwap():
    strategy = BreakoutStrategy()

    signal, reason = strategy.generate_signal(
        price=24310,
        orb_high=24395,
        orb_low=24355,
        vwap=24358,
        atr=20,
        volume_signal="STRONG",
        oi_bias="BEARISH",
        oi_trend="BEARISH",
        build_up="SHORT_BUILDUP",
        support=24290,
        resistance=24380,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BEARISH",
            "atm_ce_concentration": 0.26,
            "atm_pe_concentration": 0.10,
        },
        candle_high=24318,
        candle_low=24302,
        candle_close=24305,
        candle_open=24317,
        candle_tick_count=6,
        candle_time=datetime(2026, 4, 16, 9, 35),
    )

    assert signal == "PE"
    assert "breakdown" in reason.lower()


def test_opening_reversal_is_blocked_for_option_buyer():
    strategy = BreakoutStrategy()

    signal, _ = strategy.generate_signal(
        price=56532.5,
        orb_high=56734.35,
        orb_low=56681.55,
        vwap=56610,
        atr=60,
        volume_signal="NORMAL",
        oi_bias="BEARISH",
        oi_trend="BEARISH",
        build_up="LONG_UNWINDING",
        support=56480,
        resistance=56540,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BEARISH",
            "atm_ce_concentration": 0.24,
            "atm_pe_concentration": 0.12,
        },
        candle_high=56540,
        candle_low=56528,
        candle_close=56532,
        candle_open=56538,
        candle_tick_count=5,
        candle_time=datetime(2026, 4, 16, 9, 35),
    )

    assert signal is None
    assert strategy.last_signal_type == "NONE"


def test_sensex_expiry_allows_high_conviction_midday_trend_trade():
    original_symbol = Config.SYMBOL
    Config.SYMBOL = "SENSEX"
    try:
        rules = ExpiryDayRules(type("TU", (), {})())
        rules.time_utils.now_ist = lambda: datetime(2026, 4, 16, 12, 0)
        rules.time_utils.current_time = lambda: datetime(2026, 4, 16, 12, 0).time()

        result = rules.evaluate(
            expiry_value="2026-04-16",
            score=80,
            confidence="MEDIUM",
            price=78420,
            vwap=78310,
            volume_signal="STRONG",
            pressure_metrics={"pressure_bias": "NEUTRAL"},
            current_signal="PE",
            blockers=[],
            cautions=[],
        )

        assert result["is_expiry_day"] is True
        assert result["allow_trade"] is True
        assert "expiry_midday_chop_window" not in result["blockers"]
        assert "expiry_too_far_from_vwap" not in result["blockers"]
    finally:
        Config.SYMBOL = original_symbol


def test_low_adx_context_marks_caution_when_recent_candles_available():
    strategy = BreakoutStrategy()
    recent_candles = []
    for i in range(18):
        base = 23950 + (i % 2)
        recent_candles.append(
            {
                "time": datetime(2026, 4, 16, 10, 0),
                "open": base,
                "high": base + 2,
                "low": base - 2,
                "close": base + 1,
                "volume": 1000000,
            }
        )

    signal, _ = strategy.generate_signal(
        price=23955,
        orb_high=23990,
        orb_low=23920,
        vwap=23955,
        atr=25,
        volume_signal="STRONG",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=23940,
        resistance=23980,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BULLISH",
            "atm_ce_concentration": 0.10,
            "atm_pe_concentration": 0.22,
        },
        candle_high=23960,
        candle_low=23948,
        candle_close=23955,
        candle_open=23949,
        candle_tick_count=6,
        candle_time=datetime(2026, 4, 16, 10, 30),
        candle_volume=1200000,
        recent_candles_5m=recent_candles,
    )

    assert "adx_not_confirmed" in strategy.last_cautions


def test_higher_timeframe_misalignment_marks_caution():
    strategy = BreakoutStrategy()
    recent_candles = []
    for i in range(18):
        base = 23900 + (i * 6)
        recent_candles.append(
            {
                "time": datetime(2026, 4, 16, 10, 0),
                "open": base,
                "high": base + 8,
                "low": base - 3,
                "close": base + 6,
                "volume": 1000000,
            }
        )

    signal, _ = strategy.generate_signal(
        price=23955,
        orb_high=23995,
        orb_low=23920,
        vwap=23970,
        atr=25,
        volume_signal="NORMAL",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=23945,
        resistance=23980,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "BULLISH",
            "atm_ce_concentration": 0.10,
            "atm_pe_concentration": 0.22,
        },
        candle_high=23960,
        candle_low=23950,
        candle_close=23955,
        candle_open=23952,
        candle_tick_count=6,
        candle_time=datetime(2026, 4, 16, 10, 35),
        candle_volume=1200000,
        recent_candles_5m=recent_candles,
        trend_15m="BEARISH",
    )

    assert "higher_tf_not_aligned" in strategy.last_cautions
