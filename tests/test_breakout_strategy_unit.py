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


def test_keeps_medium_score_continuation_on_watch_for_bullish_setup():
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

    assert signal is None
    assert strategy.last_decision_state == "WATCH"
    assert "direction_present_but_filters_incomplete" in strategy.last_blockers


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


def test_retest_entry_can_trigger_on_first_clean_breakout():
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
    assert strategy.last_signal_type == "CONTINUATION"

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
    assert strategy.last_signal_type == "BREAKOUT_CONFIRM"
    assert strategy.last_decision_state == "WATCH"


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
    assert "retest" in reason_2.lower() or "confirmation" in reason_2.lower()


def test_nifty_strong_option_sweep_can_override_choppy_breakout_regime():
    base_kwargs = dict(
        price=24212,
        orb_high=24200,
        orb_low=24140,
        vwap=24192,
        atr=28,
        volume_signal="NORMAL",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=24170,
        resistance=24310,
        can_trade=True,
        buffer=10,
        pressure_metrics={
            "pressure_bias": "NEUTRAL",
            "atm_ce_concentration": 0.12,
            "atm_pe_concentration": 0.12,
        },
        candle_high=24218,
        candle_low=24200,
        candle_close=24212,
        candle_open=24204,
        candle_tick_count=5,
        candle_time=datetime(2026, 5, 6, 14, 20),
        recent_candles_5m=[
            {"time": datetime(2026, 5, 6, 14, 10), "open": 24170, "high": 24198, "low": 24152, "close": 24190, "volume": 1000},
            {"time": datetime(2026, 5, 6, 14, 15), "open": 24190, "high": 24202, "low": 24184, "close": 24198, "volume": 1200},
            {"time": datetime(2026, 5, 6, 14, 20), "open": 24204, "high": 24218, "low": 24200, "close": 24212, "volume": 1500},
        ],
    )

    without_sweep = BreakoutStrategy(instrument="NIFTY")
    without_sweep._derive_regime = lambda *args, **kwargs: "CHOPPY"
    without_sweep._evaluate_reversal_setups = lambda ctx: None
    no_signal, _ = without_sweep.generate_signal(**base_kwargs)

    strategy = BreakoutStrategy(instrument="NIFTY")
    strategy._derive_regime = lambda *args, **kwargs: "CHOPPY"
    strategy._evaluate_reversal_setups = lambda ctx: None
    signal, _ = strategy.generate_signal(
        **base_kwargs,
        option_sweep_context={
            "direction": "CE",
            "quality": "STRONG",
            "micro_confirmed": True,
            "persistence_pairs": 5,
            "score_boost": 10,
        },
    )

    assert no_signal is None
    assert signal == "CE"
    assert "option_sweep_regime_override" in strategy.last_score_components


def test_banknifty_strong_option_sweep_can_break_with_weak_underlying_volume():
    base_kwargs = dict(
        price=55572,
        orb_high=55344,
        orb_low=54980,
        vwap=55310,
        atr=110,
        volume_signal="WEAK",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="SHORT_COVERING",
        support=55210,
        resistance=55720,
        can_trade=True,
        buffer=20,
        pressure_metrics={
            "pressure_bias": "NEUTRAL",
            "atm_ce_concentration": 0.11,
            "atm_pe_concentration": 0.11,
        },
        candle_high=55590,
        candle_low=55480,
        candle_close=55572,
        candle_open=55495,
        candle_tick_count=7,
        candle_time=datetime(2026, 5, 6, 14, 20),
        recent_candles_5m=[
            {"time": datetime(2026, 5, 6, 14, 10), "open": 55040, "high": 55140, "low": 54990, "close": 55110, "volume": 900},
            {"time": datetime(2026, 5, 6, 14, 15), "open": 55110, "high": 55165.9, "low": 55090, "close": 55130, "volume": 950},
            {"time": datetime(2026, 5, 6, 14, 20), "open": 55495, "high": 55590, "low": 55480, "close": 55572, "volume": 980},
        ],
    )

    without_sweep = BreakoutStrategy(instrument="BANKNIFTY")
    without_sweep._derive_regime = lambda *args, **kwargs: "CHOPPY"
    without_sweep._evaluate_reversal_setups = lambda ctx: None
    without_sweep._score_signal = lambda **kwargs: (82, "CE", ["synthetic_score"])
    no_signal, _ = without_sweep.generate_signal(**base_kwargs)

    strategy = BreakoutStrategy(instrument="BANKNIFTY")
    strategy._derive_regime = lambda *args, **kwargs: "CHOPPY"
    strategy._evaluate_reversal_setups = lambda ctx: None
    strategy._score_signal = lambda **kwargs: (82, "CE", ["synthetic_score"])
    signal, _ = strategy.generate_signal(
        **base_kwargs,
        option_sweep_context={
            "direction": "CE",
            "quality": "STRONG",
            "micro_confirmed": True,
            "persistence_pairs": 5,
            "score_boost": 10,
        },
    )

    assert no_signal is None
    assert signal == "CE"


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


def test_late_day_price_expansion_ready_uses_pe_breakdown_without_index_volume():
    strategy = BreakoutStrategy()

    result = strategy._late_day_price_expansion_ready(
        current_now=datetime(2026, 5, 11, 14, 35).time(),
        scored_direction="PE",
        price=23870,
        vwap=23920,
        prev_low=23890,
        prev_high=23935,
        candle_open=23905,
        candle_high=23908,
        candle_low=23860,
        candle_close=23870,
        candle_range=48,
        candle_body=35,
        atr=32,
        buffer=8,
        volume_signal="NORMAL",
        candle_liquidity_ok=True,
        pressure_conflict_level="NONE",
    )

    assert result["level"] == 23890
    assert result["wall_aligned"] is False


def test_late_day_price_expansion_ready_allows_ce_breakout_too():
    strategy = BreakoutStrategy()

    result = strategy._late_day_price_expansion_ready(
        current_now=datetime(2026, 5, 11, 14, 35).time(),
        scored_direction="CE",
        price=24025,
        vwap=23970,
        prev_low=23955,
        prev_high=24000,
        candle_open=23985,
        candle_high=24035,
        candle_low=23982,
        candle_close=24025,
        candle_range=53,
        candle_body=40,
        atr=35,
        buffer=8,
        volume_signal="NORMAL",
        candle_liquidity_ok=True,
        pressure_conflict_level="NONE",
        wall_break_alert="RESISTANCE_BREAK_RISK",
    )

    assert result["level"] == 24000
    assert result["wall_aligned"] is True


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
    assert strategy.last_signal_type == "BREAKOUT_CONFIRM"
    assert strategy.last_decision_state == "WATCH"


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


def test_nifty_pre_expiry_allows_medium_confidence_setup_with_weak_volume_watch():
    rules = ExpiryDayRules(type("TU", (), {})(), instrument="NIFTY")
    rules.time_utils.now_ist = lambda: datetime(2026, 5, 4, 10, 0)
    rules.time_utils.current_time = lambda: datetime(2026, 5, 4, 10, 0).time()

    result = rules.evaluate(
        expiry_value=None,
        score=72,
        confidence="MEDIUM",
        price=24220,
        vwap=24200,
        volume_signal="WEAK",
        pressure_metrics={"pressure_bias": "BULLISH"},
        current_signal="CE",
        blockers=[],
        cautions=[],
    )

    assert result["is_expiry_day"] is False
    assert result["session_mode"] == "PRE_EXPIRY_POSITIONING"
    assert result["allow_trade"] is True
    assert "pre_expiry_weak_volume" not in result["blockers"]
    assert "pre_expiry_weak_volume_watch" in result["cautions"]


def test_nifty_pre_expiry_still_blocks_low_confidence_weak_volume_setup():
    rules = ExpiryDayRules(type("TU", (), {})(), instrument="NIFTY")
    rules.time_utils.now_ist = lambda: datetime(2026, 5, 4, 10, 0)
    rules.time_utils.current_time = lambda: datetime(2026, 5, 4, 10, 0).time()

    result = rules.evaluate(
        expiry_value=None,
        score=72,
        confidence="LOW",
        price=24220,
        vwap=24200,
        volume_signal="WEAK",
        pressure_metrics={"pressure_bias": "BULLISH"},
        current_signal="CE",
        blockers=[],
        cautions=[],
    )

    assert result["allow_trade"] is False
    assert "pre_expiry_requires_medium_plus_confidence" in result["blockers"]
    assert "pre_expiry_weak_volume" in result["blockers"]


def test_sensex_blocks_new_signal_after_235_pm():
    strategy = BreakoutStrategy(instrument="SENSEX")

    signal, reason = strategy.generate_signal(
        price=77000,
        orb_high=76910,
        orb_low=76780,
        vwap=76920,
        atr=55,
        volume_signal="STRONG",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=76880,
        resistance=77080,
        can_trade=True,
        buffer=12,
        pressure_metrics={
            "pressure_bias": "BULLISH",
            "atm_ce_concentration": 0.10,
            "atm_pe_concentration": 0.26,
        },
        candle_high=77010,
        candle_low=76960,
        candle_close=77000,
        candle_open=76970,
        candle_tick_count=6,
        candle_time=datetime(2026, 4, 16, 14, 36),
        candle_volume=1200000,
    )

    assert signal is None
    assert "late-day guard blocked trade" in reason.lower()
    assert "sensex_no_fresh_option_buys_after_1435" in strategy.last_blockers


def test_sensex_requires_elite_quality_after_225_pm():
    strategy = BreakoutStrategy(instrument="SENSEX")

    signal, reason = strategy.generate_signal(
        price=77000,
        orb_high=76910,
        orb_low=76780,
        vwap=76920,
        atr=55,
        volume_signal="NORMAL",
        oi_bias="BULLISH",
        oi_trend="BULLISH",
        build_up="LONG_BUILDUP",
        support=76880,
        resistance=77080,
        can_trade=True,
        buffer=12,
        pressure_metrics={
            "pressure_bias": "BULLISH",
            "atm_ce_concentration": 0.10,
            "atm_pe_concentration": 0.26,
        },
        candle_high=77010,
        candle_low=76960,
        candle_close=77000,
        candle_open=76970,
        candle_tick_count=6,
        candle_time=datetime(2026, 4, 16, 14, 28),
        candle_volume=1200000,
    )

    assert signal is None
    assert "late-day guard blocked trade" in reason.lower()
    assert any(flag.startswith("sensex_late_day_requires_") for flag in strategy.last_blockers)


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
