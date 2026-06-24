from strategies.shared.breakout_strategy import BreakoutSignalStrategy, BreakoutStrategy
from strategies.shared.breakout.watch_engine import WatchEngine
from datetime import datetime
from config import Config
from strategies.shared.breakout.trend_quality_refiner import TrendQualityRefiner
from strategies.shared.expiry_context import ExpirySessionContext
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


def test_trend_quality_refiner_cleans_low_value_trend_cautions():
    clean_leg = TrendQualityRefiner.is_clean_trend_leg(
        scored_direction="CE",
        day_state={"state": "BULL_TREND_ACTIVE", "direction": "CE"},
        price=23900,
        vwap=23800,
        volume_signal="STRONG",
        breakout_body_ok=True,
        breakout_structure_ok=True,
        candle_liquidity_ok=True,
        pressure_conflict_level="MILD",
        opposite_pressure_present=False,
        adx_trade_ok=False,
        mtf_trade_ok=False,
        score=85,
        entry_score=87,
    )

    refined = TrendQualityRefiner.refine_cautions(
        ["far_from_vwap", "participation_baseline_weak", "adx_not_confirmed", "opposite_pressure"],
        clean_trend_leg=clean_leg,
    )

    assert clean_leg is True
    assert "far_from_vwap" not in refined
    assert "participation_baseline_weak" not in refined
    assert "adx_not_confirmed" not in refined
    assert "opposite_pressure" in refined


def test_breakout_signal_strategy_alias_and_preferred_method_work():
    strategy = BreakoutSignalStrategy()

    signal, reason = strategy.generate_trade_signal(
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
        or "pre_expiry_requires_medium_plus_confidence" in strategy.last_blockers
        or "expiry_requires_medium_plus_confidence" in strategy.last_blockers
    )
    assert (
        "volume_weak" in strategy.last_blockers
        or "low_tick_density" in strategy.last_blockers
        or "pre_expiry_weak_volume" in strategy.last_blockers
        or "expiry_weak_volume" in strategy.last_blockers
    )


def test_high_score_no_setup_returns_pending_confirmation_reason():
    strategy = BreakoutStrategy()
    from strategies.shared.breakout.no_setup_finalizer import NoSetupFinalizer

    strategy.last_context_score = 96
    strategy.last_entry_score = 94
    ctx = {
        "price": 23932,
        "vwap": 23905,
        "volume_signal": "NORMAL",
        "oi_bias": "BULLISH",
        "oi_trend": "BULLISH",
        "build_up": "LONG_BUILDUP",
        "buffer": 10,
        "pressure_metrics": {"pressure_bias": "BULLISH", "atm_ce_concentration": 0.11, "atm_pe_concentration": 0.22},
        "score": 96,
        "scored_direction": "CE",
        "components": [],
        "blockers": ["no_valid_setup", "pressure_conflict", "orb_breakout_missing"],
        "cautions": [],
        "tuning": {"extension_buffer_mult": 1.6},
        "candle_liquidity_ok": True,
        "divergence_against_direction": False,
        "wall_break_supports_direction": False,
        "pressure_conflict_level": "MILD",
        "bullish_buildups": {"LONG_BUILDUP", "SHORT_COVERING"},
        "bearish_buildups": {"SHORT_BUILDUP", "LONG_UNWINDING"},
        "fallback_mode": False,
        "bullish_build_up_ok": True,
        "bearish_build_up_ok": False,
        "expiry_eval": {"is_expiry_day": False, "score_floor": 56},
        "expiry_session_mode": "NORMAL",
        "soften_build_up_requirement": False,
        "soften_pressure_conflict": False,
        "time_regime": "MID_MORNING",
        "orb_high": 23950,
        "orb_low": 23880,
        "regime": "TRENDING",
    }

    signal, reason = NoSetupFinalizer.finalize(strategy, ctx)

    assert signal is None
    assert reason.startswith("High-score setup pending confirmation")
    assert "high_score_confirmation_pending" in strategy.last_blockers


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


def test_watch_engine_option_sweep_breakout_uses_support_resistance_from_context():
    strategy = BreakoutStrategy(instrument="BANKNIFTY")
    signal, reason = WatchEngine.finalize_watch_state(
        strategy,
        {
            "scored_direction": "CE",
            "score": 88,
            "pressure_metrics": {"pressure_bias": "BULLISH"},
            "cautions": [],
            "blockers": [],
            "orb_ready": True,
            "price": 55580,
            "orb_high": 55344,
            "orb_low": 54980,
            "vwap": 55310,
            "volume_signal": "NORMAL",
            "oi_bias": "BULLISH",
            "oi_trend": "BULLISH",
            "bullish_build_up_ok": True,
            "bearish_build_up_ok": False,
            "candle_liquidity_ok": True,
            "opening_session": False,
            "continuation_regime_ok": True,
            "breakout_body_ok": True,
            "breakout_structure_ok": True,
            "retest_regime_ok": True,
            "buffer": 20,
            "tuning": strategy._instrument_tuning(),
            "time_thresholds": strategy._get_time_regime_thresholds("LATE", False, market_regime="TRENDING"),
            "breakout_regime_ok": True,
            "reversal_regime_ok": True,
            "pressure_conflict_level": "NONE",
            "candle_high": 55600,
            "candle_low": 55470,
            "candle_close": 55572,
            "atr": 110,
            "support": 55210,
            "resistance": 55720,
            "candle_time": datetime(2026, 5, 6, 14, 20),
            "opening_breakout_override": False,
            "expiry_eval": {"allow_trade": True, "is_expiry_day": False},
            "regime": "CHOPPY",
            "active_confirmation": None,
            "strong_sweep_trade_ready": True,
            "prev_high": 55344,
            "prev_low": 54980,
        },
    )

    assert signal == "CE"
    assert "option sweep breakout confirmation" in reason.lower()


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
            entry_score=80,
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


def test_expiry_rules_soften_vwap_distance_when_day_state_is_aligned():
    rules = ExpiryDayRules(type("TU", (), {})(), instrument="SENSEX")
    rules.time_utils.now_ist = lambda: datetime(2026, 5, 14, 12, 0)
    rules.time_utils.current_time = lambda: datetime(2026, 5, 14, 12, 0).time()

    result = rules.evaluate(
        expiry_value="2026-05-14",
        score=83,
        entry_score=86,
        confidence="MEDIUM",
        price=75150,
        vwap=74950,
        volume_signal="STRONG",
        pressure_metrics={"pressure_bias": "BULLISH"},
        current_signal="CE",
        blockers=[],
        cautions=[],
        day_state={"state": "REVERSAL_UNDERWAY", "direction": "CE"},
    )

    assert result["allow_trade"] is True
    assert "expiry_too_far_from_vwap" not in result["blockers"]
    assert "expiry_too_far_from_vwap" in result["cautions"]


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


def test_expiry_session_context_uses_tuesday_for_nifty_and_thursday_for_sensex():
    nifty_ctx = ExpirySessionContext(
        current_date=datetime(2026, 5, 4).date(),
        instrument="NIFTY",
    )
    sensex_ctx = ExpirySessionContext(
        current_date=datetime(2026, 5, 6).date(),
        instrument="SENSEX",
    )

    assert nifty_ctx.session_mode() == "PRE_EXPIRY_POSITIONING"
    assert sensex_ctx.session_mode() == "PRE_EXPIRY_POSITIONING"


def test_sensex_holiday_adjusted_expiry_uses_profile_date():
    ctx = ExpirySessionContext(
        current_date=datetime(2026, 5, 27).date(),
        instrument="SENSEX",
    )

    assert ctx.session_mode() == "EXPIRY_DAY"


def test_sensex_pre_expiry_weak_volume_stays_watch_like_without_nifty_special_flag():
    rules = ExpiryDayRules(type("TU", (), {})(), instrument="SENSEX")
    rules.time_utils.now_ist = lambda: datetime(2026, 5, 6, 10, 0)
    rules.time_utils.current_time = lambda: datetime(2026, 5, 6, 10, 0).time()

    result = rules.evaluate(
        expiry_value=None,
        score=70,
        confidence="MEDIUM",
        price=78420,
        vwap=78390,
        volume_signal="WEAK",
        pressure_metrics={"pressure_bias": "BEARISH"},
        current_signal="PE",
        blockers=[],
        cautions=[],
    )

    assert result["is_expiry_day"] is False
    assert result["session_mode"] == "PRE_EXPIRY_POSITIONING"
    assert result["allow_trade"] is True
    assert "pre_expiry_weak_volume" not in result["blockers"]
    assert "pre_expiry_weak_volume_watch" in result["cautions"]
    assert "pre_expiry_watch_friendly" not in result["cautions"]


def test_sensex_allows_clean_signal_after_235_pm_when_quality_is_good():
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

    assert signal == "CE"
    assert "sensex_no_fresh_option_buys_after_1435" not in strategy.last_blockers


def test_sensex_late_day_guard_no_longer_blocks_clean_quality_after_225_pm():
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

    assert signal == "CE"
    assert not any(flag.startswith("sensex_late_day_requires_") for flag in strategy.last_blockers)


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


def test_sensex_trend_day_context_helper_is_available_beyond_nifty():
    strategy = BreakoutStrategy(instrument="SENSEX")
    recent_candles = [
        {"time": datetime(2026, 4, 16, 9, 45), "open": 77020, "high": 77040, "low": 77010, "close": 77032},
        {"time": datetime(2026, 4, 16, 9, 50), "open": 77032, "high": 77058, "low": 77028, "close": 77051},
        {"time": datetime(2026, 4, 16, 9, 55), "open": 77051, "high": 77082, "low": 77048, "close": 77076},
    ]

    ready = strategy._nifty_trend_day_context_ready(
        direction="CE",
        price=77064,
        vwap=77030,
        orb_high=77040,
        orb_low=76980,
        candle_close=77064,
        candle_range=34,
        atr=24,
        time_regime="MID_MORNING",
        volume_signal="NORMAL",
        pressure_conflict_level="MILD",
        ha_strength="BULLISH_STRONG",
        recent_candles_5m=recent_candles,
    )

    assert ready is True


def test_day_state_classifier_marks_bearish_open_then_bullish_reclaim_as_reversal():
    strategy = BreakoutStrategy()
    recent = [
        {"time": datetime(2026, 5, 13, 9, 15), "open": 100, "high": 101, "low": 97, "close": 98},
        {"time": datetime(2026, 5, 13, 9, 20), "open": 98, "high": 99, "low": 95, "close": 96},
        {"time": datetime(2026, 5, 13, 9, 25), "open": 96, "high": 97, "low": 94, "close": 95},
        {"time": datetime(2026, 5, 13, 9, 30), "open": 95, "high": 96, "low": 93, "close": 94},
        {"time": datetime(2026, 5, 13, 9, 35), "open": 94, "high": 95, "low": 92, "close": 93},
        {"time": datetime(2026, 5, 13, 9, 40), "open": 93, "high": 94, "low": 91, "close": 92},
        {"time": datetime(2026, 5, 13, 9, 45), "open": 92, "high": 94, "low": 91, "close": 93},
        {"time": datetime(2026, 5, 13, 9, 50), "open": 93, "high": 96, "low": 92, "close": 95},
        {"time": datetime(2026, 5, 13, 9, 55), "open": 95, "high": 99, "low": 94, "close": 98},
        {"time": datetime(2026, 5, 13, 10, 0), "open": 98, "high": 102, "low": 97, "close": 101},
        {"time": datetime(2026, 5, 13, 10, 5), "open": 101, "high": 104, "low": 100, "close": 103},
        {"time": datetime(2026, 5, 13, 10, 10), "open": 103, "high": 106, "low": 102, "close": 105},
    ]

    opening_bias, _ = strategy._derive_opening_bias(recent, vwap=99, atr=4)
    day_state = strategy._derive_active_day_state(
        recent_candles_5m=recent,
        price=105,
        vwap=99,
        atr=4,
        pressure_metrics={"pressure_bias": "BULLISH"},
        opening_bias=opening_bias,
        time_regime="MID_MORNING",
    )

    assert opening_bias == "OPEN_BEARISH"
    assert day_state["state"] == "REVERSAL_UNDERWAY"
    assert day_state["direction"] == "CE"


def test_day_state_classifier_marks_clean_bear_trend_active():
    strategy = BreakoutStrategy()
    recent = [
        {"time": datetime(2026, 5, 13, 11, 0), "open": 200, "high": 201, "low": 198, "close": 199},
        {"time": datetime(2026, 5, 13, 11, 5), "open": 199, "high": 200, "low": 196, "close": 197},
        {"time": datetime(2026, 5, 13, 11, 10), "open": 197, "high": 198, "low": 194, "close": 195},
        {"time": datetime(2026, 5, 13, 11, 15), "open": 195, "high": 196, "low": 192, "close": 193},
        {"time": datetime(2026, 5, 13, 11, 20), "open": 193, "high": 194, "low": 190, "close": 191},
        {"time": datetime(2026, 5, 13, 11, 25), "open": 191, "high": 192, "low": 188, "close": 189},
    ]
    day_state = strategy._derive_active_day_state(
        recent_candles_5m=recent,
        price=189,
        vwap=195,
        atr=4,
        pressure_metrics={"pressure_bias": "BEARISH"},
        opening_bias="OPEN_BALANCED",
        time_regime="MIDDAY",
    )

    assert day_state["state"] == "BEAR_TREND_ACTIVE"
    assert day_state["direction"] == "PE"


def test_day_state_adjustment_penalizes_opposite_direction():
    strategy = BreakoutStrategy()
    components = []
    score, cautions = strategy._apply_day_state_adjustment(
        score=78,
        scored_direction="CE",
        cautions=[],
        components=components,
        day_state={"state": "BEAR_TREND_ACTIVE", "direction": "PE", "detail": "rolling_bear_trend"},
    )

    assert score == 70.0
    assert "day_state_opposes_direction" in cautions


def test_reversal_duplicate_is_suppressed_on_next_bar():
    strategy = BreakoutStrategy(instrument="SENSEX")
    first_bar = datetime(2026, 4, 16, 13, 55)
    second_bar = datetime(2026, 4, 16, 14, 0)
    later_bar = datetime(2026, 4, 16, 14, 20)

    strategy._mark_signal_emitted("PE", "REVERSAL", first_bar, level=75500, buffer=20)

    assert strategy._should_suppress_duplicate("PE", "REVERSAL", second_bar, level=75500) is True
    assert strategy._should_suppress_duplicate("PE", "REVERSAL", later_bar, level=75500) is False
