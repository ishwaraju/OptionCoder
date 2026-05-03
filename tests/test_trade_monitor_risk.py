from datetime import datetime, timedelta

from services.signal_service import SignalService


def build_monitor_service():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.config = type("Cfg", (), {"STOP_LOSS_PERCENT": 15.0, "TARGET_PERCENT": 20.0, "TRAIL_PERCENT": 10.0})()
    service.vwap = type("VWAP", (), {"get_vwap": lambda self: 23980.0})()
    service.pressure = type("Pressure", (), {"analyze": lambda self, option_data: None})()
    service.option_data = None
    service.strategy = type("Strategy", (), {"last_time_regime": "MID_MORNING", "last_heikin_ashi": {"bias": "BULLISH"}})()
    service.active_trade_monitor = {
        "signal": "CE",
        "signal_type": "BREAKOUT",
        "entry_time": datetime(2026, 4, 16, 10, 0),
        "entry_price": 100.0,
        "entry_underlying_price": 24000.0,
        "invalidate_underlying_price": 23960.0,
        "strike": 24000,
        "quality": "A",
        "time_regime": "MID_MORNING",
        "last_notified_minute": None,
        "minutes_active": 0,
        "max_favorable_option_ltp": 100.0,
        "max_adverse_option_ltp": 100.0,
        "stop_loss_pct": 12.0,
        "target_pct": 20.0,
        "trail_pct": 10.0,
        "setup_bucket": "BREAKOUT",
        "risk_note": "Breakout/retest setups use a tighter premium cap because clean follow-through should come quickly.",
        "time_stop_warn_minutes": 3,
        "time_stop_exit_minutes": 5,
        "stop_loss_option_price": 88.0,
        "first_target_option_price": 120.0,
        "partial_booked": False,
        "profit_lock_armed": False,
        "entry_pressure_bias": "BULLISH",
        "entry_pressure_strength": "STRONG",
    }
    return service


def test_option_pnl_percent_calculation():
    assert SignalService._option_pnl_percent(100, 120) == 20.0
    assert SignalService._option_pnl_percent(100, 85) == -15.0


def test_risk_profile_breakout_nifty_uses_twelve_percent_cap():
    service = build_monitor_service()
    profile = service._resolve_trade_risk_profile(setup_type="BREAKOUT", quality="A", confidence="HIGH")

    assert profile["setup_bucket"] == "BREAKOUT"
    assert profile["session_bucket"] == "NON_EXPIRY"
    assert profile["iv_bucket"] == "NORMAL"
    assert profile["hard_premium_stop_pct"] == 16.0
    assert profile["target_pct"] == 32.0
    assert profile["time_stop_warn_minutes"] == 3
    assert profile["time_stop_exit_minutes"] == 5


def test_risk_profile_breakout_sensex_uses_fifteen_percent_cap():
    service = build_monitor_service()
    service.instrument = "SENSEX"
    profile = service._resolve_trade_risk_profile(setup_type="RETEST", quality="A", confidence="HIGH")

    assert profile["setup_bucket"] == "BREAKOUT"
    assert profile["hard_premium_stop_pct"] == 17.0
    assert profile["target_pct"] == 34.0


def test_risk_profile_breakout_banknifty_uses_fifteen_percent_cap():
    service = build_monitor_service()
    service.instrument = "BANKNIFTY"
    profile = service._resolve_trade_risk_profile(setup_type="BREAKOUT_CONFIRM", quality="A", confidence="HIGH")

    assert profile["setup_bucket"] == "BREAKOUT"
    assert profile["hard_premium_stop_pct"] == 18.0
    assert profile["target_pct"] == 36.0


def test_risk_profile_reversal_sensex_uses_eighteen_percent_cap():
    service = build_monitor_service()
    service.instrument = "SENSEX"
    profile = service._resolve_trade_risk_profile(setup_type="REVERSAL", quality="A", confidence="HIGH")

    assert profile["setup_bucket"] == "REVERSAL"
    assert profile["hard_premium_stop_pct"] == 21.0
    assert profile["target_pct"] == 40.0


def test_risk_profile_reversal_banknifty_uses_eighteen_percent_cap():
    service = build_monitor_service()
    service.instrument = "BANKNIFTY"
    profile = service._resolve_trade_risk_profile(setup_type="TRAP_REVERSAL", quality="A", confidence="HIGH")

    assert profile["setup_bucket"] == "REVERSAL"
    assert profile["hard_premium_stop_pct"] == 22.0
    assert profile["target_pct"] == 42.0


def test_expiry_breakout_nifty_tightens_stop_and_time_window():
    service = build_monitor_service()
    profile = service._resolve_trade_risk_profile(
        setup_type="BREAKOUT",
        quality="A",
        confidence="HIGH",
        cautions=["expiry_day_mode", "expiry_fast_decay"],
    )

    assert profile["setup_bucket"] == "BREAKOUT"
    assert profile["session_bucket"] == "EXPIRY"
    assert profile["hard_premium_stop_pct"] == 12.0
    assert profile["target_pct"] == 22.0
    assert profile["time_stop_warn_minutes"] == 2
    assert profile["time_stop_exit_minutes"] == 4
    assert profile["trail_from_peak_pct"] == 8.0


def test_expiry_reversal_banknifty_keeps_wider_stop_but_tightens_time_window():
    service = build_monitor_service()
    service.instrument = "BANKNIFTY"
    profile = service._resolve_trade_risk_profile(
        setup_type="REVERSAL",
        quality="A",
        confidence="HIGH",
        cautions=["expiry_day_mode"],
    )

    assert profile["setup_bucket"] == "REVERSAL"
    assert profile["session_bucket"] == "EXPIRY"
    assert profile["hard_premium_stop_pct"] == 18.0
    assert profile["target_pct"] == 32.0
    assert profile["time_stop_warn_minutes"] == 2
    assert profile["time_stop_exit_minutes"] == 4
    assert profile["trail_from_peak_pct"] == 8.0


def test_non_expiry_breakout_iv_rich_compresses_stop_and_target():
    service = build_monitor_service()
    service._current_risk_option_contract = {"iv": 25.0}
    service._current_risk_reference_contract = {"iv": 22.0}

    profile = service._resolve_trade_risk_profile(
        setup_type="BREAKOUT",
        quality="A",
        confidence="HIGH",
    )

    assert profile["iv_bucket"] == "RICH"
    assert profile["hard_premium_stop_pct"] == 14.0
    assert profile["target_pct"] == 26.0


def test_non_expiry_breakout_iv_cheap_expands_stop_and_target():
    service = build_monitor_service()
    service._current_risk_option_contract = {"iv": 20.0}
    service._current_risk_reference_contract = {"iv": 22.0}

    profile = service._resolve_trade_risk_profile(
        setup_type="BREAKOUT",
        quality="A",
        confidence="HIGH",
    )

    assert profile["iv_bucket"] == "CHEAP"
    assert profile["hard_premium_stop_pct"] == 18.0
    assert profile["target_pct"] == 40.0


def test_market_iv_high_tightens_non_expiry_profile():
    service = build_monitor_service()
    service._current_risk_option_contract = {"iv": 22.0}
    service._current_risk_reference_contract = {"iv": 22.0, "option_type": "CE", "ts": datetime(2026, 4, 16, 10, 0)}
    service.db_reader = type("Reader", (), {
        "fetch_recent_atm_iv_series": lambda self, instrument, option_type, before_ts=None, limit=20: [
            {"ts": datetime(2026, 4, 16, 9, 59), "iv": 18.0},
            {"ts": datetime(2026, 4, 16, 9, 58), "iv": 18.5},
            {"ts": datetime(2026, 4, 16, 9, 57), "iv": 19.0},
        ]
    })()

    profile = service._resolve_trade_risk_profile(setup_type="BREAKOUT", quality="A", confidence="HIGH")

    assert profile["market_iv_regime"] == "HIGH"
    assert profile["hard_premium_stop_pct"] == 15.0
    assert profile["target_pct"] == 29.0


def test_market_iv_event_tightens_expiry_profile_and_time_stop():
    service = build_monitor_service()
    service._current_risk_option_contract = {"iv": 30.0}
    service._current_risk_reference_contract = {"iv": 30.0, "option_type": "CE", "ts": datetime(2026, 4, 16, 10, 0)}
    service.db_reader = type("Reader", (), {
        "fetch_recent_atm_iv_series": lambda self, instrument, option_type, before_ts=None, limit=20: [
            {"ts": datetime(2026, 4, 16, 9, 59), "iv": 24.0},
            {"ts": datetime(2026, 4, 16, 9, 58), "iv": 24.5},
            {"ts": datetime(2026, 4, 16, 9, 57), "iv": 25.0},
        ]
    })()

    profile = service._resolve_trade_risk_profile(
        setup_type="BREAKOUT",
        quality="A",
        confidence="HIGH",
        cautions=["expiry_day_mode", "expiry_fast_decay"],
    )

    assert profile["market_iv_regime"] == "EVENT"
    assert profile["hard_premium_stop_pct"] == 10.0
    assert profile["target_pct"] == 16.0
    assert profile["time_stop_warn_minutes"] == 1
    assert profile["time_stop_exit_minutes"] == 3


def test_trade_monitor_hits_hard_stoploss():
    service = build_monitor_service()
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 88.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 4), "open": 23990, "high": 24005, "low": 23985, "close": 24000, "volume": 1000},
        {"time": datetime(2026, 4, 16, 10, 5), "open": 24000, "high": 24010, "low": 23998, "close": 24008, "volume": 1000},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24015, "low": 23995, "close": 24008},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "EXIT_STOPLOSS"
    assert monitor["pnl_percent"] == -12.0


def test_trade_monitor_underlying_invalidation_exits_before_premium_cap():
    service = build_monitor_service()
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 96.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 2), "open": 23992, "high": 24003, "low": 23988, "close": 23994, "volume": 900},
        {"time": datetime(2026, 4, 16, 10, 3), "open": 23994, "high": 23998, "low": 23955, "close": 23966, "volume": 1100},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 23998, "low": 23955, "close": 23966},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "EXIT_BIAS"
    assert monitor["decision_label"] == "CONFIRMED_PE_FLIP"
    assert monitor["pnl_percent"] == -4.0


def test_trade_monitor_books_partial_at_twenty_percent():
    service = build_monitor_service()
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 120.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 4), "open": 24000, "high": 24020, "low": 23998, "close": 24012, "volume": 1000},
        {"time": datetime(2026, 4, 16, 10, 5), "open": 24012, "high": 24030, "low": 24010, "close": 24025, "volume": 1200},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24025, "low": 23995, "close": 24025},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "HOLD_STRONG"
    assert monitor["pnl_percent"] == 20.0


def test_trade_monitor_exits_on_trail_after_partial():
    service = build_monitor_service()
    service.active_trade_monitor["partial_booked"] = True
    service.active_trade_monitor["max_favorable_option_ltp"] = 140.0
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 124.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 9), "open": 24020, "high": 24028, "low": 24010, "close": 24015, "volume": 900},
        {"time": datetime(2026, 4, 16, 10, 10), "open": 24015, "high": 24018, "low": 24005, "close": 24008, "volume": 850},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24030, "low": 24000, "close": 24025},
        {"time": datetime(2026, 4, 16, 10, 10), "high": 24018, "low": 24002, "close": 24008},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "EXIT_TRAIL"
    assert monitor["drawdown_from_peak_pct"] >= 10.0


def test_trade_monitor_arms_profit_lock_and_holds_on_small_momentum_pause():
    service = build_monitor_service()
    service.active_trade_monitor["profit_lock_armed"] = True
    service.active_trade_monitor["max_favorable_option_ltp"] = 112.0
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 109.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 4), "open": 24009, "high": 24018, "low": 24006, "close": 24012, "volume": 980},
        {"time": datetime(2026, 4, 16, 10, 5), "open": 24012, "high": 24014, "low": 24008, "close": 24010, "volume": 930},
        {"time": datetime(2026, 4, 16, 10, 6), "open": 24010, "high": 24012, "low": 24007, "close": 24009, "volume": 900},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24014, "low": 24007, "close": 24009},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "HOLD_WITH_TRAIL"
    assert monitor["profit_lock_armed"] is True


def test_trade_monitor_exits_to_protect_profit_after_profit_lock_and_invalidation():
    service = build_monitor_service()
    service.active_trade_monitor["profit_lock_armed"] = True
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 108.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 4), "open": 23995, "high": 24000, "low": 23970, "close": 23982, "volume": 1000},
        {"time": datetime(2026, 4, 16, 10, 5), "open": 23982, "high": 23985, "low": 23958, "close": 23960, "volume": 1100},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 23985, "low": 23958, "close": 23960},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "EXIT_PROFIT_PROTECT"
    assert monitor["pnl_percent"] > 0


def test_trade_monitor_uses_dynamic_trail_and_holds_winner_at_target():
    service = build_monitor_service()
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 120.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 4), "open": 24000, "high": 24020, "low": 23998, "close": 24012, "volume": 1000},
        {"time": datetime(2026, 4, 16, 10, 5), "open": 24012, "high": 24030, "low": 24010, "close": 24025, "volume": 1200},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24040, "low": 23992, "close": 24025},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "HOLD_STRONG"
    assert monitor["dynamic_trail_pct"] >= 10.0


def test_trade_monitor_psar_style_break_protects_profit():
    service = build_monitor_service()
    service.active_trade_monitor["profit_lock_armed"] = True
    service.active_trade_monitor["psar_style_level"] = 24005.0
    service.active_trade_monitor["max_favorable_option_ltp"] = 118.0
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 111.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 6), "open": 24010, "high": 24014, "low": 24006, "close": 24009, "volume": 900},
        {"time": datetime(2026, 4, 16, 10, 7), "open": 24009, "high": 24010, "low": 23998, "close": 24003, "volume": 980},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24015, "low": 24000, "close": 24009},
        {"time": datetime(2026, 4, 16, 10, 10), "high": 24010, "low": 23996, "close": 24003},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "EXIT_PROFIT_PROTECT"
    assert monitor["psar_style_level"] >= 24005.0


def test_trade_monitor_warns_time_decay_after_three_minutes_without_expansion():
    service = build_monitor_service()
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 101.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 1), "open": 24001, "high": 24006, "low": 23998, "close": 24002, "volume": 900},
        {"time": datetime(2026, 4, 16, 10, 2), "open": 24002, "high": 24004, "low": 23999, "close": 24001, "volume": 850},
        {"time": datetime(2026, 4, 16, 10, 3), "open": 24001, "high": 24003, "low": 23999, "close": 24000, "volume": 820},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24004, "low": 23999, "close": 24000},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "TIME_DECAY_RISK"
    assert monitor["decision_label"] == "HIGH_NOISE_SKIP"


def test_trade_monitor_exits_after_five_minutes_without_expansion():
    service = build_monitor_service()
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 99.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 3), "open": 24001, "high": 24006, "low": 23998, "close": 24002, "volume": 900},
        {"time": datetime(2026, 4, 16, 10, 4), "open": 24002, "high": 24004, "low": 23999, "close": 24001, "volume": 850},
        {"time": datetime(2026, 4, 16, 10, 5), "open": 24001, "high": 24003, "low": 23999, "close": 24000, "volume": 820},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24004, "low": 23999, "close": 24000},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "EXIT_TIMESTOP"
    assert monitor["decision_label"] == "TIME_STOP_EXIT"


def test_expiry_trade_monitor_warns_faster_after_two_minutes_without_expansion():
    service = build_monitor_service()
    service.active_trade_monitor["time_stop_warn_minutes"] = 2
    service.active_trade_monitor["time_stop_exit_minutes"] = 4
    service.active_trade_monitor["trail_pct"] = 8.0
    service.active_trade_monitor["risk_note"] = (
        "Expiry mode active: faster decay and noisy premium swings. "
        "If expansion does not come quickly, exit bias should accelerate."
    )
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 100.5}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 1), "open": 24001, "high": 24004, "low": 23999, "close": 24001, "volume": 850},
        {"time": datetime(2026, 4, 16, 10, 2), "open": 24001, "high": 24003, "low": 23999, "close": 24000, "volume": 820},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24003, "low": 23999, "close": 24000},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "TIME_DECAY_RISK"
    assert monitor["time_stop_warn_minutes"] == 2


def test_expiry_trade_monitor_protects_small_profit_if_move_stays_slow():
    service = build_monitor_service()
    service.active_trade_monitor["time_stop_warn_minutes"] = 2
    service.active_trade_monitor["time_stop_exit_minutes"] = 4
    service.active_trade_monitor["expiry_fast_decay"] = True
    service.active_trade_monitor["profit_lock_trigger_pct"] = 10.0
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 106.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 1), "open": 24001, "high": 24004, "low": 23999, "close": 24001, "volume": 850},
        {"time": datetime(2026, 4, 16, 10, 2), "open": 24001, "high": 24003, "low": 23999, "close": 24000, "volume": 820},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24003, "low": 23999, "close": 24000},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "EXIT_PROFIT_PROTECT"
    assert monitor["theta_risk_high"] is True


def test_trade_monitor_exits_when_live_pressure_flips_against_trade():
    service = build_monitor_service()
    service.option_data = {"mock": True}
    service.pressure = type(
        "Pressure",
        (),
        {
            "analyze": lambda self, option_data: {
                "near_put_pressure_ratio": 0.6,
                "near_call_pressure_ratio": 2.4,
                "full_put_pressure_ratio": 0.7,
                "full_call_pressure_ratio": 2.0,
                "atm_pe_volume": 100,
                "atm_ce_volume": 280,
                "atm_pe_oi": 80,
                "atm_ce_oi": 210,
                "mid_pe_volume": 120,
                "mid_ce_volume": 260,
                "near_pe_oi": 90,
                "near_ce_oi": 180,
            }
        },
    )()
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 98.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 1), "open": 24002, "high": 24005, "low": 23999, "close": 24001, "volume": 920},
        {"time": datetime(2026, 4, 16, 10, 2), "open": 24001, "high": 24002, "low": 23997, "close": 23999, "volume": 980},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24003, "low": 23997, "close": 23999},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "EXIT_BIAS"
    assert monitor["live_pressure_bias"] == "BEARISH"
    assert "pressure bias" in monitor["reason"]


def test_trade_monitor_warns_when_pressure_flips_but_structure_not_broken_yet():
    service = build_monitor_service()
    service.option_data = {"mock": True}
    service.pressure = type(
        "Pressure",
        (),
        {
            "analyze": lambda self, option_data: {
                "near_put_pressure_ratio": 0.9,
                "near_call_pressure_ratio": 1.8,
                "full_put_pressure_ratio": 1.0,
                "full_call_pressure_ratio": 1.6,
                "atm_pe_volume": 140,
                "atm_ce_volume": 220,
                "atm_pe_oi": 100,
                "atm_ce_oi": 170,
                "mid_pe_volume": 160,
                "mid_ce_volume": 210,
                "near_pe_oi": 110,
                "near_ce_oi": 150,
            }
        },
    )()
    service._get_option_contract_snapshot = lambda strike, signal, before_ts=None: {"ltp": 107.0}

    recent_1m = [
        {"time": datetime(2026, 4, 16, 10, 1), "open": 24000, "high": 24007, "low": 23999, "close": 24004, "volume": 950},
        {"time": datetime(2026, 4, 16, 10, 2), "open": 24004, "high": 24009, "low": 24001, "close": 24006, "volume": 980},
    ]
    recent_5m = [
        {"time": datetime(2026, 4, 16, 10, 0), "high": 24010, "low": 23970, "close": 24000},
        {"time": datetime(2026, 4, 16, 10, 5), "high": 24009, "low": 24000, "close": 24006},
    ]

    monitor = service._evaluate_trade_monitor(recent_1m, recent_5m)

    assert monitor["guidance"] == "THESIS_WEAKENING"
    assert monitor["live_pressure_bias"] == "BEARISH"
