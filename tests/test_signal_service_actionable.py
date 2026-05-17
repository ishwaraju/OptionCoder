from services.signal_service import SignalService
from services.signal_service_support import OptionSignalGuard
from datetime import datetime, timedelta
from shared.market.oi_quote_confirmation import OIQuoteConfirmation
from shared.market.option_spike_detector import OptionSpikeDetector
from zoneinfo import ZoneInfo


def build_service(signal_type, signal_grade, confidence):
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.strategy = type(
        "StrategyStub",
        (),
        {
            "last_signal_type": signal_type,
            "last_signal_grade": signal_grade,
            "last_confidence": confidence,
            "last_regime": "TRENDING",
            "last_score": 80,
            "last_entry_score": 80,
            "last_cautions": [],
            "last_pressure_conflict_level": "NONE",
            "last_active_day_state": "UNKNOWN",
            "last_day_state_direction": "NONE",
            "last_trend_leg_stage": "NEUTRAL",
            "last_session_map_phase": "AFTERNOON_EXPANSION",
            "last_futures_acceptance": None,
            "last_initiative_strength_score": 0,
            "last_signal_family": "UNKNOWN",
            "last_entry_plan": {},
            "last_emitted_signal": None,
        },
    )()
    return service


def test_option_buyer_actionable_accepts_a_grade_breakout():
    service = build_service("BREAKOUT", "A", "MEDIUM")
    assert service._is_option_buyer_actionable("PE") is True


def test_option_buyer_actionable_blocks_watch_grade():
    service = build_service("BREAKOUT", "WATCH", "MEDIUM")
    assert service._is_option_buyer_actionable("PE") is False


def test_option_buyer_actionable_blocks_reversal_even_with_a_grade():
    service = build_service("REVERSAL", "A", "HIGH")
    assert service._is_option_buyer_actionable("CE") is False


def test_option_buyer_actionable_allows_elite_reversal_without_conflict():
    service = build_service("REVERSAL", "A", "HIGH")
    service.strategy.last_score = 92
    service.strategy.last_entry_score = 86
    service.strategy.last_pressure_conflict_level = "NONE"
    assert service._is_option_buyer_actionable("CE", candle_time=datetime(2026, 4, 16, 11, 5)) is True


def test_option_buyer_actionable_blocks_nifty_elite_reversal_in_late_session():
    service = build_service("REVERSAL", "A", "HIGH")
    service.strategy.last_score = 92
    service.strategy.last_entry_score = 86
    service.strategy.last_pressure_conflict_level = "NONE"

    assert service._is_option_buyer_actionable("CE", candle_time=datetime(2026, 4, 16, 13, 35)) is False


def test_banknifty_allows_b_grade_breakout_after_11_am():
    service = build_service("BREAKOUT", "B", "MEDIUM")
    service.instrument = "BANKNIFTY"
    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 11, 10)) is True


def test_banknifty_allows_b_grade_breakout_before_11_am_when_quality_is_clean():
    service = build_service("BREAKOUT", "B", "MEDIUM")
    service.instrument = "BANKNIFTY"
    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 10, 25)) is True


def test_option_buyer_actionable_blocks_opening_breakout_when_participation_is_weak():
    service = build_service("BREAKOUT_CONFIRM", "B", "HIGH")
    service.instrument = "NIFTY"
    service.strategy.last_score = 86
    service.strategy.last_entry_score = 84
    service.strategy.last_cautions = [
        "participation_weak",
        "participation_delta_missing",
    ]

    assert service._is_option_buyer_actionable("CE", candle_time=datetime(2026, 4, 16, 9, 50)) is False


def test_sensex_allows_a_grade_breakout_confirm():
    service = build_service("BREAKOUT_CONFIRM", "A", "MEDIUM")
    service.instrument = "SENSEX"
    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 11, 5)) is True


def test_option_buyer_actionable_blocks_late_chase_breakdown_when_theta_and_vwap_flags_hit():
    service = build_service("BREAKOUT_CONFIRM", "B", "HIGH")
    service.instrument = "SENSEX"
    service.strategy.last_score = 90
    service.strategy.last_entry_score = 88
    service.strategy.last_pressure_conflict_level = "NONE"
    service.strategy.last_cautions = [
        "far_from_vwap",
        "theta_fast_exit_required",
        "late_day_breakdown_watch",
    ]

    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 14, 50)) is False


def test_high_expectancy_gate_holds_breakout_until_premium_confirms():
    service = build_service("BREAKOUT_CONFIRM", "A", "HIGH")
    service.instrument = "NIFTY"
    service.strategy.last_score = 90
    service.strategy.last_entry_score = 92
    service.strategy.last_cautions = []
    service.strategy.last_pressure_conflict_level = "NONE"
    service.strategy.last_entry_plan = {"entry_above": 100.0}
    service.strategy.last_active_day_state = "BULL_TREND_ACTIVE"
    service.strategy.last_day_state_direction = "CE"
    service._entry_too_extended = lambda *_args, **_kwargs: False
    service.atr = type("ATRStub", (), {"get_buffer": staticmethod(lambda: 5.0), "atr": 10.0})()

    profile = OptionSignalGuard.assess_high_expectancy(
        service,
        "CE",
        datetime(2026, 4, 16, 11, 15),
        premium_guard={"label": "PREMIUM_SLEEPY", "premium_momentum_pct": 0.0, "spread_pct": 2.0},
        price=101.0,
    )

    assert profile["allow_trade"] is False
    assert profile["watch_only"] is True
    assert profile["quality_tag"] == "TQ_CLEAN"


def test_price_action_strong_watch_bucket_when_premium_is_pending():
    service = build_service("BREAKOUT_CONFIRM", "A", "HIGH")
    service.instrument = "NIFTY"
    service.strategy.last_score = 90
    service.strategy.last_entry_score = 92
    service.strategy.last_cautions = []
    service.strategy.last_pressure_conflict_level = "NONE"
    service.strategy.last_entry_plan = {"entry_above": 100.0}
    service.strategy.last_active_day_state = "BULL_TREND_ACTIVE"
    service.strategy.last_day_state_direction = "CE"
    service.strategy.last_trend_leg_stage = "FIRST_IMPULSE"
    service.strategy.last_futures_acceptance = {"accepted": True, "score": 64}
    service.strategy.last_initiative_strength_score = 34
    service._entry_too_extended = lambda *_args, **_kwargs: False
    service.atr = type("ATRStub", (), {"get_buffer": staticmethod(lambda: 5.0), "atr": 10.0})()

    profile = OptionSignalGuard.assess_high_expectancy(
        service,
        "CE",
        datetime(2026, 4, 16, 11, 15),
        premium_guard={"label": "PREMIUM_OK", "premium_momentum_pct": 0.2, "spread_pct": 2.1, "volume_supporting": False},
        price=100.8,
    )

    assert profile["allow_trade"] is False
    assert profile["watch_only"] is True
    assert profile["quality_tag"] == "PA_STRONG_WAIT_PREMIUM"


def test_price_action_strong_can_enter_small_when_sponsorship_is_clean():
    service = build_service("BREAKOUT_CONFIRM", "A", "HIGH")
    service.instrument = "BANKNIFTY"
    service.strategy.last_score = 88
    service.strategy.last_entry_score = 90
    service.strategy.last_cautions = []
    service.strategy.last_pressure_conflict_level = "NONE"
    service.strategy.last_entry_plan = {"entry_above": 54000.0}
    service.strategy.last_active_day_state = "BULL_TREND_ACTIVE"
    service.strategy.last_day_state_direction = "CE"
    service.strategy.last_trend_leg_stage = "FIRST_IMPULSE"
    service.strategy.last_futures_acceptance = {"accepted": True, "score": 66}
    service.strategy.last_initiative_strength_score = 36
    service._entry_too_extended = lambda *_args, **_kwargs: False
    service.atr = type("ATRStub", (), {"get_buffer": staticmethod(lambda: 10.0), "atr": 20.0})()

    profile = OptionSignalGuard.assess_high_expectancy(
        service,
        "CE",
        datetime(2026, 4, 16, 12, 15),
        premium_guard={
            "label": "PREMIUM_OK",
            "premium_momentum_pct": 0.15,
            "spread_pct": 2.0,
            "volume_supporting": True,
            "previous_ltp": 100.0,
            "current_ltp": 101.2,
        },
        price=54004.0,
    )

    assert profile["allow_trade"] is True
    assert profile["watch_only"] is False
    assert profile["quality_tag"] == "PA_STRONG_ENTER_SMALL"


def test_high_expectancy_gate_promotes_clean_first_signal_to_hq():
    service = build_service("BREAKOUT_CONFIRM", "A", "HIGH")
    service.instrument = "NIFTY"
    service.strategy.last_score = 84
    service.strategy.last_entry_score = 88
    service.strategy.last_cautions = ["far_from_vwap"]
    service.strategy.last_pressure_conflict_level = "MILD"
    service.strategy.last_entry_plan = {"entry_above": 100.0}
    service.strategy.last_active_day_state = "BULL_TREND_ACTIVE"
    service.strategy.last_day_state_direction = "CE"
    service.strategy.last_trend_leg_stage = "FIRST_IMPULSE"
    service.strategy.last_emitted_signal = None
    service._entry_too_extended = lambda *_args, **_kwargs: False
    service.atr = type("ATRStub", (), {"get_buffer": staticmethod(lambda: 5.0), "atr": 10.0})()

    profile = OptionSignalGuard.assess_high_expectancy(
        service,
        "CE",
        datetime(2026, 4, 16, 11, 15),
        premium_guard={
            "label": "PREMIUM_OK",
            "premium_momentum_pct": 1.4,
            "spread_pct": 2.0,
            "volume_supporting": True,
            "clean_breakout_premium": True,
        },
        price=100.5,
    )

    assert profile["allow_trade"] is True
    assert profile["quality_tag"] == "HQ"


def test_high_expectancy_marks_volatile_tactical_when_conflicts_exist():
    service = build_service("BREAKOUT_CONFIRM", "A", "HIGH")
    service.instrument = "BANKNIFTY"
    service.strategy.last_score = 82
    service.strategy.last_entry_score = 84
    service.strategy.last_cautions = ["participation_weak", "far_from_vwap"]
    service.strategy.last_pressure_conflict_level = "MODERATE"
    service.strategy.last_entry_plan = {"entry_above": 54000.0}
    service.strategy.last_active_day_state = "BULL_TREND_ACTIVE"
    service.strategy.last_day_state_direction = "CE"
    service.strategy.last_emitted_signal = None
    service._entry_too_extended = lambda *_args, **_kwargs: False
    service.atr = type("ATRStub", (), {"get_buffer": staticmethod(lambda: 10.0), "atr": 20.0})()

    profile = OptionSignalGuard.assess_high_expectancy(
        service,
        "CE",
        datetime(2026, 4, 16, 12, 15),
        premium_guard={"label": "PREMIUM_OK", "premium_momentum_pct": 1.2, "spread_pct": 4.2},
        price=54020.0,
    )

    assert profile["allow_trade"] is False
    assert profile["watch_only"] is True
    assert profile["quality_tag"] == "TQ_VOLATILE"


def test_high_expectancy_promotes_clean_retest_to_tq_clean():
    service = build_service("RETEST", "B", "MEDIUM")
    service.instrument = "BANKNIFTY"
    service.strategy.last_score = 80
    service.strategy.last_entry_score = 84
    service.strategy.last_cautions = ["far_from_vwap"]
    service.strategy.last_pressure_conflict_level = "MILD"
    service.strategy.last_entry_plan = {"entry_above": 54000.0}
    service.strategy.last_active_day_state = "BULL_TREND_ACTIVE"
    service.strategy.last_day_state_direction = "CE"
    service.strategy.last_trend_leg_stage = "FIRST_RETEST"
    service.strategy.last_emitted_signal = None
    service._entry_too_extended = lambda *_args, **_kwargs: False
    service.atr = type("ATRStub", (), {"get_buffer": staticmethod(lambda: 10.0), "atr": 20.0})()

    profile = OptionSignalGuard.assess_high_expectancy(
        service,
        "CE",
        datetime(2026, 4, 16, 12, 15),
        premium_guard={
            "label": "PREMIUM_OK",
            "premium_momentum_pct": 1.2,
            "spread_pct": 2.8,
            "volume_supporting": True,
            "clean_breakout_premium": True,
        },
        price=54020.0,
    )

    assert profile["allow_trade"] is True
    assert profile["quality_tag"] == "TQ_CLEAN"


def test_high_expectancy_blocks_dead_premium_risk_even_when_signal_is_strong():
    service = build_service("BREAKOUT_CONFIRM", "A", "HIGH")
    service.instrument = "BANKNIFTY"
    service.strategy.last_score = 91
    service.strategy.last_entry_score = 92
    service.strategy.last_entry_plan = {"entry_above": 54000.0}
    service.strategy.last_active_day_state = "BULL_TREND_ACTIVE"
    service.strategy.last_day_state_direction = "CE"
    service.strategy.last_trend_leg_stage = "FIRST_IMPULSE"
    service.strategy.last_futures_acceptance = {"accepted": True, "score": 70}
    service.strategy.last_initiative_strength_score = 40
    service._entry_too_extended = lambda *_args, **_kwargs: False
    service.atr = type("ATRStub", (), {"get_buffer": staticmethod(lambda: 10.0), "atr": 20.0})()

    profile = OptionSignalGuard.assess_high_expectancy(
        service,
        "CE",
        datetime(2026, 4, 16, 12, 15),
        premium_guard={
            "label": "PREMIUM_OK",
            "premium_momentum_pct": 0.1,
            "spread_pct": 5.1,
            "volume_supporting": False,
            "current_ltp": 210.0,
            "previous_ltp": 209.7,
        },
        price=54020.0,
    )

    assert profile["allow_trade"] is False
    assert profile["quality_tag"] == "AVOID"
    assert "dead_premium_risk" in profile["reasons"]


def test_high_expectancy_allows_clean_breakout_with_baseline_weak_only():
    service = build_service("BREAKOUT_CONFIRM", "A", "HIGH")
    service.instrument = "NIFTY"
    service.strategy.last_score = 82
    service.strategy.last_entry_score = 84
    service.strategy.last_cautions = ["participation_baseline_weak"]
    service.strategy.last_pressure_conflict_level = "MILD"
    service.strategy.last_entry_plan = {"entry_above": 100.0}
    service.strategy.last_active_day_state = "BULL_TREND_ACTIVE"
    service.strategy.last_day_state_direction = "CE"
    service.strategy.last_trend_leg_stage = "FIRST_IMPULSE"
    service.strategy.last_emitted_signal = None
    service._entry_too_extended = lambda *_args, **_kwargs: False
    service.atr = type("ATRStub", (), {"get_buffer": staticmethod(lambda: 5.0), "atr": 10.0})()

    profile = OptionSignalGuard.assess_high_expectancy(
        service,
        "CE",
        datetime(2026, 4, 16, 11, 45),
        premium_guard={
            "label": "PREMIUM_OK",
            "premium_momentum_pct": 1.3,
            "spread_pct": 2.5,
            "volume_supporting": True,
            "clean_breakout_premium": True,
        },
        price=100.6,
    )

    assert profile["allow_trade"] is True
    assert profile["quality_tag"] == "TQ_CLEAN"


def test_entry_phase_does_not_mark_different_breakout_family_as_reentry():
    service = build_service("BREAKOUT_CONFIRM", "A", "HIGH")
    service.strategy.last_cautions = []
    service.strategy.last_trend_leg_stage = "FIRST_IMPULSE"
    service.strategy.last_emitted_signal = {
        "direction": "CE",
        "signal_type": "REVERSAL",
        "time": datetime(2026, 4, 16, 11, 0),
        "session_day": datetime(2026, 4, 16, 11, 0).date(),
        "level": 100.0,
        "buffer": 5.0,
    }
    service._entry_too_extended = lambda *_args, **_kwargs: False
    service.atr = type("ATRStub", (), {"get_buffer": staticmethod(lambda: 5.0), "atr": 10.0})()

    phase = OptionSignalGuard.classify_entry_phase(
        service,
        "CE",
        "BREAKOUT_CONFIRM",
        datetime(2026, 4, 16, 11, 15),
        price=101.0,
        trigger_price=100.0,
    )

    assert phase == "FIRST_SIGNAL_IN_MOVE"


def test_entry_phase_uses_trend_leg_stage_for_retest():
    service = build_service("BREAKOUT_CONFIRM", "A", "HIGH")
    service.strategy.last_cautions = []
    service.strategy.last_trend_leg_stage = "FIRST_RETEST"
    service._entry_too_extended = lambda *_args, **_kwargs: False
    service.atr = type("ATRStub", (), {"get_buffer": staticmethod(lambda: 5.0), "atr": 10.0})()

    phase = OptionSignalGuard.classify_entry_phase(
        service,
        "CE",
        "BREAKOUT_CONFIRM",
        datetime(2026, 4, 16, 11, 15),
        price=101.0,
        trigger_price=100.0,
    )

    assert phase == "RETEST_SIGNAL"


def test_high_expectancy_gate_allows_elite_reversal_quality():
    service = build_service("REVERSAL", "B", "MEDIUM")
    service.instrument = "SENSEX"
    service.strategy.last_score = 87
    service.strategy.last_entry_score = 93
    service.strategy.last_cautions = ["far_from_vwap"]
    service.strategy.last_pressure_conflict_level = "NONE"
    service.strategy.last_entry_plan = {"entry_below": 75500.0}
    service.strategy.last_active_day_state = "BEAR_TREND_ACTIVE"
    service.strategy.last_day_state_direction = "PE"
    service._entry_too_extended = lambda *_args, **_kwargs: False
    service.atr = type("ATRStub", (), {"get_buffer": staticmethod(lambda: 10.0), "atr": 20.0})()

    profile = OptionSignalGuard.assess_high_expectancy(
        service,
        "PE",
        datetime(2026, 4, 16, 13, 55),
        premium_guard={"label": "PREMIUM_OK", "premium_momentum_pct": 0.5, "spread_pct": 2.5},
        price=75490.0,
    )

    assert profile["allow_trade"] is True
    assert profile["quality_tag"] == "RQ"


def test_sensex_allows_clean_b_grade_continuation_even_when_global_continuation_flag_is_off():
    service = build_service("CONTINUATION", "B", "MEDIUM")
    service.instrument = "SENSEX"
    service.strategy.last_score = 71
    service.strategy.last_entry_score = 74
    service.strategy.last_pressure_conflict_level = "NONE"
    assert service._is_option_buyer_actionable("CE", candle_time=datetime(2026, 4, 16, 11, 40)) is True


def test_sensex_blocks_b_grade_breakout():
    service = build_service("BREAKOUT", "B", "MEDIUM")
    service.instrument = "SENSEX"
    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 10, 25)) is False


def test_sensex_allows_a_grade_reversal_when_elite_and_clean():
    service = build_service("REVERSAL", "A", "HIGH")
    service.instrument = "SENSEX"
    service.strategy.last_score = 92
    service.strategy.last_entry_score = 94
    service.strategy.last_pressure_conflict_level = "NONE"
    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 11, 25)) is True


def test_sensex_option_sweep_override_allows_clean_reversal():
    service = build_service("REVERSAL", "B", "MEDIUM")
    service.instrument = "SENSEX"
    service.strategy.last_score = 86
    service.strategy.last_entry_score = 88
    service.strategy.last_pressure_conflict_level = "MILD"
    service.option_sweep_context = {
        "direction": "CE",
        "quality": "STRONG",
        "micro_confirmed": True,
        "persistence_pairs": 4,
    }

    assert service._is_option_buyer_actionable("CE", candle_time=datetime(2026, 4, 16, 14, 25)) is True


def test_option_volume_signal_uses_band_volume_when_candle_volume_missing():
    signal = SignalService._derive_option_volume_signal(
        {
            "ce_volume_band": 120_000,
            "pe_volume_band": 115_000,
            "ce_volume": 8_000,
            "pe_volume": 9_000,
        }
    )

    assert signal == "NORMAL"


def test_option_volume_signal_marks_strong_when_one_side_dominates():
    signal = SignalService._derive_option_volume_signal(
        {
            "ce_volume_band": 180_000,
            "pe_volume_band": 100_000,
            "ce_volume": 15_000,
            "pe_volume": 10_000,
        }
    )

    assert signal == "STRONG"


def test_nifty_option_sweep_override_allows_clean_reversal():
    service = build_service("REVERSAL", "B", "MEDIUM")
    service.instrument = "NIFTY"
    service.strategy.last_score = 86
    service.strategy.last_entry_score = 88
    service.strategy.last_pressure_conflict_level = "MILD"
    service.option_sweep_context = {
        "direction": "CE",
        "quality": "STRONG",
        "micro_confirmed": True,
        "persistence_pairs": 4,
    }

    assert service._is_option_buyer_actionable("CE", candle_time=datetime(2026, 4, 16, 14, 25)) is True


def test_banknifty_option_sweep_override_allows_clean_reversal():
    service = build_service("REVERSAL", "B", "MEDIUM")
    service.instrument = "BANKNIFTY"
    service.strategy.last_score = 86
    service.strategy.last_entry_score = 88
    service.strategy.last_pressure_conflict_level = "MILD"
    service.option_sweep_context = {
        "direction": "CE",
        "quality": "STRONG",
        "micro_confirmed": True,
        "persistence_pairs": 4,
    }

    assert service._is_option_buyer_actionable("CE", candle_time=datetime(2026, 4, 16, 14, 25)) is True


def test_collect_unprocessed_5m_candles_replays_backlog_after_gap():
    service = SignalService.__new__(SignalService)
    service.last_processed_5m_ts = datetime(2026, 5, 14, 11, 15, tzinfo=ZoneInfo("Asia/Kolkata"))

    recent_candles = [
        {"time": datetime(2026, 5, 14, 11, 15, tzinfo=ZoneInfo("Asia/Kolkata")), "close_time": None, "close": 100},
        {"time": datetime(2026, 5, 14, 11, 20, tzinfo=ZoneInfo("Asia/Kolkata")), "close_time": None, "close": 101},
        {"time": datetime(2026, 5, 14, 11, 25, tzinfo=ZoneInfo("Asia/Kolkata")), "close_time": None, "close": 102},
        {"time": datetime(2026, 5, 14, 11, 30, tzinfo=ZoneInfo("Asia/Kolkata")), "close_time": None, "close": 103},
    ]

    candles = service._collect_unprocessed_5m_candles(
        recent_candles,
        datetime(2026, 5, 14, 11, 36, tzinfo=ZoneInfo("Asia/Kolkata")),
    )

    assert [candle["time"].minute for candle in candles] == [20, 25, 30]


def test_collect_unprocessed_5m_candles_warms_recent_context_on_fresh_restart():
    service = SignalService.__new__(SignalService)
    service.last_processed_5m_ts = None

    recent_candles = [
        {"time": datetime(2026, 5, 14, 11, 10, tzinfo=ZoneInfo("Asia/Kolkata")), "close_time": None, "close": 100},
        {"time": datetime(2026, 5, 14, 11, 15, tzinfo=ZoneInfo("Asia/Kolkata")), "close_time": None, "close": 101},
        {"time": datetime(2026, 5, 14, 11, 20, tzinfo=ZoneInfo("Asia/Kolkata")), "close_time": None, "close": 102},
        {"time": datetime(2026, 5, 14, 11, 25, tzinfo=ZoneInfo("Asia/Kolkata")), "close_time": None, "close": 103},
        {"time": datetime(2026, 5, 14, 11, 30, tzinfo=ZoneInfo("Asia/Kolkata")), "close_time": None, "close": 104},
    ]

    candles = service._collect_unprocessed_5m_candles(
        recent_candles,
        datetime(2026, 5, 14, 11, 36, tzinfo=ZoneInfo("Asia/Kolkata")),
    )

    assert [candle["time"].minute for candle in candles] == [20, 25, 30]


def test_handle_runtime_gap_recovery_preserves_recent_pending_watch():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.time_utils = type(
        "TimeUtilsStub",
        (),
        {"now_ist": staticmethod(lambda: datetime(2026, 5, 14, 11, 36, tzinfo=ZoneInfo("Asia/Kolkata")))},
    )()
    service._log = lambda *_args, **_kwargs: None
    service._restore_indicator_state = lambda: None
    service.last_processed_5m_ts = datetime(2026, 5, 14, 11, 15, tzinfo=ZoneInfo("Asia/Kolkata"))
    service.data_pause_active = False
    service.last_data_pause_reason = None
    service.last_monitor_check_minute = datetime(2026, 5, 14, 11, 35, tzinfo=ZoneInfo("Asia/Kolkata"))
    service.active_trade_monitor = None
    service.pending_entry_watch = {
        "created_at": datetime(2026, 5, 14, 11, 24, tzinfo=ZoneInfo("Asia/Kolkata")),
        "direction": "CE",
        "signal_type": "BREAKOUT_CONFIRM",
        "last_checked_minute": datetime(2026, 5, 14, 11, 24, tzinfo=ZoneInfo("Asia/Kolkata")),
    }

    service._handle_runtime_gap_recovery("system sleep/wake")

    assert service.last_processed_5m_ts == datetime(2026, 5, 14, 11, 15, tzinfo=ZoneInfo("Asia/Kolkata"))
    assert service.pending_entry_watch is not None
    assert service.pending_entry_watch["last_checked_minute"] is None


def test_microstructure_allows_strict_option_sweep_override_when_oi_data_missing():
    service = SignalService.__new__(SignalService)
    service.option_data = {"band_snapshots": [{"strike": 24200}]}
    service.option_sweep_context = {
        "direction": "CE",
        "quality": "STRONG",
        "micro_confirmed": True,
        "persistence_pairs": 5,
    }
    service.strategy = type(
        "StrategyStub",
        (),
        {
            "last_entry_score": 92,
            "last_signal_type": "BREAKOUT_CONFIRM",
            "last_pressure_conflict_level": "NONE",
        },
    )()
    service._prime_oi_quote_confirmation = lambda *args, **kwargs: None
    service.spread_filter = type(
        "SpreadStub",
        (),
        {"should_filter_signal": lambda *args, **kwargs: (False, None, None)},
    )()
    service.oi_quote_confirmation = type(
        "QuoteStub",
        (),
        {
            "confirm_signal_with_oi": lambda *args, **kwargs: (
                False,
                "Insufficient OI data",
                None,
                {"raw_confidence": 0.0},
            )
        },
    )()

    confirmed, reason, metrics, alternative = service._confirm_signal_microstructure(
        signal="CE",
        selected_strike=24200,
        timestamp=datetime(2026, 5, 6, 14, 25),
        price=24220,
        strict=True,
    )

    assert confirmed is True
    assert reason == "Option sweep microstructure override"
    assert metrics["override"] == "option_sweep_microstructure"
    assert alternative is None


def test_strong_option_sweep_softens_call_wall_heavy_guard():
    service = SignalService.__new__(SignalService)
    service.option_sweep_context = {
        "direction": "CE",
        "quality": "STRONG",
        "micro_confirmed": True,
        "persistence_pairs": 4,
    }
    service.strategy = type(
        "StrategyStub",
        (),
        {
            "last_signal_type": "BREAKOUT_CONFIRM",
            "last_entry_score": 92,
            "last_score": 92,
            "last_pressure_conflict_level": "MILD",
        },
    )()
    service.profile = {"strike_step": 50}

    guard = service._evaluate_oi_wall_guard(
        signal="CE",
        price=24202,
        oi_ladder_data={
            "support": 24100,
            "resistance": 24200,
            "support_wall_state": "STABLE",
            "resistance_wall_state": "WEAKENING",
            "support_strength": 100.0,
            "resistance_strength": 140.0,
        },
        pressure_metrics={
            "pressure_bias": "NEUTRAL",
            "call_wall_strength_ratio": 1.4,
            "put_wall_strength_ratio": 1.0,
        },
    )

    assert guard is None


def test_strong_option_sweep_softens_sleepy_premium_guard():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.option_sweep_context = {
        "direction": "CE",
        "quality": "STRONG",
        "micro_confirmed": True,
        "persistence_pairs": 5,
    }
    service.strategy = type(
        "StrategyStub",
        (),
        {
            "last_signal_type": "BREAKOUT_CONFIRM",
            "last_entry_score": 91,
            "last_score": 91,
            "last_pressure_conflict_level": "NONE",
            "last_regime": "CHOPPY",
        },
    )()
    service.db_reader = type(
        "ReaderStub",
        (),
        {
            "fetch_option_contract_snapshot": lambda *args, **kwargs: {"ltp": 100.0, "volume": 1000},
        },
    )()
    service.option_data = {"atm": 24200}
    service._get_option_contract_snapshot = lambda *args, **kwargs: {"iv": 18.0}

    guard = service._evaluate_premium_quality_guard(
        signal="CE",
        selected_option_contract={"strike": 24200, "ltp": 100.4, "volume": 1000, "spread": 2.0, "iv": 18.5},
        candle_time=datetime(2026, 5, 6, 14, 20),
    )

    assert guard["label"] == "PREMIUM_OK"
    assert "soften" in guard["reason"].lower()


def test_breakout_confirm_b_grade_allowed_when_score_is_high():
    service = build_service("BREAKOUT_CONFIRM", "B", "MEDIUM")
    service.strategy.last_score = 80
    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 11, 35)) is True


def test_build_option_sweep_context_detects_broad_bullish_sweep():
    service = SignalService.__new__(SignalService)
    service.instrument = "SENSEX"
    service.profile = {"strike_step": 100}

    base_ts = datetime(2026, 5, 6, 14, 15)
    prev_ts = base_ts + timedelta(minutes=5, seconds=53)
    latest_ts = base_ts + timedelta(minutes=10, seconds=54)

    def make_snapshot(ts, shift, call_base, put_base):
        rows = []
        atm = 77500
        for idx, strike in enumerate(range(77000, 78100, 100)):
            distance = int((strike - atm) / 100)
            rows.append(
                {
                    "ts": ts,
                    "atm_strike": atm,
                    "strike": strike,
                    "distance_from_atm": distance,
                    "option_type": "CE",
                    "oi": 1000000 - (shift * 10000) - (idx * 1000),
                    "volume": call_base + shift + (idx * 100000),
                    "ltp": 200 + (idx * 15) + (shift / 10000),
                }
            )
            rows.append(
                {
                    "ts": ts,
                    "atm_strike": atm,
                    "strike": strike,
                    "distance_from_atm": distance,
                    "option_type": "PE",
                    "oi": 800000 + (shift * 5000) + (idx * 500),
                    "volume": put_base + shift + (idx * 60000),
                    "ltp": 500 - (idx * 18) - (shift / 12000),
                }
            )
        return rows

    older = make_snapshot(base_ts, 0, 1000000, 900000)
    previous = make_snapshot(prev_ts, 1200000, 3000000, 2400000)
    latest = make_snapshot(latest_ts, 3200000, 5200000, 4700000)

    class ReaderStub:
        def fetch_recent_option_band_snapshots(self, instrument, before_ts=None, limit=3):
            return [older, previous, latest]

        def fetch_recent_candles_1m(self, instrument, limit=4, before_ts=None):
            return [
                {"time": base_ts + timedelta(minutes=8), "close": 77580.0},
                {"time": base_ts + timedelta(minutes=9), "close": 77660.0},
                {"time": base_ts + timedelta(minutes=10), "close": 77740.0},
                {"time": base_ts + timedelta(minutes=11), "close": 77820.0},
            ]

    service.db_reader = ReaderStub()

    ctx = service._build_option_sweep_context(
        candle_time=base_ts + timedelta(minutes=5),
        price=77635.74,
        atr=133.86,
        recent_candles_5m=[
            {"time": base_ts, "close": 77259.07},
            {"time": base_ts + timedelta(minutes=5), "close": 77635.74},
        ],
    )

    assert ctx is not None
    assert ctx["direction"] == "CE"
    assert ctx["quality"] == "STRONG"
    assert ctx["pair_support"] >= 7
    assert ctx["micro_confirmed"] is True
    assert ctx["persistence_pairs"] >= 3
    assert ctx["trigger_ready"] is True


def test_option_spike_detector_detects_broad_bullish_one_minute_spike():
    detector = OptionSpikeDetector(strike_step=100)
    base_ts = datetime(2026, 5, 6, 14, 19)
    previous_ts = datetime(2026, 5, 6, 14, 20)

    def make_snapshot(ts, call_shift, put_shift, call_volume, put_volume):
        rows = []
        atm = 77500
        for idx, strike in enumerate(range(77000, 78100, 100)):
            distance = int((strike - atm) / 100)
            rows.append(
                {
                    "ts": ts,
                    "atm_strike": atm,
                    "strike": strike,
                    "distance_from_atm": distance,
                    "option_type": "CE",
                    "volume": call_volume + (idx * 10000),
                    "ltp": 120 + (idx * 8) + call_shift,
                }
            )
            rows.append(
                {
                    "ts": ts,
                    "atm_strike": atm,
                    "strike": strike,
                    "distance_from_atm": distance,
                    "option_type": "PE",
                    "volume": put_volume + (idx * 8000),
                    "ltp": 220 - (idx * 7) - put_shift,
                }
            )
        return rows

    previous = make_snapshot(base_ts, 0, 0, 100000, 90000)
    latest = make_snapshot(previous_ts, 18, 12, 260000, 180000)
    recent_1m = [
        {"time": datetime(2026, 5, 6, 14, 18), "open": 77190, "high": 77205, "low": 77185, "close": 77200, "volume": 1000},
        {"time": datetime(2026, 5, 6, 14, 19), "open": 77200, "high": 77215, "low": 77198, "close": 77208, "volume": 1100},
        {"time": datetime(2026, 5, 6, 14, 20), "open": 77208, "high": 77310, "low": 77205, "close": 77300, "volume": 1800},
    ]

    ctx = detector.detect(
        recent_1m_candles=recent_1m,
        snapshot_groups=[previous, latest],
        recent_candles_5m=[
            {"time": datetime(2026, 5, 6, 14, 5), "open": 77120.0, "high": 77180.0, "low": 77100.0, "close": 77175.0},
            {"time": datetime(2026, 5, 6, 14, 10), "open": 77175.0, "high": 77220.0, "low": 77160.0, "close": 77205.0},
            {"time": datetime(2026, 5, 6, 14, 15), "open": 77205.0, "high": 77280.0, "low": 77200.0, "close": 77259.0},
            {"time": datetime(2026, 5, 6, 14, 20), "open": 77259.0, "high": 77340.0, "low": 77250.0, "close": 77310.0},
        ],
    )

    assert ctx is not None
    assert ctx["direction"] == "CE"
    assert ctx["price_breadth"] >= 7
    assert ctx["volume_breadth"] >= 6
    assert ctx["trigger_price"] == 77310.0
    assert ctx["stage"] == "15M_STRUCTURE_5M_WATCH_1M_ACTIVE"


def test_build_option_spike_watch_payload_creates_pending_early_watch():
    service = SignalService.__new__(SignalService)
    service.instrument = "SENSEX"
    service.option_spike_detector = OptionSpikeDetector(strike_step=100)
    service.db_reader = type(
        "ReaderStub",
        (),
        {
            "fetch_recent_option_band_snapshots": lambda *args, **kwargs: [
                [
                    {"ts": datetime(2026, 5, 6, 14, 19), "atm_strike": 77500, "strike": strike, "distance_from_atm": int((strike - 77500) / 100), "option_type": opt, "volume": 100000, "ltp": (120 + idx * 8) if opt == "CE" else (220 - idx * 7)}
                    for idx, strike in enumerate(range(77000, 78100, 100))
                    for opt in ("CE", "PE")
                ],
                [
                    {"ts": datetime(2026, 5, 6, 14, 20), "atm_strike": 77500, "strike": strike, "distance_from_atm": int((strike - 77500) / 100), "option_type": opt, "volume": 260000 if opt == "CE" else 180000, "ltp": (138 + idx * 8) if opt == "CE" else (208 - idx * 7)}
                    for idx, strike in enumerate(range(77000, 78100, 100))
                    for opt in ("CE", "PE")
                ],
            ]
        },
    )()
    service._build_journal_note = lambda *args, **kwargs: "spike watch"

    recent_1m = [
        {"time": datetime(2026, 5, 6, 14, 18), "open": 77190, "high": 77205, "low": 77185, "close": 77200, "volume": 1000},
        {"time": datetime(2026, 5, 6, 14, 19), "open": 77200, "high": 77215, "low": 77198, "close": 77208, "volume": 1100},
        {"time": datetime(2026, 5, 6, 14, 20), "open": 77208, "high": 77310, "low": 77205, "close": 77300, "volume": 1800},
    ]

    service.db_reader.fetch_recent_candles_5m = lambda instrument, limit=9: [
        {"time": datetime(2026, 5, 6, 14, 5), "open": 77120.0, "high": 77180.0, "low": 77100.0, "close": 77175.0},
        {"time": datetime(2026, 5, 6, 14, 10), "open": 77175.0, "high": 77220.0, "low": 77160.0, "close": 77205.0},
        {"time": datetime(2026, 5, 6, 14, 15), "open": 77205.0, "high": 77280.0, "low": 77200.0, "close": 77259.0},
        {"time": datetime(2026, 5, 6, 14, 20), "open": 77259.0, "high": 77340.0, "low": 77250.0, "close": 77310.0},
    ]
    service._build_oi_ladder_context = lambda price: {"support": 77200, "resistance": 77500}
    payload = service._build_option_spike_watch_payload(recent_1m)

    assert payload is not None
    assert payload["direction"] == "CE"
    assert payload["watch_bucket"] == "WATCH_CONFIRMATION_PENDING"
    assert payload["decision_label"] == "WATCH_CE_SPIKE"
    assert payload["spike_context"]["quality"] == "STRONG"
    assert "15m" in payload["context"].lower()


def test_banknifty_style_spike_is_classified_strong_with_broad_option_volume():
    detector = OptionSpikeDetector(strike_step=100)
    previous = []
    latest = []
    atm = 55200
    ce_deltas = [24000, 31000, 70200, 67230, 38490, 52000, 61000, 43000, 36000, 28000, 22000]
    pe_deltas = [18000, 22000, 45300, 55020, 42000, 30000, 28000, 25000, 22000, 18000, 15000]
    for idx, strike in enumerate(range(54700, 55800, 100)):
        distance = int((strike - atm) / 100)
        previous.extend([
            {"ts": datetime(2026, 5, 6, 14, 21), "atm_strike": atm, "strike": strike, "distance_from_atm": distance, "option_type": "CE", "volume": 100000, "ltp": 100 + idx * 10},
            {"ts": datetime(2026, 5, 6, 14, 21), "atm_strike": atm, "strike": strike, "distance_from_atm": distance, "option_type": "PE", "volume": 90000, "ltp": 220 - idx * 9},
        ])
        latest.extend([
            {"ts": datetime(2026, 5, 6, 14, 22), "atm_strike": atm, "strike": strike, "distance_from_atm": distance, "option_type": "CE", "volume": 100000 + ce_deltas[idx], "ltp": 128 + idx * 10},
            {"ts": datetime(2026, 5, 6, 14, 22), "atm_strike": atm, "strike": strike, "distance_from_atm": distance, "option_type": "PE", "volume": 90000 + pe_deltas[idx], "ltp": 205 - idx * 9},
        ])

    recent_1m = [
        {"time": datetime(2026, 5, 6, 14, 19), "open": 55135.2, "high": 55357.15, "low": 55128.55, "close": 55307.5, "volume": 27120},
        {"time": datetime(2026, 5, 6, 14, 20), "open": 55310.0, "high": 55409.1, "low": 55309.35, "close": 55390.9, "volume": 27120},
        {"time": datetime(2026, 5, 6, 14, 21), "open": 55393.8, "high": 55510.35, "low": 55381.1, "close": 55500.65, "volume": 27120},
    ]

    ctx = detector.detect(recent_1m, [previous, latest])

    assert ctx is not None
    assert ctx["quality"] == "STRONG"


def test_spike_watch_can_trigger_even_if_microstructure_data_is_unavailable():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.pending_entry_watch = {
        "instrument": "NIFTY",
        "direction": "CE",
        "trigger_price": 24220.0,
        "invalidate_price": 24200.0,
        "first_target_price": 24260.0,
        "score": 86,
        "entry_score": 80,
        "confidence": "HIGH",
        "signal_type": "BREAKOUT_CONFIRM",
        "signal_grade": "A",
        "watch_bucket": "WATCH_CONFIRMATION_PENDING",
        "quality": "A",
        "time_regime": "INTRAMINUTE_SPIKE",
        "created_at": datetime(2026, 5, 6, 14, 22),
        "last_checked_minute": None,
        "reason": "1m option spike watch",
        "fast_track_ready": True,
        "strong_watch_setup": True,
        "hybrid_mode": True,
        "cautions": ["one_minute_spike_watch"],
        "blockers": [],
        "pressure_conflict_level": "NONE",
        "spike_origin": True,
        "spike_context": {"quality": "STRONG", "price_breadth": 7, "volume_breadth": 7},
    }
    service.option_data = {"atm": 24200}
    service.strike_selector = type(
        "StrikeSelectorStub",
        (),
        {"select_strike_with_reason": lambda *args, **kwargs: (24200, "test")},
    )()
    service._pending_watch_risk_reward_ok = lambda pending: True
    service._confirm_signal_microstructure = lambda **kwargs: (False, "Microstructure data unavailable", None, None)

    recent_1m = [
        {"time": datetime(2026, 5, 6, 14, 22), "open": 24205.0, "high": 24225.0, "low": 24203.0, "close": 24218.0, "volume": 1000},
        {"time": datetime(2026, 5, 6, 14, 23), "open": 24218.0, "high": 24242.0, "low": 24216.0, "close": 24234.0, "volume": 1300},
    ]

    result = service._evaluate_pending_entry_watch(recent_1m)

    assert result is not None
    assert result["status"] == "TRIGGERED"


def test_elite_manual_watch_can_trigger_on_clean_reclaim_without_extra_buffer():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.pending_entry_watch = {
        "instrument": "NIFTY",
        "direction": "CE",
        "trigger_price": 24220.0,
        "invalidate_price": 24198.0,
        "first_target_price": 24270.0,
        "score": 84,
        "entry_score": 78,
        "confidence": "HIGH",
        "signal_type": "BREAKOUT_CONFIRM",
        "signal_grade": "A",
        "watch_bucket": "WATCH_CONFIRMATION_PENDING",
        "quality": "A",
        "time_regime": "MID_MORNING",
        "created_at": datetime(2026, 5, 6, 14, 22),
        "last_checked_minute": None,
        "reason": "manual breakout confirm watch",
        "fast_track_ready": False,
        "strong_watch_setup": True,
        "elite_watch_ready": True,
        "hybrid_mode": False,
        "cautions": [],
        "blockers": [],
        "pressure_conflict_level": "NONE",
    }
    service.option_data = {"atm": 24200}
    service.strike_selector = type(
        "StrikeSelectorStub",
        (),
        {"select_strike_with_reason": lambda *args, **kwargs: (24200, "test")},
    )()
    service._pending_watch_risk_reward_ok = lambda pending: True
    service._confirm_signal_microstructure = lambda **kwargs: (True, "micro confirmed", None, None)

    recent_1m = [
        {"time": datetime(2026, 5, 6, 14, 22), "open": 24210.0, "high": 24219.0, "low": 24206.0, "close": 24214.0, "volume": 900},
        {"time": datetime(2026, 5, 6, 14, 23), "open": 24214.0, "high": 24224.0, "low": 24212.0, "close": 24221.0, "volume": 760},
    ]

    result = service._evaluate_pending_entry_watch(recent_1m)

    assert result is not None
    assert result["status"] == "TRIGGERED"


def test_entry_decision_1m_audit_row_is_written_for_wait_state():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.option_data_source = "CACHE"
    rows = []
    service.db_writer = type(
        "WriterStub",
        (),
        {"insert_entry_decision_1m": lambda self, row: rows.append(row)},
    )()
    service._log = lambda message: None

    pending = {
        "created_at": datetime(2026, 5, 6, 14, 22),
        "direction": "CE",
        "trigger_price": 24220.0,
        "invalidate_price": 24198.0,
        "first_target_price": 24270.0,
        "score": 84,
        "entry_score": 78,
        "signal_type": "BREAKOUT_CONFIRM",
        "signal_grade": "A",
        "confidence": "HIGH",
        "watch_bucket": "WATCH_CONFIRMATION_PENDING",
        "time_regime": "MID_MORNING",
        "minutes_since_watch": 1,
        "reason": "manual breakout confirm watch",
        "blockers": [],
        "cautions": ["one_minute_spike_watch"],
    }
    latest_1m = {
        "time": datetime(2026, 5, 6, 14, 23),
        "open": 24214.0,
        "high": 24218.0,
        "low": 24212.0,
        "close": 24216.0,
        "volume": 760,
    }

    service._safe_save_entry_decision_1m(
        ts=latest_1m["time"],
        pending=pending,
        decision="WAIT",
        latest_1m=latest_1m,
        reason="Waiting for 1m trigger confirmation",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row[1] == "NIFTY"
    assert row[3] == "CE"
    assert row[4] == "WAIT"
    assert row[11] == 24220.0
    assert row[20] == 78
    assert row[27] >= 0
    assert row[28] in {"ACTION", "READY", "WAIT", "AVOID"}
    assert row[30] == []
    assert row[31] == ["one_minute_spike_watch"]
    assert row[32] == "CACHE"


def test_watch_alert_eligibility_allows_only_strong_spike_watch():
    service = SignalService.__new__(SignalService)

    strong_payload = {
        "score": 86,
        "entry_score": 80,
        "confidence": "HIGH",
        "signal_grade": "A",
        "watch_bucket": "WATCH_CONFIRMATION_PENDING",
        "setup": "BREAKOUT_CONFIRM",
        "blockers": [],
        "cautions": ["one_minute_spike_watch"],
        "pressure_conflict_level": "NONE",
        "spike_context": {"quality": "STRONG", "price_breadth": 8, "volume_breadth": 8},
    }
    weak_payload = {
        **strong_payload,
        "signal_grade": "B",
        "spike_context": {"quality": "MODERATE", "price_breadth": 6, "volume_breadth": 6},
    }

    assert service._watch_alert_is_eligible(strong_payload) is True
    assert service._watch_alert_is_eligible(weak_payload) is False


def test_preserve_existing_pending_watch_handles_mixed_tz_datetimes():
    service = SignalService.__new__(SignalService)
    service.pending_entry_watch = {
        "created_at": datetime(2026, 5, 12, 9, 56),
        "signal_type": "BREAKOUT_CONFIRM",
    }
    service._pending_watch_max_minutes = lambda pending: 10

    result = service._preserve_existing_pending_watch(
        {"time": datetime(2026, 5, 12, 10, 1, tzinfo=ZoneInfo("Asia/Kolkata"))}
    )

    assert result is True


def test_evaluate_pending_entry_watch_handles_aware_recent_candles():
    service = SignalService.__new__(SignalService)
    service.instrument = "SENSEX"
    service.pending_entry_watch = {
        "instrument": "SENSEX",
        "direction": "CE",
        "trigger_price": 75398.53,
        "invalidate_price": 75354.21,
        "first_target_price": 75487.17,
        "score": 86,
        "entry_score": 80,
        "confidence": "HIGH",
        "signal_type": "BREAKOUT_CONFIRM",
        "signal_grade": "A",
        "watch_bucket": "WATCH_CONFIRMATION_PENDING",
        "quality": "A",
        "time_regime": "INTRAMINUTE_SPIKE",
        "created_at": datetime(2026, 5, 12, 9, 56),
        "last_checked_minute": None,
        "reason": "1m option spike watch",
        "fast_track_ready": True,
        "strong_watch_setup": True,
        "hybrid_mode": True,
        "cautions": ["one_minute_spike_watch"],
        "blockers": [],
        "pressure_conflict_level": "NONE",
        "spike_origin": True,
        "spike_context": {"quality": "STRONG", "price_breadth": 11, "volume_breadth": 11},
    }
    service.option_data = {"atm": 75400}
    service.strike_selector = type(
        "StrikeSelectorStub",
        (),
        {"select_strike_with_reason": lambda *args, **kwargs: (75400, "test")},
    )()
    service._pending_watch_risk_reward_ok = lambda pending: True
    service._confirm_signal_microstructure = lambda **kwargs: (False, "Microstructure data unavailable", None, None)

    recent_1m = [
        {
            "time": datetime(2026, 5, 12, 9, 56, tzinfo=ZoneInfo("Asia/Kolkata")),
            "open": 75380.0,
            "high": 75400.0,
            "low": 75378.0,
            "close": 75397.0,
            "volume": 1000,
        },
        {
            "time": datetime(2026, 5, 12, 9, 57, tzinfo=ZoneInfo("Asia/Kolkata")),
            "open": 75397.0,
            "high": 75415.0,
            "low": 75396.0,
            "close": 75405.0,
            "volume": 1400,
        },
    ]

    result = service._evaluate_pending_entry_watch(recent_1m)

    assert result is not None
    assert result["status"] == "TRIGGERED"


def test_watch_alert_eligibility_filters_weak_manual_setup_watch():
    service = SignalService.__new__(SignalService)

    weak_manual = {
        "score": 79,
        "entry_score": 73,
        "confidence": "HIGH",
        "signal_grade": "A",
        "watch_bucket": "WATCH_SETUP",
        "setup": "BREAKOUT_CONFIRM",
        "blockers": [],
        "cautions": [],
        "pressure_conflict_level": "NONE",
    }
    clean_manual = {
        **weak_manual,
        "score": 84,
        "entry_score": 78,
    }

    assert service._watch_alert_is_eligible(weak_manual) is False
    assert service._watch_alert_is_eligible(clean_manual) is True


def test_infer_flip_context_marks_pe_failure_for_ce_reversal():
    context = SignalService._infer_flip_context(
        direction="CE",
        setup="REVERSAL",
        candidate_reason="Trap reversal reclaim above 77000 | score=82",
    )

    assert context == {"buy_side": "CE", "failed_side": "PE", "decision_label": "WATCH_CE_FLIP"}


def test_infer_flip_context_none_for_plain_continuation():
    context = SignalService._infer_flip_context(
        direction="CE",
        setup="CONTINUATION",
        candidate_reason="High-score bullish continuation | score=78",
    )

    assert context is None


def test_oi_quote_confirmation_uses_quantities_for_liquidity():
    confirmer = OIQuoteConfirmation()
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    confirmer.add_quote_snapshot(
        now,
        {
            "ce_spread": 2.5,
            "pe_spread": 2.0,
            "ce_top_bid_quantity": 120,
            "ce_top_ask_quantity": 150,
            "pe_top_bid_quantity": 110,
            "pe_top_ask_quantity": 140,
            "atm": 77000,
            "underlying_price": 77020,
        },
    )

    confirmed, reason, metrics = confirmer.check_liquidity_confirmation("CE")

    assert confirmed is True
    assert reason == "Liquidity confirmed"
    assert metrics["ce_liquidity"] is True


def test_option_expansion_metrics_flags_supportive_premium():
    metrics = SignalService._option_expansion_metrics(
        entry_option_price=100,
        option_price=108,
        entry_underlying_price=24000,
        underlying_price=24012,
        entry_delta=0.65,
    )

    assert metrics["expected_option_move"] == 7.8
    assert metrics["expansion_ratio"] == 1.03
    assert metrics["premium_supportive"] is True


def test_no_trade_zone_marks_high_noise_skip():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.strategy = type(
        "StrategyStub",
        (),
        {
            "last_cautions": ["participation_weak", "pressure_conflict"],
            "last_pressure_conflict_level": "MODERATE",
            "last_entry_score": 68,
            "last_score": 68,
            "last_signal_type": "BREAKOUT_CONFIRM",
            "last_regime": "TRENDING",
        },
    )()

    zone = service._classify_no_trade_zone(
        balanced_pro={"time_regime": "MID_MORNING", "setup": "BREAKOUT_CONFIRM"},
        signal="CE",
        selected_option_contract={"ltp": 100, "spread": 2.0},
    )

    assert zone["label"] == "HIGH_NOISE_SKIP"


def test_no_trade_zone_allows_single_soft_noise_flag_for_elite_breakout():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.last_data_health = {"label": "GOOD"}
    service.strategy = type(
        "StrategyStub",
        (),
        {
            "last_cautions": ["adx_not_confirmed"],
            "last_pressure_conflict_level": "NONE",
            "last_entry_score": 90,
            "last_score": 90,
            "last_signal_type": "BREAKOUT_CONFIRM",
            "last_regime": "TRENDING",
            "last_confidence": "HIGH",
        },
    )()

    zone = service._classify_no_trade_zone(
        balanced_pro={"time_regime": "MID_MORNING", "setup": "BREAKOUT_CONFIRM"},
        signal="CE",
        selected_option_contract={"ltp": 100, "spread": 1.0},
    )

    assert zone is None


def test_no_trade_zone_allows_clean_high_confidence_midday_breakout_even_in_chop():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.last_data_health = {"label": "GOOD"}
    service.strategy = type(
        "StrategyStub",
        (),
        {
            "last_cautions": ["adx_not_confirmed"],
            "last_pressure_conflict_level": "NONE",
            "last_entry_score": 81,
            "last_score": 81,
            "last_signal_type": "BREAKOUT_CONFIRM",
            "last_regime": "CHOPPY",
            "last_confidence": "HIGH",
        },
    )()

    zone = service._classify_no_trade_zone(
        balanced_pro={"time_regime": "MIDDAY", "setup": "BREAKOUT_CONFIRM"},
        signal="PE",
        selected_option_contract={"ltp": 100, "spread": 1.2},
    )

    assert zone is None


def test_feed_reject_softens_for_elite_aligned_setup_when_only_oi_health_is_weak():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.option_sweep_context = {}
    service.last_data_health = None
    service._should_soften_option_sweep_filters = lambda _signal: False
    service.strategy = type(
        "StrategyStub",
        (),
        {
            "last_score": 92,
            "last_entry_score": 91,
            "last_confidence": "HIGH",
            "last_signal_type": "BREAKOUT_CONFIRM",
            "last_active_day_state": "BULL_TREND_ACTIVE",
            "last_day_state_direction": "CE",
            "last_pressure_conflict_level": "NONE",
        },
    )()

    feed_health = {
        "label": "REJECT",
        "summary": "feed=REJECT",
        "reasons": ["coverage_too_low", "large_gap_detected"],
        "candle_health": {"coverage_pct": 94.0, "count": 12, "expected_count": 13, "max_gap_seconds": 600},
        "oi_health": {"coverage_pct": 28.0, "distinct_minutes": 17, "expected_minutes": 61, "max_gap_seconds": 1080},
    }

    from services.signal_service_support.option_signal_guard import OptionSignalGuard

    result = OptionSignalGuard.maybe_relax_reject_for_strong_setup(service, feed_health, signal="CE")

    assert result["label"] == "RISKY"
    assert "strong_setup_oi_softened" in result["reasons"]
    assert service.last_data_health["label"] == "RISKY"


def test_feed_reject_stays_reject_for_non_elite_setup():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.option_sweep_context = {}
    service.last_data_health = None
    service._should_soften_option_sweep_filters = lambda _signal: False
    service.strategy = type(
        "StrategyStub",
        (),
        {
            "last_score": 74,
            "last_entry_score": 72,
            "last_confidence": "MEDIUM",
            "last_signal_type": "BREAKOUT_CONFIRM",
            "last_active_day_state": "BULL_TREND_ACTIVE",
            "last_day_state_direction": "CE",
            "last_pressure_conflict_level": "NONE",
        },
    )()
    feed_health = {
        "label": "REJECT",
        "summary": "feed=REJECT",
        "reasons": ["coverage_too_low", "large_gap_detected"],
        "candle_health": {"coverage_pct": 94.0, "count": 12, "expected_count": 13, "max_gap_seconds": 600},
        "oi_health": {"coverage_pct": 28.0, "distinct_minutes": 17, "expected_minutes": 61, "max_gap_seconds": 1080},
    }

    from services.signal_service_support.option_signal_guard import OptionSignalGuard

    result = OptionSignalGuard.maybe_relax_reject_for_strong_setup(service, feed_health, signal="CE")

    assert result["label"] == "REJECT"


def test_structure_suggestion_prefers_bull_call_spread_in_expiry_noise():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.profile = {"strike_step": 50}
    service.option_data = {"atm": 24000}
    service.strategy = type("StrategyStub", (), {"last_cautions": ["expiry_day_mode", "expiry_fast_decay"]})()
    service._get_option_contract_snapshot = lambda strike, signal: {"iv": 22.0} if strike == 24000 else None

    suggestion = service._build_option_structure_suggestion(
        signal="CE",
        selected_strike=24000,
        selected_option_contract={"strike": 24000, "ltp": 100.0, "spread": 4.0, "iv": 25.0},
        balanced_pro={"time_regime": "MIDDAY"},
        risk_profile={"target_pct": 20.0},
    )

    assert suggestion["type"] == "BULL_CALL_SPREAD"
    assert suggestion["long_strike"] == 24000
    assert suggestion["short_strike"] == 24050
    assert suggestion["width_steps"] == 1


def test_structure_suggestion_uses_wider_spread_when_target_move_is_bigger():
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.profile = {"strike_step": 50}
    service.option_data = {"atm": 24000}
    service.strategy = type(
        "StrategyStub",
        (),
        {
            "last_cautions": [],
            "last_entry_plan": {"entry_above": 24010.0, "first_target_price": 24135.0},
        },
    )()
    service._get_option_contract_snapshot = lambda strike, signal: {"iv": 22.0} if strike == 24000 else None

    suggestion = service._build_option_structure_suggestion(
        signal="CE",
        selected_strike=24000,
        selected_option_contract={"strike": 24000, "ltp": 100.0, "spread": 1.0, "iv": 22.5},
        balanced_pro={"time_regime": "MID_MORNING"},
        risk_profile={"target_pct": 30.0},
    )

    assert suggestion["type"] == "BULL_CALL_SPREAD"
    assert suggestion["short_strike"] == 24100
    assert suggestion["width_steps"] == 2
