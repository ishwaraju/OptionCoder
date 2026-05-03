from services.signal_service import SignalService
from datetime import datetime
from shared.market.oi_quote_confirmation import OIQuoteConfirmation
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


def test_banknifty_allows_b_grade_breakout_after_11_am():
    service = build_service("BREAKOUT", "B", "MEDIUM")
    service.instrument = "BANKNIFTY"
    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 11, 10)) is True


def test_banknifty_blocks_b_grade_breakout_before_11_am():
    service = build_service("BREAKOUT", "B", "MEDIUM")
    service.instrument = "BANKNIFTY"
    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 10, 25)) is False


def test_sensex_allows_a_grade_breakout_confirm():
    service = build_service("BREAKOUT_CONFIRM", "A", "MEDIUM")
    service.instrument = "SENSEX"
    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 11, 5)) is True


def test_sensex_blocks_b_grade_breakout():
    service = build_service("BREAKOUT", "B", "MEDIUM")
    service.instrument = "SENSEX"
    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 10, 25)) is False


def test_breakout_confirm_b_grade_allowed_when_score_is_high():
    service = build_service("BREAKOUT_CONFIRM", "B", "MEDIUM")
    service.strategy.last_score = 80
    assert service._is_option_buyer_actionable("PE", candle_time=datetime(2026, 4, 16, 11, 35)) is True


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
