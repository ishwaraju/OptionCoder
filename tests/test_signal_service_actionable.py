from services.signal_service import SignalService
from datetime import datetime, timedelta
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


def test_option_buyer_actionable_allows_elite_reversal_without_conflict():
    service = build_service("REVERSAL", "A", "HIGH")
    service.strategy.last_score = 92
    service.strategy.last_pressure_conflict_level = "NONE"
    assert service._is_option_buyer_actionable("CE") is True


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
        for idx, strike in enumerate(range(77200, 77900, 100)):
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
    assert ctx["pair_support"] >= 5
    assert ctx["micro_confirmed"] is True
    assert ctx["persistence_pairs"] >= 3
    assert ctx["trigger_ready"] is True


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
