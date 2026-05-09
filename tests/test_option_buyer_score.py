from datetime import datetime

from services.signal_service import SignalService
from strategies.shared.option_buyer_score import calculate_option_buyer_entry_score, option_buyer_action


def test_option_buyer_entry_score_rewards_clean_expanding_liquid_setup():
    score = calculate_option_buyer_entry_score(
        base_entry_score=78,
        strategy_score=84,
        momentum_score=82,
        premium_state="EXPANDING",
        liquidity_quality="GOOD",
        spread_percent=1.4,
        blockers=[],
        cautions=[],
        confidence="HIGH",
        signal_grade="A",
    )

    assert score >= 80
    assert option_buyer_action(score) == "ACTION"


def test_option_buyer_entry_score_penalizes_fading_illiquid_setup():
    score = calculate_option_buyer_entry_score(
        base_entry_score=76,
        strategy_score=82,
        momentum_score=58,
        premium_state="FADING",
        liquidity_quality="POOR",
        spread_percent=8.0,
        blockers=["microstructure"],
        cautions=["opposite_pressure"],
        confidence="LOW",
        signal_grade="B",
    )

    assert score < 55
    assert option_buyer_action(score) == "AVOID"


def test_signal_service_saves_fixed_horizon_option_outcome():
    rows = []
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.active_trade_monitor = {
        "entry_time": datetime(2026, 5, 6, 14, 23),
        "entry_underlying_price": 24234.0,
        "strike": 24200,
    }
    service.db_writer = type(
        "WriterStub",
        (),
        {"insert_option_signal_horizon_outcome": lambda self, row: rows.append(row)},
    )()
    service._log = lambda message: None

    service._safe_save_option_signal_horizon_outcome(
        datetime(2026, 5, 6, 14, 26),
        {
            "signal": "CE",
            "price": 24252.0,
            "entry_price": 100.0,
            "option_price": 116.0,
            "pnl_points": 16.0,
            "max_favorable_ltp": 121.0,
            "max_adverse_ltp": 98.0,
        },
        minutes_since_signal=3,
    )

    assert len(rows) == 1
    assert rows[0][1] == 3
    assert rows[0][10] == 16.0
    assert rows[0][11] == 16.0
    assert rows[0][14] == "WIN"
