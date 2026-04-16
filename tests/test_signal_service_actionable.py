from services.signal_service import SignalService
from datetime import datetime


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
