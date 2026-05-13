from datetime import datetime, timedelta

from services.signal_service import SignalService
from strategies.shared.one_minute_momentum import OneMinuteMomentumQuality


class _StrikeSelectorStub:
    def select_strike_with_reason(self, *args, **kwargs):
        return 24200, "atm test strike"


class _ReaderStub:
    def __init__(self, recent_1m):
        self.recent_1m = recent_1m

    def fetch_recent_candles_1m(self, instrument, limit=6):
        return self.recent_1m[-limit:]


class _WriterStub:
    def __init__(self):
        self.entry_decisions = []

    def insert_entry_decision_1m(self, row):
        self.entry_decisions.append(row)


def _pending_watch(**overrides):
    pending = {
        "instrument": "NIFTY",
        "direction": "CE",
        "trigger_price": 24220.0,
        "invalidate_price": 24200.0,
        "first_target_price": 24260.0,
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
        "elite_watch_ready": False,
        "hybrid_mode": False,
        "cautions": [],
        "blockers": [],
        "pressure_conflict_level": "NONE",
    }
    pending.update(overrides)
    return pending


def _service(recent_1m):
    service = SignalService.__new__(SignalService)
    service.instrument = "NIFTY"
    service.pending_entry_watch = _pending_watch()
    service.option_data = {"atm": 24200}
    service.option_data_source = "TEST"
    service.strike_selector = _StrikeSelectorStub()
    service.db_reader = _ReaderStub(recent_1m)
    service.db_writer = _WriterStub()
    service.notifier = type("NotifierStub", (), {"send_entry_trigger_notification": lambda self, payload: None})()
    service.strategy = type("StrategyStub", (), {"last_entry_plan": {}})()
    service.signals_generated = 0
    service.one_minute_momentum = OneMinuteMomentumQuality(min_score=30)
    service._confirm_signal_microstructure = lambda **kwargs: (True, "micro confirmed", None, None)
    service._get_option_contract_snapshot = lambda *args, **kwargs: {"ltp": 105.0, "top_bid_price": 104.5, "top_ask_price": 105.5, "spread": 1.0}
    service._greek_enriched_option_contract = lambda contract, *args, **kwargs: contract
    service._safe_save_signal_issued = lambda *args, **kwargs: True
    service._run_async_notification = lambda callback, payload: callback(payload)
    service._start_trade_monitor = lambda *args, **kwargs: None
    service._log = lambda message: None
    return service


def test_1m_ce_trigger_fires_and_clears_watch():
    recent_1m = [
        {"time": datetime(2026, 5, 6, 14, 20), "open": 24204.0, "high": 24209.0, "low": 24202.0, "close": 24207.0, "volume": 1000},
        {"time": datetime(2026, 5, 6, 14, 21), "open": 24207.0, "high": 24213.0, "low": 24205.0, "close": 24211.0, "volume": 1100},
        {"time": datetime(2026, 5, 6, 14, 22), "open": 24211.0, "high": 24218.0, "low": 24208.0, "close": 24216.0, "volume": 1200},
        {"time": datetime(2026, 5, 6, 14, 23), "open": 24221.0, "high": 24236.0, "low": 24220.0, "close": 24234.0, "volume": 1500},
    ]
    service = _service(recent_1m)

    service._maybe_fire_pending_entry_watch(latest_5m_candle={"time": datetime(2026, 5, 6, 14, 20)})

    assert service.pending_entry_watch is None
    assert service.signals_generated == 1
    assert service.db_writer.entry_decisions[-1][4] == "TRIGGERED"
    assert service.db_writer.entry_decisions[-1][27] >= 70
    assert service.db_writer.entry_decisions[-1][28] in {"ACTION", "READY"}
    assert "1m momentum" in service.db_writer.entry_decisions[-1][29]


def test_1m_watch_expiry_clears_watch_and_writes_expired_decision():
    expired_minute = datetime(2026, 5, 6, 14, 43)
    recent_1m = [
        {"time": expired_minute - timedelta(minutes=1), "open": 24210.0, "high": 24212.0, "low": 24206.0, "close": 24208.0, "volume": 1000},
        {"time": expired_minute, "open": 24208.0, "high": 24211.0, "low": 24202.0, "close": 24205.0, "volume": 1100},
    ]
    service = _service(recent_1m)

    signal = service._maybe_fire_pending_entry_watch(latest_5m_candle={"time": expired_minute})

    assert signal is None
    assert service.pending_entry_watch is None
    assert service.db_writer.entry_decisions[-1][4] == "EXPIRED"


def test_1m_trigger_is_invalidated_when_momentum_quality_rejects_plain_watch():
    recent_1m = [
        {"time": datetime(2026, 5, 6, 14, 21), "open": 24207.0, "high": 24213.0, "low": 24205.0, "close": 24211.0, "volume": 1100},
        {"time": datetime(2026, 5, 6, 14, 22), "open": 24211.0, "high": 24218.0, "low": 24208.0, "close": 24216.0, "volume": 1200},
        {"time": datetime(2026, 5, 6, 14, 23), "open": 24221.0, "high": 24236.0, "low": 24220.0, "close": 24234.0, "volume": 1500},
    ]
    service = _service(recent_1m)
    service.one_minute_momentum = type(
        "MomentumRejector",
        (),
        {"evaluate": lambda self, **kwargs: {"ok": False, "score": 12, "reason": "1m momentum weak"}},
    )()

    result = service._evaluate_pending_entry_watch(recent_1m)

    assert result == {"status": "INVALIDATED", "reason": "1m momentum weak"}


def test_1m_trigger_still_notifies_when_signal_save_fails_in_parallel_mode():
    recent_1m = [
        {"time": datetime(2026, 5, 6, 14, 20), "open": 24204.0, "high": 24209.0, "low": 24202.0, "close": 24207.0, "volume": 1000},
        {"time": datetime(2026, 5, 6, 14, 21), "open": 24207.0, "high": 24213.0, "low": 24205.0, "close": 24211.0, "volume": 1100},
        {"time": datetime(2026, 5, 6, 14, 22), "open": 24211.0, "high": 24218.0, "low": 24208.0, "close": 24216.0, "volume": 1200},
        {"time": datetime(2026, 5, 6, 14, 23), "open": 24221.0, "high": 24236.0, "low": 24220.0, "close": 24234.0, "volume": 1500},
    ]
    service = _service(recent_1m)
    calls = []
    service.notifier = type(
        "NotifierStub",
        (),
        {"send_entry_trigger_notification": lambda self, payload: calls.append(payload)},
    )()
    service._safe_save_signal_issued = lambda *args, **kwargs: False

    service._maybe_fire_pending_entry_watch(latest_5m_candle={"time": datetime(2026, 5, 6, 14, 20)})

    assert len(calls) == 1


def test_one_minute_momentum_quality_accepts_clean_directional_move():
    checker = OneMinuteMomentumQuality(min_score=30)
    candles = [
        {"time": datetime(2026, 5, 6, 14, 20), "open": 24204.0, "high": 24209.0, "low": 24202.0, "close": 24207.0, "volume": 1000},
        {"time": datetime(2026, 5, 6, 14, 21), "open": 24207.0, "high": 24213.0, "low": 24205.0, "close": 24211.0, "volume": 1100},
        {"time": datetime(2026, 5, 6, 14, 22), "open": 24211.0, "high": 24218.0, "low": 24208.0, "close": 24216.0, "volume": 1200},
        {"time": datetime(2026, 5, 6, 14, 23), "open": 24221.0, "high": 24236.0, "low": 24220.0, "close": 24234.0, "volume": 1500},
    ]

    result = checker.evaluate("CE", candles, volume_signal="STRONG", oi_bias="BULLISH", pressure_bias="BULLISH")

    assert result["ok"] is True
    assert result["score"] >= 30
    assert "body_aligned" in result["reason"]


def test_one_minute_momentum_quality_rejects_opposite_body():
    checker = OneMinuteMomentumQuality(min_score=30)
    candles = [
        {"time": datetime(2026, 5, 6, 14, 22), "open": 24220.0, "high": 24225.0, "low": 24210.0, "close": 24212.0, "volume": 1200},
    ]

    result = checker.evaluate("CE", candles)

    assert result["ok"] is False
    assert result["reason"] == "1m candle body against trigger direction"
