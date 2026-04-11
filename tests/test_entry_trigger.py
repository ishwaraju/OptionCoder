from datetime import datetime, timedelta

# from engine.event_engine import EventEngine  # OLD STRUCTURE - NEEDS UPDATE


class DummyFeed:
    def get_live_data(self):
        return {}


def test_1m_ce_trigger_fires_and_clears_watch():
    engine = EventEngine(DummyFeed())
    engine.pending_entry_watch = {
        "signal": "CE",
        "signal_type": "CONTINUATION",
        "signal_grade": "A",
        "confidence": "MEDIUM",
        "strike": 23900,
        "trigger_price": 23900,
        "setup_price": 23895,
        "valid_until": datetime(2026, 4, 10, 10, 7),
    }

    candle = {
        "datetime": datetime(2026, 4, 10, 10, 5),
        "close_time": datetime(2026, 4, 10, 10, 6),
        "open": 23896,
        "high": 23905,
        "low": 23895,
        "close": 23904,
        "volume": 100,
        "tick_count": 3,
    }

    engine.notifier.enabled = False
    engine._process_1m_entry_trigger(candle)
    assert engine.pending_entry_watch is None


def test_1m_trigger_expires_cleanly():
    engine = EventEngine(DummyFeed())
    engine.pending_entry_watch = {
        "signal": "PE",
        "signal_type": "BREAKOUT",
        "signal_grade": "B",
        "confidence": "MEDIUM",
        "strike": 23800,
        "trigger_price": 23810,
        "setup_price": 23812,
        "valid_until": datetime(2026, 4, 10, 10, 7),
    }

    candle = {
        "datetime": datetime(2026, 4, 10, 10, 8),
        "close_time": datetime(2026, 4, 10, 10, 8) + timedelta(minutes=1),
        "open": 23820,
        "high": 23821,
        "low": 23815,
        "close": 23816,
        "volume": 100,
        "tick_count": 3,
    }

    engine.notifier.enabled = False
    engine._process_1m_entry_trigger(candle)
    assert engine.pending_entry_watch is None
