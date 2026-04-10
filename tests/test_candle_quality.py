from core.candle_manager import CandleManager
from datetime import datetime
import pytz


def test_tick_count_rolls_up_from_1m_to_5m():
    manager = CandleManager()

    candle, new_minute = manager.add_tick(100, 10)
    assert new_minute is True
    manager.add_tick(101, 10)
    manager.add_tick(102, 10)

    minute_one = manager.current_1m_candle
    assert minute_one["tick_count"] == 3


def test_completed_minute_is_preserved_on_rollover():
    manager = CandleManager()
    ist = pytz.timezone("Asia/Kolkata")
    times = [
        ist.localize(datetime(2026, 4, 10, 9, 30, 0)),
        ist.localize(datetime(2026, 4, 10, 9, 30, 10)),
        ist.localize(datetime(2026, 4, 10, 9, 30, 40)),
        ist.localize(datetime(2026, 4, 10, 9, 31, 0)),
    ]
    manager.time_utils.now_ist = lambda: times.pop(0)

    candle, new_minute = manager.add_tick(100, 10)
    assert new_minute is True
    manager.add_tick(101, 10)
    manager.add_tick(102, 10)

    next_candle, next_new_minute = manager.add_tick(103, 10)
    assert next_new_minute is True

    completed = manager.get_last_1min_candle()
    assert completed is not None
    assert completed["tick_count"] == 3
    assert completed["close"] == 102
    assert completed["close_time"] == next_candle["datetime"]


def test_reset_incomplete_candles_clears_open_state_only():
    manager = CandleManager()
    manager.add_tick(100, 10)
    manager.reset_incomplete_candles()

    assert manager.current_1m_candle is None
    assert manager.current_5m_candle is None
