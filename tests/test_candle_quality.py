from shared.indicators.candle_manager import CandleManager
from datetime import datetime
import pytz


def test_tick_count_rolls_up_from_1m_to_5m():
    manager = CandleManager()

    completed, candle, new_minute = manager.add_tick(100, 10)
    assert completed == []
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

    completed, candle, new_minute = manager.add_tick(100, 10)
    assert completed == []
    assert new_minute is True
    manager.add_tick(101, 10)
    manager.add_tick(102, 10)

    completed_candles, next_candle, next_new_minute = manager.add_tick(103, 10)
    assert next_new_minute is True

    completed = manager.get_last_1min_candle()
    assert completed is not None
    assert len(completed_candles) == 1
    assert completed_candles[0]["close"] == 102
    assert completed["tick_count"] == 3
    assert completed["close"] == 102
    assert completed["close_time"] == next_candle["datetime"]


def test_reset_incomplete_candles_clears_open_state_only():
    manager = CandleManager()
    manager.add_tick(100, 10)
    manager.reset_incomplete_candles()

    assert manager.current_1m_candle is None
    assert manager.current_5m_candle is None


def test_gap_minutes_are_backfilled_as_flat_candles():
    manager = CandleManager()
    ist = pytz.timezone("Asia/Kolkata")
    times = [
        ist.localize(datetime(2026, 4, 10, 10, 9, 0)),
        ist.localize(datetime(2026, 4, 10, 10, 19, 0)),
    ]
    manager.time_utils.now_ist = lambda: times.pop(0)

    manager.add_tick(100, 10)
    completed_candles, _, is_new_minute = manager.add_tick(101, 5)

    assert is_new_minute is True
    assert len(completed_candles) == 10
    assert completed_candles[0]["datetime"].strftime("%H:%M") == "10:09"
    assert completed_candles[-1]["datetime"].strftime("%H:%M") == "10:18"
    synthetic = completed_candles[1]
    assert synthetic["synthetic"] is True
    assert synthetic["open"] == 100
    assert synthetic["high"] == 100
    assert synthetic["low"] == 100
    assert synthetic["close"] == 100
