from datetime import datetime
from zoneinfo import ZoneInfo

from shared.utils.time_utils import TimeUtils


def test_floor_to_minute_zeroes_seconds_and_microseconds():
    dt = datetime(2026, 5, 6, 14, 20, 53, 497976, tzinfo=ZoneInfo("Asia/Kolkata"))

    floored = TimeUtils.floor_to_minute(dt)

    assert floored == datetime(2026, 5, 6, 14, 20, 0, 0, tzinfo=ZoneInfo("Asia/Kolkata"))
