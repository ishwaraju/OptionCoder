from datetime import time

from config import Config
from core.live_feed import LiveFeed
import core.live_feed as live_feed_module


class _DummyTimeUtils:
    def __init__(self, current_times, market_open=False):
        self.current_times = list(current_times)
        self.market_open = market_open

    def current_time(self):
        if len(self.current_times) > 1:
            return self.current_times.pop(0)
        return self.current_times[0]

    def is_market_open(self):
        return self.market_open

    def _parse_clock(self, value):
        hour, minute = map(int, value.split(":"))
        return time(hour, minute)


class _ImmediateThread:
    def __init__(self, target=None, daemon=None):
        self.target = target
        self.daemon = daemon

    def start(self):
        if self.target:
            self.target()


def test_connect_before_market_open_schedules_auto_connect():
    original_test_mode = Config.TEST_MODE
    original_auto_switch = Config.AUTO_SWITCH_TO_MOCK_AFTER_CLOSE
    try:
        Config.TEST_MODE = False
        Config.AUTO_SWITCH_TO_MOCK_AFTER_CLOSE = False

        feed = LiveFeed([])
        feed.time_utils = _DummyTimeUtils([time(9, 10)], market_open=False)

        called = {"scheduled": 0}

        def fake_schedule():
            called["scheduled"] += 1

        feed._schedule_market_open_connect = fake_schedule
        feed.connect()

        assert called["scheduled"] == 1
        assert feed.ws is None
    finally:
        Config.TEST_MODE = original_test_mode
        Config.AUTO_SWITCH_TO_MOCK_AFTER_CLOSE = original_auto_switch


def test_market_open_waiter_calls_connect_once_market_opens():
    original_test_mode = Config.TEST_MODE
    original_thread = live_feed_module.threading.Thread
    original_sleep = live_feed_module.time.sleep
    try:
        Config.TEST_MODE = False

        feed = LiveFeed([])
        feed.time_utils = _DummyTimeUtils([time(9, 10), time(9, 15)], market_open=False)

        connect_calls = {"count": 0}

        def fake_connect():
            connect_calls["count"] += 1

        feed.connect = fake_connect
        live_feed_module.threading.Thread = _ImmediateThread
        live_feed_module.time.sleep = lambda _: None

        feed._schedule_market_open_connect()

        assert connect_calls["count"] == 1
        assert feed.market_open_wait_scheduled is False
    finally:
        Config.TEST_MODE = original_test_mode
        live_feed_module.threading.Thread = original_thread
        live_feed_module.time.sleep = original_sleep
