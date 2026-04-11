from shared.feeds.connection_manager import ConnectionManager


class _DummyFeed:
    def __init__(self):
        self.force_reconnect_calls = 0

    def force_reconnect(self):
        self.force_reconnect_calls += 1


class _DummyCandleManager:
    def __init__(self):
        self.reset_calls = 0
        self.last_candle_time = None

    def reset_incomplete_candles(self):
        self.reset_calls += 1

    def get_last_candle_time(self):
        return self.last_candle_time


class _DummyBackfill:
    def __init__(self, candles=None):
        self.candles = candles or []

    def get_missing_candles_for_reconnect(self, last_candle_time):
        return self.candles


def test_reconnect_cooldown_blocks_signal():
    manager = ConnectionManager(
        live_feed=_DummyFeed(),
        candle_manager=_DummyCandleManager(),
        historical_backfill=_DummyBackfill(),
    )
    manager.reconnect_cooldown_remaining = 2

    signal, reason = manager.apply_reconnect_cooldown("CE", "original", 70)

    assert signal is None
    assert "Reconnect cooldown active (2 bars left)" in reason
    assert manager.reconnect_cooldown_remaining == 1


def test_healthy_feed_skips_no_processing():
    manager = ConnectionManager(
        live_feed=_DummyFeed(),
        candle_manager=_DummyCandleManager(),
        historical_backfill=_DummyBackfill(),
    )

    state = manager.evaluate_feed_health(feed_connected=True, effective_data_age_seconds=0.5)

    assert state["skip_processing"] is False
    assert state["recovered"] is False


def test_high_quality_breakout_bypasses_reconnect_cooldown():
    manager = ConnectionManager(
        live_feed=_DummyFeed(),
        candle_manager=_DummyCandleManager(),
        historical_backfill=_DummyBackfill(),
    )
    manager.reconnect_cooldown_remaining = 2

    signal, reason = manager.apply_reconnect_cooldown(
        "CE",
        "original",
        78,
        signal_type="BREAKOUT",
        confidence="MEDIUM",
    )

    assert signal == "CE"
    assert reason == "original"
    assert manager.reconnect_cooldown_remaining == 2
