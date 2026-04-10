import time

from config import Config


class ConnectionManager:
    def __init__(self, live_feed, candle_manager, historical_backfill):
        self.live_feed = live_feed
        self.candle_manager = candle_manager
        self.historical_backfill = historical_backfill

        self.feed_stale = False
        self.last_stale_log = 0
        self.last_stale_force_reconnect = 0
        self.stale_started_at = None
        self.reconnect_cooldown_remaining = 0

    def evaluate_feed_health(self, feed_connected, effective_data_age_seconds):
        if (
                not Config.TEST_MODE
                and (
                    not feed_connected
                    or effective_data_age_seconds is None
                    or effective_data_age_seconds > Config.LIVE_DATA_STALE_SECONDS
                )
        ):
            if not self.feed_stale:
                print("Live feed stale. Resetting open candles and waiting for fresh ticks...")
                self.candle_manager.reset_incomplete_candles()
                self.feed_stale = True
                self.stale_started_at = time.time()
                self.last_stale_log = time.time()
                self.last_stale_force_reconnect = time.time()
            elif time.time() - self.last_stale_log > 30:
                print("Live feed still stale. Skipping candle generation...")
                self.last_stale_log = time.time()

            if time.time() - self.last_stale_force_reconnect > Config.STALE_FEED_FORCE_RECONNECT_SECONDS:
                print("Feed stale for too long. Forcing reconnection...")
                self.live_feed.force_reconnect()
                self.last_stale_force_reconnect = time.time()

            return {
                "skip_processing": True,
                "recovered": False,
                "missing_candles": [],
                "stale_duration": 0,
            }

        if not self.feed_stale:
            return {
                "skip_processing": False,
                "recovered": False,
                "missing_candles": [],
                "stale_duration": 0,
            }

        print("Live feed recovered. Resuming candle generation.")
        self.feed_stale = False
        stale_duration = 0
        if self.stale_started_at is not None:
            stale_duration = max(0, time.time() - self.stale_started_at)
        self.stale_started_at = None

        missing_candles = []
        last_candle_time = self.candle_manager.get_last_candle_time()
        if last_candle_time:
            recovered = self.historical_backfill.get_missing_candles_for_reconnect(last_candle_time)
            if recovered:
                missing_candles = recovered

        if stale_duration >= 180:
            self.reconnect_cooldown_remaining = Config.RECONNECT_COOLDOWN_BARS
        elif stale_duration >= 90:
            self.reconnect_cooldown_remaining = max(1, Config.RECONNECT_COOLDOWN_BARS - 1)
        else:
            self.reconnect_cooldown_remaining = 0

        print(
            "Reconnect cooldown bars:",
            self.reconnect_cooldown_remaining,
            "| stale seconds:",
            round(stale_duration, 1),
        )

        return {
            "skip_processing": False,
            "recovered": True,
            "missing_candles": missing_candles,
            "stale_duration": stale_duration,
        }

    def apply_reconnect_cooldown(self, signal, reason, score, signal_type=None, confidence=None):
        if self.reconnect_cooldown_remaining <= 0:
            return signal, reason

        if signal and signal_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "OPENING_DRIVE"} and confidence in {"MEDIUM", "HIGH"}:
            return signal, reason

        self.reconnect_cooldown_remaining -= 1
        return None, f"Reconnect cooldown active ({self.reconnect_cooldown_remaining + 1} bars left) | score={score}"
