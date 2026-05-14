"""Class-first wrappers for pending watch utilities."""

from services.signal_pending_watch_utils import (
    candle_body_ratio as _candle_body_ratio,
    candle_close_strength as _candle_close_strength,
    pending_watch_elite_ready as _pending_watch_elite_ready,
    pending_watch_max_minutes as _pending_watch_max_minutes,
    pending_watch_retrigger_eligible as _pending_watch_retrigger_eligible,
)


class PendingEntryWatchUtils:
    @staticmethod
    def pending_watch_max_minutes(instrument, pending):
        return _pending_watch_max_minutes(instrument, pending)

    @staticmethod
    def pending_watch_retrigger_eligible(pending, latest, previous):
        return _pending_watch_retrigger_eligible(pending, latest, previous)

    @staticmethod
    def pending_watch_elite_ready(pending):
        return _pending_watch_elite_ready(pending)

    @staticmethod
    def candle_close_strength(candle, direction):
        return _candle_close_strength(candle, direction)

    @staticmethod
    def candle_body_ratio(candle):
        return _candle_body_ratio(candle)
