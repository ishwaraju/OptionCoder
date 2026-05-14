"""Class-first wrapper for pending watch trigger flow."""

from services.signal_pending_watch_trigger import maybe_fire_pending_entry_watch as _maybe_fire_pending_entry_watch


class PendingEntryWatchTriggerEngine:
    @staticmethod
    def maybe_fire_pending_entry_watch(service, latest_5m_candle):
        return _maybe_fire_pending_entry_watch(service, latest_5m_candle)
