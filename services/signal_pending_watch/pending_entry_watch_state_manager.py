"""Class-first wrapper for pending watch state management."""

from services.signal_pending_watch_state import set_pending_entry_watch as _set_pending_entry_watch


class PendingEntryWatchStateManager:
    @staticmethod
    def set_pending_entry_watch(service, watch_payload, balanced_pro, candle_5m):
        return _set_pending_entry_watch(service, watch_payload, balanced_pro, candle_5m)
