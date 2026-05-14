"""Class-first wrapper for pending watch evaluation."""

from services.signal_pending_watch_eval import evaluate_pending_entry_watch as _evaluate_pending_entry_watch


class PendingEntryWatchEvaluator:
    @staticmethod
    def evaluate_pending_entry_watch(service, recent_1m_candles):
        return _evaluate_pending_entry_watch(service, recent_1m_candles)
