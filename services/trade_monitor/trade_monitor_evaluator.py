"""Class-first wrapper for trade monitor evaluation."""

from services.signal_trade_monitor_eval import evaluate_trade_monitor as _evaluate_trade_monitor


class TradeMonitorEvaluator:
    @staticmethod
    def evaluate_trade_monitor(service, recent_1m_candles, recent_5m_candles):
        return _evaluate_trade_monitor(service, recent_1m_candles, recent_5m_candles)
