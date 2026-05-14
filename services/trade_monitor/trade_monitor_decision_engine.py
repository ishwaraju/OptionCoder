"""Class-first wrapper for trade monitor decisions."""

from services.signal_trade_monitor_decision import monitor_decision as _monitor_decision


class TradeMonitorDecisionEngine:
    @staticmethod
    def monitor_decision(*args, **kwargs):
        return _monitor_decision(*args, **kwargs)
