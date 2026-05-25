"""Class-first wrapper for trade monitor state setup."""

from services.signal_trade_monitor_state import start_trade_monitor as _start_trade_monitor


class TradeMonitorStateManager:
    @staticmethod
    def start_trade_monitor(service, signal, candle_5m, price, balanced_pro, selected_strike, entry_time=None):
        return _start_trade_monitor(service, signal, candle_5m, price, balanced_pro, selected_strike, entry_time=entry_time)
