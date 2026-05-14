"""Class-first wrappers for trade monitor dispatch decisions."""

from services.signal_trade_monitor_dispatch import (
    maybe_send_trade_monitor_update as _maybe_send_trade_monitor_update,
    monitor_alert_is_high_priority as _monitor_alert_is_high_priority,
    should_send_trade_monitor_alert as _should_send_trade_monitor_alert,
)


class TradeMonitorDispatcher:
    @staticmethod
    def monitor_alert_is_high_priority(guidance):
        return _monitor_alert_is_high_priority(guidance)

    @staticmethod
    def should_send_trade_monitor_alert(service, monitor_data, minute_key):
        return _should_send_trade_monitor_alert(service, monitor_data, minute_key)

    @staticmethod
    def maybe_send_trade_monitor_update(service, latest_5m_candle):
        return _maybe_send_trade_monitor_update(service, latest_5m_candle)
