"""Class-first wrappers for trade monitor utilities."""

from services.signal_trade_monitor_utils import (
    drawdown_from_peak_percent as _drawdown_from_peak_percent,
    dynamic_trail_percent as _dynamic_trail_percent,
    estimate_live_atr as _estimate_live_atr,
    option_pnl_percent as _option_pnl_percent,
    spread_widening_percent as _spread_widening_percent,
    update_psar_style_level as _update_psar_style_level,
)


class TradeMonitorUtils:
    @staticmethod
    def option_pnl_percent(entry_price, option_price):
        return _option_pnl_percent(entry_price, option_price)

    @staticmethod
    def drawdown_from_peak_percent(peak_price, option_price):
        return _drawdown_from_peak_percent(peak_price, option_price)

    @staticmethod
    def spread_widening_percent(entry_spread, current_spread):
        return _spread_widening_percent(entry_spread, current_spread)

    @staticmethod
    def estimate_live_atr(recent_5m_candles, fallback=None):
        return _estimate_live_atr(recent_5m_candles, fallback)

    @staticmethod
    def dynamic_trail_percent(base_trail_pct, setup_bucket, live_atr, underlying_price, time_regime=None):
        return _dynamic_trail_percent(base_trail_pct, setup_bucket, live_atr, underlying_price, time_regime)

    @staticmethod
    def update_psar_style_level(signal, existing_level, latest_1m, previous_1m, live_atr=None):
        return _update_psar_style_level(signal, existing_level, latest_1m, previous_1m, live_atr)
