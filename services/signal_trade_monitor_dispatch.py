"""Dispatch helpers for live trade monitor updates."""


def monitor_alert_is_high_priority(guidance):
    return guidance in {
        "BOOK_PARTIAL",
        "TRIM",
        "EXIT_BIAS",
        "EXIT_STOPLOSS",
        "EXIT_TRAIL",
        "EXIT_TIMESTOP",
        "EXIT_PROFIT_PROTECT",
        "EXIT_PROTECT",
        "THESIS_BROKEN",
    }


def should_send_trade_monitor_alert(service, monitor_data, minute_key):
    if not service.active_trade_monitor:
        return False

    guidance = monitor_data.get("guidance")
    decision_label = monitor_data.get("decision_label")
    last_guidance = service.active_trade_monitor.get("last_sent_guidance")
    last_label = service.active_trade_monitor.get("last_sent_decision_label")
    last_sent_minute = service.active_trade_monitor.get("last_sent_monitor_minute")
    last_pnl_percent = service.active_trade_monitor.get("last_sent_pnl_percent")
    pnl_percent = monitor_data.get("pnl_percent")

    if last_sent_minute is None:
        return True
    if monitor_alert_is_high_priority(guidance):
        return True
    if guidance != last_guidance or decision_label != last_label:
        return True

    minutes_since_last_sent = max(
        0,
        int((minute_key - last_sent_minute).total_seconds() // 60),
    )
    if guidance in {"THESIS_WEAKENING", "NORMAL_PULLBACK", "HOLD_WITH_TRAIL"}:
        return minutes_since_last_sent >= 3
    if guidance == "HOLD_STRONG":
        pnl_jump = (
            abs(float(pnl_percent) - float(last_pnl_percent))
            if pnl_percent is not None and last_pnl_percent is not None
            else 0.0
        )
        return minutes_since_last_sent >= 3 or pnl_jump >= 4.0
    return minutes_since_last_sent >= 2


def maybe_send_trade_monitor_update(service, latest_5m_candle):
    """Send Telegram/manual monitor update every minute after a live signal."""
    if not service.active_trade_monitor:
        return

    recent_1m_candles = service.db_reader.fetch_recent_candles_1m(service.instrument, limit=20)
    if not recent_1m_candles:
        return

    latest_1m = recent_1m_candles[-1]
    minute_key = latest_1m["time"]
    if service.active_trade_monitor["last_notified_minute"] == minute_key:
        return

    recent_5m_candles = service.db_writer.fetch_recent_candles_5m(service.instrument, limit=3)
    monitor_data = service._evaluate_trade_monitor(recent_1m_candles, recent_5m_candles)
    if not monitor_data:
        return

    service.active_trade_monitor["last_notified_minute"] = minute_key
    service._safe_save_trade_monitor_event(minute_key, monitor_data)
    service._safe_save_option_signal_outcome(minute_key, monitor_data)
    if service._should_send_trade_monitor_alert(monitor_data, minute_key):
        service.notifier.send_trade_monitor_update(monitor_data)
        service.active_trade_monitor["last_sent_monitor_minute"] = minute_key
        service.active_trade_monitor["last_sent_guidance"] = monitor_data.get("guidance")
        service.active_trade_monitor["last_sent_decision_label"] = monitor_data.get("decision_label")
        service.active_trade_monitor["last_sent_pnl_percent"] = monitor_data.get("pnl_percent")
    if monitor_data["guidance"] in {"BOOK_PARTIAL", "TRIM"}:
        service.active_trade_monitor["partial_booked"] = True

    exit_guidances = {"EXIT_BIAS", "EXIT_STOPLOSS", "EXIT_TRAIL", "EXIT_TIMESTOP", "EXIT_PROFIT_PROTECT", "EXIT_PROTECT", "THESIS_BROKEN"}
    if monitor_data["guidance"] in exit_guidances or service.active_trade_monitor["minutes_active"] >= 20:
        service._sync_ml_outcome_from_monitor()
        service.active_trade_monitor = None
