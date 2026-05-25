"""Pending watch trigger and notification helpers for SignalService."""

from datetime import timedelta

from config import Config
from shared.utils.option_greeks import format_greek_summary


def maybe_fire_pending_entry_watch(service, latest_5m_candle):
    if not service.pending_entry_watch:
        return

    recent_1m_candles = service.db_reader.fetch_recent_candles_1m(service.instrument, limit=6)
    if len(recent_1m_candles) < 2:
        return

    latest_1m = recent_1m_candles[-1]
    minute_key = latest_1m["time"]
    if service.pending_entry_watch.get("last_checked_minute") == minute_key:
        return
    service.pending_entry_watch["last_checked_minute"] = minute_key

    evaluation = service._evaluate_pending_entry_watch(recent_1m_candles)
    if not evaluation:
        service._safe_save_entry_decision_1m(
            ts=minute_key,
            pending=service.pending_entry_watch,
            decision="WAIT",
            latest_1m=latest_1m,
            reason="Waiting for 1m trigger confirmation",
        )
        return

    if evaluation["status"] in {"EXPIRED", "INVALIDATED"}:
        service._safe_save_entry_decision_1m(
            ts=minute_key,
            pending=service.pending_entry_watch,
            decision=evaluation["status"],
            latest_1m=latest_1m,
            evaluation=evaluation,
            reason=evaluation.get("reason"),
        )
        service._clear_pending_entry_watch()
        return
    if evaluation["status"] == "REARM":
        service._safe_save_entry_decision_1m(
            ts=minute_key,
            pending=service.pending_entry_watch,
            decision="REARM",
            latest_1m=latest_1m,
            evaluation=evaluation,
            reason=evaluation.get("reason"),
        )
        service._rearm_pending_entry_watch(latest_1m, evaluation.get("reason"))
        return

    pending = service.pending_entry_watch
    signal = pending["direction"]
    trigger_price = pending["trigger_price"]
    strike = evaluation.get("selected_strike")
    option_contract = None
    if service.option_data:
        if strike is None:
            strike, _ = service.strike_selector.select_strike_with_reason(
                price=evaluation["price"],
                signal=signal,
                volume_signal="NORMAL",
                strategy_score=pending["score"],
                pressure_metrics=None,
                candle_time=evaluation["time"],
            )
        option_contract = service._get_option_contract_snapshot(strike, signal, before_ts=evaluation["time"])
        option_contract = service._greek_enriched_option_contract(
            option_contract,
            signal,
            evaluation["price"],
            before_ts=evaluation["time"],
        )

    service._safe_save_entry_decision_1m(
        ts=evaluation["time"],
        pending=pending,
        decision="TRIGGERED",
        latest_1m=latest_1m,
        evaluation=evaluation,
        option_contract=option_contract,
        strike=strike,
        reason=f"{evaluation.get('reason')} | {evaluation.get('micro_reason') or 'micro confirmed'}",
    )

    option_price = (option_contract or {}).get("ltp")
    stop_loss_option_price = None
    first_target_option_price = None
    if option_price is not None:
        sl_pct = pending.get("option_stop_loss_pct")
        t1_pct = pending.get("option_target_pct")
        if sl_pct is not None:
            stop_loss_option_price = round(float(option_price) * (1 - float(sl_pct) / 100.0), 2)
        if t1_pct is not None:
            first_target_option_price = round(float(option_price) * (1 + float(t1_pct) / 100.0), 2)

    balanced_pro = {
        "quality": pending.get("quality"),
        "setup": pending.get("signal_type"),
        "tradability": "ACTION",
        "time_regime": pending.get("time_regime"),
    }
    service.strategy.last_entry_plan = {
        "entry_above": trigger_price if signal == "CE" else None,
        "entry_below": trigger_price if signal == "PE" else None,
        "invalidate_price": pending.get("invalidate_price"),
        "first_target_price": pending.get("first_target_price"),
    }
    entry_notification = {
        "instrument": service.instrument,
        "signal": signal,
        "strike": strike,
        "confidence": pending.get("confidence"),
        "confidence_summary": evaluation.get("micro_reason") or pending.get("reason"),
        "signal_type": pending.get("signal_type"),
        "signal_grade": pending.get("signal_grade"),
        "price": round(option_price, 2) if option_price is not None else round(evaluation["price"], 2),
        "underlying_price": round(evaluation["price"], 2),
        "trigger_price": trigger_price,
        "invalidate_price": pending.get("invalidate_price"),
        "first_target_price": pending.get("first_target_price"),
        "stop_loss_option_price": stop_loss_option_price,
        "first_target_option_price": first_target_option_price,
        "option_stop_loss_pct": pending.get("option_stop_loss_pct"),
        "option_target_pct": pending.get("option_target_pct"),
        "entry_bid": option_contract.get("top_bid_price") if option_contract else None,
        "entry_ask": option_contract.get("top_ask_price") if option_contract else None,
        "entry_spread": option_contract.get("spread") if option_contract else None,
        "execution_model": "15m context | 5m setup | 1m execution",
        "trade_type": service._classify_action_trade_type(
            setup_type=pending.get("signal_type"),
            high_expectancy_profile={
                "quality_tag": pending.get("quality_tag"),
                "signal_family": pending.get("signal_family"),
                "likely_runner": pending.get("likely_runner"),
            },
            premium_guard={"label": "PREMIUM_OK"},
        ),
        "exit_if": service._action_exit_if_line(
            signal,
            trigger_price=trigger_price,
            invalidate_price=pending.get("invalidate_price"),
            premium_sl=stop_loss_option_price,
        ),
        "context": pending.get("context"),
        "risk_note": pending.get("risk_note"),
        "greek_summary": format_greek_summary(option_contract),
        "decision_label": f"CONFIRMED_{signal}_ENTRY",
        "reason": f"{evaluation.get('reason')} | {evaluation.get('micro_reason') or 'micro confirmed'}",
    }
    service._run_async_notification(service.notifier.send_entry_trigger_notification, entry_notification)

    signal_saved = service._safe_save_signal_issued(
        ts=evaluation["time"],
        signal=signal,
        price=(option_contract or {}).get("ltp") if option_contract else evaluation["price"],
        strike=strike,
        reason=(
            f"1m entry trigger after 5m watch | setup={pending.get('signal_type')} "
            f"| watch_bucket={pending.get('watch_bucket')} | base_reason={pending.get('reason')}"
        ),
        balanced_pro=balanced_pro,
        oi_mode="WATCH_TO_1M_TRIGGER",
        telegram_sent=Config.ENABLE_ALERTS,
        monitor_started=True,
        entry_window_end=evaluation["time"] + timedelta(minutes=Config.SIGNAL_VALIDITY_MINUTES),
        underlying_price=evaluation["price"],
        option_contract=option_contract,
        strike_reason="watch-trigger strike selection",
        option_data_source=service.option_data_source,
    )
    if not signal_saved:
        service._log("Signal issued save failed after entry notification dispatch")
    service._clear_pending_entry_watch()
    service._start_trade_monitor(
        signal,
        latest_5m_candle,
        evaluation["price"],
        balanced_pro,
        strike,
        entry_time=evaluation["time"],
    )
    service.signals_generated += 1
