"""Trade monitor state bootstrap helpers for SignalService."""

from shared.utils.option_greeks import format_greek_summary


def start_trade_monitor(service, signal, candle_5m, price, balanced_pro, selected_strike):
    """Start 1-minute manual trade monitor after a signal."""
    option_contract = service._get_option_contract_snapshot(
        selected_strike,
        signal,
        before_ts=candle_5m.get("close_time") or candle_5m["time"],
    )
    option_contract = service._greek_enriched_option_contract(
        option_contract,
        signal,
        price,
        before_ts=candle_5m.get("close_time") or candle_5m["time"],
    )
    reference_contract = service._get_atm_reference_option_contract(
        signal=signal,
        before_ts=candle_5m.get("close_time") or candle_5m["time"],
    )
    service._current_risk_option_contract = option_contract
    service._current_risk_reference_contract = reference_contract
    option_entry_price = option_contract.get("ltp") if option_contract else None
    risk_profile = service._resolve_trade_risk_profile(
        setup_type=(balanced_pro or {}).get("setup"),
        quality=(balanced_pro or {}).get("quality"),
        confidence=getattr(service.strategy, "last_confidence", None),
        cautions=getattr(service.strategy, "last_cautions", None),
    )
    service._current_risk_option_contract = None
    service._current_risk_reference_contract = None
    stop_loss_pct = float(risk_profile["hard_premium_stop_pct"])
    target_pct = float(risk_profile["target_pct"])
    trail_pct = float(risk_profile["trail_from_peak_pct"])
    stop_loss_price = round(option_entry_price * (1 - stop_loss_pct / 100.0), 2) if option_entry_price else None
    first_target_option_price = round(option_entry_price * (1 + target_pct / 100.0), 2) if option_entry_price else None
    pressure_summary = (balanced_pro or {}).get("pressure_summary") or {}
    entry_participation = ((getattr(service.strategy, "last_participation_metrics", None) or {}).get(signal) or {})
    signal_key = service._build_signal_key(candle_5m["time"], service.instrument, signal, selected_strike)
    service.active_trade_monitor = {
        "signal": signal,
        "signal_key": signal_key,
        "signal_ts": candle_5m["time"],
        "signal_type": (balanced_pro or {}).get("setup"),
        "signal_grade": getattr(service.strategy, "last_signal_grade", None),
        "entry_confidence": getattr(service.strategy, "last_confidence", None),
        "entry_score": getattr(service.strategy, "last_entry_score", None),
        "entry_time": candle_5m["time"],
        "entry_price": option_entry_price if option_entry_price is not None else price,
        "entry_underlying_price": price,
        "invalidate_underlying_price": (service.strategy.last_entry_plan or {}).get("invalidate_price"),
        "strike": selected_strike,
        "entry_bid": option_contract.get("top_bid_price") if option_contract else None,
        "entry_ask": option_contract.get("top_ask_price") if option_contract else None,
        "entry_spread": option_contract.get("spread") if option_contract else None,
        "entry_iv": option_contract.get("iv") if option_contract else None,
        "entry_delta": option_contract.get("delta") if option_contract else None,
        "entry_gamma": option_contract.get("gamma") if option_contract else None,
        "entry_theta": option_contract.get("theta") if option_contract else None,
        "entry_vega": option_contract.get("vega") if option_contract else None,
        "entry_greeks_source": option_contract.get("greeks_source") if option_contract else None,
        "entry_greek_summary": format_greek_summary(option_contract),
        "max_favorable_option_ltp": option_entry_price,
        "max_adverse_option_ltp": option_entry_price,
        "stop_loss_pct": stop_loss_pct,
        "target_pct": target_pct,
        "trail_pct": trail_pct,
        "setup_bucket": risk_profile["setup_bucket"],
        "risk_note": risk_profile["risk_note"],
        "session_bucket": risk_profile.get("session_bucket"),
        "iv_bucket": risk_profile.get("iv_bucket"),
        "market_iv_regime": risk_profile.get("market_iv_regime"),
        "time_stop_warn_minutes": risk_profile["time_stop_warn_minutes"],
        "time_stop_exit_minutes": risk_profile["time_stop_exit_minutes"],
        "profit_lock_trigger_pct": risk_profile.get("profit_lock_trigger_pct"),
        "partial_trigger_pct": risk_profile.get("partial_trigger_pct"),
        "runner_trigger_pct": risk_profile.get("runner_trigger_pct"),
        "runner_trail_bonus": risk_profile.get("runner_trail_bonus"),
        "allow_endgame_runner": bool(risk_profile.get("allow_endgame_runner")),
        "time_extension_minutes": risk_profile.get("time_extension_minutes"),
        "expiry_fast_decay": bool(risk_profile.get("expiry_fast_decay")),
        "stop_loss_option_price": stop_loss_price,
        "first_target_option_price": first_target_option_price,
        "partial_booked": False,
        "profit_lock_armed": False,
        "quality": balanced_pro["quality"],
        "time_regime": balanced_pro["time_regime"],
        "entry_pressure_bias": pressure_summary.get("bias"),
        "entry_pressure_strength": pressure_summary.get("strength"),
        "entry_participation_breadth": entry_participation.get("same_side_weighted_breadth"),
        "entry_participation_spread_pct": entry_participation.get("atm_spread_pct"),
        "entry_participation_delta": entry_participation.get("same_side_weighted_delta"),
        "psar_style_level": (service.strategy.last_entry_plan or {}).get("invalidate_price"),
        "entry_atr": (service.strategy.last_entry_plan or {}).get("atr"),
        "last_notified_minute": None,
        "last_sent_monitor_minute": None,
        "last_sent_guidance": None,
        "last_sent_decision_label": None,
        "last_sent_pnl_percent": None,
        "minutes_active": 0,
    }
