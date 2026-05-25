"""Trade monitor evaluation engine for SignalService."""

from config import Config
from shared.utils.option_greeks import format_greek_summary
from services.signal_trade_monitor_decision import monitor_decision


def evaluate_trade_monitor(service, recent_1m_candles, recent_5m_candles):
    """Generate momentum-focused option-buyer guidance using 1m and 5m structure."""
    if not service.active_trade_monitor or not recent_1m_candles:
        return None

    signal = service.active_trade_monitor["signal"]
    entry_price = service.active_trade_monitor["entry_price"]
    strike = service.active_trade_monitor.get("strike")
    latest_1m = recent_1m_candles[-1]
    previous_1m = recent_1m_candles[-2] if len(recent_1m_candles) >= 2 else latest_1m
    prior_window = recent_1m_candles[-6:-1] if len(recent_1m_candles) >= 6 else recent_1m_candles[:-1]
    if not prior_window:
        prior_window = recent_1m_candles
    micro_high = max(candle["high"] for candle in prior_window)
    micro_low = min(candle["low"] for candle in prior_window)
    recent_closes = [candle["close"] for candle in recent_1m_candles[-3:]]
    last_two_closes = [candle["close"] for candle in recent_1m_candles[-2:]]
    last_close = latest_1m["close"]
    option_snapshot = service._get_option_contract_snapshot(strike, signal, before_ts=latest_1m["time"])
    option_snapshot = service._greek_enriched_option_contract(option_snapshot, signal, last_close, before_ts=latest_1m["time"])
    option_price = option_snapshot.get("ltp") if option_snapshot and option_snapshot.get("ltp") is not None else last_close
    vwap_value = service.vwap.get_vwap()
    time_regime = service.strategy.last_time_regime
    pnl_points = float(option_price) - float(entry_price) if option_price is not None and entry_price is not None else None
    pnl_percent = service._option_pnl_percent(entry_price, option_price)
    expansion_metrics = service._option_expansion_metrics(
        entry_option_price=entry_price,
        option_price=option_price,
        entry_underlying_price=service.active_trade_monitor.get("entry_underlying_price"),
        underlying_price=last_close,
        entry_delta=service.active_trade_monitor.get("entry_delta"),
    )
    recent_5m_candles = recent_5m_candles or []
    active_5m = recent_5m_candles[-1] if recent_5m_candles else None
    prior_5m = recent_5m_candles[-2] if len(recent_5m_candles) >= 2 else active_5m
    live_pressure_summary = None
    try:
        current_pressure_metrics = None
        current_participation_metrics = None
        if getattr(service, "pressure", None) and getattr(service, "option_data", None):
            try:
                current_pressure_metrics = service.pressure.analyze(service.option_data, underlying_price=last_close)
            except TypeError:
                current_pressure_metrics = service.pressure.analyze(service.option_data)
        if getattr(service, "option_data", None):
            try:
                current_participation_metrics = service._calculate_participation_metrics(latest_1m["time"])
            except Exception:
                current_participation_metrics = None
        live_pressure_summary = service._build_pressure_summary(
            pressure_metrics=current_pressure_metrics,
            participation_metrics=current_participation_metrics,
            direction=signal,
        )
    except Exception:
        live_pressure_summary = None
        current_participation_metrics = None

    if option_price is not None:
        max_fav = service.active_trade_monitor.get("max_favorable_option_ltp")
        max_adv = service.active_trade_monitor.get("max_adverse_option_ltp")
        service.active_trade_monitor["max_favorable_option_ltp"] = option_price if max_fav is None else max(max_fav, option_price)
        service.active_trade_monitor["max_adverse_option_ltp"] = option_price if max_adv is None else min(max_adv, option_price)

    if signal == "CE":
        structure_break = last_close < micro_low
        vwap_break = vwap_value is not None and last_close < vwap_value
        momentum_strong = last_close >= micro_high or (len(recent_closes) >= 3 and recent_closes[-1] > recent_closes[-2] > recent_closes[-3])
        momentum_fading = len(recent_closes) >= 3 and recent_closes[-1] < recent_closes[-2] and recent_closes[-2] <= recent_closes[-3]
        five_min_break = prior_5m is not None and last_close < prior_5m["low"]
        structure_text = f"1m intact | prev5m {'safe' if not five_min_break else 'under test'}"
    else:
        structure_break = last_close > micro_high
        vwap_break = vwap_value is not None and last_close > vwap_value
        momentum_strong = last_close <= micro_low or (len(recent_closes) >= 3 and recent_closes[-1] < recent_closes[-2] < recent_closes[-3])
        momentum_fading = len(recent_closes) >= 3 and recent_closes[-1] > recent_closes[-2] and recent_closes[-2] >= recent_closes[-3]
        five_min_break = prior_5m is not None and last_close > prior_5m["high"]
        structure_text = f"1m intact | prev5m {'safe' if not five_min_break else 'under test'}"
    confirmed_flip = service._infer_monitor_flip_signal(
        current_signal=signal,
        latest_1m=latest_1m,
        previous_1m=previous_1m,
        micro_high=micro_high,
        micro_low=micro_low,
        vwap_break=vwap_break,
        structure_break=structure_break,
    )
    flip_score = (confirmed_flip or {}).get("flip_score")

    entry_time = service.active_trade_monitor["entry_time"]
    latest_time = latest_1m["time"]
    entry_time, latest_time = service._coerce_comparable_datetimes(entry_time, latest_time)
    latest_1m = {**latest_1m, "time": latest_time}
    if previous_1m is latest_1m:
        previous_1m = latest_1m
    elif previous_1m.get("time") is not None:
        _, previous_time = service._coerce_comparable_datetimes(entry_time, previous_1m["time"])
        previous_1m = {**previous_1m, "time": previous_time}
    minutes_active = max(1, int((latest_1m["time"] - entry_time).total_seconds() // 60))
    service.active_trade_monitor["minutes_active"] = minutes_active
    stop_loss_pct = float(service.active_trade_monitor.get("stop_loss_pct") or getattr(service.config, "STOP_LOSS_PERCENT", Config.STOP_LOSS_PERCENT))
    target_pct = float(service.active_trade_monitor.get("target_pct") or getattr(service.config, "TARGET_PERCENT", Config.TARGET_PERCENT))
    trail_pct = float(service.active_trade_monitor.get("trail_pct") or getattr(service.config, "TRAIL_PERCENT", Config.TRAIL_PERCENT))
    time_stop_warn_minutes = int(service.active_trade_monitor.get("time_stop_warn_minutes") or 3)
    time_stop_exit_minutes = int(service.active_trade_monitor.get("time_stop_exit_minutes") or 5)
    partial_trigger_pct = float(service.active_trade_monitor.get("partial_trigger_pct") or target_pct)
    runner_trigger_pct = float(service.active_trade_monitor.get("runner_trigger_pct") or max(target_pct, partial_trigger_pct + 8.0))
    runner_trail_bonus = float(service.active_trade_monitor.get("runner_trail_bonus") or 0.0)
    allow_endgame_runner = bool(service.active_trade_monitor.get("allow_endgame_runner"))
    time_extension_minutes = int(service.active_trade_monitor.get("time_extension_minutes") or 0)
    partial_booked = bool(service.active_trade_monitor.get("partial_booked"))
    profit_lock_armed = bool(service.active_trade_monitor.get("profit_lock_armed"))
    expiry_fast_decay = bool(service.active_trade_monitor.get("expiry_fast_decay"))
    peak_option_price = service.active_trade_monitor.get("max_favorable_option_ltp")
    drawdown_from_peak_pct = service._drawdown_from_peak_percent(peak_option_price, option_price)
    invalidate_underlying_price = service.active_trade_monitor.get("invalidate_underlying_price")
    setup_bucket = service.active_trade_monitor.get("setup_bucket")
    risk_note = service.active_trade_monitor.get("risk_note")
    live_atr = service._estimate_live_atr(recent_5m_candles, fallback=service.active_trade_monitor.get("entry_atr"))
    dynamic_trail_pct = service._dynamic_trail_percent(
        base_trail_pct=trail_pct,
        setup_bucket=setup_bucket,
        live_atr=live_atr,
        underlying_price=last_close,
        time_regime=time_regime,
    )
    expansion_ratio = expansion_metrics.get("expansion_ratio")
    premium_supportive = expansion_metrics.get("premium_supportive")
    option_expanding = bool((pnl_percent is not None and pnl_percent >= max(4.0, target_pct * 0.35)) or premium_supportive)
    entry_pressure_bias = (service.active_trade_monitor.get("entry_pressure_bias") or "").upper()
    live_pressure_bias = ((live_pressure_summary or {}).get("bias") or "").upper()
    live_pressure_strength = ((live_pressure_summary or {}).get("strength") or "").upper()
    live_pressure_edge = float(((live_pressure_summary or {}).get("edge") or 0.0))
    entry_confidence = (service.active_trade_monitor.get("entry_confidence") or "").upper()
    entry_score = float(service.active_trade_monitor.get("entry_score") or 0)
    signal_grade = (service.active_trade_monitor.get("signal_grade") or "").upper()
    signal_type = (service.active_trade_monitor.get("signal_type") or "").upper()
    strong_breakout_runner = bool(
        signal_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "OPENING_DRIVE", "AGGRESSIVE_CONTINUATION"}
        and entry_confidence == "HIGH"
        and signal_grade in {"A", "A+"}
        and entry_score >= 80
    )
    participation_now = (current_participation_metrics or {}).get(signal, {}) if signal in {"CE", "PE"} else {}
    entry_breadth = service.active_trade_monitor.get("entry_participation_breadth")
    entry_spread_pct = service.active_trade_monitor.get("entry_participation_spread_pct")
    entry_participation_delta = service.active_trade_monitor.get("entry_participation_delta")
    current_breadth = participation_now.get("same_side_weighted_breadth")
    current_spread_pct = participation_now.get("atm_spread_pct")
    current_participation_delta = participation_now.get("same_side_weighted_delta")
    opposite_participation_delta = participation_now.get("opposite_side_weighted_delta")
    spread_widening_pct = service._spread_widening_percent(entry_spread_pct, current_spread_pct)
    option_three_bar_momentum = service._option_three_bar_momentum(recent_1m_candles, strike, signal)
    breadth_collapse = bool(entry_breadth not in {None, 0} and current_breadth is not None and float(current_breadth) <= max(float(entry_breadth) * 0.55, float(entry_breadth) - 1.5))
    participation_delta_fading = bool(entry_participation_delta not in {None, 0} and current_participation_delta is not None and float(current_participation_delta) <= max(float(entry_participation_delta) * 0.45, 0.0))
    opposite_premium_reexpansion = bool(opposite_participation_delta is not None and current_participation_delta is not None and float(opposite_participation_delta) >= max(float(current_participation_delta) * 1.05, 0.0) and breadth_collapse)
    spread_risk_high = bool(spread_widening_pct is not None and spread_widening_pct >= 35.0)
    option_structure_supportive = bool(current_participation_metrics is None or (option_three_bar_momentum != "FAILING" and not breadth_collapse and not spread_risk_high and not opposite_premium_reexpansion))
    option_structure_weak = bool(current_participation_metrics is not None and (option_three_bar_momentum == "FAILING" or spread_risk_high or breadth_collapse or opposite_premium_reexpansion))
    opposite_pressure_active = ((signal == "CE" and live_pressure_bias == "BEARISH") or (signal == "PE" and live_pressure_bias == "BULLISH"))
    pressure_flip_exit = bool(opposite_pressure_active and (live_pressure_strength == "STRONG" or live_pressure_edge >= 14.0) and minutes_active >= 2 and (not option_expanding or structure_break or vwap_break))
    if strong_breakout_runner:
        pressure_flip_exit = bool(opposite_pressure_active and (live_pressure_strength == "STRONG" or live_pressure_edge >= 18.0) and minutes_active >= 3 and (structure_break or five_min_break or (not option_expanding and momentum_fading and vwap_break)))
    pressure_flip_warning = bool(opposite_pressure_active and minutes_active >= 2 and live_pressure_edge >= 8.0 and not pressure_flip_exit)
    theta_risk_high = bool(expiry_fast_decay or time_regime in {"LATE_DAY", "ENDGAME"})
    structure_improving = momentum_strong or ((signal == "CE" and latest_1m["close"] >= previous_1m["close"]) or (signal == "PE" and latest_1m["close"] <= previous_1m["close"]))
    no_expansion = not option_expanding and not structure_improving
    profit_lock_trigger_pct = float(service.active_trade_monitor.get("profit_lock_trigger_pct") or max(8.0, round(target_pct * 0.6, 2)))
    if theta_risk_high:
        profit_lock_trigger_pct = max(6.0, round(profit_lock_trigger_pct - 1.0, 2))
    profit_lock_should_arm = bool(pnl_percent is not None and pnl_percent >= profit_lock_trigger_pct)
    if profit_lock_should_arm:
        profit_lock_armed = True
        service.active_trade_monitor["profit_lock_armed"] = True
    psar_style_level = service.active_trade_monitor.get("psar_style_level")
    if profit_lock_armed or momentum_strong:
        psar_style_level = service._update_psar_style_level(signal=signal, existing_level=psar_style_level, latest_1m=latest_1m, previous_1m=previous_1m, live_atr=live_atr)
        service.active_trade_monitor["psar_style_level"] = psar_style_level
    psar_break = bool(psar_style_level is not None and ((signal == "CE" and latest_1m["close"] <= float(psar_style_level)) or (signal == "PE" and latest_1m["close"] >= float(psar_style_level))))
    momentum_and_pressure_supportive = bool(momentum_strong and not pressure_flip_warning and not pressure_flip_exit and not psar_break and option_structure_supportive and (not live_pressure_bias or live_pressure_bias == "NEUTRAL" or (signal == "CE" and live_pressure_bias == "BULLISH") or (signal == "PE" and live_pressure_bias == "BEARISH")))
    run_profile = service._classify_trade_run_profile(
        pnl_percent=pnl_percent,
        expansion_metrics=expansion_metrics,
        minutes_active=minutes_active,
        momentum_strong=momentum_strong,
        pressure_flip_exit=pressure_flip_exit,
        drawdown_from_peak_pct=drawdown_from_peak_pct,
        setup_bucket=setup_bucket,
        session_bucket=service.active_trade_monitor.get("session_bucket"),
    )
    runner_mode = run_profile == "RUNNER"
    if runner_mode:
        time_stop_exit_minutes += time_extension_minutes
        dynamic_trail_pct = min(16.0, dynamic_trail_pct + runner_trail_bonus)
    break_even_underlying_exit = bool(profit_lock_armed and invalidate_underlying_price is not None and ((signal == "CE" and latest_1m["close"] <= float(invalidate_underlying_price)) or (signal == "PE" and latest_1m["close"] >= float(invalidate_underlying_price))))
    trail_active_without_partial = bool(profit_lock_armed and not partial_booked and drawdown_from_peak_pct is not None and drawdown_from_peak_pct >= max(dynamic_trail_pct, 8.0))
    slow_positive_theta_risk = bool(theta_risk_high and pnl_percent is not None and pnl_percent > 0 and pnl_percent < max(target_pct, profit_lock_trigger_pct) and minutes_active >= time_stop_warn_minutes and (no_expansion or (not momentum_strong and not structure_improving and (drawdown_from_peak_pct is None or drawdown_from_peak_pct < max(dynamic_trail_pct, 8.0)))))
    if strong_breakout_runner and pnl_percent is not None and pnl_percent > 0:
        slow_positive_theta_risk = bool(slow_positive_theta_risk and minutes_active >= (time_stop_warn_minutes + 2) and (drawdown_from_peak_pct is None or drawdown_from_peak_pct >= max(dynamic_trail_pct * 0.6, 5.0)))
    guidance, decision_label, action_text, reason = monitor_decision(
        signal,
        latest_1m,
        last_two_closes,
        pnl_points,
        pnl_percent,
        stop_loss_pct,
        target_pct,
        partial_trigger_pct,
        runner_trigger_pct,
        dynamic_trail_pct,
        drawdown_from_peak_pct,
        invalidate_underlying_price,
        minutes_active,
        time_stop_warn_minutes,
        time_stop_exit_minutes,
        profit_lock_trigger_pct,
        profit_lock_armed,
        partial_booked,
        momentum_strong,
        momentum_fading,
        option_structure_weak,
        opposite_premium_reexpansion,
        pressure_flip_warning,
        pressure_flip_exit,
        runner_mode,
        momentum_and_pressure_supportive,
        break_even_underlying_exit,
        trail_active_without_partial,
        psar_break,
        psar_style_level,
        slow_positive_theta_risk,
        structure_break,
        vwap_break,
        five_min_break,
        confirmed_flip,
        live_pressure_summary,
        live_pressure_bias,
        entry_pressure_bias,
        no_expansion,
    )
    if time_regime == "ENDGAME" and (pnl_points is not None and pnl_points > 0) and guidance in {"HOLD_STRONG", "HOLD_WITH_TRAIL"} and not (runner_mode and allow_endgame_runner and momentum_and_pressure_supportive):
        guidance = "BOOK_PARTIAL"
        decision_label = "BOOK_PARTIAL_NOW"
        action_text = "Late-day profit hai. Partial book karna safer hai."
        reason = "Late-day move is in profit; partial booking is safer for an option buyer."
    if signal == "CE":
        exit_if = f"Exit if next 1m closes below {round(float(psar_style_level or micro_low), 2)}"
    else:
        exit_if = f"Exit if next 1m closes above {round(float(psar_style_level or micro_high), 2)}"
    return {
        "instrument": service.instrument, "signal": signal, "signal_type": service.active_trade_monitor.get("signal_type"),
        "setup_bucket": setup_bucket, "guidance": guidance, "decision_label": decision_label,
        "journal_note": service._build_journal_note(decision_label, action_text, f"flip_score={(flip_score or {}).get('score')}" if flip_score else None),
        "action_text": action_text, "reason": reason, "structure": structure_text, "price": last_close,
        "option_price": option_price, "entry_price": entry_price, "entry_underlying_price": service.active_trade_monitor.get("entry_underlying_price"),
        "option_bid": option_snapshot.get("top_bid_price") if option_snapshot else None, "option_ask": option_snapshot.get("top_ask_price") if option_snapshot else None,
        "option_spread": option_snapshot.get("spread") if option_snapshot else None, "strike": strike, "pnl_points": pnl_points, "pnl_percent": pnl_percent,
        "max_favorable_ltp": service.active_trade_monitor.get("max_favorable_option_ltp"), "max_adverse_ltp": service.active_trade_monitor.get("max_adverse_option_ltp"),
        "drawdown_from_peak_pct": drawdown_from_peak_pct, "stop_loss_pct": stop_loss_pct, "target_pct": target_pct, "trail_pct": trail_pct,
        "dynamic_trail_pct": dynamic_trail_pct, "flip_score": (flip_score or {}).get("score"), "flip_confidence": (flip_score or {}).get("confidence"),
        "expansion_ratio": expansion_ratio, "actual_option_move": expansion_metrics.get("actual_option_move"), "expected_option_move": expansion_metrics.get("expected_option_move"),
        "premium_supportive": premium_supportive, "stop_loss_option_price": service.active_trade_monitor.get("stop_loss_option_price"),
        "first_target_option_price": service.active_trade_monitor.get("first_target_option_price"), "invalidate_underlying_price": invalidate_underlying_price,
        "time_stop_warn_minutes": time_stop_warn_minutes, "time_stop_exit_minutes": time_stop_exit_minutes, "partial_booked": partial_booked,
        "profit_lock_armed": profit_lock_armed, "profit_lock_trigger_pct": profit_lock_trigger_pct, "psar_style_level": psar_style_level,
        "live_atr": live_atr, "expiry_fast_decay": expiry_fast_decay, "theta_risk_high": theta_risk_high, "run_profile": run_profile,
        "runner_mode": runner_mode, "partial_trigger_pct": partial_trigger_pct, "runner_trigger_pct": runner_trigger_pct,
        "quality": service.active_trade_monitor["quality"], "time_regime": time_regime, "heikin_ashi": (service.strategy.last_heikin_ashi or {}).get("bias"),
        "risk_note": risk_note, "entry_pressure_bias": entry_pressure_bias, "live_pressure_bias": live_pressure_bias,
        "exit_if": exit_if,
        "live_pressure_summary": (live_pressure_summary or {}).get("summary"), "entry_participation_breadth": entry_breadth,
        "current_participation_breadth": current_breadth, "entry_participation_delta": entry_participation_delta,
        "current_participation_delta": current_participation_delta, "opposite_participation_delta": opposite_participation_delta,
        "entry_spread_pct": entry_spread_pct, "current_spread_pct": current_spread_pct, "spread_widening_pct": spread_widening_pct,
        "breadth_collapse": breadth_collapse, "participation_delta_fading": participation_delta_fading,
        "opposite_premium_reexpansion": opposite_premium_reexpansion, "option_three_bar_momentum": option_three_bar_momentum,
        "greek_summary": format_greek_summary(option_snapshot),
    }
