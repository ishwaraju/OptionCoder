from config import Config


def evaluate_reversal_setups(strategy, ctx):
    scored_direction = ctx["scored_direction"]
    reversal_trap_context = ctx["reversal_trap_context"]
    candle_close = ctx["candle_close"]
    price = ctx["price"]
    vwap = ctx["vwap"]
    opening_session = ctx["opening_session"]
    bullish_build_up_ok = ctx["bullish_build_up_ok"]
    bearish_build_up_ok = ctx["bearish_build_up_ok"]
    bullish_ha_ok = ctx["bullish_ha_ok"]
    bearish_ha_ok = ctx["bearish_ha_ok"]
    candle_liquidity_ok = ctx["candle_liquidity_ok"]
    volume_signal = ctx["volume_signal"]
    score = ctx["score"]
    time_thresholds = ctx["time_thresholds"]
    pressure_conflict_level = ctx["pressure_conflict_level"]
    candle_time = ctx["candle_time"]
    support = ctx["support"]
    resistance = ctx["resistance"]
    prev_low = ctx["prev_low"]
    prev_high = ctx["prev_high"]
    atr = ctx["atr"]
    pressure_metrics = ctx["pressure_metrics"]
    cautions = ctx["cautions"]
    blockers = ctx["blockers"]
    expiry_eval = ctx["expiry_eval"]
    regime = ctx["regime"]
    buffer = ctx["buffer"]
    reversal_regime_ok = ctx["reversal_regime_ok"]
    oi_trend = ctx["oi_trend"]
    candle_low = ctx["candle_low"]
    candle_high = ctx["candle_high"]
    tuning = ctx["tuning"]
    candle_open = ctx["candle_open"]
    prev_close = ctx["prev_close"]
    breakout_body_ok = ctx["breakout_body_ok"]
    breakout_structure_ok = ctx["breakout_structure_ok"]
    ha_strength = ctx["ha_strength"]
    time_regime = ctx["time_regime"]

    if scored_direction == "CE" and reversal_trap_context["CE"]["ready"] and candle_close is not None and price is not None and vwap is not None and price >= vwap and not opening_session and bullish_build_up_ok and bullish_ha_ok and candle_liquidity_ok and volume_signal in ["NORMAL", "STRONG"] and strategy.last_entry_score >= 54 and score >= max(time_thresholds["reversal_min_score"], 60) and pressure_conflict_level in {"NONE", "MILD"}:
        trigger_level = max(vwap, support) if support is not None else vwap
        if strategy._should_suppress_duplicate("CE", "REVERSAL", candle_time, trigger_level):
            return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate trap reversal suppressed")
        return strategy._emit_trade_signal("CE", "REVERSAL", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Trap reversal reclaim above {round(trigger_level, 2)}", trigger_price=trigger_level, invalidate_price=min(vwap or trigger_level, prev_low or trigger_level), atr=atr, support=support, resistance=resistance, emitted_level=trigger_level, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

    if scored_direction == "PE" and reversal_trap_context["PE"]["ready"] and candle_close is not None and price is not None and vwap is not None and price <= vwap and not opening_session and bearish_build_up_ok and bearish_ha_ok and candle_liquidity_ok and volume_signal in ["NORMAL", "STRONG"] and strategy.last_entry_score >= 54 and score >= max(time_thresholds["reversal_min_score"], 60) and pressure_conflict_level in {"NONE", "MILD"}:
        trigger_level = min(vwap, resistance) if resistance is not None else vwap
        if strategy._should_suppress_duplicate("PE", "REVERSAL", candle_time, trigger_level):
            return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate trap reversal suppressed")
        return strategy._emit_trade_signal("PE", "REVERSAL", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Trap reversal rejection below {round(trigger_level, 2)}", trigger_price=trigger_level, invalidate_price=max(vwap or trigger_level, prev_high or trigger_level), atr=atr, support=support, resistance=resistance, emitted_level=trigger_level, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

    if not Config.FOCUSED_MANUAL_MODE and support is not None and candle_low is not None and candle_low <= support + max(buffer * 1.2, tuning["retest_zone_floor"]) and not opening_session and oi_trend != "BEARISH" and bullish_build_up_ok and score >= max(time_thresholds["reversal_min_score"], 62) and reversal_regime_ok and strategy._reversal_setup_ready(direction="CE", price=price, vwap=vwap, support=support, resistance=resistance, prev_high=prev_high, prev_low=prev_low, prev_close=prev_close, candle_open=candle_open, candle_high=candle_high, candle_low=candle_low, candle_close=candle_close, buffer=buffer, volume_signal=volume_signal, score=score, entry_score=strategy.last_entry_score, pressure_metrics=pressure_metrics, pressure_conflict_level=pressure_conflict_level, breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok, candle_liquidity_ok=candle_liquidity_ok, ha_strength=ha_strength, build_up_ok=bullish_build_up_ok, regime_ok=reversal_regime_ok, time_regime=time_regime, cautions=cautions):
        confidence = strategy._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
        strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence=confidence, regime=strategy._effective_signal_regime(expiry_eval, regime), signal_type="REVERSAL")
        strategy.last_entry_plan = strategy._build_entry_plan("CE", "REVERSAL", support, support - max(buffer, 5) if support is not None else None, atr, support, resistance)
        strategy._reset_confirmation_setup()
        return "CE", f"Support Bounce + Bullish OI | score={score}"

    if not Config.FOCUSED_MANUAL_MODE and resistance is not None and candle_high is not None and candle_high >= resistance - max(buffer * 1.2, tuning["retest_zone_floor"]) and not opening_session and oi_trend != "BULLISH" and bearish_build_up_ok and score >= max(time_thresholds["reversal_min_score"], 62) and reversal_regime_ok and strategy._reversal_setup_ready(direction="PE", price=price, vwap=vwap, support=support, resistance=resistance, prev_high=prev_high, prev_low=prev_low, prev_close=prev_close, candle_open=candle_open, candle_high=candle_high, candle_low=candle_low, candle_close=candle_close, buffer=buffer, volume_signal=volume_signal, score=score, entry_score=strategy.last_entry_score, pressure_metrics=pressure_metrics, pressure_conflict_level=pressure_conflict_level, breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok, candle_liquidity_ok=candle_liquidity_ok, ha_strength=ha_strength, build_up_ok=bearish_build_up_ok, regime_ok=reversal_regime_ok, time_regime=time_regime, cautions=cautions):
        confidence = strategy._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
        strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence=confidence, regime=strategy._effective_signal_regime(expiry_eval, regime), signal_type="REVERSAL")
        strategy.last_entry_plan = strategy._build_entry_plan("PE", "REVERSAL", resistance, resistance + max(buffer, 5) if resistance is not None else None, atr, support, resistance)
        strategy._reset_confirmation_setup()
        return "PE", f"Resistance Rejection + Bearish OI | score={score}"

    return None
