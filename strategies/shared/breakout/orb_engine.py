class OrbEngine:
    @staticmethod
    def evaluate_manual_confirmations(strategy, ctx):
        prev_high = ctx["prev_high"]
        prev_low = ctx["prev_low"]
        prev_close = ctx["prev_close"]
        scored_direction = ctx["scored_direction"]
        price = ctx["price"]
        vwap = ctx["vwap"]
        candle_close = ctx["candle_close"]
        candle_high = ctx["candle_high"]
        candle_low = ctx["candle_low"]
        atr = ctx["atr"]
        buffer = ctx["buffer"]
        orb_ready = ctx["orb_ready"]
        orb_high = ctx["orb_high"]
        orb_low = ctx["orb_low"]
        volume_signal = ctx["volume_signal"]
        oi_bias = ctx["oi_bias"]
        oi_trend = ctx["oi_trend"]
        bullish_build_up_ok = ctx["bullish_build_up_ok"]
        bearish_build_up_ok = ctx["bearish_build_up_ok"]
        candle_liquidity_ok = ctx["candle_liquidity_ok"]
        bullish_ha_ok = ctx["bullish_ha_ok"]
        bearish_ha_ok = ctx["bearish_ha_ok"]
        continuation_regime_ok = ctx["continuation_regime_ok"]
        opening_session = ctx["opening_session"]
        cautions = ctx["cautions"]
        score = ctx["score"]
        time_thresholds = ctx["time_thresholds"]
        pressure_conflict_level = ctx["pressure_conflict_level"]
        pressure_metrics = ctx["pressure_metrics"]
        blockers = ctx["blockers"]
        expiry_eval = ctx["expiry_eval"]
        regime = ctx["regime"]
        candle_time = ctx["candle_time"]
        support = ctx["support"]
        resistance = ctx["resistance"]
        breakout_structure_ok = ctx["breakout_structure_ok"]
        strong_sweep_trade_ready = ctx.get("strong_sweep_trade_ready", False)

        if prev_high is not None and scored_direction == "CE" and price > vwap and candle_close is not None and candle_close > prev_high and (strong_sweep_trade_ready or not strategy._entry_too_extended("CE", candle_close, prev_high, atr, buffer)) and (not orb_ready or orb_high is None or candle_close > orb_high or prev_high > orb_high) and candle_high is not None and candle_high >= prev_high + max(buffer * 0.3, 4) and (volume_signal in ["NORMAL", "STRONG"] or strong_sweep_trade_ready) and oi_bias in ["BULLISH", "NEUTRAL"] and oi_trend in ["BULLISH", "NEUTRAL", None] and bullish_build_up_ok and candle_liquidity_ok and bullish_ha_ok and continuation_regime_ok and not opening_session and "near_resistance" not in cautions and score >= max(time_thresholds["confirm_min_score"] - 2, 60) and (pressure_conflict_level in {"NONE", "MILD"} or (score >= 74 and "opposite_pressure" not in cautions)):
            if strategy._should_suppress_duplicate("CE", "BREAKOUT_CONFIRM", candle_time, prev_high):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate previous-candle breakout suppressed")
            return strategy._emit_trade_signal("CE", "BREAKOUT_CONFIRM", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Previous-candle breakout confirmation above {prev_high}", trigger_price=prev_high, invalidate_price=orb_low, atr=atr, support=support, resistance=resistance, remember_level=prev_high, emitted_level=prev_high, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

        if prev_low is not None and scored_direction == "PE" and price < vwap and candle_close is not None and candle_close < prev_low and (strong_sweep_trade_ready or not strategy._entry_too_extended("PE", candle_close, prev_low, atr, buffer)) and (not orb_ready or orb_low is None or candle_close < orb_low or prev_low < orb_low) and candle_low is not None and candle_low <= prev_low - max(buffer * 0.3, 4) and (volume_signal in ["NORMAL", "STRONG"] or strong_sweep_trade_ready) and oi_bias in ["BEARISH", "NEUTRAL"] and oi_trend in ["BEARISH", "NEUTRAL", None] and bearish_build_up_ok and candle_liquidity_ok and bearish_ha_ok and continuation_regime_ok and not opening_session and "near_support" not in cautions and score >= max(time_thresholds["confirm_min_score"] - 2, 60) and (pressure_conflict_level in {"NONE", "MILD"} or (score >= 74 and "opposite_pressure" not in cautions)):
            if strategy._should_suppress_duplicate("PE", "BREAKOUT_CONFIRM", candle_time, prev_low):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate previous-candle breakdown suppressed")
            return strategy._emit_trade_signal("PE", "BREAKOUT_CONFIRM", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Previous-candle breakdown confirmation below {prev_low}", trigger_price=prev_low, invalidate_price=orb_high, atr=atr, support=support, resistance=resistance, remember_level=prev_low, emitted_level=prev_low, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

        reclaim_level_ce = max(vwap, prev_high) if vwap is not None and prev_high is not None else vwap or prev_high
        if reclaim_level_ce is not None and scored_direction == "CE" and strategy._crossed_from_below_to_above(prev_low, prev_close, candle_close, vwap) and candle_close is not None and candle_close > reclaim_level_ce and (strong_sweep_trade_ready or not strategy._entry_too_extended("CE", candle_close, reclaim_level_ce, atr, buffer)) and candle_high is not None and candle_high >= reclaim_level_ce + max(buffer * 0.3, 4) and (volume_signal in ["NORMAL", "STRONG"] or strong_sweep_trade_ready) and oi_bias in ["BULLISH", "NEUTRAL"] and oi_trend in ["BULLISH", "NEUTRAL", None] and bullish_build_up_ok and candle_liquidity_ok and breakout_structure_ok and bullish_ha_ok and not opening_session and continuation_regime_ok and score >= max(time_thresholds["confirm_min_score"], 64) and "near_resistance" not in cautions and pressure_conflict_level in {"NONE", "MILD"}:
            if strategy._should_suppress_duplicate("CE", "BREAKOUT_CONFIRM", candle_time, reclaim_level_ce):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate VWAP reclaim suppressed")
            return strategy._emit_trade_signal("CE", "BREAKOUT_CONFIRM", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"VWAP reclaim confirmation above {round(reclaim_level_ce, 2)}", trigger_price=reclaim_level_ce, invalidate_price=min(vwap or reclaim_level_ce, prev_low or reclaim_level_ce), atr=atr, support=support, resistance=resistance, remember_level=reclaim_level_ce, emitted_level=reclaim_level_ce, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

        reclaim_level_pe = min(vwap, prev_low) if vwap is not None and prev_low is not None else vwap or prev_low
        if reclaim_level_pe is not None and scored_direction == "PE" and strategy._crossed_from_above_to_below(prev_high, prev_close, candle_close, vwap) and candle_close is not None and candle_close < reclaim_level_pe and (strong_sweep_trade_ready or not strategy._entry_too_extended("PE", candle_close, reclaim_level_pe, atr, buffer)) and candle_low is not None and candle_low <= reclaim_level_pe - max(buffer * 0.3, 4) and (volume_signal in ["NORMAL", "STRONG"] or strong_sweep_trade_ready) and oi_bias in ["BEARISH", "NEUTRAL"] and oi_trend in ["BEARISH", "NEUTRAL", None] and bearish_build_up_ok and candle_liquidity_ok and breakout_structure_ok and bearish_ha_ok and not opening_session and continuation_regime_ok and score >= max(time_thresholds["confirm_min_score"], 64) and "near_support" not in cautions and pressure_conflict_level in {"NONE", "MILD"}:
            if strategy._should_suppress_duplicate("PE", "BREAKOUT_CONFIRM", candle_time, reclaim_level_pe):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate VWAP rejection suppressed")
            return strategy._emit_trade_signal("PE", "BREAKOUT_CONFIRM", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"VWAP rejection confirmation below {round(reclaim_level_pe, 2)}", trigger_price=reclaim_level_pe, invalidate_price=max(vwap or reclaim_level_pe, prev_high or reclaim_level_pe), atr=atr, support=support, resistance=resistance, remember_level=reclaim_level_pe, emitted_level=reclaim_level_pe, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

        return None

    @staticmethod
    def evaluate_orb_breakouts(strategy, ctx):
        orb_ready = ctx["orb_ready"]
        price = ctx["price"]
        orb_high = ctx["orb_high"]
        orb_low = ctx["orb_low"]
        buffer = ctx["buffer"]
        vwap = ctx["vwap"]
        volume_signal = ctx["volume_signal"]
        score = ctx["score"]
        time_regime = ctx["time_regime"]
        candle_range = ctx["candle_range"]
        atr = ctx["atr"]
        oi_bias = ctx["oi_bias"]
        oi_trend = ctx["oi_trend"]
        bullish_build_up_ok = ctx["bullish_build_up_ok"]
        bearish_build_up_ok = ctx["bearish_build_up_ok"]
        pressure_metrics = ctx["pressure_metrics"]
        time_thresholds = ctx["time_thresholds"]
        expiry_eval = ctx["expiry_eval"]
        fallback_mode = ctx["fallback_mode"]
        breakout_body_ok = ctx["breakout_body_ok"]
        breakout_structure_ok = ctx["breakout_structure_ok"]
        candle_liquidity_ok = ctx["candle_liquidity_ok"]
        pressure_conflict_level = ctx["pressure_conflict_level"]
        candle_close = ctx["candle_close"]
        bullish_ha_ok = ctx["bullish_ha_ok"]
        bearish_ha_ok = ctx["bearish_ha_ok"]
        opening_session = ctx["opening_session"]
        opening_breakout_override = ctx["opening_breakout_override"]
        breakout_regime_ok = ctx["breakout_regime_ok"]
        blockers = ctx["blockers"]
        cautions = ctx["cautions"]
        regime = ctx["regime"]
        candle_time = ctx["candle_time"]
        support = ctx["support"]
        resistance = ctx["resistance"]

        if orb_ready and price > orb_high + buffer and price > vwap and strategy._sensex_volume_flexible(volume_signal, score, time_regime, candle_range, atr) and oi_bias != "BEARISH" and oi_trend != "BEARISH" and bullish_build_up_ok and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BULLISH", "NEUTRAL"]) and score >= max(time_thresholds["breakout_min_score"], expiry_eval["score_floor"], 60 if fallback_mode else 0) and ((breakout_body_ok and breakout_structure_ok) or strategy._early_impulse_breakout_ready(direction="CE", score=score, volume_signal=volume_signal, candle_liquidity_ok=candle_liquidity_ok, breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok, pressure_conflict_level=pressure_conflict_level, time_regime=time_regime, close_price=candle_close or price, trigger_level=orb_high, atr=atr, buffer=buffer)) and candle_liquidity_ok and bullish_ha_ok and (candle_close is None or candle_close > orb_high) and (not opening_session or opening_breakout_override) and breakout_regime_ok:
            if fallback_mode and time_regime in ["MIDDAY", "LATE_DAY"] and volume_signal != "STRONG":
                blockers.append("fallback_volume_not_strong")
                strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
                return None, f"Fallback breakout needs stronger volume | score={score}"
            if strategy._should_suppress_duplicate("CE", "BREAKOUT", candle_time, orb_high):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate breakout suppressed")
            return strategy._emit_trade_signal("CE", "BREAKOUT", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message="ORB Breakout Up + VWAP + Volume + Long Build-up", trigger_price=orb_high, invalidate_price=orb_low, atr=atr, support=support, resistance=resistance, remember_level=orb_high, emitted_level=orb_high, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

        if orb_ready and price < orb_low - buffer and price < vwap and strategy._sensex_volume_flexible(volume_signal, score, time_regime, candle_range, atr) and oi_bias != "BULLISH" and oi_trend != "BULLISH" and bearish_build_up_ok and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BEARISH", "NEUTRAL"]) and score >= max(time_thresholds["breakout_min_score"], expiry_eval["score_floor"], 60 if fallback_mode else 0) and ((breakout_body_ok and breakout_structure_ok) or strategy._early_impulse_breakout_ready(direction="PE", score=score, volume_signal=volume_signal, candle_liquidity_ok=candle_liquidity_ok, breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok, pressure_conflict_level=pressure_conflict_level, time_regime=time_regime, close_price=candle_close or price, trigger_level=orb_low, atr=atr, buffer=buffer)) and candle_liquidity_ok and bearish_ha_ok and (candle_close is None or candle_close < orb_low) and (not opening_session or opening_breakout_override) and breakout_regime_ok:
            if fallback_mode and time_regime in ["MIDDAY", "LATE_DAY"] and volume_signal != "STRONG":
                blockers.append("fallback_volume_not_strong")
                strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
                return None, f"Fallback breakdown needs stronger volume | score={score}"
            if strategy._should_suppress_duplicate("PE", "BREAKOUT", candle_time, orb_low):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate breakdown suppressed")
            return strategy._emit_trade_signal("PE", "BREAKOUT", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message="ORB Breakdown Down + VWAP + Volume + Short Build-up", trigger_price=orb_low, invalidate_price=orb_high, atr=atr, support=support, resistance=resistance, remember_level=orb_low, emitted_level=orb_low, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

        return None


def evaluate_manual_confirmations(strategy, ctx):
    return OrbEngine.evaluate_manual_confirmations(strategy, ctx)


def evaluate_orb_breakouts(strategy, ctx):
    return OrbEngine.evaluate_orb_breakouts(strategy, ctx)
