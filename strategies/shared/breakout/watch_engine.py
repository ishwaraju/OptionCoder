from config import Config


class WatchEngine:
    @staticmethod
    def finalize_watch_state(strategy, ctx):
        scored_direction = ctx["scored_direction"]
        score = ctx["score"]
        pressure_metrics = ctx["pressure_metrics"]
        cautions = ctx["cautions"]
        blockers = ctx["blockers"]
        orb_ready = ctx["orb_ready"]
        price = ctx["price"]
        orb_high = ctx["orb_high"]
        orb_low = ctx["orb_low"]
        vwap = ctx["vwap"]
        volume_signal = ctx["volume_signal"]
        oi_bias = ctx["oi_bias"]
        oi_trend = ctx["oi_trend"]
        bullish_build_up_ok = ctx["bullish_build_up_ok"]
        bearish_build_up_ok = ctx["bearish_build_up_ok"]
        candle_liquidity_ok = ctx["candle_liquidity_ok"]
        opening_session = ctx["opening_session"]
        continuation_regime_ok = ctx["continuation_regime_ok"]
        breakout_body_ok = ctx["breakout_body_ok"]
        breakout_structure_ok = ctx["breakout_structure_ok"]
        retest_regime_ok = ctx["retest_regime_ok"]
        buffer = ctx["buffer"]
        tuning = ctx["tuning"]
        time_thresholds = ctx["time_thresholds"]
        breakout_regime_ok = ctx["breakout_regime_ok"]
        reversal_regime_ok = ctx["reversal_regime_ok"]
        pressure_conflict_level = ctx["pressure_conflict_level"]
        candle_high = ctx["candle_high"]
        candle_low = ctx["candle_low"]
        candle_close = ctx["candle_close"]
        atr = ctx["atr"]
        support = ctx["support"]
        resistance = ctx["resistance"]
        candle_time = ctx["candle_time"]
        opening_breakout_override = ctx["opening_breakout_override"]
        expiry_eval = ctx["expiry_eval"]
        regime = ctx["regime"]
        active_confirmation = ctx["active_confirmation"]
        strong_sweep_trade_ready = ctx.get("strong_sweep_trade_ready", False)
        prev_high = ctx.get("prev_high")
        prev_low = ctx.get("prev_low")

        if not (scored_direction and score >= Config.MIN_SCORE_THRESHOLD):
            return None

        pressure_conflict_ce = strategy._pressure_conflict(cautions, pressure_metrics, "CE")
        pressure_conflict_pe = strategy._pressure_conflict(cautions, pressure_metrics, "PE")
        if orb_ready and scored_direction == "CE" and price > orb_high and price > vwap and (volume_signal in ["NORMAL", "STRONG"] or strong_sweep_trade_ready) and oi_bias in ["BULLISH", "NEUTRAL"] and oi_trend in ["BULLISH", "NEUTRAL", None] and bullish_build_up_ok and candle_liquidity_ok and not opening_session and continuation_regime_ok and ((not breakout_body_ok or not breakout_structure_ok) or pressure_conflict_ce):
            strategy._set_confirmation_setup("CE", orb_high, candle_time, score)
            cautions = cautions + ["confirmation_watch_active"]

        if orb_ready and scored_direction == "PE" and price < orb_low and price < vwap and (volume_signal in ["NORMAL", "STRONG"] or strong_sweep_trade_ready) and oi_bias in ["BEARISH", "NEUTRAL"] and oi_trend in ["BEARISH", "NEUTRAL", None] and bearish_build_up_ok and candle_liquidity_ok and not opening_session and continuation_regime_ok and ((not breakout_body_ok or not breakout_structure_ok) or pressure_conflict_pe):
            strategy._set_confirmation_setup("PE", orb_low, candle_time, score)
            cautions = cautions + ["confirmation_watch_active"]

        if orb_ready and scored_direction == "CE" and price > orb_high and price > vwap and (volume_signal in ["NORMAL", "STRONG"] or strong_sweep_trade_ready) and oi_bias in ["BULLISH", "NEUTRAL"] and oi_trend in ["BULLISH", "NEUTRAL", None] and bullish_build_up_ok and "opposite_pressure" not in cautions and candle_liquidity_ok and not opening_session and retest_regime_ok and not (orb_high is not None and price > orb_high + (buffer * tuning["extension_buffer_mult"])):
            strategy._set_retest_setup("CE", orb_high, candle_time, score)
            cautions = cautions + ["retest_watch_active"]

        if orb_ready and scored_direction == "PE" and price < orb_low and price < vwap and (volume_signal in ["NORMAL", "STRONG"] or strong_sweep_trade_ready) and oi_bias in ["BEARISH", "NEUTRAL"] and oi_trend in ["BEARISH", "NEUTRAL", None] and bearish_build_up_ok and "opposite_pressure" not in cautions and candle_liquidity_ok and not opening_session and retest_regime_ok and not (orb_low is not None and price < orb_low - (buffer * tuning["extension_buffer_mult"])):
            strategy._set_retest_setup("PE", orb_low, candle_time, score)
            cautions = cautions + ["retest_watch_active"]

        if orb_ready and scored_direction == "CE" and price > orb_high + (buffer * tuning["extension_buffer_mult"]) and price > vwap and (volume_signal in ["NORMAL", "STRONG"] or strong_sweep_trade_ready) and bullish_build_up_ok and candle_liquidity_ok and not opening_session and retest_regime_ok:
            strategy._set_retest_setup("CE", orb_high, candle_time, score)
            cautions = cautions + ["retest_watch_active"]

        if orb_ready and scored_direction == "PE" and price < orb_low - (buffer * tuning["extension_buffer_mult"]) and price < vwap and (volume_signal in ["NORMAL", "STRONG"] or strong_sweep_trade_ready) and bearish_build_up_ok and candle_liquidity_ok and not opening_session and retest_regime_ok:
            strategy._set_retest_setup("PE", orb_low, candle_time, score)
            cautions = cautions + ["retest_watch_active"]

        if not any([breakout_regime_ok, continuation_regime_ok, retest_regime_ok, reversal_regime_ok]):
            blockers.append("regime_filter")

        if strategy.instrument == "SENSEX" and time_thresholds.get("allow_weak_volume_watch") and volume_signal == "WEAK" and scored_direction in {"CE", "PE"} and score >= max(time_thresholds["breakout_min_score"], 54) and strategy._direction_vwap_aligned(scored_direction, price, vwap) and candle_liquidity_ok:
            cautions = strategy._append_cautions(cautions, "volume_weak")

        if strategy.instrument == "SENSEX" and time_thresholds.get("allow_mild_pressure_watch") and pressure_conflict_level == "MILD" and scored_direction in {"CE", "PE"} and score >= max(time_thresholds["confirm_min_score"], 56) and candle_liquidity_ok:
            cautions = strategy._append_cautions(cautions, "pressure_conflict")

        strong_directional_watch = scored_direction in {"CE", "PE"} and score >= max(time_thresholds["breakout_min_score"], 56 if strategy.instrument == "SENSEX" else 60) and strategy.last_entry_score >= (50 if strategy.instrument == "SENSEX" else 54) and strategy._direction_vwap_aligned(scored_direction, price, vwap) and candle_liquidity_ok
        confirmation_level = None
        if orb_ready:
            confirmation_level = orb_high if scored_direction == "CE" else orb_low
        elif candle_high is not None and candle_low is not None:
            confirmation_level = candle_high if scored_direction == "CE" else candle_low

        late_confirmation_extension = confirmation_level is not None and strategy._entry_too_extended(scored_direction, candle_close or price, confirmation_level, atr, buffer)
        if late_confirmation_extension and confirmation_level is not None and not opening_session and retest_regime_ok:
            strategy._set_retest_setup(scored_direction, confirmation_level, candle_time, score)
            cautions = strategy._append_cautions(cautions, "retest_watch_active", "late_confirmation_wait_retest")

        sweep_trigger_level = prev_high if scored_direction == "CE" and prev_high is not None else prev_low if scored_direction == "PE" and prev_low is not None else confirmation_level
        strong_sweep_breakout_now = strong_sweep_trade_ready and sweep_trigger_level is not None and not opening_session and score >= max(time_thresholds["confirm_min_score"], 78) and breakout_structure_ok and (((scored_direction == "CE" and price > max(vwap, sweep_trigger_level)) or (scored_direction == "PE" and price < min(vwap, sweep_trigger_level))) and ((scored_direction == "CE" and candle_high is not None and candle_high >= sweep_trigger_level + max(buffer * 0.2, 3)) or (scored_direction == "PE" and candle_low is not None and candle_low <= sweep_trigger_level - max(buffer * 0.2, 3))))
        if strong_sweep_breakout_now:
            return strategy._emit_trade_signal(scored_direction, "BREAKOUT_CONFIRM", score, volume_signal, pressure_metrics, strategy._append_cautions(cautions, "option_sweep_breakout_override"), blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Option sweep breakout confirmation {'above' if scored_direction == 'CE' else 'below'} {sweep_trigger_level}", trigger_price=sweep_trigger_level, invalidate_price=orb_low if scored_direction == "CE" else orb_high, atr=atr, support=support if scored_direction == "CE" else None, resistance=resistance if scored_direction == "PE" else None, remember_level=sweep_trigger_level, emitted_level=sweep_trigger_level, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

        blockers.append("direction_present_but_filters_incomplete")
        if strong_sweep_trade_ready and confirmation_level is not None:
            strategy._set_confirmation_setup(scored_direction, confirmation_level, candle_time, score)
            cautions = strategy._append_cautions(cautions, "confirmation_watch_active", "option_sweep_confirmation_ready")
        if opening_session and not opening_breakout_override:
            if strong_directional_watch and confirmation_level is not None:
                strategy._set_confirmation_setup(scored_direction, confirmation_level, candle_time, score)
                cautions = strategy._append_cautions(cautions, "confirmation_watch_active", "opening_session_confirmation_pending")
            else:
                blockers.append("opening_session_confirmation_pending")
        if not breakout_body_ok:
            if strong_directional_watch and confirmation_level is not None:
                if active_confirmation is None:
                    strategy._set_confirmation_setup(scored_direction, confirmation_level, candle_time, score)
                cautions = strategy._append_cautions(cautions, "confirmation_watch_active", "weak_breakout_body")
            else:
                blockers.append("weak_breakout_body")
        if not breakout_structure_ok:
            blockers.append("breakout_structure_weak")
        if not candle_liquidity_ok:
            blockers.append("low_tick_density")
        strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence=strategy._confidence_from_score(score, volume_signal, pressure_metrics, cautions), regime=strategy._effective_signal_regime(expiry_eval, regime), signal_type=strategy._watch_signal_type(cautions, "NONE"))
        strategy.last_decision_state = strategy._derive_decision_state(signal_type=strategy._watch_signal_type(cautions, "NONE"), signal=None, score=strategy.last_context_score, entry_score=strategy.last_entry_score, confidence=strategy.last_confidence, blockers=strategy.last_blockers, cautions=strategy.last_cautions)
        return None, f"No setup | score={score}"


def finalize_watch_state(strategy, ctx):
    return WatchEngine.finalize_watch_state(strategy, ctx)
