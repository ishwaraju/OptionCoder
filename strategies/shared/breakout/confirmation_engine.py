from strategies.shared.confirmation_retest_evaluator import confirmation_ready, retest_ready


class ConfirmationEngine:
    @staticmethod
    def evaluate_confirmation_and_retest(strategy, ctx):
        active_confirmation = ctx["active_confirmation"]
        active_retest = ctx["active_retest"]
        price = ctx["price"]
        vwap = ctx["vwap"]
        candle_close = ctx["candle_close"]
        candle_high = ctx["candle_high"]
        candle_low = ctx["candle_low"]
        atr = ctx["atr"]
        buffer = ctx["buffer"]
        volume_signal = ctx["volume_signal"]
        oi_bias = ctx["oi_bias"]
        oi_trend = ctx["oi_trend"]
        bullish_build_up_ok = ctx["bullish_build_up_ok"]
        bearish_build_up_ok = ctx["bearish_build_up_ok"]
        cautions = ctx["cautions"]
        blockers = ctx["blockers"]
        candle_liquidity_ok = ctx["candle_liquidity_ok"]
        continuation_regime_ok = ctx["continuation_regime_ok"]
        retest_regime_ok = ctx["retest_regime_ok"]
        score = ctx["score"]
        time_thresholds = ctx["time_thresholds"]
        breakout_structure_ok = ctx["breakout_structure_ok"]
        bullish_ha_ok = ctx["bullish_ha_ok"]
        bearish_ha_ok = ctx["bearish_ha_ok"]
        pressure_metrics = ctx["pressure_metrics"]
        candle_time = ctx["candle_time"]
        orb_low = ctx["orb_low"]
        orb_high = ctx["orb_high"]
        support = ctx["support"]
        resistance = ctx["resistance"]
        expiry_eval = ctx["expiry_eval"]
        regime = ctx["regime"]
        opening_session = ctx["opening_session"]
        retest_zone = ctx["retest_zone"]

        if (
            active_confirmation
            and confirmation_ready(
                direction="CE",
                active_direction=active_confirmation["direction"],
                price_vwap_aligned=price > vwap,
                close_beyond_level=candle_close is not None and candle_close > active_confirmation["level"],
                not_extended=not strategy._entry_too_extended("CE", candle_close, active_confirmation["level"], atr, buffer),
                level_extension_ok=candle_high is not None and candle_high >= active_confirmation["level"] + max(buffer * 0.4, 5),
                volume_signal=volume_signal,
                oi_bias_ok=oi_bias in ["BULLISH", "NEUTRAL"],
                oi_trend_ok=oi_trend in ["BULLISH", "NEUTRAL", None],
                build_up_ok=bullish_build_up_ok,
                no_opposite_pressure="opposite_pressure" not in cautions,
                candle_liquidity_ok=candle_liquidity_ok,
                continuation_regime_ok=continuation_regime_ok,
                score=score,
                confirm_min_score=time_thresholds["confirm_min_score"],
            )
        ):
            if strategy._should_suppress_duplicate("CE", "BREAKOUT_CONFIRM", candle_time, active_confirmation["level"]):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate breakout confirmation suppressed")
            level = active_confirmation["level"]
            return strategy._emit_trade_signal("CE", "BREAKOUT_CONFIRM", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Breakout confirmation above {level}", trigger_price=level, invalidate_price=orb_low, atr=atr, support=support, resistance=resistance, remember_level=level, emitted_level=level, buffer=buffer, reset_confirmation=True, mark_emitted=True)

        if (
            active_confirmation
            and confirmation_ready(
                direction="CE",
                active_direction=active_confirmation["direction"],
                price_vwap_aligned=price > vwap,
                close_beyond_level=candle_close is not None and candle_close > active_confirmation["level"],
                not_extended=not strategy._entry_too_extended("CE", candle_close, active_confirmation["level"], atr, buffer),
                level_extension_ok=candle_high is not None and candle_high >= active_confirmation["level"] + max(buffer * 0.6, 8),
                volume_signal=volume_signal,
                oi_bias_ok=oi_bias in ["BULLISH", "NEUTRAL"],
                oi_trend_ok=oi_trend in ["BULLISH", "NEUTRAL", None],
                build_up_ok=bullish_build_up_ok,
                no_opposite_pressure=True,
                candle_liquidity_ok=candle_liquidity_ok,
                continuation_regime_ok=continuation_regime_ok,
                score=score,
                confirm_min_score=time_thresholds["confirm_min_score"],
                strong_context=True,
                breakout_structure_ok=breakout_structure_ok,
                ha_ok=bullish_ha_ok,
                strong_context_ready=strategy._strong_context_soft_entry_ready(score=score, entry_score=strategy.last_entry_score, volume_signal=volume_signal, candle_liquidity_ok=candle_liquidity_ok, breakout_structure_ok=breakout_structure_ok, regime_ok=continuation_regime_ok, cautions=cautions, direction_ok=bullish_ha_ok and bullish_build_up_ok),
            )
        ):
            if strategy._should_suppress_duplicate("CE", "BREAKOUT_CONFIRM", candle_time, active_confirmation["level"]):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate breakout confirmation suppressed")
            level = active_confirmation["level"]
            return strategy._emit_trade_signal("CE", "BREAKOUT_CONFIRM", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Strong-context breakout confirmation above {level}", trigger_price=level, invalidate_price=orb_low, atr=atr, support=support, resistance=resistance, remember_level=level, emitted_level=level, buffer=buffer, reset_confirmation=True, mark_emitted=True)

        if (
            active_confirmation
            and confirmation_ready(
                direction="PE",
                active_direction=active_confirmation["direction"],
                price_vwap_aligned=price < vwap,
                close_beyond_level=candle_close is not None and candle_close < active_confirmation["level"],
                not_extended=not strategy._entry_too_extended("PE", candle_close, active_confirmation["level"], atr, buffer),
                level_extension_ok=candle_low is not None and candle_low <= active_confirmation["level"] - max(buffer * 0.4, 5),
                volume_signal=volume_signal,
                oi_bias_ok=oi_bias in ["BEARISH", "NEUTRAL"],
                oi_trend_ok=oi_trend in ["BEARISH", "NEUTRAL", None],
                build_up_ok=bearish_build_up_ok,
                no_opposite_pressure="opposite_pressure" not in cautions,
                candle_liquidity_ok=candle_liquidity_ok,
                continuation_regime_ok=continuation_regime_ok,
                score=score,
                confirm_min_score=time_thresholds["confirm_min_score"],
            )
        ):
            if strategy._should_suppress_duplicate("PE", "BREAKOUT_CONFIRM", candle_time, active_confirmation["level"]):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate breakdown confirmation suppressed")
            level = active_confirmation["level"]
            return strategy._emit_trade_signal("PE", "BREAKOUT_CONFIRM", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Breakdown confirmation below {level}", trigger_price=level, invalidate_price=orb_high, atr=atr, support=support, resistance=resistance, remember_level=level, emitted_level=level, buffer=buffer, reset_confirmation=True, mark_emitted=True)

        if (
            active_confirmation
            and confirmation_ready(
                direction="PE",
                active_direction=active_confirmation["direction"],
                price_vwap_aligned=price < vwap,
                close_beyond_level=candle_close is not None and candle_close < active_confirmation["level"],
                not_extended=not strategy._entry_too_extended("PE", candle_close, active_confirmation["level"], atr, buffer),
                level_extension_ok=candle_low is not None and candle_low <= active_confirmation["level"] - max(buffer * 0.6, 8),
                volume_signal=volume_signal,
                oi_bias_ok=oi_bias in ["BEARISH", "NEUTRAL"],
                oi_trend_ok=oi_trend in ["BEARISH", "NEUTRAL", None],
                build_up_ok=bearish_build_up_ok,
                no_opposite_pressure=True,
                candle_liquidity_ok=candle_liquidity_ok,
                continuation_regime_ok=continuation_regime_ok,
                score=score,
                confirm_min_score=time_thresholds["confirm_min_score"],
                strong_context=True,
                breakout_structure_ok=breakout_structure_ok,
                ha_ok=bearish_ha_ok,
                strong_context_ready=strategy._strong_context_soft_entry_ready(score=score, entry_score=strategy.last_entry_score, volume_signal=volume_signal, candle_liquidity_ok=candle_liquidity_ok, breakout_structure_ok=breakout_structure_ok, regime_ok=continuation_regime_ok, cautions=cautions, direction_ok=bearish_ha_ok and bearish_build_up_ok),
            )
        ):
            if strategy._should_suppress_duplicate("PE", "BREAKOUT_CONFIRM", candle_time, active_confirmation["level"]):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate breakdown confirmation suppressed")
            level = active_confirmation["level"]
            return strategy._emit_trade_signal("PE", "BREAKOUT_CONFIRM", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Strong-context breakdown confirmation below {level}", trigger_price=level, invalidate_price=orb_high, atr=atr, support=support, resistance=resistance, remember_level=level, emitted_level=level, buffer=buffer, reset_confirmation=True, mark_emitted=True)

        if (
            active_retest
            and retest_ready(direction="CE", active_direction=active_retest["direction"], price_vwap_aligned=price > vwap, retest_touch_ok=candle_low is not None and candle_low <= active_retest["level"] + retest_zone, close_holds_level=candle_close is not None and candle_close >= active_retest["level"], volume_signal=volume_signal, oi_bias_ok=oi_bias in ["BULLISH", "NEUTRAL"], oi_trend_ok=oi_trend in ["BULLISH", "NEUTRAL", None], build_up_ok=bullish_build_up_ok, no_opposite_pressure="opposite_pressure" not in cautions, candle_liquidity_ok=candle_liquidity_ok, score=score, retest_min_score=time_thresholds["retest_min_score"], opening_session=opening_session, retest_regime_ok=retest_regime_ok)
        ):
            if strategy._should_suppress_duplicate("CE", "RETEST", candle_time, active_retest["level"]):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate retest suppressed")
            level = active_retest["level"]
            return strategy._emit_trade_signal("CE", "RETEST", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Breakout retest support entry above {level}", trigger_price=level, invalidate_price=support, atr=atr, support=support, resistance=resistance, emitted_level=level, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

        if (
            active_retest
            and retest_ready(direction="CE", active_direction=active_retest["direction"], price_vwap_aligned=price > vwap, retest_touch_ok=candle_low is not None and candle_low <= active_retest["level"] + retest_zone, close_holds_level=candle_close is not None and candle_close >= active_retest["level"], volume_signal=volume_signal, oi_bias_ok=oi_bias in ["BULLISH", "NEUTRAL"], oi_trend_ok=oi_trend in ["BULLISH", "NEUTRAL", None], build_up_ok=bullish_build_up_ok, no_opposite_pressure=True, candle_liquidity_ok=candle_liquidity_ok, score=score, retest_min_score=time_thresholds["retest_min_score"], opening_session=opening_session, retest_regime_ok=retest_regime_ok, strong_context=True, breakout_structure_ok=breakout_structure_ok, strong_context_ready=strategy._strong_context_soft_entry_ready(score=score, entry_score=strategy.last_entry_score, volume_signal=volume_signal, candle_liquidity_ok=candle_liquidity_ok, breakout_structure_ok=breakout_structure_ok, regime_ok=retest_regime_ok, cautions=cautions, direction_ok=bullish_build_up_ok))
        ):
            if strategy._should_suppress_duplicate("CE", "RETEST", candle_time, active_retest["level"]):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate retest suppressed")
            level = active_retest["level"]
            return strategy._emit_trade_signal("CE", "RETEST", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Strong-context retest support entry above {level}", trigger_price=level, invalidate_price=support, atr=atr, support=support, resistance=resistance, emitted_level=level, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

        if (
            active_retest
            and retest_ready(direction="PE", active_direction=active_retest["direction"], price_vwap_aligned=price < vwap, retest_touch_ok=candle_high is not None and candle_high >= active_retest["level"] - retest_zone, close_holds_level=candle_close is not None and candle_close <= active_retest["level"], volume_signal=volume_signal, oi_bias_ok=oi_bias in ["BEARISH", "NEUTRAL"], oi_trend_ok=oi_trend in ["BEARISH", "NEUTRAL", None], build_up_ok=bearish_build_up_ok, no_opposite_pressure="opposite_pressure" not in cautions, candle_liquidity_ok=candle_liquidity_ok, score=score, retest_min_score=time_thresholds["retest_min_score"], opening_session=opening_session, retest_regime_ok=retest_regime_ok)
        ):
            if strategy._should_suppress_duplicate("PE", "RETEST", candle_time, active_retest["level"]):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate retest suppressed")
            level = active_retest["level"]
            return strategy._emit_trade_signal("PE", "RETEST", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Breakdown retest resistance entry below {level}", trigger_price=level, invalidate_price=resistance, atr=atr, support=support, resistance=resistance, emitted_level=level, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

        if (
            active_retest
            and retest_ready(direction="PE", active_direction=active_retest["direction"], price_vwap_aligned=price < vwap, retest_touch_ok=candle_high is not None and candle_high >= active_retest["level"] - retest_zone, close_holds_level=candle_close is not None and candle_close <= active_retest["level"], volume_signal=volume_signal, oi_bias_ok=oi_bias in ["BEARISH", "NEUTRAL"], oi_trend_ok=oi_trend in ["BEARISH", "NEUTRAL", None], build_up_ok=bearish_build_up_ok, no_opposite_pressure=True, candle_liquidity_ok=candle_liquidity_ok, score=score, retest_min_score=time_thresholds["retest_min_score"], opening_session=opening_session, retest_regime_ok=retest_regime_ok, strong_context=True, breakout_structure_ok=breakout_structure_ok, strong_context_ready=strategy._strong_context_soft_entry_ready(score=score, entry_score=strategy.last_entry_score, volume_signal=volume_signal, candle_liquidity_ok=candle_liquidity_ok, breakout_structure_ok=breakout_structure_ok, regime_ok=retest_regime_ok, cautions=cautions, direction_ok=bearish_build_up_ok))
        ):
            if strategy._should_suppress_duplicate("PE", "RETEST", candle_time, active_retest["level"]):
                return strategy._emit_duplicate_signal(blockers, cautions, regime, score, "Duplicate retest suppressed")
            level = active_retest["level"]
            return strategy._emit_trade_signal("PE", "RETEST", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message=f"Strong-context retest resistance entry below {level}", trigger_price=level, invalidate_price=resistance, atr=atr, support=support, resistance=resistance, emitted_level=level, buffer=buffer, reset_retest=True, reset_confirmation=True, mark_emitted=True)

        return None


def evaluate_confirmation_and_retest(strategy, ctx):
    return ConfirmationEngine.evaluate_confirmation_and_retest(strategy, ctx)
