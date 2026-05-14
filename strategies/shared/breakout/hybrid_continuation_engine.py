class HybridContinuationEngine:
    @staticmethod
    def evaluate_hybrid_continuations(strategy, ctx):
        score = ctx["score"]
        volume_signal = ctx["volume_signal"]
        pressure_metrics = ctx["pressure_metrics"]
        cautions = ctx["cautions"]
        blockers = ctx["blockers"]
        expiry_eval = ctx["expiry_eval"]
        regime = ctx["regime"]
        candle_time = ctx["candle_time"]
        price = ctx["price"]
        vwap = ctx["vwap"]
        atr = ctx["atr"]
        support = ctx["support"]
        resistance = ctx["resistance"]
        orb_high = ctx["orb_high"]
        orb_low = ctx["orb_low"]
        candle_close = ctx["candle_close"]
        candle_range = ctx["candle_range"]
        candle_liquidity_ok = ctx["candle_liquidity_ok"]
        breakout_body_ok = ctx["breakout_body_ok"]
        breakout_structure_ok = ctx["breakout_structure_ok"]
        bullish_ha_ok = ctx["bullish_ha_ok"]
        bearish_ha_ok = ctx["bearish_ha_ok"]
        pressure_conflict_level = ctx["pressure_conflict_level"]
        time_regime = ctx["time_regime"]
        oi_bias = ctx["oi_bias"]
        bullish_build_up_ok = ctx["bullish_build_up_ok"]
        bearish_build_up_ok = ctx["bearish_build_up_ok"]
        nifty_trend_day_context = ctx["nifty_trend_day_context"]
        expiry_session_mode = ctx["expiry_session_mode"]
        scored_direction = ctx["scored_direction"]
        opening_session = ctx["opening_session"]

        if (
            strategy._sensex_hybrid_fallback_ready(
                direction="CE", score=score, time_regime=time_regime, price=price, vwap=vwap,
                orb_high=orb_high, orb_low=orb_low, candle_close=candle_close, candle_range=candle_range,
                atr=atr, candle_liquidity_ok=candle_liquidity_ok, breakout_body_ok=breakout_body_ok,
                breakout_structure_ok=breakout_structure_ok, ha_ok=bullish_ha_ok,
                pressure_conflict_level=pressure_conflict_level,
            )
            and oi_bias in {"BULLISH", "NEUTRAL"}
            and pressure_conflict_level in {"NONE", "MILD", "MODERATE"}
        ):
            hybrid_cautions = strategy._append_cautions(cautions, "hybrid_price_led_setup")
            return strategy._emit_trade_signal(
                "CE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="SENSEX hybrid price-led continuation up", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        if (
            strategy.instrument == "NIFTY" and expiry_session_mode == "POST_EXPIRY_REBUILD" and nifty_trend_day_context
            and scored_direction == "CE" and price > vwap and oi_bias in {"BULLISH", "NEUTRAL"}
            and ctx["oi_trend"] in {"BULLISH", "NEUTRAL", None} and bullish_build_up_ok and score >= 68
            and strategy.last_entry_score >= 58 and pressure_conflict_level in {"NONE", "MILD", "MODERATE"} and not opening_session
        ):
            hybrid_cautions = strategy._append_cautions(cautions, "trend_day_price_override", "hybrid_price_led_setup")
            return strategy._emit_trade_signal(
                "CE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="NIFTY trend-day continuation up", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        if (
            strategy._price_led_hybrid_fallback_ready(
                direction="CE", score=score, entry_score=strategy.last_entry_score, time_regime=time_regime,
                price=price, vwap=vwap, orb_high=orb_high, orb_low=orb_low, candle_close=candle_close,
                candle_range=candle_range, atr=atr, candle_liquidity_ok=candle_liquidity_ok,
                breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok,
                ha_ok=bullish_ha_ok, pressure_conflict_level=pressure_conflict_level,
                volume_signal=volume_signal, trend_day_context=nifty_trend_day_context,
            )
            and oi_bias in {"BULLISH", "NEUTRAL"} and bullish_build_up_ok
        ):
            hybrid_cautions = strategy._append_cautions(cautions, "hybrid_price_led_setup")
            return strategy._emit_trade_signal(
                "CE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="Hybrid price-led continuation up", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        if (
            strategy._sensex_hybrid_fallback_ready(
                direction="PE", score=score, time_regime=time_regime, price=price, vwap=vwap,
                orb_high=orb_high, orb_low=orb_low, candle_close=candle_close, candle_range=candle_range,
                atr=atr, candle_liquidity_ok=candle_liquidity_ok, breakout_body_ok=breakout_body_ok,
                breakout_structure_ok=breakout_structure_ok, ha_ok=bearish_ha_ok,
                pressure_conflict_level=pressure_conflict_level,
            )
            and oi_bias in {"BEARISH", "NEUTRAL"}
            and pressure_conflict_level in {"NONE", "MILD", "MODERATE"}
        ):
            hybrid_cautions = strategy._append_cautions(cautions, "hybrid_price_led_setup")
            return strategy._emit_trade_signal(
                "PE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="SENSEX hybrid price-led continuation down", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        if (
            strategy.instrument == "NIFTY" and expiry_session_mode == "POST_EXPIRY_REBUILD" and nifty_trend_day_context
            and scored_direction == "PE" and price < vwap and oi_bias in {"BEARISH", "NEUTRAL"}
            and ctx["oi_trend"] in {"BEARISH", "NEUTRAL", None} and bearish_build_up_ok and score >= 68
            and strategy.last_entry_score >= 58 and pressure_conflict_level in {"NONE", "MILD", "MODERATE"} and not opening_session
        ):
            hybrid_cautions = strategy._append_cautions(cautions, "trend_day_price_override", "hybrid_price_led_setup")
            return strategy._emit_trade_signal(
                "PE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="NIFTY trend-day continuation down", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        if (
            strategy._price_led_hybrid_fallback_ready(
                direction="PE", score=score, entry_score=strategy.last_entry_score, time_regime=time_regime,
                price=price, vwap=vwap, orb_high=orb_high, orb_low=orb_low, candle_close=candle_close,
                candle_range=candle_range, atr=atr, candle_liquidity_ok=candle_liquidity_ok,
                breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok,
                ha_ok=bearish_ha_ok, pressure_conflict_level=pressure_conflict_level,
                volume_signal=volume_signal, trend_day_context=nifty_trend_day_context,
            )
            and oi_bias in {"BEARISH", "NEUTRAL"} and bearish_build_up_ok
        ):
            hybrid_cautions = strategy._append_cautions(cautions, "hybrid_price_led_setup")
            return strategy._emit_trade_signal(
                "PE", "CONTINUATION", score, "NORMAL" if volume_signal == "WEAK" else volume_signal, pressure_metrics, hybrid_cautions,
                blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time,
                message="Hybrid price-led continuation down", trigger_price=price, invalidate_price=vwap,
                atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True,
            )

        return None


def evaluate_hybrid_continuations(strategy, ctx):
    return HybridContinuationEngine.evaluate_hybrid_continuations(strategy, ctx)
