class OpeningDriveEngine:
    @staticmethod
    def evaluate_opening_drive(strategy, ctx):
        opening_drive_window = ctx["opening_drive_window"]
        orb_ready = ctx["orb_ready"]
        price = ctx["price"]
        orb_high = ctx["orb_high"]
        orb_low = ctx["orb_low"]
        buffer = ctx["buffer"]
        vwap = ctx["vwap"]
        volume_signal = ctx["volume_signal"]
        oi_bias = ctx["oi_bias"]
        oi_trend = ctx["oi_trend"]
        bullish_build_up_ok = ctx["bullish_build_up_ok"]
        bearish_build_up_ok = ctx["bearish_build_up_ok"]
        pressure_metrics = ctx["pressure_metrics"]
        score = ctx["score"]
        time_thresholds = ctx["time_thresholds"]
        breakout_body_ok = ctx["breakout_body_ok"]
        breakout_structure_ok = ctx["breakout_structure_ok"]
        candle_liquidity_ok = ctx["candle_liquidity_ok"]
        bullish_ha_ok = ctx["bullish_ha_ok"]
        bearish_ha_ok = ctx["bearish_ha_ok"]
        cautions = ctx["cautions"]
        blockers = ctx["blockers"]
        expiry_eval = ctx["expiry_eval"]
        regime = ctx["regime"]
        candle_time = ctx["candle_time"]
        atr = ctx["atr"]
        support = ctx["support"]
        resistance = ctx["resistance"]
        opening_far_vwap_override = ctx["opening_far_vwap_override"]

        if (
            opening_drive_window and orb_ready and price > orb_high + buffer and price > vwap
            and volume_signal == "STRONG" and oi_bias == "BULLISH"
            and oi_trend in ["BULLISH", "NEUTRAL", None]
            and bullish_build_up_ok and pressure_metrics and pressure_metrics["pressure_bias"] == "BULLISH"
            and score >= time_thresholds["opening_drive_min_score"]
            and breakout_body_ok and breakout_structure_ok and candle_liquidity_ok and bullish_ha_ok
            and "opposite_pressure" not in cautions
            and ("far_from_vwap" not in cautions or opening_far_vwap_override)
        ):
            return strategy._emit_trade_signal(
                "CE", "OPENING_DRIVE", score, volume_signal, pressure_metrics, cautions,
                blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime),
                candle_time=candle_time, message="Opening drive breakout up",
                trigger_price=orb_high, invalidate_price=orb_low, atr=atr, support=support, resistance=resistance,
                remember_level=orb_high, reset_retest=True, reset_confirmation=True,
            )

        if (
            opening_drive_window and orb_ready and price < orb_low - buffer and price < vwap
            and volume_signal == "STRONG" and oi_bias == "BEARISH"
            and oi_trend in ["BEARISH", "NEUTRAL", None]
            and bearish_build_up_ok and pressure_metrics and pressure_metrics["pressure_bias"] == "BEARISH"
            and score >= time_thresholds["opening_drive_min_score"]
            and breakout_body_ok and breakout_structure_ok and candle_liquidity_ok and bearish_ha_ok
            and "opposite_pressure" not in cautions
            and ("far_from_vwap" not in cautions or opening_far_vwap_override)
        ):
            return strategy._emit_trade_signal(
                "PE", "OPENING_DRIVE", score, volume_signal, pressure_metrics, cautions,
                blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime),
                candle_time=candle_time, message="Opening drive breakdown down",
                trigger_price=orb_low, invalidate_price=orb_high, atr=atr, support=support, resistance=resistance,
                remember_level=orb_low, reset_retest=True, reset_confirmation=True,
            )

        return None


def evaluate_opening_drive(strategy, ctx):
    return OpeningDriveEngine.evaluate_opening_drive(strategy, ctx)
